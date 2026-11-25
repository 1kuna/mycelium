package myce

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	warmPieceFD     = "MYCE_WARM_PIECES_FD"
	warmBootstrapFD = 5 // The native launcher reserves 3 and 4 for start receipts.
	// The peer is executing a synchronous call once it starts a frame. Bound
	// partial headers and unread replies independently of model and item time.
	warmFrameTimeout = 5 * time.Second
	warmReplyLimit   = 4 << 20
)

type warmPieceItem struct {
	Key           string            `json:"key"`
	Args          []string          `json:"args"`
	Inputs        map[string]string `json:"inputs"`
	OutputDir     string            `json:"output_dir"`
	CheckpointDir string            `json:"checkpoint_dir,omitempty"`
}

type warmHelperReply struct {
	Item  *warmPieceItem `json:"item,omitempty"`
	Error string         `json:"error,omitempty"`
}

func newWarmPieceChannel() (*net.UnixConn, *os.File, error) {
	// Keep a concurrent fork from inheriting either socket during creation
	// on systems where socketpair has no atomic close-on-exec flag.
	syscall.ForkLock.RLock()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err == nil {
		unix.CloseOnExec(pair[0])
		unix.CloseOnExec(pair[1])
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	server := os.NewFile(uintptr(pair[0]), "warm-piece-worker")
	child := os.NewFile(uintptr(pair[1]), "warm-piece-main")
	conn, err := net.FileConn(server)
	server.Close()
	if err != nil {
		child.Close()
		return nil, nil, err
	}
	return conn.(*net.UnixConn), child, nil
}

func (g *nativeGroup) warmCall(ctx context.Context, req warmPieceRequest) (warmPieceReply, error) {
	req.Execution = Execution{ID: g.p.job.ID, Attempt: g.p.job.Attempt}
	req.Machine, req.Session = g.worker.Machine.ID, g.session
	var reply warmPieceReply
	err := g.worker.Client.RetryTransient(ctx, func(ctx context.Context, c Client) error {
		reply = warmPieceReply{}
		return c.Call(ctx, "POST", "/api/workers/warm-piece", req, &reply)
	}, nil)
	return reply, err
}

// Read ordinary stream bytes only, so the kernel never installs ancillary
// descriptors in the worker. The complete request body is one operation byte.
func readWarmRequest(conn *net.UnixConn, clock Clock) (byte, error) {
	var frame [5]byte
	// Between requests main can be loading or running an item. The channel
	// goroutine can wait here until attempt cancellation closes the socket.
	if _, err := io.ReadFull(conn, frame[:1]); err != nil {
		return 0, err
	}
	if err := conn.SetReadDeadline(clock.Now().Add(warmFrameTimeout)); err != nil {
		return 0, err
	}
	if _, err := io.ReadFull(conn, frame[1:4]); err != nil {
		return 0, err
	}
	if binary.BigEndian.Uint32(frame[:4]) != 1 {
		return 0, errors.New("invalid warm request frame size")
	}
	if _, err := io.ReadFull(conn, frame[4:]); err != nil {
		return 0, err
	}
	return frame[4], conn.SetReadDeadline(time.Time{})
}

func writeWarmReply(conn *net.UnixConn, clock Clock, reply warmHelperReply) error {
	data, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	if len(data) > warmReplyLimit {
		return errors.New("warm reply exceeds frame size limit")
	}
	if err := conn.SetWriteDeadline(clock.Now().Add(warmFrameTimeout)); err != nil {
		return err
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if _, err := conn.Write(header[:]); err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

func (g *nativeGroup) serveWarmPieces(ctx context.Context, conn *net.UnixConn, j Job) error {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	var delivery *WarmPieceDelivery
	var item *warmPieceItem
	for {
		op, err := readWarmRequest(conn, g.worker.Clock)
		if err != nil || ctx.Err() != nil {
			return nil // Transport failure ends just this channel.
		}
		var end bool
		var response warmHelperReply
		switch op {
		case 'N':
			if delivery != nil {
				return nil
			}
			delivery, item, end, err = g.beginWarmItem(ctx, j)
			response.Item = item
		case 'C':
			if item == nil || item.CheckpointDir == "" {
				return nil
			}
			_, err = g.worker.publishFileCheckpointContext(ctx, j, g.dir)
		case 'S', 'F':
			if delivery == nil {
				return nil
			}
			err = g.settleWarmItem(ctx, j, *delivery, *item, op == 'S')
			if err == nil {
				g.retireWarmItem(j, *delivery, *item)
				delivery, item = nil, nil
			}
		default:
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			// Keep worker and queue internals outside the authored payload.
			_ = writeWarmReply(conn, g.worker.Clock, warmHelperReply{Error: "warm piece operation refused"})
			return err
		}
		if err := writeWarmReply(conn, g.worker.Clock, response); err != nil || end {
			return nil
		}
	}
}

func (g *nativeGroup) beginWarmItem(ctx context.Context, j Job) (*WarmPieceDelivery, *warmPieceItem, bool, error) {
	offered, err := g.warmCall(ctx, warmPieceRequest{Op: "next"})
	if err != nil || offered.End {
		return nil, nil, offered.End, err
	}
	delivery := offered.Delivery
	item, err := prepareWarmItem(j, delivery)
	if err != nil {
		return nil, nil, false, err
	}
	started, err := g.warmCall(ctx, warmPieceRequest{Op: "start", Key: delivery.Key, Sequence: delivery.Sequence})
	if err != nil || started.End {
		return nil, nil, started.End, err
	}
	if started.Delivery == nil || started.Delivery.Key != delivery.Key || started.Delivery.Sequence != delivery.Sequence || started.Delivery.StartedAt == nil {
		return nil, nil, false, errors.New("missing warm item start authorization")
	}
	if item.CheckpointDir != "" {
		root := attemptPublicationRoot(j, g.dir)
		if err := os.MkdirAll(root, 0700); err != nil {
			return nil, nil, false, err
		}
		if err := saveWorkerJSON(root, "warm-checkpoint.json", delivery.Sequence); err != nil {
			return nil, nil, false, err
		}
	}
	return delivery, item, false, nil
}

func prepareWarmItem(j Job, d *WarmPieceDelivery) (*warmPieceItem, error) {
	if d == nil || d.Sequence < 1 || d.Sequence > len(j.Spec.FanOut.Pieces) || d.Attempt != j.Attempt {
		return nil, errors.New("invalid warm item delivery")
	}
	p := j.Spec.FanOut.Pieces[d.Sequence-1]
	if p.Key != d.Key {
		return nil, errors.New("warm item differs from the frozen declaration")
	}
	work := j.Spec.Run.Workdir
	item := &warmPieceItem{Key: p.Key, Args: p.Args, Inputs: map[string]string{}}
	prefix := PlainPiecePrefix + p.Key + "/"
	for name := range j.Spec.Files {
		if relative, ok := strings.CutPrefix(name, prefix); ok {
			item.Inputs[relative] = filepath.Join(work, name)
		}
	}
	var err error
	item.OutputDir, err = os.MkdirTemp(work, ".myce-warm-output-")
	if err != nil {
		return nil, err
	}
	if j.Spec.Resumable {
		item.CheckpointDir = warmCheckpointPath(j, p.Key)
		if err := os.MkdirAll(item.CheckpointDir, 0700); err != nil {
			return nil, err
		}
	}
	return item, nil
}

func (g *nativeGroup) settleWarmItem(ctx context.Context, j Job, d WarmPieceDelivery, item warmPieceItem, success bool) error {
	req := warmPieceRequest{Op: "failure", Key: d.Key, Sequence: d.Sequence}
	if success {
		g.p.mu.Lock()
		// A report that reached this gate before stop delivery can still win
		// against a priority stop that is in transit from the queue.
		req.BeforeStop = !g.p.stopped && !g.p.killed && !g.p.userCancel
		g.p.mu.Unlock()
		if !req.BeforeStop {
			return ErrStale
		}
		j.Spec.Run.Workdir = item.OutputDir
		j.Spec.Files = nil // This delivery directory contains only its own outputs.
		step, name := "", ""
		uploadCtx := withFileStoreAttempt(ctx, j)
		publication := filepath.Join(attemptPublicationRoot(j, g.dir), "warm-"+strconv.Itoa(d.Sequence))
		refs, _, err := g.worker.publishOutputFilesAt(uploadCtx, j, g.dir, publication, &step, &name)
		if err != nil {
			return fmt.Errorf("warm item output publication: %w", err)
		}
		req.Op, req.OutputRefs = "success", refs
	}
	_, err := g.warmCall(ctx, req)
	return err
}

func (g *nativeGroup) retireWarmItem(j Job, delivery WarmPieceDelivery, item warmPieceItem) {
	root := attemptPublicationRoot(j, g.dir)
	// Receipt acknowledgement is the authority to retire only this item.
	// Any unremoved files are still covered by ordinary attempt retention.
	_ = os.Remove(filepath.Join(root, "warm-checkpoint.json"))
	if item.CheckpointDir != "" {
		_ = os.RemoveAll(item.CheckpointDir)
	}
	_ = os.RemoveAll(item.OutputDir)
	_ = os.RemoveAll(filepath.Join(root, "warm-"+strconv.Itoa(delivery.Sequence)))
}
