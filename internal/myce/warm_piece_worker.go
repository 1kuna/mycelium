package myce

import (
	"context"
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

	"golang.org/x/sys/unix"
)

const warmPieceFD = "MYCE_WARM_PIECES_FD"

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

// A caller sends the operation byte and one endpoint of a fresh Unix
// socketpair over the inherited channel. The kernel identifies the process
// that owns the other endpoint. Inheriting or opening a copy of the bootstrap
// descriptor does not give another process main's identity or any replies.
func readWarmRequest(conn *net.UnixConn, payloadPID int) (byte, net.Conn, error) {
	var op [1]byte
	var control [128]byte
	_, n, flags, _, err := conn.ReadMsgUnix(op[:], control[:])
	if err != nil {
		return 0, nil, err
	}
	messages, err := unix.ParseSocketControlMessage(control[:n])
	if err != nil {
		return 0, nil, err
	}
	var fds []int
	for _, message := range messages {
		rights, err := unix.ParseUnixRights(&message)
		if err == nil {
			fds = append(fds, rights...)
		}
	}
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()
	if len(fds) != 1 || len(messages) != 1 || flags&unix.MSG_CTRUNC != 0 {
		return 0, nil, nil
	}
	fd := fds[0]
	addr, err := unix.Getpeername(fd)
	if _, unixPeer := addr.(*unix.SockaddrUnix); err != nil || !unixPeer {
		return 0, nil, nil
	}
	pid, err := warmPeerPID(fd)
	if err != nil || pid != payloadPID {
		return 0, nil, nil
	}
	fds = nil
	file := os.NewFile(uintptr(fd), "warm-piece-response")
	defer file.Close()
	reply, err := net.FileConn(file)
	return op[0], reply, err
}

func (g *nativeGroup) serveWarmPieces(ctx context.Context, conn *net.UnixConn, j Job, payloadPID int) error {
	defer conn.Close()
	var delivery *WarmPieceDelivery
	var item *warmPieceItem
	for {
		op, reply, err := readWarmRequest(conn, payloadPID)
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if reply == nil {
			continue
		}
		closeOnStop := context.AfterFunc(ctx, func() { reply.Close() })
		end, responseErr := func() (bool, error) {
			respond := func(response warmHelperReply) error {
				return json.NewEncoder(reply).Encode(response)
			}
			switch op {
			case 'N':
				if delivery != nil {
					return false, errors.New("settle the current warm item before requesting another")
				}
				offered, err := g.warmCall(ctx, warmPieceRequest{Op: "next"})
				if err != nil {
					return false, err
				}
				if offered.End && offered.Delivery == nil {
					return true, respond(warmHelperReply{})
				}
				delivery = offered.Delivery
				item, err = prepareWarmItem(j, g.dir, delivery)
				if err != nil {
					return false, err
				}
				started, err := g.warmCall(ctx, warmPieceRequest{Op: "start", Key: delivery.Key, Sequence: delivery.Sequence})
				if err != nil {
					return false, err
				}
				if started.Delivery == nil || started.Delivery.Key != delivery.Key || started.Delivery.Sequence != delivery.Sequence || started.Delivery.StartedAt == nil {
					return false, errors.New("missing warm item start authorization")
				}
				return false, respond(warmHelperReply{Item: item})
			case 'C', 'S', 'F':
				if delivery == nil {
					return false, errors.New("warm item operation requires an active delivery")
				}
				if op == 'C' {
					if item.CheckpointDir == "" {
						return false, errors.New("warm item checkpoint requires a resumable job")
					}
					if _, err := g.worker.publishFileCheckpoint(j, g.dir); err != nil {
						return false, err
					}
				} else {
					if err := g.settleWarmItem(ctx, j, *delivery, *item, op == 'S'); err != nil {
						return false, err
					}
					g.retireWarmItem(j, *delivery, *item)
					delivery, item = nil, nil
				}
				return false, respond(warmHelperReply{})
			default:
				return false, errors.New("unknown warm helper operation")
			}
		}()
		if responseErr != nil && ctx.Err() == nil {
			// Keep worker and queue internals, including server responses,
			// outside the authored payload.
			_ = json.NewEncoder(reply).Encode(warmHelperReply{Error: "warm piece operation refused"})
		}
		closeOnStop()
		_ = reply.Close()
		if end || responseErr != nil {
			return responseErr
		}
	}
}

func prepareWarmItem(j Job, state string, d *WarmPieceDelivery) (*warmPieceItem, error) {
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
		root := attemptPublicationRoot(j, state)
		if err := os.MkdirAll(root, 0700); err != nil {
			return nil, err
		}
		if err := saveWorkerJSON(root, "warm-checkpoint.json", d.Sequence); err != nil {
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
