package myce

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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
	// Socket deadlines are kernel wall-clock instants, so they use real time
	// rather than the worker's injected clock.
	warmFrameTimeout = 5 * time.Second
	warmReplyLimit   = 4 << 20
	// A settle keeps retrying transient publication and report errors for
	// nearly three times the measured 11-second production p95 queue mutex
	// wait, the same bound as a hinted upload response. Past it the item stays
	// unsettled and the parent requeues, costing one model load rather than
	// holding the reservation while the queue cannot accept the item.
	warmSettleBound = maxHintedUploadResponseWindow
)

// errWarmSettleUnavailable ends the helper loop with the item unsettled. The
// attempt then reports an interruption, so the parent requeues its tail.
var errWarmSettleUnavailable = errors.New("warm item settle unavailable")

// warmSettleCause is the interruption reported for errWarmSettleUnavailable.
func warmSettleCause() *RuntimeCause {
	return &RuntimeCause{Operation: "warm_settle", Class: "control_unreachable"}
}

func warmSettleInterruption(c *RuntimeCause) bool {
	return c != nil && c.Operation == "warm_settle" && c.Class == "control_unreachable"
}

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
	// End answers a settle that a stop or queue fence refused. The item stays
	// unsettled for the queue's stop decision, and main has no more work.
	End bool `json:"end,omitempty"`
	// Pending answers a checkpoint the worker could not publish. Main keeps
	// running; a later checkpoint or the final save can still publish.
	Pending bool `json:"pending,omitempty"`
	// Failed answers a success whose outputs the queue rejected. The item
	// is settled as failed and main continues with the next item.
	Failed bool `json:"failed,omitempty"`
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

// The worker, never main, supplies the attempt identity on every request.
func (g *nativeGroup) warmIdentity(req warmPieceRequest) warmPieceRequest {
	req.Execution = Execution{ID: g.p.job.ID, Attempt: g.p.job.Attempt}
	req.Machine, req.Session = g.worker.Machine.ID, g.session
	return req
}

func (g *nativeGroup) warmCall(ctx context.Context, req warmPieceRequest) (warmPieceReply, error) {
	req = g.warmIdentity(req)
	var reply warmPieceReply
	err := g.worker.Client.RetryTransient(ctx, func(ctx context.Context, c Client) error {
		reply = warmPieceReply{}
		return c.Call(ctx, "POST", "/api/workers/warm-piece", req, &reply)
	}, nil)
	return reply, err
}

// Read ordinary stream bytes only, so the kernel never installs ancillary
// descriptors in the worker. The complete request body is one operation byte.
func readWarmRequest(conn *net.UnixConn) (byte, error) {
	var frame [5]byte
	// Between requests main can be loading or running an item. The channel
	// goroutine can wait here until attempt cancellation closes the socket.
	if _, err := io.ReadFull(conn, frame[:1]); err != nil {
		return 0, err
	}
	if err := conn.SetReadDeadline(time.Now().Add(warmFrameTimeout)); err != nil {
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

func writeWarmReply(conn *net.UnixConn, reply warmHelperReply) error {
	data, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	if len(data) > warmReplyLimit {
		return errors.New("warm reply exceeds frame size limit")
	}
	if err := conn.SetWriteDeadline(time.Now().Add(warmFrameTimeout)); err != nil {
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
	var fenced bool
	var accepted int64 // Generation last accepted for the current delivery.
	for {
		op, err := readWarmRequest(conn)
		if err != nil || ctx.Err() != nil {
			return nil // Transport failure ends just this channel.
		}
		var end bool
		var response warmHelperReply
		switch op {
		case 'N':
			if fenced {
				response.End = true
				break
			}
			if delivery != nil {
				return nil
			}
			delivery, item, end, err = g.beginWarmItem(ctx, j)
			response.Item, accepted = item, 0
		case 'C':
			if item == nil || item.CheckpointDir == "" {
				return nil
			}
			response.Pending = !g.publishWarmCheckpoint(ctx, j, item.CheckpointDir, &accepted)
		case 'S', 'F':
			if delivery == nil {
				return nil
			}
			response.Failed, err = g.settleWarmItem(ctx, j, *delivery, *item, op == 'S')
			if err == nil {
				g.retireWarmItem(j, *delivery, *item)
				delivery, item = nil, nil
			} else if ctx.Err() == nil {
				// A stop, queue fence or unavailable queue leaves this item
				// unsettled. Keep its files and checkpoint marker for the final
				// save and attempt retention, and let main exit cleanly instead
				// of failing the attempt. Unavailability marks the attempt so the
				// queue requeues the tail; a stop or fence already decides it.
				if errors.Is(err, errWarmSettleUnavailable) {
					g.warmUnsettled.Store(true)
				}
				delivery, item, fenced, err = nil, nil, true, nil
				response.Failed, response.End = false, true
			}
		default:
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			// Keep worker and queue internals outside the authored payload.
			_ = writeWarmReply(conn, warmHelperReply{Error: "warm piece operation refused"})
			return err
		}
		if err := writeWarmReply(conn, response); err != nil || end {
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

// Publication retries a transient error within its own bound. Any remaining
// error leaves main running, so one failed save never fails the attempt.
// Repeated calls at the accepted generation capture and publish nothing.
func (g *nativeGroup) publishWarmCheckpoint(ctx context.Context, j Job, dir string, accepted *int64) bool {
	if *accepted > 0 {
		generation, err := scanCheckpointProgressContext(ctx, dir, false, filepath.WalkDir, true)
		if err == nil && generation == *accepted {
			return true
		}
	}
	published, err := g.worker.publishFileCheckpointContext(ctx, j, g.dir)
	if err != nil {
		return false
	}
	if published > 0 {
		*accepted = published
	}
	return true
}

// warmItemRejected marks a deterministic refusal of this item's outputs.
type warmItemRejected struct{ error }

func (e warmItemRejected) Unwrap() error { return e.error }

// Output publication is content-addressed and resumes from its confirmation
// journal, and the queue accepts an identical receipt again, so every
// transient error in a settle is safe to replay.
func warmSettleTransient(err error) bool {
	var rejected warmItemRejected
	if errors.As(err, &rejected) {
		return false // Publication judged the outputs themselves invalid.
	}
	if retryTransient(err) {
		return true
	}
	var api *HTTPError
	if !errors.As(err, &api) {
		return false
	}
	switch api.Code {
	case "file_upload_interrupted", "file_upload_unavailable", "status_unavailable":
		return api.Status == http.StatusRequestTimeout || api.Status >= 500
	case "file_upload_offset_mismatch", "file_upload_conflict":
		return true // A replay rechecks the accepted offset and published blob.
	}
	return false
}

// warmFenced reports the queue's attempt or delivery fence, which a stop,
// loss or replacement decides. Upload-protocol conflicts are not fences.
func warmFenced(err error) bool {
	var api *HTTPError
	return errors.Is(err, ErrStale) || errors.As(err, &api) && api.Status == http.StatusConflict && (api.Code == "" || api.Code == "request_stale")
}

// retryWarmSettle paces replays on the client's clock and returns
// errWarmSettleUnavailable once transient errors outlast warmSettleBound.
// Each operation makes one pass, so nested client retries cannot extend it.
func (g *nativeGroup) retryWarmSettle(ctx context.Context, operation func(context.Context) error) error {
	c := g.worker.Client
	bound := warmSettleBound
	if g.worker.warmSettleBound > 0 {
		bound = g.worker.warmSettleBound
	}
	deadline := c.clock().Now().Add(bound)
	pace := ClientPace{}
	for {
		started := c.clock().Now()
		err := operation(ctx)
		if err == nil || ctx.Err() != nil || !warmSettleTransient(err) {
			return err
		}
		now := c.clock().Now()
		if !now.Before(deadline) {
			return fmt.Errorf("%w: %w", errWarmSettleUnavailable, err)
		}
		pace.Observe(now.Sub(started), false)
		if err := c.pause(ctx, min(pace.Delay(now, time.Time{}, err, c.Jitter), deadline.Sub(now))); err != nil {
			return err
		}
	}
}

// settleWarmItem returns nil once the queue holds the item's receipt. failed
// reports a success whose outputs were rejected and settled as a failure. Any
// other error leaves the item unsettled: a stop or fence (stale), exhausted
// transient errors or a refused failure report (errWarmSettleUnavailable).
func (g *nativeGroup) settleWarmItem(ctx context.Context, j Job, d WarmPieceDelivery, item warmPieceItem, success bool) (failed bool, err error) {
	req := warmPieceRequest{Op: "failure", Key: d.Key, Sequence: d.Sequence}
	g.p.mu.Lock()
	// A report that reached this gate before stop delivery can still win
	// against a priority stop that is in transit from the queue.
	req.BeforeStop = !g.p.stopped && !g.p.killed && !g.p.userCancel
	g.p.mu.Unlock()
	if !req.BeforeStop {
		return false, ErrStale
	}
	w := g.worker
	w.Client = w.Client.WithoutRetries()
	report := func(ctx context.Context) error {
		return w.Client.Call(ctx, "POST", "/api/workers/warm-piece", g.warmIdentity(req), &warmPieceReply{})
	}
	if success {
		j.Spec.Run.Workdir = item.OutputDir
		j.Spec.Files = nil // This delivery directory contains only its own outputs.
		uploadCtx := withFileStoreAttempt(ctx, j)
		publication := filepath.Join(attemptPublicationRoot(j, g.dir), "warm-"+strconv.Itoa(d.Sequence))
		err = g.retryWarmSettle(ctx, func(context.Context) error {
			step, name := "", ""
			refs, terminal, err := w.publishOutputFilesAt(uploadCtx, j, g.dir, publication, &step, &name)
			if terminal {
				return warmItemRejected{err}
			}
			req.Op, req.OutputRefs = "success", refs
			return err
		})
		if err == nil {
			err = g.retryWarmSettle(ctx, report)
		}
		if err == nil || ctx.Err() != nil || warmFenced(err) || errors.Is(err, errWarmSettleUnavailable) {
			return false, err
		}
		// The queue or publication refused these outputs. Settle the item as
		// failed so main continues and the parent reports the failed key.
		failed, req.Op, req.OutputRefs = true, "failure", nil
	}
	err = g.retryWarmSettle(ctx, report)
	if err != nil && ctx.Err() == nil && !warmFenced(err) && !errors.Is(err, errWarmSettleUnavailable) {
		// A refused failure report cannot settle the item. Leave it for the
		// requeued attempt rather than failing every other item with it.
		err = fmt.Errorf("%w: %w", errWarmSettleUnavailable, err)
	}
	return failed, err
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
