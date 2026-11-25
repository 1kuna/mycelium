package myce

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// warmChannel drives the worker's helper loop from main's side of the real
// stream, against the real queue API behind an optional fault handler.
type warmChannel struct {
	q     *Queue
	clock *fakeClock
	run   *Job
	g     *nativeGroup
	main  net.Conn
	done  chan error
	posts atomic.Int32 // Checkpoint publications that reached the API.
}

func newWarmChannel(t *testing.T, s Spec, fault func(http.ResponseWriter, *http.Request, int32) bool) *warmChannel {
	t.Helper()
	q, clock, m, base := warmFixture(t)
	s.WarmPieces, s.SubmissionKey, s.FanOut, s.Outputs = base.WarmPieces, base.SubmissionKey, base.FanOut, base.Outputs
	parent := submit(t, q, s)
	c := &warmChannel{q: q, clock: clock, run: warmStart(t, q, m, parent.ID), done: make(chan error, 1)}
	handler := API{Queue: q, Token: "test"}.Handler()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/files/checkpoint" {
			if n := c.posts.Add(1); fault != nil && fault(w, r, n) {
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(api.Close)
	w := recoveryWorker(t)
	w.Machine, w.Clock = m, clock
	w.Client = Client{URL: api.URL, Token: "test"}
	j := *c.run
	j.Checkpoint, j.Spec.Run.Workdir = t.TempDir(), t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	c.g = &nativeGroup{p: &process{job: j}, worker: w, session: j.Session, dir: t.TempDir(), ctx: ctx, cancel: cancel}
	conn, child, err := newWarmPieceChannel()
	if err != nil {
		t.Fatal(err)
	}
	c.main, err = net.FileConn(child)
	child.Close()
	if err != nil {
		t.Fatal(err)
	}
	go func() { c.done <- c.g.serveWarmPieces(ctx, conn, j) }()
	t.Cleanup(func() {
		c.main.Close()
		cancel()
		<-c.done
	})
	return c
}

// call sends one operation and decodes the reply generically, so these
// regressions do not depend on reply fields added by a fix.
func (c *warmChannel) call(t *testing.T, op byte) (map[string]any, error) {
	t.Helper()
	if err := c.main.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame := []byte{0, 0, 0, 1, op}
	if _, err := c.main.Write(frame); err != nil {
		return nil, err
	}
	var header [4]byte
	if _, err := io.ReadFull(c.main, header[:]); err != nil {
		return nil, err
	}
	data := make([]byte, binary.BigEndian.Uint32(header[:]))
	if _, err := io.ReadFull(c.main, data); err != nil {
		return nil, err
	}
	reply := map[string]any{}
	if err := json.Unmarshal(data, &reply); err != nil {
		t.Fatal(err)
	}
	if reply["error"] != nil {
		return reply, errors.New(reply["error"].(string))
	}
	return reply, nil
}

func (c *warmChannel) next(t *testing.T, key string) map[string]any {
	t.Helper()
	reply, err := c.call(t, 'N')
	item, _ := reply["item"].(map[string]any)
	if err != nil || item == nil || item["key"] != key {
		t.Fatalf("next = %v, %v; want item %s", reply, err, key)
	}
	return item
}

// The serving goroutine ends without a run error, so main's clean exit and
// the queue's stop state decide the attempt.
func (c *warmChannel) finished(t *testing.T) {
	t.Helper()
	select {
	case err := <-c.done:
		c.done <- nil // Let cleanup drain a closed loop.
		if err != nil {
			t.Fatalf("helper loop returned a run error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("helper loop did not finish")
	}
}

func (c *warmChannel) save(t *testing.T, item map[string]any, generation int) {
	t.Helper()
	dir := item["checkpoint_dir"].(string)
	data := []byte(`{"myce_checkpoint":{"generation":` + string(rune('0'+generation)) + `}}`)
	tmp := filepath.Join(dir, "progress.tmp")
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, "progress.json")); err != nil {
		t.Fatal(err)
	}
}

func (c *warmChannel) writeOutput(t *testing.T, item map[string]any) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(item["output_dir"].(string), "out"), []byte(item["key"].(string)), 0600); err != nil {
		t.Fatal(err)
	}
}

// Review round 3, P1 scenario A: the queue enters a maintenance stop, which
// admits no boundary receipts, before the worker sees it. Main's settle must end its
// loop cleanly instead of failing the production parent.
func TestWarmPieceStopRefusingSettleEndsMainCleanly(t *testing.T) {
	for _, tc := range []struct {
		name          string
		op            byte
		workerStopped bool
	}{
		{"failure before worker stop", 'F', false},
		{"success before worker stop", 'S', false},
		{"success after worker stop", 'S', true},
		{"failure after worker stop", 'F', true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := spec("production")
			s.Class = "production"
			c := newWarmChannel(t, s, nil)
			if c.run.Spec.Priority != "production" {
				t.Fatalf("fixture parent priority %q", c.run.Spec.Priority)
			}
			item := c.next(t, "a")
			c.writeOutput(t, item)
			if err := c.q.updateRequest(t.Context(), func() error {
				c.q.stopBatchForMaintenance(c.q.s.Jobs[c.run.ID], maintenanceStopCause, nil, false)
				return nil
			}, nil); err != nil {
				t.Fatal(err)
			}
			if got := job(t, c.q, c.run.ID); got.State != "stopping" || !plannedMaintenanceStop(got) || plannedPriorityStop(got) {
				t.Fatalf("fixture stop = %s %q", got.State, got.Reason)
			}
			if tc.workerStopped {
				c.g.p.mu.Lock()
				c.g.p.stopped = true
				c.g.p.mu.Unlock()
			}
			if reply, err := c.call(t, tc.op); err != nil {
				t.Fatalf("refused settle raised in main: %v %v", reply, err)
			}
			if reply, err := c.call(t, 'N'); err != nil || reply["item"] != nil {
				t.Fatalf("main did not receive end of work: %v %v", reply, err)
			}
			c.main.Close()
			c.finished(t)
			// Main exits zero after end of work; the stop decides the outcome.
			if err := c.q.Complete(result(c.run, true)); err != nil {
				t.Fatal(err)
			}
			got := job(t, c.q, c.run.ID)
			if got.State != "queued" || got.FanOut.Next != 0 || len(got.FanOut.Warm.Failures) != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("stop did not keep the unsettled item: %s (%s) %+v", got.State, got.Reason, got.FanOut)
			}
		})
	}
}

// Review round 3, P1 scenario B: one transient checkpoint publication error
// is retried and never ends main's loop.
func TestWarmPieceCheckpointTransientErrorRetries(t *testing.T) {
	s := spec("production")
	s.Class, s.Resumable = "production", true
	c := newWarmChannel(t, s, func(w http.ResponseWriter, _ *http.Request, n int32) bool {
		if n == 1 {
			http.Error(w, "fixture checkpoint service unavailable", http.StatusServiceUnavailable)
			return true
		}
		return false
	})
	item := c.next(t, "a")
	c.save(t, item, 1)
	if reply, err := c.call(t, 'C'); err != nil || reply["pending"] != nil {
		t.Fatalf("transient checkpoint error reached main: %v %v", reply, err)
	}
	if got := job(t, c.q, c.run.ID); c.posts.Load() != 2 || got.FileCheckpoints[c.run.Attempt].Generation != 1 {
		t.Fatalf("checkpoint was not retried: posts=%d checkpoints=%+v", c.posts.Load(), got.FileCheckpoints)
	}
	c.writeOutput(t, item)
	if _, err := c.call(t, 'S'); err != nil {
		t.Fatalf("channel did not survive the retried checkpoint: %v", err)
	}
	c.next(t, "b")
}

// A checkpoint that cannot publish leaves main running. A later item or the
// final save can still make progress portable.
func TestWarmPieceCheckpointRefusalKeepsChannel(t *testing.T) {
	s := spec("production")
	s.Class, s.Resumable = "production", true
	c := newWarmChannel(t, s, func(w http.ResponseWriter, _ *http.Request, _ int32) bool {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "fixture refusal", "code": "checkpoint_invalid"})
		return true
	})
	item := c.next(t, "a")
	c.save(t, item, 1)
	if reply, err := c.call(t, 'C'); err != nil || reply["pending"] != true {
		t.Fatalf("refused checkpoint reply = %v, %v; want pending", reply, err)
	}
	c.writeOutput(t, item)
	if _, err := c.call(t, 'S'); err != nil {
		t.Fatalf("channel did not survive the refused checkpoint: %v", err)
	}
	if got := job(t, c.q, c.run.ID); got.FanOut.Next != 1 || got.State != "running" {
		t.Fatalf("item after refused checkpoint: %s %+v", got.State, got.FanOut)
	}
}

// Review round 3, P2: repeated checkpoint calls at an already accepted
// generation neither recapture nor publish again.
func TestWarmPieceCheckpointSkipsAcceptedGeneration(t *testing.T) {
	s := spec("experiment")
	s.Resumable = true
	c := newWarmChannel(t, s, nil)
	item := c.next(t, "a")
	c.save(t, item, 1)
	for range 3 {
		if _, err := c.call(t, 'C'); err != nil {
			t.Fatal(err)
		}
	}
	if c.posts.Load() != 1 {
		t.Fatalf("same generation published %d times", c.posts.Load())
	}
	c.save(t, item, 2)
	if _, err := c.call(t, 'C'); err != nil {
		t.Fatal(err)
	}
	if got := job(t, c.q, c.run.ID); c.posts.Load() != 2 || got.FileCheckpoints[c.run.Attempt].Generation != 2 {
		t.Fatalf("new generation was not published: posts=%d %+v", c.posts.Load(), got.FileCheckpoints)
	}
	c.writeOutput(t, item)
	if _, err := c.call(t, 'S'); err != nil {
		t.Fatal(err)
	}
	// The next item starts its own generation history.
	item = c.next(t, "b")
	c.save(t, item, 2)
	if _, err := c.call(t, 'C'); err != nil || c.posts.Load() != 3 {
		t.Fatalf("next item's checkpoint was skipped: posts=%d err=%v", c.posts.Load(), err)
	}
}

// Review round 3, nit: socket deadlines are kernel wall-clock deadlines. A
// worker whose injected clock differs from real time still serves frames.
func TestWarmPieceFrameDeadlinesUseWallClock(t *testing.T) {
	c := newWarmChannel(t, spec("experiment"), nil)
	if c.clock.Now().After(time.Now().Add(-time.Hour)) {
		t.Fatalf("fixture clock %s is not behind wall time", c.clock.Now())
	}
	c.next(t, "a")
}
