package myce

import (
	"bytes"
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
	"strconv"
	"strings"
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
	// Output upload requests and settle reports that reached the API.
	uploads, reports atomic.Int32
}

type warmFault func(http.ResponseWriter, *http.Request, int32) bool

// warmFaults injects API errors by endpoint. Settle faults also give the
// client its own fake clock, so retry pacing and its bound pass instantly
// without moving the queue's clock.
type warmFaults struct {
	checkpoint, upload, report warmFault
}

func newWarmChannel(t *testing.T, s Spec, fault warmFault) *warmChannel {
	return newWarmChannelWith(t, s, warmFaults{checkpoint: fault})
}

func newWarmChannelWith(t *testing.T, s Spec, faults warmFaults) *warmChannel {
	t.Helper()
	q, clock, m, base := warmFixture(t)
	s.WarmPieces, s.SubmissionKey, s.FanOut, s.Outputs = base.WarmPieces, base.SubmissionKey, base.FanOut, base.Outputs
	parent := submit(t, q, s)
	c := &warmChannel{q: q, clock: clock, run: warmStart(t, q, m, parent.ID), done: make(chan error, 1)}
	handler := API{Queue: q, Token: "test"}.Handler()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/files/checkpoint":
			if n := c.posts.Add(1); faults.checkpoint != nil && faults.checkpoint(w, r, n) {
				return
			}
		case strings.HasPrefix(r.URL.Path, "/api/files/uploads/") && (r.Method == "POST" || r.Method == "PATCH"):
			if n := c.uploads.Add(1); faults.upload != nil && faults.upload(w, r, n) {
				return
			}
		case r.URL.Path == "/api/workers/warm-piece":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var req warmPieceRequest
			if json.Unmarshal(body, &req) == nil && (req.Op == "success" || req.Op == "failure") {
				if n := c.reports.Add(1); faults.report != nil && faults.report(w, r, n) {
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(api.Close)
	w := recoveryWorker(t)
	w.Machine, w.Clock = m, clock
	w.Client = Client{URL: api.URL, Token: "test"}
	if faults.upload != nil || faults.report != nil {
		paced := &workerFakeClock{now: time.Unix(1_700_000_000, 0)}
		w.Client.Clock = paced
		w.Client.Pause = func(_ context.Context, d time.Duration) error { paced.Advance(d); return nil }
	}
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

// uploadRefusal answers like the queue's upload envelope for a storage error.
func uploadRefusal(status int) warmFault {
	return func(w http.ResponseWriter, r *http.Request, _ int32) bool {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Myce-Error-Code", "file_upload_unavailable")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": "file_upload_unavailable", "error": "upload storage unavailable"})
		return true
	}
}

// accept settles the remaining items, then main exits at end of work.
func (c *warmChannel) accept(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		item := c.next(t, key)
		c.writeOutput(t, item)
		if reply, err := c.call(t, 'S'); err != nil || reply["end"] != nil || reply["failed"] != nil {
			t.Fatalf("settle %s = %v, %v", key, reply, err)
		}
	}
	if reply, err := c.call(t, 'N'); err != nil || reply["item"] != nil {
		t.Fatalf("main did not receive end of work: %v %v", reply, err)
	}
	c.main.Close()
	c.finished(t)
}

// Quick gate, PR #2104 round 4: an item output upload failed once while the
// queue mutex was contended, and the settle error failed the production
// parent. Content-addressed publication replays, so the item settles.
func TestWarmPieceOutputUploadTransientErrorRetries(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			s := spec("production")
			s.Class = "production"
			refuse := uploadRefusal(status)
			c := newWarmChannelWith(t, s, warmFaults{upload: func(w http.ResponseWriter, r *http.Request, n int32) bool {
				return n == 1 && refuse(w, r, n)
			}})
			item := c.next(t, "a")
			c.writeOutput(t, item)
			if reply, err := c.call(t, 'S'); err != nil || reply["end"] != nil || reply["failed"] != nil {
				t.Fatalf("transient upload error reached main: %v %v", reply, err)
			}
			if c.uploads.Load() < 2 {
				t.Fatalf("upload was not retried: %d requests", c.uploads.Load())
			}
			c.accept(t, "b", "c")
			if err := c.q.Complete(result(c.run, true)); err != nil {
				t.Fatal(err)
			}
			got := job(t, c.q, c.run.ID)
			if got.State != "succeeded" || len(got.FanOut.Results) != 3 || got.InfrastructureRetries != 0 {
				t.Fatalf("parent after retried upload: %s (%s) %+v", got.State, got.Reason, got.FanOut)
			}
		})
	}
}

// Transient errors that outlast the settle bound end main cleanly with the
// item unsettled. The interrupted result requeues the tail without failures.
func TestWarmPieceSettleUnavailableRequeuesParent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		faults warmFaults
	}{
		{"upload", warmFaults{upload: uploadRefusal(http.StatusServiceUnavailable)}},
		{"report", warmFaults{report: func(w http.ResponseWriter, _ *http.Request, _ int32) bool {
			http.Error(w, "fixture queue unavailable", http.StatusServiceUnavailable)
			return true
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := spec("production")
			s.Class = "production"
			c := newWarmChannelWith(t, s, tc.faults)
			item := c.next(t, "a")
			c.writeOutput(t, item)
			if reply, err := c.call(t, 'S'); err != nil || reply["end"] != true {
				t.Fatalf("unavailable settle reply = %v, %v; want end", reply, err)
			}
			if c.uploads.Load()+c.reports.Load() < 3 {
				t.Fatalf("settle gave up without retrying: uploads=%d reports=%d", c.uploads.Load(), c.reports.Load())
			}
			if reply, err := c.call(t, 'N'); err != nil || reply["item"] != nil {
				t.Fatalf("main did not receive end of work: %v %v", reply, err)
			}
			c.main.Close()
			c.finished(t)
			if !c.g.warmUnsettled.Load() {
				t.Fatal("attempt did not record the unsettled item")
			}
			r := result(c.run, false)
			r.Interrupted, r.ExecutionCause = true, warmSettleCause()
			r.Error = r.ExecutionCause.String()
			if err := c.q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, c.q, c.run.ID)
			if got.State != "queued" || got.FanOut.Next != 0 || len(got.FanOut.Warm.Failures) != 0 || got.Losses != 0 || got.InfrastructureRetries != 1 {
				t.Fatalf("parent after unavailable settle: %s (%s) next=%d failures=%v losses=%d retries=%d",
					got.State, got.Reason, got.FanOut.Next, got.FanOut.Warm.Failures, got.Losses, got.InfrastructureRetries)
			}
		})
	}
}

// A deterministic refusal of one item's outputs settles that item as failed.
// Main continues, and the parent fails with the item, not a run error.
func TestWarmPieceRejectedOutputsFailOnlyThatItem(t *testing.T) {
	for _, tc := range []struct {
		name   string
		write  bool
		faults warmFaults
	}{
		{"missing output", false, warmFaults{}},
		{"queue refusal", true, warmFaults{report: func(w http.ResponseWriter, r *http.Request, n int32) bool {
			var req warmPieceRequest
			if n != 1 || json.NewDecoder(r.Body).Decode(&req) != nil || req.Op != "success" {
				return false
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "fixture output refusal", "code": "request_failed"})
			return true
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := spec("production")
			s.Class = "production"
			c := newWarmChannelWith(t, s, tc.faults)
			item := c.next(t, "a")
			if tc.write {
				c.writeOutput(t, item)
			}
			if reply, err := c.call(t, 'S'); err != nil || reply["failed"] != true || reply["end"] != nil {
				t.Fatalf("rejected outputs reply = %v, %v; want failed", reply, err)
			}
			if got := job(t, c.q, c.run.ID); got.FanOut.Next != 1 || len(got.FanOut.Warm.Failures) != 1 {
				t.Fatalf("rejected item was not settled as failed: %+v", got.FanOut)
			}
			if tc.write && c.reports.Load() != 2 {
				t.Fatalf("deterministic refusal was retried: %d reports", c.reports.Load())
			}
			c.accept(t, "b", "c")
			if err := c.q.Complete(result(c.run, true)); err != nil {
				t.Fatal(err)
			}
			got := job(t, c.q, c.run.ID)
			if want := plainPieceFailure([]string{"a"}, 3); got.State != "failed" || got.Reason != want || len(got.FanOut.Results) != 2 {
				t.Fatalf("parent = %s (%s), results %v; want failed (%s)", got.State, got.Reason, got.FanOut.Results, want)
			}
		})
	}
}
