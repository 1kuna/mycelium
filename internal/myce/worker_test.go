package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type workerTestRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
	state  string
}

func workerFixture(t *testing.T, q *Queue) *workerTestRun {
	t.Helper()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	run := &workerTestRun{cancel: cancel, done: make(chan struct{}), state: t.TempDir()}
	// Stop events carry absolute queue deadlines. Use the queue's clock so a
	// fake deadline cannot appear already expired to the worker.
	w := Worker{StateDir: run.state, Clock: q.clock, Client: Client{URL: server.URL, Token: "test"}, Machine: machine("worker", "fake", "fixture-worker-host"), Interval: 5 * time.Millisecond}
	go func() { run.err = w.Run(ctx); close(run.done) }()
	t.Cleanup(func() {
		defer server.Close()
		cancel()
		s, _ := q.Snapshot()
		for _, j := range s.Jobs {
			_ = q.Cancel(j.ID)
		}
		await(t, func() bool {
			if clock, ok := q.clock.(*workerFakeClock); ok {
				clock.Advance(time.Millisecond)
			}
			select {
			case <-run.done:
				return true
			default:
				return false
			}
		})
		if run.err != nil {
			t.Error(run.err)
		}
	})
	return run
}

func (run *workerTestRun) result(t *testing.T) Result {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(run.state, "*.result.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("expected one persisted attempt result, got %d: %v", len(files), err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var result Result
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

// testWaitTimeout bounds real I/O and goroutine synchronization, not scheduler time.
// Passing tests return as soon as their condition is true, even on a quiet laptop.
const testWaitTimeout = 60 * time.Second

func await(t *testing.T, f func() bool) {
	t.Helper()
	timeout := time.After(testWaitTimeout)
	for {
		if f() {
			return
		}
		select {
		case <-timeout:
			t.Fatalf("%s: worker condition not met within %s", t.Name(), testWaitTimeout)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func TestWorkerCancelledBeforeJoining(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := Worker{StateDir: newTestStateRoot(t), Client: Client{URL: server.URL, Token: "test"}, Machine: machine("worker", "fake"), Interval: time.Millisecond}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, "queued")
	if job(t, q, j.ID).Attempt != 0 {
		t.Fatal("cancelled worker started a command")
	}
}
func TestWorkerGracefulStopAndCheckpoint(t *testing.T) {
	for _, stop := range []string{"interrupt", "leave"} {
		t.Run(stop, func(t *testing.T) { testWorkerGracefulStopAndCheckpoint(t, stop) })
	}
}

func testWorkerGracefulStopAndCheckpoint(t *testing.T, stop string) {
	t.Helper()
	q, _ := fixture(t)
	clock := &workerFakeClock{now: q.clock.Now()}
	q.clock = clock
	wait := func(f func() bool) {
		t.Helper()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			return f()
		})
	}
	dir := t.TempDir()
	fifo := filepath.Join(dir, "save")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	gate, err := os.OpenFile(fifo, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Unblock a TERM handler even when an assertion fails before the save.
		_, _ = gate.WriteString("save\n")
		_ = gate.Close()
	}()
	stopping := filepath.Join(dir, "stopping")
	s := spec("experiment")
	// Hold the final save after observing TERM. This also makes a premature
	// SIGKILL visible instead of racing a one-line checkpoint write.
	s.Run.Command = []string{"sh", "-c", `
if [ "$MYCE_ATTEMPT" -gt 1 ]; then
  [ "$(cat "$MYCE_CHECKPOINT_DIR/saved")" = saved ] || exit 1
  echo resumed
  exit 0
fi
exec 3<> "$SAVE_FIFO"
trap 'echo term > "$STOP_FILE"; read gate <&3; echo saved > "$MYCE_CHECKPOINT_DIR/saved"; exit 0' TERM
echo ready
while :; do read line <&3; done`}
	s.Run.Env = map[string]string{"SAVE_FIFO": fifo, "STOP_FILE": stopping}
	j := submit(t, q, s)
	run := workerFixture(t, q)
	wait(func() bool {
		ls, _ := q.Logs(j.ID, 0)
		for _, l := range ls {
			if strings.Contains(l.Text, "ready") {
				return true
			}
		}
		return false
	})
	if stop == "interrupt" {
		run.cancel()
	} else if err := q.Leave("worker", false); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		_, err := os.Stat(stopping)
		return err == nil || job(t, q, j.ID).State == "queued"
	})
	if _, err := os.Stat(stopping); err != nil {
		t.Fatalf("job requeued without delivering TERM before the checkpoint save: %v; result: %+v", err, run.result(t))
	}
	wait(func() bool { return job(t, q, j.ID).State == "stopping" })
	deadline := job(t, q, j.ID).StopAt
	if deadline == nil || !q.clock.Now().Before(*deadline) {
		t.Fatal("checkpoint save did not begin within stop grace")
	}
	if _, err := gate.WriteString("save\n"); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return job(t, q, j.ID).State == "queued" })
	if !q.clock.Now().Before(*deadline) {
		t.Fatal("checkpoint save did not finish within stop grace")
	}
	data, err := os.ReadFile(filepath.Join(j.Checkpoint, "attempt-1", "saved"))
	if err != nil || strings.TrimSpace(string(data)) != "saved" {
		t.Fatalf("checkpoint %q: %v", data, err)
	}
	wait(func() bool {
		select {
		case <-run.done:
			return true
		default:
			return false
		}
	})
	result := run.result(t)
	if !result.Success || result.Interrupted || result.Forced || result.ReleasePending {
		t.Fatalf("stop did not finish gracefully: %+v", result)
	}
	// Leave's capacity reservation expires at the original deadline, even
	// when the payload releases early. Advance scheduler time explicitly.
	clock.Advance(deadline.Sub(clock.Now()))
	// A fresh worker must actually consume the copied checkpoint, not just
	// find the prior attempt's save on disk.
	workerFixture(t, q)
	wait(func() bool { return job(t, q, j.ID).State == "succeeded" })
	if got := job(t, q, j.ID); got.Attempt != 2 || got.Losses != 0 {
		t.Fatalf("resume attempt=%d losses=%d, want attempt 2 without infrastructure losses", got.Attempt, got.Losses)
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range logs {
		if strings.Contains(l.Text, "resumed") {
			return
		}
	}
	t.Fatal("resumed command did not consume the checkpoint")
}
func TestWorkerWaitsForNonresumableOnInterrupt(t *testing.T) {
	q, _ := fixture(t)
	q.clock = RealClock{}
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", `echo ready; while [ ! -f "$MYCE_CHECKPOINT_DIR/finish" ]; do sleep 0.01; done; echo finished`}
	j := submit(t, q, s)
	run := workerFixture(t, q)
	await(t, func() bool {
		ls, _ := q.Logs(j.ID, 0)
		for _, l := range ls {
			if strings.Contains(l.Text, "ready") {
				return true
			}
		}
		return false
	})
	run.cancel()
	await(t, func() bool { sn, _ := q.Snapshot(); return len(sn.Machines) == 1 && sn.Machines[0].Leaving })
	requireState(t, q, j.ID, "running")
	if err := os.WriteFile(filepath.Join(j.Checkpoint, "attempt-1", "finish"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return job(t, q, j.ID).State == "succeeded" })
}
func TestWorkerCommandStartFailure(t *testing.T) {
	q, _ := fixture(t)
	q.clock = RealClock{}
	s := spec("production")
	s.Run.Command = []string{"/does-not-exist/myce-test"}
	j := submit(t, q, s)
	run := workerFixture(t, q)
	await(t, func() bool { return job(t, q, j.ID).State == "failed" })
	got := job(t, q, j.ID)
	if got.InfrastructureRetries != 2 || got.Reason != "infrastructure_retries_exhausted" {
		t.Fatal("launch failure bypassed bounded infrastructure policy", got)
	}
	records, err := filepath.Glob(filepath.Join(run.state, "launch-"+j.ID+"-*.outcome.json"))
	if err != nil || len(records) != 3 {
		t.Fatal("missing per-attempt Mycelium launch records", len(records), err)
	}
	for _, path := range records {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var facts memberOutcome
		if err = json.Unmarshal(data, &facts); err != nil {
			t.Fatal(err)
		}
		if len(facts.Infrastructure) != 1 || facts.Infrastructure[0].Kind != launchFailed || !facts.recordedInfrastructure() {
			t.Fatal("launch failure was not explained by a durable Mycelium fact", facts)
		}
	}
}

// workerWithPreparedCheckpoint synchronizes real filesystem setup for tests of
// process behavior. A single start may legitimately defer preparation after its
// I/O budget; Worker.Run retries it, but these tests call start directly.
// Force that path, then consume the completed operation on the caller's start.
// Scheduling and production I/O budgets remain unchanged.
func workerWithPreparedCheckpoint(t *testing.T, w Worker, store *workerStore, payload Job) Worker {
	t.Helper()
	if !needsCheckpoint(&payload) {
		return w
	}
	release := make(chan struct{})
	w.checkpointPrepare = func(value Job) (string, error) {
		<-release
		return prepareCheckpoint(value)
	}
	process, err := w.start(store, payload)
	close(release)
	if process != nil || !errors.Is(err, errWorkerCheckpoint) || !store.checkpointUnavailable {
		t.Fatalf("blocked preparation must defer startup: process=%v err=%v unavailable=%v", process, err, store.checkpointUnavailable)
	}
	pending := store.nasPrepare.pending
	if pending == nil {
		t.Fatal("timed-out preparation lost its pending filesystem operation")
	}
	select {
	case <-pending.done:
	case <-time.After(3 * time.Second):
		t.Fatal("released checkpoint preparation did not finish")
	}
	w.checkpointPrepare = func(Job) (string, error) {
		return "", errors.New("completed checkpoint preparation was repeated")
	}
	return w
}
