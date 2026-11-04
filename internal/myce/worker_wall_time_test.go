package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkerWallTimeStartedReceiptRestart(t *testing.T) {
	q, c := fixture(t)
	m := machine("worker", "fake")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
	s := spec("experiment")
	s.WallTime = "10s"
	submit(t, q, s)
	j := check(t, q, m, nil).Job
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	w := Worker{Clock: c, Machine: m, Client: Client{URL: server.URL, Token: "test"}}
	dir := t.TempDir()
	if err := w.authorizeStart(*j, j.Session, dir); err != nil {
		t.Fatal(err)
	}
	before, err := readWorkerWallTime(dir, *j)
	if err != nil || before == nil {
		t.Fatal(before, err)
	}
	c.Advance(3 * time.Second)
	if err := w.authorizeStart(*j, j.Session, dir); err != nil {
		t.Fatal(err)
	}
	after, err := readWorkerWallTime(dir, *j)
	if err != nil || !after.ExpiresAt.Equal(before.ExpiresAt) {
		t.Fatal("reconnect extended limit", after, err)
	}
	// Disk receipt, not process memory, is restart authority.
	restarted := *after
	restarted.Expired = true
	if err := saveWorkerJSON(dir, wallTimeReceiptName(*j), restarted); err != nil {
		t.Fatal(err)
	}
	r, err := applyWorkerWallTime(dir, *j, Result{Interrupted: true, ReleasePending: true})
	if err != nil || r.ExecutionCause == nil || r.ExecutionCause.Class != wallTimeLimit || !r.ReleasePending || r.Interrupted {
		t.Fatal("restart lost cause or custody", r, err)
	}
	for _, r := range []Result{{Success: true}, {ExecutionCause: &RuntimeCause{Operation: "command", Class: "command_exit"}}, {OutOfMemory: true, Interrupted: true}} {
		got, err := applyWorkerWallTime(dir, *j, r)
		if err != nil || got.Success != r.Success || got.OutOfMemory != r.OutOfMemory || got.ExecutionCause != r.ExecutionCause {
			t.Fatal("intent overrode independent outcome", got, err)
		}
	}
}

// Real API, native main + sidecar + descendant; fake time only drives expiry.
// The owned supervisor must stop without any further queue connection.
func TestWorkerWallTimeWholeTreeOffline(t *testing.T) {
	q, _ := fixture(t)
	clock := &manualWorkerClock{now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	q.clock = clock
	m := machine("worker", "fake")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
	s := spec("experiment")
	s.Resumable = false
	s.WallTime = "2s"
	s.Needs.MemoryGB = 1
	s.Needs.HostMemoryGB = 2
	s.Run.MemoryGB = .5
	s.Run.Command = []string{"sh", "-c", `trap '' TERM; sleep 300 & echo $! > child.pid; touch main; wait`}
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", MemoryGB: .5, Command: []string{"sh", "-c", `trap '' TERM; touch helper; while :; do sleep .1; done`}}}
	submitted := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("no assignment", job(t, q, submitted.ID).Reason)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	w := Worker{Interval: time.Millisecond, Clock: clock, Machine: m, StateDir: newTestStateRoot(t), Client: Client{URL: server.URL, Token: "test"}, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	w.memoryCeilings = fakeCeilings(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = j.Session
	p, err := startAfterFixtureInputs(t, w, store, *j)
	if err != nil || p == nil {
		t.Fatal(p, err)
	}
	defer p.stop(clock.Now(), 0, true)
	nativeFile(t, p.job.Spec.Run.Workdir, "main")
	nativeFile(t, p.job.Spec.Run.Workdir, "helper")
	server.Close() // No consumer polling/watchdog or queue is available.
	child, err := os.ReadFile(filepath.Join(p.job.Spec.Run.Workdir, "child.pid"))
	if err != nil || len(child) == 0 {
		t.Fatal("descendant missing", err)
	}
	w.control = &processControl{clock: clock}
	ctx, cancel := context.WithCancel(context.Background())
	a := &attemptSupervisor{store: store, job: *j, worker: w, recovering: p, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go a.run(ctx)
	defer func() {
		p.stopService(clock.Now(), 0)
		clock.Advance(StopGrace)
		p.enforceGrace(clock.Now())
		cancel()
		<-a.done
	}()
	clock.Advance(2 * time.Second)
	await(t, func() bool {
		receipt, err := readWorkerWallTime(store.dir, *j)
		return err == nil && receipt != nil && receipt.Expired
	})
	clock.Advance(StopGrace + time.Second)
	await(t, func() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.released })
	cancel()
	<-a.done
	r := *store.state.Result
	r, err = applyWorkerWallTime(store.dir, *j, r)
	if err != nil || r.ReleasePending || r.Success || r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Class != wallTimeLimit {
		t.Fatal("whole tree not released with typed cause", r, err)
	}
}
