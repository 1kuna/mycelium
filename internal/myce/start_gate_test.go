package myce

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProductionStartGateSingleDurableWinner(t *testing.T) {
	for _, boundary := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "exactly"}[boundary], func(t *testing.T) {
			q, c := fixture(t)
			submit(t, q, spec("production"))
			a := check(t, q, machine("b70", "xpu"), nil).Job
			c.Advance(ProductionStartTimeout - time.Second)
			if boundary {
				c.Advance(time.Second)
			}
			var wg sync.WaitGroup
			accepted := make(chan bool, 2)
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); accepted <- q.Started(result(a, true)) == nil }()
			}
			wg.Wait()
			close(accepted)
			wins := 0
			for yes := range accepted {
				if yes {
					wins++
				}
			}
			want := 1
			if boundary {
				want = 0
			}
			if wins != want {
				t.Fatalf("accepted %d starts, want %d", wins, want)
			}
			path := strings.TrimSuffix(q.lock.Name(), ".lock")
			if e := q.Close(); e != nil {
				t.Fatal(e)
			}
			reopened, e := Open(path, q.root, c)
			if e != nil {
				t.Fatal(e)
			}
			defer reopened.Close()
			got := job(t, reopened, a.ID)
			if boundary {
				if got.State != "failed" || got.StartedAt != nil {
					t.Fatalf("boundary: %+v", got)
				}
			} else {
				if got.StartedAt == nil || got.State != "running" {
					t.Fatalf("lost durable start: %+v", got)
				}
				if e := reopened.Started(result(a, true)); e == nil {
					t.Fatal("restarted queue accepted duplicate start")
				}
			}
		})
	}
}

func TestProductionStartGateRecoveryWindow(t *testing.T) {
	for _, outage := range []time.Duration{10 * time.Second, 151 * time.Second} {
		t.Run(outage.String(), func(t *testing.T) {
			q, c := fixture(t)
			s := spec("production")
			deadline := c.Now().Add(30 * time.Second)
			s.StartBy = &deadline
			submit(t, q, s)
			a := check(t, q, machine("b70", "xpu"), nil).Job
			if e := q.Started(result(a, true)); e != nil {
				t.Fatal(e)
			}
			c.Advance(10 * time.Minute) // A long run must not consume the recovery window.
			lostAt := c.Now()
			r := result(a, false)
			r.Interrupted = true
			if e := q.Complete(r); e != nil {
				t.Fatal(e)
			}
			got := job(t, q, a.ID)
			if got.State != "queued" || !got.Spec.StartBy.Equal(lostAt.Add(30*time.Second)) {
				t.Fatalf("no fresh original-length window: %+v", got)
			}
			c.Advance(outage)
			b := check(t, q, machine("b70", "xpu"), nil).Job
			if outage > 30*time.Second {
				got = job(t, q, a.ID)
				if b != nil || got.State != "failed" || !strings.Contains(got.Reason, "production start deadline exceeded") {
					t.Fatalf("long outage: %+v", got)
				}
			} else {
				if b == nil || b.Attempt <= a.Attempt {
					t.Fatal("brief loss did not resume")
				}
				if e := q.Started(result(b, true)); e != nil {
					t.Fatal(e)
				}
				// A second loss gets the original 30s window, not elapsed time since submission.
				c.Advance(time.Minute)
				lostAgain := c.Now()
				r = result(b, false)
				r.Interrupted = true
				if e := q.Complete(r); e != nil {
					t.Fatal(e)
				}
				if got = job(t, q, a.ID); !got.Spec.StartBy.Equal(lostAgain.Add(30 * time.Second)) {
					t.Fatalf("window grew: %+v", got)
				}
			}
		})
	}
}

func TestProductionStartGateUnreachableDoesNotExecute(t *testing.T) {
	q, c := fixture(t)
	marker := filepath.Join(t.TempDir(), "ran")
	s := spec("production")
	s.Resumable = false
	s.Run.Command = []string{"/bin/sh", "-c", "echo ran > " + marker}
	submit(t, q, s)
	a := check(t, q, machine("b70", "xpu"), nil).Job
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	server.Close()
	w := Worker{StateDir: newTestStateRoot(t), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	store, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer store.close()
	store.state.Session = a.Session
	p, e := w.start(store, *a)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case f := <-p.done:
		if f.result.Success || !strings.Contains(f.result.Error, "hand-off") {
			t.Fatalf("result: %+v", f.result)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("worker stalled")
	}
	if _, e = os.Stat(marker); !os.IsNotExist(e) {
		t.Fatalf("payload ran or marker inaccessible: %v", e)
	}
	c.Advance(ProductionStartTimeout)
	requireState(t, q, a.ID, "failed")
}

func TestProductionStartGateWorkerRestartReconciles(t *testing.T) {
	q, _ := fixture(t)
	marker := filepath.Join(t.TempDir(), "ran")
	s := spec("production")
	s.Run.Command = []string{"/bin/sh", "-c", "echo ran >> " + marker}
	submit(t, q, s)
	a := check(t, q, machine("b70", "xpu"), nil).Job
	if e := q.Started(result(a, true)); e != nil {
		t.Fatal(e)
	}
	w := recoveryWorker(t)
	w.Machine = machine("b70", "xpu")
	w.InventoryProbe = benchmarkTestInventory
	w.CPUIsolationProbe = func(context.Context) error { return nil }
	w.Clock = RealClock{}
	store, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	store.state.Session = a.Session
	store.state.Job = a
	if e = store.save(); e != nil {
		t.Fatal(e)
	}
	store.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reconciled Result
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/complete" {
			if !decode(rw, r, &reconciled) {
				return
			}
			cancel()
			respond(rw, map[string]bool{"ok": true}, nil)
			return
		}
		if r.URL.Path == "/api/workers/checkin" {
			respond(rw, Reply{AttemptProtocol: keyedAttemptProtocol, CheckpointRoot: q.root}, nil)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
		respond(rw, map[string]bool{}, nil)
	}))
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("restart stalled")
	}
	if !reconciled.Interrupted || reconciled.ID != a.ID || reconciled.Attempt != a.Attempt {
		t.Fatalf("not reconciled: %+v", reconciled)
	}
	if _, e = os.Stat(marker); !os.IsNotExist(e) {
		t.Fatalf("re-executed: %v", e)
	}
}

func TestProductionStartGateExecutionAndReplay(t *testing.T) {
	q, c := fixture(t)
	marker := filepath.Join(t.TempDir(), "executions")
	s := spec("production")
	s.Resumable = false
	s.Run.Command = []string{"/bin/sh", "-c", "echo ran >> " + marker}
	submit(t, q, s)
	a := check(t, q, machine("b70", "xpu", "test-host"), nil).Job
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	calls := 0
	var latency time.Duration
	handler := API{Queue: q, Token: "test"}.Handler()
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/start-permit" {
			t.Error("extra preparation round trip")
		}
		before := time.Now()
		handler.ServeHTTP(rw, r)
		if r.URL.Path == "/api/workers/started" {
			mu.Lock()
			calls++
			latency = time.Since(before)
			mu.Unlock()
		}
		if r.URL.Path == "/api/workers/complete" {
			cancel()
		}
	}))
	defer server.Close()
	w := recoveryWorker(t)
	w.Machine = machine("b70", "xpu", "test-host")
	w.InventoryProbe = benchmarkTestInventory
	w.CPUIsolationProbe = func(context.Context) error { return nil }
	w.Client = Client{URL: server.URL, Token: "test"}
	store, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	store.state.Session = a.Session
	p, e := w.start(store, *a)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case f := <-p.done:
		if !f.result.Success || f.err != nil {
			t.Fatalf("finish: %+v", f)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("start stalled")
	}
	// Completion is still offline in the durable spool; the queue knows the start.
	c.Advance(ProductionStartTimeout)
	requireState(t, q, a.ID, "running")
	store.close()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("replay stalled")
	}
	requireState(t, q, a.ID, "succeeded")
	data, e := os.ReadFile(marker)
	if e != nil || string(data) != "ran\n" {
		t.Fatalf("execution count: %q, %v", data, e)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("start round trips: %d", calls)
	}
	t.Logf("single localhost durable start request: %s", latency)
}

func TestProductionStartGateDetectedLoss(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "unstarted", true: "started"}[started], func(t *testing.T) {
			q, c := fixture(t)
			if e := q.SetLossTimeouts(5*time.Second, 20*time.Second, 60*time.Second); e != nil {
				t.Fatal(e)
			}
			s := spec("production")
			s.AllowedMachines = []string{"b70", "retry"}
			j := submit(t, q, s)
			m := machine("b70", "xpu")
			a := check(t, q, m, nil).Job
			check(t, q, machine("witness", "fake"), nil)
			if started {
				if e := q.Started(result(a, true)); e != nil {
					t.Fatal(e)
				}
			}
			c.Advance(5 * time.Second)
			check(t, q, machine("witness", "fake"), nil)
			got := job(t, q, j.ID)
			if got.State != "queued" || got.Losses != 1 {
				t.Fatalf("loss not detected: %+v", got)
			}
			want := *j.Spec.StartBy
			if started {
				want = c.Now().Add(ProductionStartTimeout)
			}
			if !got.Spec.StartBy.Equal(want) {
				t.Fatalf("loss window: %s, want %s", got.Spec.StartBy, want)
			}
			c.Advance(time.Second)
			// Loss leaves the original GPU slot in custody in both arms.
			resumed := check(t, q, machine("retry", "xpu"), nil).Job
			if resumed == nil || resumed.Attempt != a.Attempt+1 {
				t.Fatal("lost worker did not resume")
			}
			if e := q.Started(result(resumed, true)); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestProductionStartGateStorageFailureDoesNotExecute(t *testing.T) {
	q, c := fixture(t)
	marker := filepath.Join(t.TempDir(), "ran")
	s := spec("production")
	s.Resumable = false
	s.Run.Command = []string{"/bin/sh", "-c", "echo ran > " + marker}
	submit(t, q, s)
	a := check(t, q, machine("b70", "xpu"), nil).Job
	if _, e := q.db.Exec("PRAGMA query_only=ON"); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{StateDir: newTestStateRoot(t), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	store, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer store.close()
	store.state.Session = a.Session
	p, e := w.start(store, *a)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case f := <-p.done:
		if f.result.Success {
			t.Fatal("storage refusal succeeded")
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("storage refusal stalled")
	}
	if _, e = os.Stat(marker); !os.IsNotExist(e) {
		t.Fatalf("payload ran: %v", e)
	}
	if _, e = q.db.Exec("PRAGMA query_only=OFF"); e != nil {
		t.Fatal(e)
	}
	if got := job(t, q, a.ID); got.StartedAt != nil {
		t.Fatal("failed write retained start")
	}
}
