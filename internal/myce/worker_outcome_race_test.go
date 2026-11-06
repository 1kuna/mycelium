package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// A late start refusal and a delivered-stop receipt can touch the same outcome.
// Exercise the real gated command path without any host-observation changes.
func TestWorkerOutcomeRefusedLaunchConcurrentStopReceipt(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("outcome-worker", "outcome-worker", "cpu", 2, 8)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	deadline := clock.Now().Add(time.Minute)
	s.StartBy = &deadline // Authorize only after the supervisor is gated and tracked.
	s.Run.Command = []string{"sh", "-c", "printf ran > ran"}
	submit(t, q, s)
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("missing assignment")
	}
	control := &processControl{clock: clock, wake: make(chan struct{}, 1)}
	stop, joined := make(chan struct{}), make(chan struct{})
	var once sync.Once
	handler := API{q, "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workers/started" {
			handler.ServeHTTP(rw, r)
			return
		}
		control.mu.Lock()
		p := control.processes[jobKey(*a)]
		control.mu.Unlock()
		if p == nil {
			t.Error("start gate preceded process tracking")
			rw.WriteHeader(500)
			return
		}
		once.Do(func() {
			ready := make(chan struct{})
			go func() {
				defer close(joined)
				first := true
				for {
					select {
					case <-stop:
						return
					default:
					}
					p.mu.Lock()
					p.recordSignalLocked(syscall.SIGTERM)
					p.mu.Unlock()
					if first {
						close(ready)
						first = false
					}
				}
			}()
			<-ready
		})
		rw.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_stale", "error": "fixture withdraws launch"})
	}))
	defer server.Close()
	w := Worker{StateDir: newTestStateRoot(t), Machine: m, Clock: clock, Client: Client{URL: server.URL, Token: "test"}, control: control, memoryCeilings: fakeCeilings(t), HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = a.Session
	p, err := startAfterFixtureInputs(t, w, store, *a)
	close(stop)
	if p == nil {
		t.Fatalf("did not reach late refusal: %v", err)
	}
	<-joined
	defer p.stop(clock.Now(), 0, true)
	p.mu.Lock()
	facts := p.outcome
	p.mu.Unlock()
	if len(facts.Infrastructure) == 0 || len(facts.Signals) == 0 {
		t.Fatal("refusal or concurrent stop receipt lost", facts)
	}
	finished := <-p.done
	if finished.err != nil || finished.result.Success || !finished.result.Interrupted {
		t.Fatal("refusal outcome changed", finished)
	}
	if _, err := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "ran")); !os.IsNotExist(err) {
		t.Fatal("refused payload executed", err)
	}
}
