package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// #166: a generic attempt identity is allocated before Docker launch. A RAM
// refusal has a result but no Docker custody; a lost custody file has neither.
func TestDockerMissingStateWorkerRecovery(t *testing.T) {
	for _, refused := range []bool{true, false} {
		name := "lost-custody"
		if refused {
			name = "pre-launch-refusal"
		}
		t.Run(name, func(t *testing.T) {
			q, _ := fixture(t)
			q.clock = &workerFakeClock{now: q.clock.Now()}
			var requests atomic.Int64
			handler := API{Queue: q, Token: "test"}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); handler.ServeHTTP(w, r) }))
			defer server.Close()
			w := recoveryWorker(t)
			w.Client = Client{URL: server.URL, Token: "test"}
			w.Clock = q.clock
			w.Machine.HostID = "recovery-host"
			w.EnvironmentLimit = 1 << 20
			root, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			root.state.Session = "saved-session"
			j := dockerJob()
			j.Session = root.state.Session
			s, err := newAttemptStore(root, j)
			if err != nil {
				t.Fatal(err)
			}
			if s.state.Identity != "" {
				t.Fatal("unlaunched Docker attempt has a custody identity")
			}
			s.state.Identity = "legacy-attempt" // Existing s022 records predate the identity ordering fix.
			if refused {
				cause := s.startFailure(j, launchRefused, errWorkerMemory)
				r := startFailureOutcome(Result{Machine: w.Machine.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, cause)
				s.state.Result = &r
				if err := s.save(); err != nil {
					t.Fatal(err)
				}
			}
			if err := root.save(); err != nil {
				t.Fatal(err)
			}
			root.close()
			// First prove the attempt-local outcome and idempotent restart behavior.
			for range 2 {
				if p, err := w.recoverAttempt(s); err != nil || p != nil {
					t.Fatalf("recovery: process=%v error=%v", p, err)
				}
				r := s.state.Result
				if r == nil || !r.Interrupted || r.Success || r.ReleasePending == refused || r.ID != j.ID {
					t.Fatalf("lost attempt outcome: %+v", r)
				}
				if refused && (r.ExecutionCause == nil || r.ExecutionCause.Class != "resource_limit") {
					t.Fatalf("launch refusal replaced: %+v", r)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(testWaitTimeout):
					t.Error("worker did not stop")
				}
			}()
			if !refused {
				await(t, func() bool { q.clock.(*workerFakeClock).Advance(time.Millisecond); return requests.Load() >= 4 })
				select {
				case err := <-done:
					done <- err
					t.Fatal("worker exited on missing custody", err)
				default:
				}
				return // Unconfirmed release remains excluded; other workers stay available.
			}
			await(t, func() bool {
				q.clock.(*workerFakeClock).Advance(time.Millisecond)
				b, err := os.ReadFile(filepath.Join(s.dir, s.stateName))
				var state workerState
				return err == nil && json.Unmarshal(b, &state) == nil && state.Job == nil
			})
			native := spec("experiment")
			native.Resumable = false
			native.Run.Command = []string{"sh", "-c", "echo still-running"}
			next := submit(t, q, native)
			await(t, func() bool {
				q.clock.(*workerFakeClock).Advance(time.Millisecond)
				return job(t, q, next.ID).State == "succeeded"
			})
			if got := job(t, q, next.ID); got.Attempt != 1 {
				t.Fatalf("unrelated attempt changed: %+v", got)
			}
		})
	}
}
