package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// #225: exercise the production worker's complete gate/exec receipt path
// against the HTTP Started handler delayed behind the real queue mutex.
func TestWorkerStartHandoffQueueWait(t *testing.T) {
	for _, state := range []string{"current", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			s := spec("production")
			s.Resumable = false
			s.Run.Command = []string{"sh", "-c", `printf x >> "$MARKER"`}
			s.Run.Env = map[string]string{"MARKER": "{work}/executions"}
			submit(t, q, s)
			a := check(t, q, m, nil).Job
			handler := (API{Queue: q, Token: "test"}).Handler()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/workers/started" && calls.Add(1) == 1 {
					q.mu.Lock()
					// Real I/O and mutex waits cannot use synctest time.
					select {
					case <-time.After(3300 * time.Millisecond):
						clock.Advance(3300 * time.Millisecond)
					case <-r.Context().Done():
					}
					q.mu.Unlock()
					if state == "cancelled" {
						if err := q.Cancel(a.ID); err != nil {
							t.Error(err)
						}
					}
				}
				handler.ServeHTTP(rw, r)
			}))
			defer server.Close()
			w := Worker{StateDir: newTestStateRoot(t), Machine: m, Clock: clock, Client: Client{URL: server.URL, Token: "test"}}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = a.Session
			p, err := w.start(store, *a)
			if err != nil || p == nil {
				t.Fatalf("delayed %s start: process=%v err=%v", state, p, err)
			}
			select {
			case f := <-p.done:
				if f.result.Success != (state == "current") {
					t.Fatalf("delayed %s execution: %+v", state, f.result)
				}
			case <-time.After(testWaitTimeout):
				p.stop(clock.Now(), 0, true)
				t.Fatal("delayed start did not finish")
			}
			got := job(t, q, a.ID)
			if calls.Load() != 1 || got.Starts != 1 || got.Losses != 0 || (got.StartedAt != nil) != (state == "current") {
				t.Fatalf("handoff duplicated or escaped its fence: calls=%d job=%+v", calls.Load(), got)
			}
			data, err := os.ReadFile(filepath.Join(p.job.Spec.Run.Workdir, "executions"))
			if state == "current" && (err != nil || string(data) != "x") || state == "cancelled" && !os.IsNotExist(err) {
				t.Fatalf("wrong payload execution count: state=%s marker=%q err=%v", state, data, err)
			}
			// An old session cannot obtain an additional positive hand-off.
			if err := w.authorizeStart(*a, a.Session+"-old"); err == nil {
				t.Fatal("stale session authorized")
			}
		})
	}
}

// Synthetic time proves both client deadlines admit the measured 3.54s lock
// wait without retrying. The test above also exercises real HTTP and exec I/O.
func TestWorkerStartControlQueueWaitClock(t *testing.T) {
	for _, boundary := range []string{"launch", "started"} {
		t.Run(boundary, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				w := Worker{Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					if r.URL.Path != "/api/workers/"+boundary {
						t.Fatalf("wrong boundary: %s", r.URL.Path)
					}
					select {
					case <-time.After(3540 * time.Millisecond):
						rw := httptest.NewRecorder()
						fmt.Fprintf(rw, `{"ok":true,"start_gate":%q}`, startGateProtocol)
						return rw.Result(), nil
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				})}}}
				start := time.Now()
				var err error
				if boundary == "launch" {
					err = w.authorizeLaunch(HostLaunch{})
				} else {
					err = w.authorizeStart(Job{}, "session")
				}
				if err != nil || calls != 1 || time.Since(start) != 3540*time.Millisecond {
					t.Fatalf("queue wait rejected or retried: calls=%d elapsed=%v err=%v", calls, time.Since(start), err)
				}
			})
		})
	}
}

func TestWorkerStartHandoffUnreachableBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := Worker{Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()
			return nil, r.Context().Err()
		})}}}
		start := time.Now()
		err := w.authorizeStart(Job{}, "session")
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errWorkerStartHandoff) || time.Since(start) != 10*time.Second {
			t.Fatalf("unreachable hand-off lost its bound or diagnostic: elapsed=%v err=%v", time.Since(start), err)
		}
	})
}

func TestWorkerStartHandoffTimeoutHistory(t *testing.T) {
	for _, tc := range []struct{ path, operation string }{
		{"/api/workers/launch", "start_authorization"},
		{"/api/workers/started", "start_handoff"},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			submit(t, q, spec("production"))
			a := check(t, q, m, nil).Job
			w := Worker{Machine: m, Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != tc.path {
					t.Errorf("wrong gate: got %s, want %s", r.URL.Path, tc.path)
				}
				return nil, context.DeadlineExceeded
			})}}}
			var err error
			if tc.path == "/api/workers/started" {
				err = w.authorizeStart(*a, m.Session)
			} else {
				err = w.authorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: a.ID, Attempt: "1"})
			}
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errWorkerStartAuthorization) {
				t.Fatal("authorization family or transport cause lost", err)
			}
			facts := memberOutcome{Infrastructure: []infrastructureFact{{Kind: launchRefused, At: clock.Now()}}}
			// Both early and running-group adapters must retain the exact boundary.
			early := startFailureOutcome(result(a, false), &recordedStartError{cause: err, facts: facts})
			cause := runtimeDiagnostic(runtimeErrorFor("command", err))
			if early.ExecutionCause == nil || early.ExecutionCause.Operation != tc.operation || cause.Operation != tc.operation || cause.Class != "timeout" {
				t.Errorf("gates indistinguishable: early=%+v group=%+v", early.ExecutionCause, cause)
			}
			r := classifyAttemptOutcome(result(a, false), attemptCause{memberOutcome: facts})
			r.ExecutionCause = cause
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			got, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != "queued" || got.Losses != 1 || got.InfrastructureRetries != 1 || len(got.AttemptHistory) != 1 {
				t.Fatal("retry policy changed", got)
			}
			h := got.AttemptHistory[0]
			if h.ExecutionCause == nil || h.ExecutionCause.Operation != tc.operation || h.ExecutionCause.Class != "timeout" || h.Reason != fmt.Sprintf("execution interrupted: %s: timeout", tc.operation) {
				t.Errorf("wrong gate in public history: %+v", h)
			}
		})
	}
}
