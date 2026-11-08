package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	for _, state := range []string{"current", "cancelled", "lost-reply", "cancel-after-commit"} {
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
				if r.URL.Path == "/api/workers/started" && calls.Add(1) == 1 && state != "lost-reply" && state != "cancel-after-commit" {
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
			w := Worker{control: &processControl{clock: clock}, StateDir: newTestStateRoot(t), Machine: m, Clock: clock, Client: Client{URL: server.URL, Token: "test"}}
			if state == "lost-reply" || state == "cancel-after-commit" {
				var dropped atomic.Bool
				w.Client.HTTP = &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
					response, err := http.DefaultTransport.RoundTrip(r)
					if err == nil && r.URL.Path == "/api/workers/started" && !dropped.Swap(true) {
						if state == "cancel-after-commit" {
							if err := q.Cancel(a.ID); err != nil {
								t.Error(err)
							}
							// Ordinary check-ins continue to identify this same attempt while
							// the positive Started reply is held. Control cancels before p.mu.
							if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session}}}); err != nil {
								t.Error(err)
							}
							w.control.mu.Lock()
							proc := w.control.processes[jobKey(*a)]
							w.control.mu.Unlock()
							if proc == nil {
								t.Fatal("missing exact gated execution")
							}
							proc.stopDirective(clock.Now(), 0, true, true)
							return response, nil
						}
						io.Copy(io.Discard, response.Body)
						response.Body.Close()
						return nil, io.ErrUnexpectedEOF
					}
					return response, err
				})}
				w.Client.Pause = func(context.Context, time.Duration) error { return nil }
			}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = a.Session
			p, err := startAfterFixtureInputs(t, w, store, *a)
			if err != nil || p == nil {
				t.Fatalf("delayed %s start: process=%v err=%v", state, p, err)
			}
			select {
			case f := <-p.done:
				if f.result.Success != (state == "current" || state == "lost-reply") {
					t.Fatalf("delayed %s execution: %+v", state, f.result)
				}
				if state == "cancel-after-commit" {
					if err := q.Complete(f.result); err != nil {
						t.Fatal(err)
					}
				}
			case <-time.After(testWaitTimeout):
				p.stop(clock.Now(), 0, true)
				t.Fatal("delayed start did not finish")
			}
			got := job(t, q, a.ID)
			wantCalls := int32(1)
			if state == "lost-reply" {
				wantCalls = 2
			}
			if calls.Load() != wantCalls || got.Starts != 1 || got.Losses != 0 || got.InfrastructureRetries != 0 || (got.StartedAt != nil) != (state != "cancelled") {
				t.Fatalf("handoff duplicated or escaped its fence: calls=%d job=%+v", calls.Load(), got)
			}
			data, err := os.ReadFile(filepath.Join(p.job.Spec.Run.Workdir, "executions"))
			if (state == "current" || state == "lost-reply") && (err != nil || string(data) != "x") || (state == "cancelled" || state == "cancel-after-commit") && !os.IsNotExist(err) {
				t.Fatalf("wrong payload execution count: state=%s marker=%q err=%v", state, data, err)
			}
			// An old session cannot obtain an additional positive hand-off.
			if err := w.authorizeStart(*a, a.Session+"-old"); err == nil {
				t.Fatal("stale session authorized")
			}
		})
	}
}

// Synthetic time proves all handshake paths admit a new 37s control
// wait without retrying. The test above also exercises real HTTP and exec I/O.
func TestWorkerStartControlQueueWaitClock(t *testing.T) {
	for _, boundary := range []string{"launch", "started", "native-custody"} {
		t.Run(boundary, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				w := Worker{Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					if r.URL.Path != "/api/workers/"+boundary {
						t.Fatalf("wrong boundary: %s", r.URL.Path)
					}
					select {
					case <-time.After(37 * time.Second):
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
				} else if boundary == "started" {
					err = w.authorizeStart(Job{}, "session")
				} else {
					err = w.reportNativeCustody(Job{}, "session", "execution", "custody", "reserved")
				}
				if err != nil || calls != 1 || time.Since(start) != 37*time.Second {
					t.Fatalf("queue wait rejected or retried: calls=%d elapsed=%v err=%v", calls, time.Since(start), err)
				}
			})
		})
	}
}

// #1028 replaces the old ten-second automatic refusal with owner cancellation.
func TestWorkerStartHandoffOwnerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		w := Worker{launchCtx: ctx, Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			<-r.Context().Done()
			return nil, r.Context().Err()
		})}}}
		start := time.Now()
		go func() { <-time.After(45 * time.Second); cancel() }()
		err := w.authorizeStart(Job{}, "session")
		if !errors.Is(err, context.Canceled) || !errors.Is(err, errWorkerStartHandoff) || calls != 1 || time.Since(start) != 45*time.Second {
			t.Fatalf("owner cancellation lost: calls=%d elapsed=%v err=%v", calls, time.Since(start), err)
		}
	})
}

// Fake-clock API replay: completed request RTT, rather than report cadence,
// paces a lost reply. Each phase remains the same attempt and custody identity.
func TestWorkerControlHandshakeReconciliation(t *testing.T) {
	for _, class := range []string{"experiment", "deadline", "production"} {
		t.Run(class, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			spec := spec(class)
			spec.WallTime = "10m"
			if class == "production" {
				by := clock.Now().Add(5 * time.Minute)
				spec.StartBy = &by
			}
			submit(t, q, spec)
			j := check(t, q, m, nil).Job
			handler := (API{Queue: q, Token: "test"}).Handler()
			counts := map[string]int{}
			bodies := map[string][]byte{}
			var pauses []time.Duration
			heartbeat := func() {
				_, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			w := Worker{Clock: clock, Interval: time.Second, Machine: m, controlLatency: &workerControlLatency{}, Client: Client{URL: "http://queue.invalid", Token: "test", Jitter: func() float64 { return 0 }}}
			w.Client.Pause = func(ctx context.Context, d time.Duration) error {
				pauses = append(pauses, d)
				clock.Advance(d)
				heartbeat()
				return ctx.Err()
			}
			w.Client.HTTP = &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				key := r.URL.Path
				if key == "/api/workers/native-custody" {
					var report NativeCustodyReport
					json.Unmarshal(body, &report)
					key += "/" + report.Phase
				}
				if prior, ok := bodies[key]; ok && !bytes.Equal(prior, body) {
					t.Fatal("replay changed identity", key)
				}
				bodies[key] = body
				counts[key]++
				clock.Advance(11 * time.Second)
				heartbeat()
				rw := httptest.NewRecorder()
				handler.ServeHTTP(rw, r)
				if rw.Code != 200 {
					t.Fatalf("%s refused: %d %s", key, rw.Code, rw.Body.String())
				}
				if key != "/api/workers/launch" && counts[key] == 1 {
					if key == "/api/workers/started" {
						return nil, context.DeadlineExceeded
					}
					return nil, io.ErrUnexpectedEOF
				}
				return rw.Result(), nil
			})}
			launch := HostLaunch{Machine: m.ID, Session: j.Session, Kind: "batch", Key: j.ID, Attempt: fmt.Sprint(j.Attempt)}
			if err := w.authorizeLaunch(launch); err != nil {
				t.Fatal(err)
			}
			for _, phase := range []string{"reserved", "started"} {
				if err := w.reportNativeCustody(*j, j.Session, "execution", "custody", phase); err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			if err := w.authorizeStart(*j, j.Session, dir); err != nil {
				t.Fatal(err)
			}
			before := job(t, q, j.ID)
			receipt, err := readWorkerWallTime(dir, *j)
			if err != nil || receipt == nil {
				t.Fatal(receipt, err)
			}
			// Retry wait is not request RTT and must not be subtracted twice.
			if !receipt.ExpiresAt.Equal(before.StartedAt.Add(10*time.Minute - 11*time.Second)) {
				t.Fatal("retry pacing consumed the original wall-time budget twice", receipt)
			}
			// Replay beyond original start-by must retain the committed start and budget.
			if class == "production" {
				clock.Advance(j.Spec.StartBy.Sub(clock.Now()) + time.Second)
				heartbeat()
			}
			if err := w.authorizeStart(*j, j.Session, dir); err != nil {
				t.Fatal(err)
			}
			after, err := readWorkerWallTime(dir, *j)
			if err != nil || !after.ExpiresAt.Equal(receipt.ExpiresAt) {
				t.Fatal("replay extended wall time", after, err)
			}
			if err := w.reportNativeCustody(*j, j.Session, "execution", "custody", "released"); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			ledger := map[string]int{}
			for _, e := range got.LedgerEntries {
				ledger[e.Kind]++
			}
			if got.Starts != 1 || got.Losses != 0 || got.InfrastructureRetries != 0 || len(got.AttemptHistory) != 0 || !got.StartedAt.Equal(*before.StartedAt) || ledger["started"] != 1 || ledger["custody_reserved"] != 1 || ledger["custody_started"] != 1 || ledger["custody_released"] != 1 {
				t.Fatalf("reconciliation changed execution: counts=%v ledger=%v job=%+v", counts, ledger, got)
			}
			if len(pauses) != 4 {
				t.Fatal("wrong retries", pauses)
			}
			for _, d := range pauses {
				if d < 11*time.Second {
					t.Fatal("retry ignored observed RTT", pauses)
				}
			}
		})
	}
}

func TestWorkerHandshakeRefusalAfterLostStarted(t *testing.T) {
	for _, fence := range []string{"cancel", "session", "attempt"} {
		t.Run(fence, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			s := spec("production")
			s.WallTime = "1m"
			submit(t, q, s)
			j := check(t, q, m, nil).Job
			calls := 0
			handler := (API{Queue: q, Token: "test"}).Handler()
			w := Worker{Machine: m, Clock: clock, Client: Client{URL: "http://queue.invalid", Token: "test"}}
			w.Client.Pause = func(context.Context, time.Duration) error {
				switch fence {
				case "cancel":
					if err := q.Cancel(j.ID); err != nil {
						t.Fatal(err)
					}
				case "session":
					replacement := m
					replacement.Session = "new-session"
					if _, err := q.Checkin(Checkin{Machine: replacement, Fresh: true, PreviousSession: m.Session}); err != nil {
						t.Fatal(err)
					}
				case "attempt":
					r := result(j, false)
					r.Interrupted = true
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					check(t, q, m, nil)
				}
				return nil
			}
			w.Client.HTTP = &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				rw := httptest.NewRecorder()
				handler.ServeHTTP(rw, r)
				if calls == 1 {
					if rw.Code != 200 {
						t.Fatal(rw.Code, rw.Body.String())
					}
					return nil, io.EOF
				}
				return rw.Result(), nil
			})}
			err := w.authorizeStart(*j, j.Session, t.TempDir())
			var refused *HTTPError
			if calls != 2 || !errors.As(err, &refused) || refused.Code != "request_stale" || !errors.Is(err, errWorkerStartHandoff) {
				t.Fatalf("lost typed refusal: calls=%d err=%v", calls, err)
			}
		})
	}
}

// A native member's final gate runs under p.mu. Stop must cancel its exact
// request before waiting for that mutex; the replacement has a different cancel.
func TestWorkerNativeHandshakeCancellation(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	s := spec("production")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", "touch main"}
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sh", "-c", "sleep 300"}}}
	submit(t, q, s)
	j := check(t, q, m, nil).Job
	handler := (API{Queue: q, Token: "test"}).Handler()
	entered := make(chan struct{})
	calls := 0
	w := Worker{Machine: m, Clock: clock, StateDir: newTestStateRoot(t), Client: Client{URL: "http://queue.invalid", Token: "test"}}
	w.Client.HTTP = &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/workers/launch" {
			calls++
			if calls == 3 {
				close(entered)
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
		}
		rw := httptest.NewRecorder()
		handler.ServeHTTP(rw, r)
		return rw.Result(), nil
	})}
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
	select {
	case <-entered:
	case <-time.After(testWaitTimeout):
		t.Fatal("member never reached held final gate")
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() { p.stopDirective(clock.Now(), 0, true, true); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(testWaitTimeout):
		t.Fatal("stop blocked behind handshake mutex")
	}
	select {
	case f := <-p.done:
		if f.result.ReleasePending || f.result.Success || f.err != nil {
			t.Fatal("exact native cleanup failed", f)
		}
		if err := q.Complete(f.result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("cancelled native custody did not release")
	}
	got := job(t, q, j.ID)
	if got.Losses != 0 || got.InfrastructureRetries != 0 || !got.ReleaseConfirmed {
		t.Fatal("control wait charged or retained released custody", got)
	}
	if _, err := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "main")); !os.IsNotExist(err) {
		t.Fatal("payload escaped cancellation", err)
	}
}

// Withholding an environment offer cancels the same in-flight authorization;
// its result can acquire the actor mutex after stop has released it.
func TestWorkerPreparationHandshakeCancellation(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)}
	entered := make(chan struct{})
	w := Worker{Clock: clock, Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}}}
	recipe := binaryRecipe(t, "fake")
	recipe.Launch = &HostLaunch{Kind: "environment", Key: recipe.Key, Attempt: "exact-preparation"}
	rt := &environmentRuntime{authorize: w.authorizeLaunchContext, dir: t.TempDir(), cache: &EnvironmentCache{Root: t.TempDir(), Clock: clock, Limit: 1 << 20}, reports: map[string]EnvironmentReport{}}
	rt.sync([]EnvironmentRecipe{recipe}, false)
	select {
	case <-entered:
	case <-time.After(testWaitTimeout):
		t.Fatal("preparation never entered authorization")
	}
	rt.sync(nil, false) // Holds rt.mu while cancelling, as the worker normally does.
	done := make(chan struct{})
	go func() { rt.stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(testWaitTimeout):
		t.Fatal("preparation cancellation blocked behind its actor mutex")
	}
	rt.mu.Lock()
	report := rt.reports[recipe.Key]
	rt.mu.Unlock()
	if report.State != "paused" || report.Attempt != "exact-preparation" {
		t.Fatal("cancel lost preparation identity", report)
	}
}
