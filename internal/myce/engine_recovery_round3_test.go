package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Keep A's cleanup blocked to prove B cannot start just because its plan exists.
type replacementEngine struct {
	FakeEngine
	entered, release chan struct{}
	err              error
	loads            atomic.Int32
}

func (e *replacementEngine) Load(ctx context.Context, m Model) error {
	e.loads.Add(1)
	return e.FakeEngine.Load(ctx, m)
}
func (e *replacementEngine) Unload() error { close(e.entered); <-e.release; return e.err }

func TestEngineRecoveryReplacementWaitsForCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			old := &replacementEngine{entered: make(chan struct{}), release: make(chan struct{})}
			if fail {
				old.err = errors.New("synthetic cleanup failure")
			}
			next := &lifecycleEngine{}
			oldModel := &workerModel{model: Model{Name: "test", MemoryGB: 8}, engine: old, state: "ready", attempt: "A", inflight: 1}
			rt := &modelRuntime{models: map[string]*workerModel{"test": oldModel}, session: "session", config: ModelConfig{Wait: time.Minute, Models: []Model{{Name: "test", Engine: "fake", MemoryGB: 8}}}, memorySource: func(context.Context) (float64, error) { return 32, nil }, engineFactory: func(Model) Engine { return next }}
			plan := []ModelSlot{{Model: "test", Session: "session", Attempt: "B", State: "loading", InFlight: 1}}
			rt.sync(plan)
			rt.mu.Lock()
			if oldModel.state != "draining" || oldModel.queueFlight != 0 {
				t.Fatal("replacement did not drain old admitted work")
			}
			oldModel.inflight = 0
			rt.mu.Unlock()
			rt.sync(plan)
			select {
			case <-old.entered:
			case <-time.After(testWaitTimeout):
				t.Fatal("cleanup never started")
			}
			rt.sync(plan)
			rt.mu.Lock()
			if rt.models["test"] != oldModel || oldModel.state != "unloading" {
				t.Fatal("replacement bypassed cleanup")
			}
			rt.mu.Unlock()
			close(old.release)
			rt.wg.Wait()
			rt.sync(plan)
			rt.wg.Wait()
			reports, _ := rt.report()
			if fail {
				if reports[0].Attempt != "B" || reports[0].State != "unload_failed" || rt.models["test"] != oldModel {
					t.Fatalf("lost old cleanup custody: %+v", reports)
				}
			} else if reports[0].Attempt != "B" || reports[0].State != "ready" || rt.models["test"] == oldModel {
				t.Fatalf("replacement did not load: %+v", reports)
			}
		})
	}

	t.Run("RuntimeFailedLoadCleanupRetainsMemory", func(t *testing.T) {

		e := &lifecycleEngine{loadErr: errors.New("load failed"), unloadErr: errors.New("process survived")}
		rt, m, ctx := runtimeLoadFixture(t, e)
		released := false
		m.releaseEnvironment = func() { released = true }
		rt.load(ctx, m)
		reports, used := rt.report()
		if m.state != "unload_failed" || used != 8 || reports[0].Error != "model load or unload failed" {
			t.Fatalf("failed cleanup released reservation: %+v memory=%v", reports, used)
		}
		if used != 8 || reports[0].State != "unload_failed" || released {
			t.Fatal("cleanup failure lost custody")
		}
		q, _, machine := modelFixture(t)
		call := addModelCall(t, q, "experiment")
		slot := q.s.ModelSlots[slotKey(machine.ID, "test")]
		m.attempt = slot.Attempt
		reports, _ = rt.report()
		if _, err := q.Checkin(Checkin{Machine: machine, ModelReports: reports}); err != nil {
			t.Fatal(err)
		}
		if slot.State != "unload_failed" || slot.RetryAt != nil || q.modelMemory(machine.ID) != 8 {
			t.Fatal("queue released failed cleanup")
		}
		if err := q.finishCall(call.ID); err != nil {
			t.Fatal(err)
		}

	})
}

// Copy/environment preparation can outlast process readiness. These worker
// receipts represent a cold load still preparing weights before engine launch.
func TestEngineRecoveryLoadDeadlineCoversCopy(t *testing.T) {
	for _, pinned := range []bool{false} {
		t.Run(fmt.Sprintf("pinned=%v", pinned), func(t *testing.T) {
			q, clock, m := modelFixture(t)
			config := q.models
			config.Wait = 10 * time.Minute
			config.Models[0].ReadyTimeout = time.Minute
			if err := q.ConfigureModels(config); err != nil {
				t.Fatal(err)
			}
			m.ModelConfigHash = modelConfigHash(q.models)
			check(t, q, m, nil)
			if pinned {
				if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			}
			call := addModelCall(t, q, "experiment")
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			started := *s.RunSince
			attempt := s.Attempt
			clock.Advance(120 * time.Second)
			recoveryReport(t, q, m, s, "loading")
			if s.State != "loading" || s.LoadTimedOut || s.CrashCount != 0 || s.Attempt != attempt || call.Error != "" {
				t.Fatalf("pre-launch preparation crossed readiness timeout: %+v %+v", s, call)
			}
			if s.LoadUntil != nil || !s.RunSince.Equal(started) {
				t.Fatal("queue reintroduced a load wall deadline or moved the load start")
			}
			recoveryReport(t, q, m, s, "ready")
			if s.State != "ready" || call.Machine != m.ID || s.LoadUntil != nil {
				t.Fatal("cold load did not become routable")
			}
			if err := q.finishCall(call.ID); err != nil {
				t.Fatal(err)
			}
		})
	}

	t.Run("EngineRecoveryLoadingDeadlineClock", func(t *testing.T) {

		q, clock, m := modelFixture(t)
		call := addModelCall(t, q, "experiment")
		slot := q.s.ModelSlots[slotKey(m.ID, "test")]
		// A legacy persisted deadline is not evidence that a current worker has
		// stalled. Only the worker's exact-attempt failure can settle that load.
		until := clock.Now().Add(time.Second)
		slot.LoadUntil = &until
		clock.Advance(600 * time.Second)
		recoveryReport(t, q, m, slot, "loading")
		if slot.State != "loading" || slot.LoadUntil != nil || call.Error != "" {
			t.Fatal("queue killed a live load by wall clock")
		}
		clock.Advance(q.models.Wait)
		if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: slot.Attempt, State: "failed"}}}); err != nil {
			t.Fatal(err)
		}
		if call.Error != "model load failed" {
			t.Fatal("worker stall failure did not fail demand")
		}
		if call.Error != "model load failed" {
			t.Fatal("deadline masked a non-runtime load failure")
		}
		if call.Error != "model load failed" || call.Machine != "" || q.modelMemory(m.ID) != 0 {
			t.Fatalf("initial verification failure became a silent retry wait: %+v", call)
		}

	})
}

func TestEngineRecoveryWarmupFailureStopsUnpinnedRecovery(t *testing.T) {
	q, clock, m := modelFixture(t)
	call := addModelCall(t, q, "experiment")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	recoveryReport(t, q, m, s, "ready")
	if err := q.retryCall(call.ID); err != nil {
		t.Fatal(err)
	}
	recoveryReport(t, q, m, s, "failed")
	clock.Advance(time.Second)
	recoveryReport(t, q, m, s, "failed")
	if s.State != "loading" {
		t.Fatal("runtime recovery never started")
	}
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "failed"}}}); err != nil {
		t.Fatal(err)
	}
	if call.Error != "model load failed" || q.s.ModelSlots[slotKey(m.ID, "test")] != nil {
		t.Fatal("failed warm-up kept retrying unpinned demand")
	}
	clock.Advance(30 * time.Second)
	check(t, q, m, nil)
	if q.s.ModelSlots[slotKey(m.ID, "test")] != nil {
		t.Fatal("unrequested warm-up retried")
	}

	t.Run("EngineRecoveryRepeatedInitialLoadFailure", func(t *testing.T) {

		q, clock, m := modelFixture(t)
		q.models.Wait = time.Hour
		if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
			t.Fatal(err)
		}
		s := q.s.ModelSlots[slotKey(m.ID, "test")]
		first := addModelCall(t, q, "experiment")
		if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "failed", Error: "verification failed"}}}); err != nil {
			t.Fatal(err)
		}
		if first.Error == "" {
			t.Fatal("first call not failed")
		}
		clock.Advance(time.Second)
		check(t, q, m, nil)
		if s.State != "loading" {
			t.Fatal("pin did not retry warm-up")
		}
		second := addModelCall(t, q, "experiment")
		if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "failed", Error: "verification failed"}}}); err != nil {
			t.Fatal(err)
		}
		check(t, q, m, nil)
		if second.Error == "" {
			t.Fatalf("second call to persistently-failing pinned model not failed promptly; slot state=%s crash=%d", s.State, s.CrashCount)
		}

	})
}

func TestEngineRecoveryRejectedBeforeForwarding(t *testing.T) {
	for _, state := range []string{"stale", "exited"} {
		t.Run(state, func(t *testing.T) {
			engine := &recoveryTransportErrorEngine{}
			wm := &workerModel{model: Model{Name: "test"}, engine: engine, state: "ready", attempt: "A"}
			rt := &modelRuntime{models: map[string]*workerModel{"test": wm}, session: "session", token: "test"}
			switch state {
			case "missing":
				delete(rt.models, "test")
			case "stale":
				wm.attempt = "B"
			case "failed":
				wm.runtimeFailed = true
			case "exited", "health":
				owned := &LlamaEngine{exited: make(chan struct{})}
				if state == "exited" {
					close(owned.exited)
				} else {
					owned.healthErr = errors.New("server health failed")
				}
				wm.engine = owned
			}
			req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
			req.Header.Set("Authorization", "Bearer test")
			req.Header.Set("X-Myce-Session", "session")
			req.Header.Set("X-Myce-Engine-Attempt", "A")
			result := httptest.NewRecorder()
			rt.serve(result, req)
			rt.wg.Wait()
			if result.Code != 503 || result.Header().Get("X-Myce-Engine-Failed") != "" || result.Header().Get("X-Myce-Engine-Rejected") != "A" || engine.calls.Load() != 0 || wm.inflight != 0 {
				t.Fatalf("rejected call counted as executed: %d %+v", result.Code, result.Header())
			}
		})
	}

	t.Run("EngineRecoveryRejectsLateCallAndCompletion", func(t *testing.T) {

		q, _, m := modelFixture(t)
		old := addModelCall(t, q, "experiment")
		s := q.s.ModelSlots[slotKey(m.ID, "test")]
		recoveryReport(t, q, m, s, "ready")
		s.Attempt = ID()
		s.InFlight = 2
		if err := q.finishCall(old.ID); err != nil {
			t.Fatal(err)
		}
		if s.InFlight != 2 {
			t.Fatal("late completion consumed replacement capacity")
		}
	})
}

func TestEngineRecoveryRejectedUnsafeCallRetries(t *testing.T) {
	for _, generic := range []bool{false} {
		t.Run(fmt.Sprint(generic), func(t *testing.T) {
			for _, receipt := range []bool{true, false} {
				t.Run(fmt.Sprintf("receipt=%t", receipt), func(t *testing.T) {
					q, _, m := modelFixture(t)
					warm := addModelCall(t, q, "experiment")
					s := q.s.ModelSlots[slotKey(m.ID, "test")]
					recoveryReport(t, q, m, s, "ready")
					q.finishCall(warm.ID)
					var forwards atomic.Int32
					rejected := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						io.Copy(io.Discard, r.Body)
						if !receipt {
							// Withdraw before headers and wait for the proxy's cancellation.
							// Without the rejection receipt an unsafe call cannot be retried.
							recoveryReport(t, q, m, s, "failed")
							<-r.Context().Done()
							return
						}
						w.Header().Set("X-Myce-Engine-Rejected", r.Header.Get("X-Myce-Engine-Attempt"))
						w.WriteHeader(503)
					}))
					defer rejected.Close()
					previous := modelHTTPClient
					local := *previous
					transport := local.Transport
					if transport == nil {
						transport = http.DefaultTransport
					}
					local.Transport = clientTestTransport(func(r *http.Request) (*http.Response, error) {
						res, err := transport.RoundTrip(r)
						if receipt && err == nil && res.Header.Get("X-Myce-Engine-Rejected") != "" {
							// Observe the real HTTP receipt before withdrawing the attempt,
							// but withdraw before the proxy checks liveness. Racing these
							// events in the handler sometimes canceled away the only proof
							// that retrying this unsafe call was permitted (#384).
							recoveryReport(t, q, m, s, "failed")
						}
						return res, err
					})
					modelHTTPClient = &local
					defer func() { modelHTTPClient = previous }()
					other := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwards.Add(1); fmt.Fprint(w, "accepted") }))
					defer other.Close()
					m.ModelURL = rejected.URL
					check(t, q, m, nil)
					n := machine("two", "fake")
					n.ModelURL = other.URL
					n.ModelConfigHash = m.ModelConfigHash
					check(t, q, n, nil)
					q.s.ModelSlots[slotKey(n.ID, "test")] = &ModelSlot{Model: "test", Machine: n.ID, Session: n.Session, State: "ready", Attempt: "other", LastUsed: q.clock.Now()}
					path, body := "/models/test/work", "synthetic"
					if !generic {
						path, body = "/v1/chat/completions", `{"model":"test"}`
					}
					req := httptest.NewRequest("POST", path, strings.NewReader(body))
					req.Header.Set("Authorization", "Bearer test")
					req.Header.Set("X-Myce-Project", "test")
					req.Header.Set("X-Myce-Priority", "experiment")
					ctx, cancel := context.WithTimeout(req.Context(), testWaitTimeout)
					defer cancel()
					result := httptest.NewRecorder()
					API{Queue: q, Token: "test"}.Handler().ServeHTTP(result, req.WithContext(ctx))
					if !receipt {
						if result.Code != 503 || forwards.Load() != 0 {
							t.Fatalf("unsafe call retried without rejection proof: %d %s; forwards=%d", result.Code, result.Body.String(), forwards.Load())
						}
						return
					}
					if result.Code != 200 || result.Body.String() != "accepted" || forwards.Load() != 1 || result.Header().Get("X-Myce-Engine-Rejected") != "" {
						t.Fatalf("unsafe never-forwarded call lost retry: %d %s", result.Code, result.Body.String())
					}
				})
			}
		})
	}
}

type canceledWarmupEngine struct {
	FakeEngine
	started chan struct{}
	cleaned atomic.Int32
}

func (e *canceledWarmupEngine) Load(ctx context.Context, _ Model) error {
	close(e.started)
	<-ctx.Done()
	return ctx.Err()
}
func (e *canceledWarmupEngine) Unload() error { e.cleaned.Add(1); return nil }

func TestEngineRecoveryDeadlineCancelsWarmup(t *testing.T) {
	e := &canceledWarmupEngine{started: make(chan struct{})}
	rt := &modelRuntime{models: map[string]*workerModel{}, session: "session", config: ModelConfig{Wait: time.Minute, Models: []Model{{Name: "test", Engine: "fake", MemoryGB: 8}}}, memorySource: func(context.Context) (float64, error) { return 32, nil }, engineFactory: func(Model) Engine { return e }}
	plan := []ModelSlot{{Model: "test", Session: "session", Attempt: "B", State: "loading"}}
	rt.sync(plan)
	select {
	case <-e.started:
	case <-time.After(testWaitTimeout):
		t.Fatal("warm-up never started")
	}
	plan[0].State = "unloading"
	plan[0].LoadTimedOut = true
	rt.sync(plan)
	rt.wg.Wait()
	reports, used := rt.report()
	if reports[0].State != "failed" || reports[0].RuntimeFailed || used != 0 || e.cleaned.Load() != 1 {
		t.Fatalf("deadline cancellation lost cleanup/load-failure receipt: %+v", reports)
	}

	t.Run("RuntimeLoadReadinessRacingWithDrain", func(t *testing.T) {

		e := &lifecycleEngine{started: make(chan struct{}), release: make(chan struct{})}
		rt, m, ctx := runtimeLoadFixture(t, e)
		rt.wg.Add(1)
		go func() { defer rt.wg.Done(); rt.load(ctx, m) }()
		<-e.started
		rt.sync([]ModelSlot{{Attempt: "test-engine", Model: "test", Session: "session", State: "draining"}})
		close(e.release)
		rt.wg.Wait()
		if m.state != "unloaded" || e.unloads.Load() != 1 {
			t.Fatalf("cancelled load exposed model instead of unloading: %s unloads=%d", m.state, e.unloads.Load())
		}

	})
}
