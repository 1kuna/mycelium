package myce

import (
	"context"
	"errors"
	"fmt"
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
}

func TestEngineRecoveryLoadingDeadlineClock(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(fmt.Sprint(pinned), func(t *testing.T) {
			q, clock, m := modelFixture(t)
			q.models.Models[0].ReadyTimeout = 2 * time.Second
			m.ModelConfigHash = modelConfigHash(q.models)
			check(t, q, m, nil)
			if pinned {
				if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			}
			call := addModelCall(t, q, "experiment")
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			if s.LoadUntil == nil || s.LoadUntil.Sub(clock.Now()) != 2*time.Second+engineReadySlack {
				t.Fatal("missing ready timeout plus slack")
			}
			old := s.Attempt
			clock.Advance(s.LoadUntil.Sub(clock.Now()) - time.Nanosecond)
			check(t, q, m, nil)
			if s.State != "loading" {
				t.Fatal("early loading expiry")
			}
			clock.Advance(time.Nanosecond)
			check(t, q, m, nil)
			if s.State != "unloading" || !s.LoadTimedOut || q.modelMemory(m.ID) != 8 {
				t.Fatalf("expiry credited unconfirmed memory: %+v", s)
			}
			// A late ready receipt cannot resurrect the timed-out attempt.
			recoveryReport(t, q, m, s, "ready")
			if s.State != "unloading" {
				t.Fatal("late readiness bypassed deadline")
			}
			// The worker may never have started B; acknowledge absence under B.
			rt := &modelRuntime{models: map[string]*workerModel{}, session: m.Session}
			rt.sync(q.modelPlan(m.ID))
			reports, _ := rt.report()
			if reports[0].Attempt != old || reports[0].State != "unloaded" {
				t.Fatal("missing cleanup acknowledgement for never-started attempt")
			}
			if _, err := q.Checkin(Checkin{Machine: m, ModelReports: reports}); err != nil {
				t.Fatal(err)
			}
			if s.State != "failed" || call.Error != "" || s.CrashCount != 1 {
				t.Fatalf("deadline did not enter crash recovery: %+v %+v", s, call)
			}
			clock.Advance(time.Second)
			check(t, q, m, nil)
			if s.State != "loading" || s.Attempt == old || s.LoadTimedOut {
				t.Fatal("load deadline did not recover under a fresh attempt")
			}
		})
	}
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
}

func TestEngineRecoveryHealthFailureWindowClock(t *testing.T) {
	_, clock, _ := modelFixture(t)
	if serverProbeTimeout != 5*time.Second || serverHealthWindow != 15*time.Second {
		t.Fatal("wrong probe policy")
	}
	failures := serverProbeFailures{}
	start := clock.Now()
	for i := 0; i < 3; i++ {
		if failures.observe(false, start, clock.Now()) {
			t.Fatal("fast failures withdrew readiness before 15 seconds")
		}
	}
	clock.Advance(15*time.Second - time.Nanosecond)
	if failures.observe(false, clock.Now(), clock.Now()) {
		t.Fatal("early unhealthy-window expiry")
	}
	clock.Advance(time.Nanosecond)
	if !failures.observe(false, clock.Now(), clock.Now()) {
		t.Fatal("continuous failures did not withdraw readiness")
	}
	if failures.observe(true, clock.Now(), clock.Now()) {
		t.Fatal("success remained failed")
	}
	start = clock.Now()
	clock.Advance(time.Minute)
	for i := 0; i < 2; i++ {
		if failures.observe(false, start, clock.Now()) {
			t.Fatal("window alone bypassed failure threshold")
		}
	}
	if !failures.observe(false, clock.Now(), clock.Now()) {
		t.Fatal("third failure after full window remained ready")
	}
	failures.observe(true, clock.Now(), clock.Now())
	if failures.count != 0 || !failures.since.IsZero() {
		t.Fatal("success did not reset continuity")
	}
}

func TestEngineRecoveryRejectedBeforeForwarding(t *testing.T) {
	for _, state := range []string{"missing", "stale", "failed", "exited", "health"} {
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
}

func TestEngineRecoveryRejectedUnsafeCallRetries(t *testing.T) {
	for _, generic := range []bool{false, true} {
		t.Run(fmt.Sprint(generic), func(t *testing.T) {
			q, _, m := modelFixture(t)
			warm := addModelCall(t, q, "experiment")
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			recoveryReport(t, q, m, s, "ready")
			q.finishCall(warm.ID)
			var forwards atomic.Int32
			rejected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Race the rejection receipt with the heartbeat withdrawing this attempt.
				recoveryReport(t, q, m, s, "failed")
				w.Header().Set("X-Myce-Engine-Rejected", r.Header.Get("X-Myce-Engine-Attempt"))
				w.WriteHeader(503)
			}))
			defer rejected.Close()
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwards.Add(1); fmt.Fprint(w, "accepted") }))
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
			if result.Code != 200 || result.Body.String() != "accepted" || forwards.Load() != 1 || result.Header().Get("X-Myce-Engine-Rejected") != "" {
				t.Fatalf("unsafe never-forwarded call lost retry: %d %s", result.Code, result.Body.String())
			}
		})
	}
}

func TestEngineRecoveryUnloadingHoldBlocker(t *testing.T) {
	q, _, a, _, _ := holdFleet(t, true)
	q.s.ModelSlots[slotKey(a.ID, "test")] = &ModelSlot{Model: "test", Machine: a.ID, Session: a.Session, State: "unloading", Priority: "experiment"}
	s := q.s.ModelSlots[slotKey(a.ID, "test")]
	if text, class := q.hostBlockerState("b70"); class.Kind != "production_stopping" || !strings.Contains(text, "stopping 1 attempts") {
		t.Fatalf("unloading missing from blockers: %s %+v", text, class)
	}
	q.s.Hosts["b70"].Active = true
	q.maintainHolds()
	if s.State != "unloading" {
		t.Fatal("hold rewrote cleanup to draining")
	}
}

func TestEngineRecoveryPinnedReservationRelease(t *testing.T) {
	for _, measured := range []bool{false, true} {
		t.Run(fmt.Sprint(measured), func(t *testing.T) {
			q, _, m := modelFixture(t)
			q.models.Models[0].HostMemoryGB = 2
			m.ModelConfigHash = modelConfigHash(q.models)
			m.MemoryMeasured = measured
			m.HostMemoryMeasured = true
			m.HostAvailableMemoryGB = 10
			check(t, q, m, nil)
			if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
				t.Fatal(err)
			}
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			if measured {
				m.FreeMemoryGB = 0
			}
			recoveryReport(t, q, m, s, "ready")
			if measured {
				m.FreeMemoryGB = 8
			}
			recoveryReport(t, q, m, s, "failed")
			qm := q.s.Machines[m.ID]
			if !s.RetryReserved || q.modelMemory(m.ID) != 8 || qm.FreeMemoryGB != 0 || qm.HostMemoryInUseGB != 2 {
				t.Fatalf("pin lost planning GPU/RAM: %+v %+v", s, qm)
			}
			if err := q.Unpin("test", "test"); err != nil {
				t.Fatal(err)
			}
			if q.s.ModelSlots[slotKey(m.ID, "test")] != nil || qm.FreeMemoryGB != 8 || qm.HostMemoryInUseGB != 0 {
				t.Fatal("unpin retained a released planning reservation")
			}
		})
	}
}

func TestEngineRecoveryPlaceholderAttempts(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			rt := &modelRuntime{models: map[string]*workerModel{}, machine: "worker", session: "session", config: ModelConfig{Models: []Model{{Name: "test", AllowedMachines: []string{"elsewhere"}}}}}
			if missing {
				rt.config.Models = nil
			}
			rt.sync([]ModelSlot{{Model: "test", State: "loading", Attempt: "B", Session: "session"}})
			reports, _ := rt.report()
			if len(reports) != 1 || reports[0].Name != "test" || reports[0].Attempt != "B" || reports[0].State != "failed" {
				t.Fatalf("unbound placeholder receipt: %+v", reports)
			}
		})
	}
}

func TestEngineRecoveryReservationDoesNotInventFreeMemory(t *testing.T) {
	for _, hostLow := range []bool{false, true} {
		t.Run(fmt.Sprint(hostLow), func(t *testing.T) {
			q, clock, m := modelFixture(t)
			q.models.Models[0].HostMemoryGB = 2
			m.ModelConfigHash = modelConfigHash(q.models)
			m.MemoryMeasured = true
			m.HostMemoryMeasured = true
			m.HostAvailableMemoryGB = 10
			check(t, q, m, nil)
			if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
				t.Fatal(err)
			}
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			m.FreeMemoryGB = 0
			recoveryReport(t, q, m, s, "ready")
			m.FreeMemoryGB = 2
			if hostLow {
				m.FreeMemoryGB = 8
				m.HostAvailableMemoryGB = 1
			}
			recoveryReport(t, q, m, s, "failed")
			clock.Advance(time.Second)
			check(t, q, m, nil)
			if s.State != "failed" {
				t.Fatal("planning reservation bypassed actual memory admission")
			}
			if err := q.Unpin("test", "test"); err != nil {
				t.Fatal(err)
			}
			qm := q.s.Machines[m.ID]
			if qm.FreeMemoryGB != m.FreeMemoryGB || qm.FreeHostMemoryGB != m.HostAvailableMemoryGB {
				t.Fatalf("unpin invented memory: GPU=%v host=%v", qm.FreeMemoryGB, qm.FreeHostMemoryGB)
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
	if reports[0].State != "failed" || !reports[0].RuntimeFailed || used != 0 || e.cleaned.Load() != 1 {
		t.Fatalf("deadline cancellation lost cleanup/recovery receipt: %+v", reports)
	}
}

func TestEngineRecoveryDeadlineDoesNotMaskLoadFailure(t *testing.T) {
	q, clock, m := modelFixture(t)
	call := addModelCall(t, q, "experiment")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	clock.Advance(s.LoadUntil.Sub(clock.Now()))
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "failed"}}}); err != nil {
		t.Fatal(err)
	}
	if call.Error != "model load failed" {
		t.Fatal("deadline masked a non-runtime load failure")
	}
}
