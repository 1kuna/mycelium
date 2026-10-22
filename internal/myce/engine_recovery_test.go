package myce

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"
	"time"
)

func recoveryReport(t *testing.T, q *Queue, m Machine, s *ModelSlot, state string) Reply {
	t.Helper()
	r, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: s.Model, Attempt: s.Attempt, State: state, RuntimeFailed: state == "failed" || state == "unloading", CallsMeasured: true}}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestEngineRecoveryCrashLoopClockAndFreshAttempt(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(fmt.Sprintf("pinned=%v", pinned), func(t *testing.T) {
			q, clock, m := modelFixture(t)
			q.models.Wait = time.Hour
			if pinned {
				if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			}
			pending := addModelCall(t, q, "experiment")
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			recoveryReport(t, q, m, s, "ready")
			if pending.Machine != m.ID || pending.Attempt == "" {
				t.Fatal("no attempt-bound dispatch")
			}
			// The first transport failure excludes only this attempt on this worker.
			if err := q.retryCall(pending.ID); err != nil {
				t.Fatal(err)
			}
			for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
				old := s.Attempt
				recoveryReport(t, q, m, s, "unloading")
				if s.State == "ready" || s.CrashCount != i+1 {
					t.Fatalf("death invisible: %+v", s)
				}
				recoveryReport(t, q, m, s, "failed")
				if s.State != "failed" || s.RetryAt == nil || s.RetryDelay != delay || s.RetryAt.Sub(clock.Now()) != delay {
					t.Fatalf("backoff: %+v", s)
				}
				reserved := 0.0
				if pinned {
					reserved = q.models.Models[0].MemoryGB
				}
				if got := q.modelMemory(m.ID); got != reserved {
					t.Fatalf("incorrect planning reservation after cleanup: %v", got)
				}
				if pending.Machine != "" {
					t.Fatal("routed into dead engine")
				}
				snap, err := q.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				for _, job := range snap.Jobs {
					if job.ID == pending.ID && !strings.Contains(job.Reason, "engine restarting on "+m.ID+", retry at ") {
						t.Fatalf("missing restart reason: %s", job.Reason)
					}
				}
				clock.Advance(delay - time.Nanosecond)
				recoveryReport(t, q, m, s, "failed")
				if s.State != "failed" {
					t.Fatal("early restart")
				}
				clock.Advance(time.Nanosecond)
				recoveryReport(t, q, m, s, "failed")
				if s.State != "loading" || s.Attempt == old {
					t.Fatalf("no fresh load: %+v", s)
				}
				stale, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: old, State: "ready"}}})
				if err != nil {
					t.Fatal(err)
				}
				if s.State != "loading" || stale.ModelPlan[0].Attempt != s.Attempt {
					t.Fatal("stale readiness accepted")
				}
				recoveryReport(t, q, m, s, "ready")
				if s.CrashCount != i+1 || s.RetryDelay != delay || pending.Machine != m.ID || pending.Attempt != s.Attempt {
					t.Fatalf("fresh attempt cannot satisfy retry: %+v %+v", s, pending)
				}
				if err := q.retryCall(pending.ID); err != nil {
					t.Fatal(err)
				}
			}
			// One intermittent failure resets continuity even if readiness is retained.
			clock.Advance(59 * time.Second)
			_, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "ready", HealthRevision: 1, HealthDegraded: true}}})
			if err != nil {
				t.Fatal(err)
			}
			if s.CrashCount == 0 {
				t.Fatal("reset before continuously healthy")
			}
			clock.Advance(time.Second)
			_, err = q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "ready", HealthRevision: 1}}})
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(59 * time.Second)
			_, err = q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "ready", HealthRevision: 1}}})
			if err != nil {
				t.Fatal(err)
			}
			if s.CrashCount == 0 {
				t.Fatal("reset before 60 seconds")
			}
			clock.Advance(time.Second)
			_, err = q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "ready", HealthRevision: 1}}})
			if err != nil {
				t.Fatal(err)
			}
			if s.CrashCount != 0 || s.RetryDelay != 0 {
				t.Fatalf("healthy reset missing: %+v", s)
			}
			recoveryReport(t, q, m, s, "failed")
			if s.RetryDelay != time.Second || s.CrashCount != 1 {
				t.Fatal("reset did not restart backoff at one second")
			}
		})
	}
}

func TestEngineRecoveryRealProcessBothLogs(t *testing.T) {
	for _, tc := range []struct {
		policy string
		pinned bool
		loss   string
	}{{"", false, "kill"}, {"none", true, "exit"}, {"", true, "health"}} {
		t.Run(fmt.Sprintf("logs=%s/pinned=%v/%s", tc.policy, tc.pinned, tc.loss), func(t *testing.T) {
			policy, pinned, loss := tc.policy, tc.pinned, tc.loss
			q, clock := fixture(t)
			model := serverTestModel(t, "ready")
			weights := []byte("synthetic immutable weights")
			model.Path = filepath.Join(t.TempDir(), "weights")
			if err := os.WriteFile(model.Path, weights, 0600); err != nil {
				t.Fatal(err)
			}
			model.SizeBytes = int64(len(weights))
			model.SHA256 = fmt.Sprintf("%x", sha256.Sum256(weights))
			model.Logs = policy
			model.Device = ""
			model.DeviceEnv = nil
			config := ModelConfig{Models: []Model{model}, Idle: time.Minute, Wait: time.Minute}
			if err := q.ConfigureModels(config); err != nil {
				t.Fatal(err)
			}
			m := machine("engine-worker", "fake")
			m.Capabilities = []string{"logs_none"}
			m.ModelConfigHash = modelConfigHash(config)
			rt, err := startModels(Worker{Machine: m, ModelConfig: config, Clock: RealClock{}, Client: Client{Token: "test"}, MemorySource: func(context.Context) (float64, error) { return 32, nil }}, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := rt.close(); err != nil {
					t.Error(err)
				}
			}()
			m.ModelURL = rt.url
			heartbeat := func() Reply {
				t.Helper()
				reports, _ := rt.report()
				reply, err := q.Checkin(Checkin{Machine: m, ModelReports: reports})
				if err != nil {
					t.Fatal(err)
				}
				rt.sync(reply.ModelPlan)
				return reply
			}
			heartbeat()
			if pinned {
				if err := q.Pin(ModelPin{Model: model.Name, Project: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			}
			call := &ModelCall{Model: model.Name, Project: "test", Priority: "experiment"}
			if err := q.addCall(call); err != nil {
				t.Fatal(err)
			}
			heartbeat()
			await(t, func() bool { reports, _ := rt.report(); return len(reports) == 1 && reports[0].State == "ready" })
			heartbeat()
			if err := q.finishCall(call.ID); err != nil {
				t.Fatal(err)
			}
			rt.mu.Lock()
			engine := rt.models[model.Name].engine.(*LlamaEngine)
			rt.mu.Unlock()
			old := q.s.ModelSlots[slotKey(m.ID, model.Name)].Attempt
			switch loss {
			case "kill":
				if err := syscall.Kill(-engine.cmd.Process.Pid, syscall.SIGKILL); err != nil {
					t.Fatal(err)
				}
			case "exit":
				res, _ := engine.CallRequest(context.Background(), "POST", "/exit", nil, nil)
				if res != nil {
					res.Body.Close()
				}
			case "health":
				res, err := engine.CallRequest(context.Background(), "POST", "/fail-health", nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				res.Body.Close()
				await(t, func() bool { err, _, _ := engine.runtimeHealth(); return err != nil })
				if err := syscall.Kill(engine.cmd.Process.Pid, 0); err != nil {
					t.Fatal("health fixture process exited before withdrawal")
				}
			}
			if loss != "health" {
				select {
				case <-engine.exited:
				case <-time.After(testWaitTimeout):
					t.Fatal("Wait not observed")
				}
			}

			heartbeat()
			if s := q.s.ModelSlots[slotKey(m.ID, model.Name)]; s != nil && s.State == "ready" {
				t.Fatal("ready survived death heartbeat")
			}
			await(t, func() bool { reports, used := rt.report(); return reports[0].State == "failed" && used == 0 })
			heartbeat()
			s := q.s.ModelSlots[slotKey(m.ID, model.Name)]
			if !pinned {
				if s != nil || q.modelMemory(m.ID) != 0 {
					t.Fatal("idle failure kept queue reservation")
				}
				return
			}
			if s.State != "failed" || s.RetryAt == nil {
				t.Fatal("pinned retry not pending")
			}
			queued := &ModelCall{Model: model.Name, Project: "test", Priority: "experiment"}
			if err := q.addCall(queued); err != nil {
				t.Fatal(err)
			}
			if queued.Machine != "" {
				t.Fatal("queued call routed to dead slot")
			}
			clock.Advance(time.Second)
			heartbeat()
			await(t, func() bool { reports, _ := rt.report(); return reports[0].State == "ready" })
			heartbeat()
			if s.Attempt == old || queued.Machine != m.ID || s.State != "ready" {
				t.Fatalf("pin did not reload: %+v %+v", s, queued)
			}
			if err := q.finishCall(queued.ID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEngineRecoveryHealthThresholdAndTimeout(t *testing.T) {
	for _, code := range []int{503, 302, 0} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var probes atomic.Int32
			permits := make(chan bool, 8)
			health := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				probes.Add(1)
				select {
				case healthy := <-permits:
					if healthy {
						return
					}
				case <-r.Context().Done():
					return
				}
				if code == 0 {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(code)
			}))
			defer health.Close()
			clock := &manualWorkerClock{now: time.Unix(0, 0)}
			e := &LlamaEngine{exited: make(chan struct{}), Clock: clock}
			e.startHealthMonitor(health.URL)
			defer func() { e.monitorCancel(); <-e.monitorDone }()
			// Two failures, then a success: failure count must start over.
			for _, healthy := range []bool{false, false, true, false, false} {
				_, revision, _ := e.runtimeHealth()
				before := probes.Load()
				permits <- healthy
				await(t, func() bool {
					_, rev, degraded := e.runtimeHealth()
					return probes.Load() > before && (healthy && !degraded || !healthy && rev > revision)
				})
				if err, _, _ := e.runtimeHealth(); err != nil {
					t.Fatalf("failed before third consecutive failure: %v", err)
				}
			}
			clock.Advance(serverHealthWindow)
			permits <- false
			await(t, func() bool { err, _, _ := e.runtimeHealth(); return err != nil })
		})
	}
}

func TestEngineRecoveryCleanupFailureKeepsCustody(t *testing.T) {
	q, _, m := modelFixture(t)
	call := addModelCall(t, q, "experiment")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	recoveryReport(t, q, m, s, "ready")
	e := &lifecycleEngine{unloadErr: errors.New("synthetic cleanup failure")}
	wm := &workerModel{model: Model{Name: "test", MemoryGB: 8}, engine: e, state: "ready", runtimeFailed: true, attempt: s.Attempt}
	released := false
	wm.releaseEnvironment = func() { released = true }
	rt := &modelRuntime{models: map[string]*workerModel{"test": wm}}
	rt.unload(wm)
	rt.wg.Wait()
	reports, used := rt.report()
	if used != 8 || reports[0].State != "unload_failed" || released {
		t.Fatal("cleanup failure lost custody")
	}
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: reports}); err != nil {
		t.Fatal(err)
	}
	if s.State != "unload_failed" || s.RetryAt != nil || q.modelMemory(m.ID) != 8 {
		t.Fatal("queue released failed cleanup")
	}
	if err := q.finishCall(call.ID); err != nil {
		t.Fatal(err)
	}
}

func TestEngineRecoveryInitialLoadFailureStillFailsCall(t *testing.T) {
	q, _, m := modelFixture(t)
	call := addModelCall(t, q, "experiment")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	_, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "failed", Error: "synthetic verification failure"}}})
	if err != nil {
		t.Fatal(err)
	}
	if call.Error != "model load failed" || call.Machine != "" || q.modelMemory(m.ID) != 0 {
		t.Fatalf("initial verification failure became a silent retry wait: %+v", call)
	}
}

// Synthetic worker transport keeps the request blocked until heartbeat withdraws
// readiness, independently of the engine's own TCP shutdown behavior.
func TestEngineRecoveryInFlightReplaySafety(t *testing.T) {
	for _, generic := range []bool{false, true} {
		for _, retrySafe := range []bool{false, true} {
			t.Run(fmt.Sprintf("generic=%v/safe=%v", generic, retrySafe), func(t *testing.T) {
				q, _, m := modelFixture(t)
				q.models.Models[0].RetrySafe = retrySafe
				m.ModelConfigHash = modelConfigHash(q.models)
				started := make(chan struct{}, 2)
				var hits atomic.Int32
				worker := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.Copy(io.Discard, r.Body)
					if hits.Add(1) == 1 {
						started <- struct{}{}
						<-r.Context().Done()
						return
					}
					fmt.Fprint(w, "recovered")
				}))
				defer worker.Close()
				m.ModelURL = worker.URL
				check(t, q, m, nil)
				warm := addModelCall(t, q, "experiment")
				s := q.s.ModelSlots[slotKey(m.ID, "test")]
				recoveryReport(t, q, m, s, "ready")
				q.finishCall(warm.ID)
				ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
				defer cancel()
				path, body := "/models/test/work", "synthetic"
				if !generic {
					path, body = "/v1/chat/completions", `{"model":"test"}`
				}
				req := httptest.NewRequest("POST", path, strings.NewReader(body)).WithContext(ctx)
				req.Header.Set("Authorization", "Bearer test")
				req.Header.Set("X-Myce-Project", "test")
				req.Header.Set("X-Myce-Priority", "experiment")
				result := httptest.NewRecorder()
				done := make(chan struct{})
				go func() { API{Queue: q, Token: "test"}.Handler().ServeHTTP(result, req); close(done) }()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("request never started")
				}
				recoveryReport(t, q, m, s, "failed")
				if retrySafe {
					// Drive fake time only while the handler is stopped at a queue lock, keeping
					// this Clock fixture free of concurrent reads/writes.
					q.mu.Lock()
					q.s.ModelSlots[slotKey(m.ID, "test")] = s
					s.RetryAt = nil
					s.State = "loading"
					s.Attempt = ID()
					q.mu.Unlock()
					recoveryReport(t, q, m, s, "ready")
				}
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("dead attempt did not release client")
				}
				if retrySafe {
					if result.Code != 200 || result.Body.String() != "recovered" || hits.Load() != 2 {
						t.Fatalf("safe retry: %d %s hits=%d", result.Code, result.Body.String(), hits.Load())
					}
				} else if result.Code != 503 || hits.Load() != 1 {
					t.Fatalf("unsafe replay: %d hits=%d", result.Code, hits.Load())
				}
			})
		}
	}
}

func TestEngineRecoveryWaitBoundAndStatus(t *testing.T) {
	q, clock, m := modelFixture(t)
	warm := addModelCall(t, q, "experiment")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	recoveryReport(t, q, m, s, "ready")
	q.finishCall(warm.ID)
	if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	recoveryReport(t, q, m, s, "failed")
	req := httptest.NewRequest("POST", "/models/test/work", strings.NewReader("synthetic"))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Project", "test")
	req.Header.Set("X-Myce-Priority", "experiment")
	ctx, cancel := context.WithTimeout(req.Context(), testWaitTimeout)
	defer cancel()
	req = req.WithContext(ctx)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { API{Queue: q, Token: "test"}.Handler().ServeHTTP(recorder, req); close(done) }()
	await(t, func() bool {
		snap, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, job := range snap.Jobs {
			if strings.Contains(job.Reason, "engine restarting on ") {
				return true
			}
		}
		return false
	})
	q.mu.Lock()
	clock.Advance(q.models.Wait)
	q.s.Machines[m.ID].LastSeen = clock.Now()
	q.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("max_wait did not bound recovery")
	}
	if recorder.Code != 504 || !strings.Contains(recorder.Body.String(), "maximum model wait exceeded") {
		t.Fatalf("%d %s", recorder.Code, recorder.Body.String())
	}
}

func TestEngineRecoveryRejectsLateCallAndCompletion(t *testing.T) {
	rt := &modelRuntime{models: map[string]*workerModel{"test": {model: Model{Name: "test"}, engine: &FakeEngine{Model: "test"}, state: "ready", attempt: "new"}}, session: "session", token: "test"}
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Session", "session")
	req.Header.Set("X-Myce-Engine-Attempt", "old")
	recorder := httptest.NewRecorder()
	rt.serve(recorder, req)
	if recorder.Code != 503 || recorder.Header().Get("X-Myce-Engine-Failed") != "" || recorder.Header().Get("X-Myce-Engine-Rejected") != "old" {
		t.Fatal("late call reached replacement engine")
	}
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
}

func TestEngineRecoveryPinReservesBackoffMemory(t *testing.T) {
	q, clock, m := modelFixture(t)
	if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	recoveryReport(t, q, m, s, "ready")
	recoveryReport(t, q, m, s, "failed")
	borrowed := submit(t, q, spec("experiment"))
	running := check(t, q, m, nil).Job
	if running != nil || q.s.Jobs[borrowed.ID].State != "queued" || !s.RetryReserved {
		t.Fatal("batch borrowed pinned crash-backoff reservation")
	}
	clock.Advance(time.Second)
	check(t, q, m, nil)
	if s.State != "loading" || s.CrashCount != 1 || q.s.Jobs[borrowed.ID].State != "queued" {
		t.Fatalf("pin recovery lost priority/history: %+v", s)
	}
}

// The engine's TCP error may arrive before Wait completes. The authenticated
// worker receipt must still prevent an unsafe OpenAI transport replay.
type recoveryTransportErrorEngine struct {
	FakeEngine
	calls       atomic.Int32
	bodyFailure bool
	bodyPrefix  string
}

func (e *recoveryTransportErrorEngine) Call(context.Context, string, []byte) (*http.Response, error) {
	e.calls.Add(1)
	if e.bodyFailure {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(io.MultiReader(strings.NewReader(e.bodyPrefix), iotest.ErrReader(io.ErrUnexpectedEOF)))}, nil
	}
	return nil, errors.New("synthetic engine transport failure")
}
func (e *recoveryTransportErrorEngine) StreamRequest(ctx context.Context, _ string, path string, _ io.Reader, _ int64, _ http.Header) (*http.Response, error) {
	return e.Call(ctx, path, nil)
}

func TestEngineRecoveryUnsafeTransportBeforeWait(t *testing.T) {
	for _, tc := range []struct{ generic, bodyFailure, stream bool }{{false, false, false}, {true, false, false}, {false, true, false}, {false, true, true}} {
		t.Run(fmt.Sprintf("generic=%v/bodyFailure=%v/stream=%v", tc.generic, tc.bodyFailure, tc.stream), func(t *testing.T) {
			generic := tc.generic
			q, _, m := modelFixture(t)
			warm := addModelCall(t, q, "experiment")
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			recoveryReport(t, q, m, s, "ready")
			q.finishCall(warm.ID)
			engine := &recoveryTransportErrorEngine{bodyFailure: tc.bodyFailure}
			if !tc.stream {
				engine.bodyPrefix = `{"partial":`
			}
			rt := &modelRuntime{models: map[string]*workerModel{"test": {model: q.models.Models[0], engine: engine, state: "ready", attempt: s.Attempt}}, session: m.Session, token: "test"}
			worker := workerTestServer(http.HandlerFunc(rt.serve))
			defer worker.Close()
			m.ModelURL = worker.URL
			check(t, q, m, nil)
			var replays atomic.Int32
			otherWorker := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { replays.Add(1); fmt.Fprint(w, "unexpected replay") }))
			defer otherWorker.Close()
			other := machine("two", "fake")
			other.ModelURL = otherWorker.URL
			other.ModelConfigHash = m.ModelConfigHash
			check(t, q, other, nil)
			q.s.ModelSlots[slotKey(other.ID, "test")] = &ModelSlot{Model: "test", Machine: other.ID, Session: other.Session, State: "ready", Priority: "experiment", LastUsed: q.clock.Now(), Attempt: ID()}
			path, body := "/models/test/work", "synthetic"
			if !generic {
				path, body = "/v1/chat/completions", fmt.Sprintf(`{"model":"test","stream":%v}`, tc.stream)
			}
			ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
			defer cancel()
			req := httptest.NewRequest("POST", path, strings.NewReader(body)).WithContext(ctx)
			req.Header.Set("Authorization", "Bearer test")
			req.Header.Set("X-Myce-Project", "test")
			req.Header.Set("X-Myce-Priority", "experiment")
			recorder := httptest.NewRecorder()
			API{Queue: q, Token: "test"}.Handler().ServeHTTP(recorder, req)
			if recorder.Code != 503 || engine.calls.Load() != 1 || replays.Load() != 0 || recorder.Header().Get("X-Myce-Engine-Failed") != "" {
				t.Fatalf("unsafe replay or leaked control receipt: %d calls=%d replays=%d", recorder.Code, engine.calls.Load(), replays.Load())
			}
		})
	}
}
