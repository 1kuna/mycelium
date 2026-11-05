package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// The same policy must work on installations with very different resource counts,
// retained history, service latency and client rates. Counts/times are fixtures,
// never controller parameters.
func adaptiveObservationFixture(t *testing.T, hosts, jobs int) (*Queue, *fakeClock, *observationLimit) {
	t.Helper()
	q, clock := fixture(t)
	for i := range hosts {
		id := fmt.Sprintf("host-%d", i)
		q.s.Machines[id] = &Machine{ID: id}
	}
	for i := range jobs {
		id := fmt.Sprintf("job-%08d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "queued", Spec: spec("experiment")}
	}
	for _, class := range []string{"experiment", "deadline", "production"} {
		id := "selected-" + class
		q.s.Jobs[id] = &Job{ID: id, State: "queued", Spec: spec(class)}
	}
	return q, clock, newObservationLimit(clock, q)
}
func observationAdmit(l *observationLimit, path string) (func(), *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", path, nil)
	done, ok := l.admit(w, r)
	if ok {
		w.WriteHeader(200)
	}
	return done, w
}
func observationPressure(l *observationLimit, clock *fakeClock, latency time.Duration) func() {
	// A real uncontended occupied interval establishes this queue's baseline.
	l.pressure.acquired(nil)
	clock.Advance(latency)
	l.pressure.release()
	wait := l.pressure.wait()
	clock.Advance(latency + time.Nanosecond)
	return func() { l.pressure.acquired(wait); l.pressure.release() }
}
func TestObservationLimitHealthyAtAnyRate(t *testing.T) {
	for _, size := range []struct{ hosts, jobs, reads int }{{1, 4, 500}, {200, 6500, 10000}} {
		t.Run(fmt.Sprint(size.hosts), func(t *testing.T) {
			q, clock, l := adaptiveObservationFixture(t, size.hosts, size.jobs)
			for i := range size.reads {
				r := httptest.NewRequest("GET", "/api/jobs/selected-experiment", nil)
				r.RemoteAddr = fmt.Sprintf("client-%d:1234", i) // exceeds former identity ceiling
				w := httptest.NewRecorder()
				done, ok := l.admit(w, r)
				if !ok {
					t.Fatal("healthy queue refused", i, w.Code)
				}
				done()
			}
			// Real HTTP, one peer, no elapsed refill time and no first-party User-Agent.
			handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
			for range 200 {
				w := httptest.NewRecorder()
				r := httptest.NewRequest("GET", "/api/jobs/selected-experiment?view=wait", nil)
				r.Header.Set("Authorization", "Bearer fixture")
				handler.ServeHTTP(w, r)
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
			}
		})
	}
}
func TestObservationLimitCostClassPressureAndRecovery(t *testing.T) {
	for _, size := range []struct {
		hosts, jobs int
		latency     time.Duration
	}{{2, 4, time.Microsecond}, {200, 6500, 50 * time.Millisecond}} {
		t.Run(fmt.Sprint(size.hosts), func(t *testing.T) {
			_, clock, l := adaptiveObservationFixture(t, size.hosts, size.jobs)
			// Learn different costs from this installation, not a route-specific fixed cap.
			cheapPath, expensivePath := "/api/status", "/api/inspection"
			if size.hosts > 2 {
				cheapPath, expensivePath = expensivePath, cheapPath
			}
			for _, tc := range []struct {
				path string
				cost time.Duration
			}{{cheapPath, size.latency}, {expensivePath, size.latency * 3}} {
				done, w := observationAdmit(l, tc.path)
				if w.Code != 200 {
					t.Fatal(w.Code)
				}
				clock.Advance(tc.cost)
				done()
			}
			expensive, w := observationAdmit(l, expensivePath)
			if w.Code != 200 {
				t.Fatal(w.Code)
			}
			defer expensive()
			recover := observationPressure(l, clock, size.latency)
			reject := func(path string) {
				t.Helper()
				done, w := observationAdmit(l, path)
				if done != nil {
					done()
				}
				var api map[string]any
				_ = json.Unmarshal(w.Body.Bytes(), &api)
				if w.Code != 429 || api["code"] != "client_rate_limited" || w.Header().Get("Retry-After") == "" {
					t.Fatal(path, w.Code, api, w.Header())
				}
			}
			admit := func(path string) func() {
				t.Helper()
				done, w := observationAdmit(l, path)
				if w.Code != 200 {
					t.Fatal(path, w.Code, w.Body.String())
				}
				return done
			}
			reject(expensivePath)
			cheap := admit(cheapPath)
			defer cheap()
			reject(cheapPath)
			reject(expensivePath)
			detail := admit("/api/jobs/selected-experiment")
			defer detail()
			reject("/api/jobs/selected-experiment")
			scalar := admit("/api/jobs/selected-experiment?view=wait")
			defer scalar()
			reject("/api/jobs/selected-experiment?view=wait")
			deadline := admit("/api/jobs/selected-deadline")
			defer deadline()
			reject("/api/jobs/selected-experiment?view=wait")
			for range 200 {
				admit("/api/jobs/selected-production")()
			}
			for _, tc := range []struct{ method, path string }{{"POST", "/api/wait"}, {"POST", "/api/workers/checkin"}, {"POST", "/api/workers/launch"}, {"POST", "/api/workers/started"}, {"GET", "/api/workers/worker/events"}, {"GET", "/api/files/blob"}, {"GET", "/api/instructions"}, {"GET", "/api/environments/env/reference"}} {
				done, ok := l.admit(httptest.NewRecorder(), httptest.NewRequest(tc.method, tc.path, nil))
				if !ok {
					t.Fatal("dependency shed", tc.path)
				}
				done()
			}
			recover()
			// Recovery is immediate, even with prior reads in flight; no refill/cooldown.
			for range 200 {
				admit(expensivePath)()
			}
		})
	}
}

func TestObservationLimitProductionHTTPAndPolicy(t *testing.T) {
	q, clock, l := adaptiveObservationFixture(t, 2, 4)
	handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
	busy := holdObservationHTTP(t, handler, "/api/jobs/selected-deadline?view=wait")
	defer busy()
	recover := observationPressure(l, clock, time.Millisecond)
	// A real deadline observer is in flight and the control mutex is pressured.
	// Another experiment must be shed while production keeps working.
	r := httptest.NewRequest("GET", "/api/jobs/selected-experiment", nil)
	r.Header.Set("Authorization", "Bearer fixture")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 429 {
		t.Fatal("pressure not connected to HTTP admission", w.Code)
	}

	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		// Even the exemption lookup and legacy pending HTTP read work while q.mu is
		// occupied. Full/HEAD/subresource admission never needs that lock either.
		for _, method := range []string{"GET", "HEAD"} {
			for _, path := range []string{"/api/jobs/selected-production", "/api/jobs/selected-production/logs", "/api/jobs/selected-production/ledger"} {
				done, ok := l.admit(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
				if !ok {
					t.Fatal("production shed", method, path)
				}
				done()
			}
		}

		for _, id := range []string{"selected-production", "selected-prod"} {
			r := httptest.NewRequest("GET", "/api/jobs/"+id+"?view=wait", nil)
			r.Header.Set("Authorization", "Bearer fixture")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
		}
	}()

	recover()
	// Class matching follows the committed policy, not UA or a caller header.
	policy := q.PriorityPolicy()
	policy.Policy.Tiers[0].Selectors = []string{"*/deadline"}
	if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
		t.Fatal(err)
	}
	_, production := l.index.observationPriority("selected-deadline")
	if !production {
		t.Fatal("policy tier not reflected")
	}
}

// Each operation is a 200-waiter wave. The healthy scalar wait path is measured
// separately by BenchmarkWaitIndex200; here the expensive reads must all be shed
// before reaching the handler or queue lock once measured pressure is present.
func BenchmarkObservationLimitFlood200(b *testing.B) {
	for _, size := range []struct {
		hosts, jobs int
		latency     time.Duration
	}{{2, 4, time.Microsecond}, {200, 6500, 50 * time.Millisecond}} {
		b.Run(fmt.Sprint(size.hosts), func(b *testing.B) {
			clock := &workerFakeClock{now: time.Unix(1, 0)}
			q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock, WithCheckpointAvailability(func() bool { return true }))
			if err != nil {
				b.Fatal(err)
			}
			defer q.Close()
			for i := range size.hosts {
				id := fmt.Sprintf("host-%d", i)
				q.s.Machines[id] = &Machine{ID: id}
			}
			for i := range size.jobs {
				id := fmt.Sprintf("job-%08d", i)
				q.s.Jobs[id] = &Job{ID: id, State: "queued", Spec: spec("experiment")}
			}
			handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
			active := holdObservationHTTP(b, handler, "/api/jobs/job-00000000?view=wait")
			defer active()
			q.mu.Lock()
			clock.Advance(size.latency)
			q.mu.Unlock()
			// A real control acquisition queued behind the occupied queue mutex. The
			// in-flight HTTP read and all 200 incoming requests use the actual API.
			q.mu.Lock()
			acquired := make(chan struct{})
			go func() { unlock := requestLock(context.Background(), &q.mu); unlock(); close(acquired) }()
			p := q.mu.pressure.Load()
			for {
				p.mu.Lock()
				pending := len(p.waiting)
				p.mu.Unlock()
				if pending > 0 {
					break
				}
				runtime.Gosched()
			}
			clock.Advance(size.latency + time.Nanosecond)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var wg sync.WaitGroup
				for range 200 {
					wg.Go(func() {
						r := httptest.NewRequest("GET", "/api/inspection", nil)
						r.Header.Set("Authorization", "Bearer fixture")
						w := httptest.NewRecorder()
						handler.ServeHTTP(w, r)
						if w.Code != http.StatusTooManyRequests {
							b.Error("flood reached expensive handler", w.Code)
						}
					})
				}
				wg.Wait()
			}
			b.StopTimer()
			p.mu.Lock()
			pending := len(p.waiting)
			p.mu.Unlock()
			q.mu.Unlock()
			<-acquired
			if pending != 1 {
				b.Fatal("flood added queue work", pending)
			}
			b.ReportMetric(0, "added-lock-waits/200")
			b.ReportMetric(200, "shed/200")
		})
	}
}

// Hold only response delivery, outside q.mu, so the HTTP limiter has a real
// observation in flight while the test supplies deterministic lock pressure.
type observationBlockedResponse struct {
	*httptest.ResponseRecorder
	entered, release chan struct{}
	once             sync.Once
}

func (w *observationBlockedResponse) block() { w.once.Do(func() { close(w.entered); <-w.release }) }
func (w *observationBlockedResponse) WriteHeader(code int) {
	w.block()
	w.ResponseRecorder.WriteHeader(code)
}
func (w *observationBlockedResponse) Write(data []byte) (int, error) {
	w.block()
	return w.ResponseRecorder.Write(data)
}
func holdObservationHTTP(t testing.TB, handler http.Handler, path string) func() {
	t.Helper()
	w := &observationBlockedResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer fixture")
	r.Header.Set("User-Agent", "myce/s073")
	done := make(chan struct{})
	go func() { defer close(done); handler.ServeHTTP(w, r) }()
	select {
	case <-w.entered:
	case <-time.After(testWaitTimeout):
		t.Fatal("observation did not enter response")
	}
	return func() { close(w.release); <-done }
}
