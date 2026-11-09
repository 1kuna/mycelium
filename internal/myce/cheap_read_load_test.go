package myce

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Replay the incident's important distinction: a completed response is cheap
// even while a diagnostic projection and a control transaction are in flight.
// The fake clock keeps the learned response leases valid throughout the burst.
func TestCheapCachedReadsUnderProjectionPressure(t *testing.T) {
	cheapReadLoad(t, true)
}

// Run the identical fixture on the parent and candidate for latency evidence.
// Refusals are reported separately: a fast 429 is never a successful read.
func TestCheapReadLoadMeasurements(t *testing.T) {
	if os.Getenv("MYCE_CHEAP_READ_MEASURE") == "" {
		t.Skip("explicit before/after measurement")
	}
	cheapReadLoad(t, false)
}

func cheapReadLoad(t *testing.T, requireProgress bool) {
	t.Helper()
	q, clock := fixture(t)
	for i := range 300 {
		id := fmt.Sprintf("retained-%08d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now(), Spec: spec("experiment")}
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	budgetHost(q, 256<<20, 3)
	job := "retained-00000000"
	// Warm through the real HTTP handler, including projection accounting.
	pressureClock := &fakeClock{now: clock.Now()}
	handler := (API{Queue: q, Token: "test"}).handler("", pressureClock)
	request := func(path string) int {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer test")
		if path == "/api/status" {
			r.Header.Set("X-Myce-Request-Budgets", requestBudgetProtocol)
			r.Header.Set("X-Myce-Max-Response-Bytes", "67108864")
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r.WithContext(ctx))
		return w.Code
	}
	paths := []string{"/api/status", "/api/jobs/" + job}
	for _, path := range paths {
		if code := request(path); code != 200 {
			t.Fatalf("warm %s: %d", path, code)
		}
	}
	// Hold a real uncached inspection after capture, outside the live lock.
	entered, releaseProjection, projected := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	q.observeRead = func(kind string, _, _ time.Duration) {
		if kind == "status" && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-releaseProjection
		}
	}
	go func() { defer close(projected); request("/api/inspection") }()
	<-entered
	// Establish real measured pressure without advancing past the cache leases.
	recoverPressure := func() {}
	if requireProgress {
		l := newObservationLimit(pressureClock, q)
		recoverPressure = observationPressure(l, pressureClock, time.Nanosecond)
	}
	q.mu.Lock()
	var unlock sync.Once
	releaseControl := func() { unlock.Do(q.mu.Unlock) }
	defer func() {
		releaseControl()
		recoverPressure()
		close(releaseProjection)
		<-projected
	}()
	type sample struct {
		route   int
		elapsed time.Duration
		code    int
	}
	const readers, each = 16, 16
	results := make(chan sample, readers*each)
	start, done := make(chan struct{}), make(chan struct{})
	var workers sync.WaitGroup
	for i := range readers {
		workers.Add(1)
		go func(route int) {
			defer workers.Done()
			<-start
			for range each {
				started := time.Now() // observation only
				code := request(paths[route])
				results <- sample{route, time.Since(started), code}
			}
		}(i % len(paths))
	}
	go func() { workers.Wait(); close(done) }()
	close(start)
	if requireProgress {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			releaseControl()
			<-done
			t.Error("completed responses waited behind the live queue transaction")
		}
	} else {
		// A fixed wall-time control occupancy makes the parent comparison finite.
		<-time.After(50 * time.Millisecond)
		releaseControl()
		<-done
	}
	close(results)
	var durations [2][]time.Duration
	var all [2][]time.Duration
	codes := [2]map[int]int{{}, {}}
	for s := range results {
		codes[s.route][s.code]++
		all[s.route] = append(all[s.route], s.elapsed)
		if s.code == 200 {
			durations[s.route] = append(durations[s.route], s.elapsed)
		}
	}
	for route, path := range paths {
		a := all[route]
		sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
		t.Logf("%s all replies p50=%s p99=%s", path, a[len(a)/2], a[min(len(a)-1, len(a)*99/100)])
		d := durations[route]
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		if len(d) > 0 {
			t.Logf("%s codes=%v successful p50=%s p99=%s", path, codes[route], d[len(d)/2], d[min(len(d)-1, len(d)*99/100)])
		} else {
			t.Logf("%s codes=%v no successful reads", path, codes[route])
		}
		if requireProgress && codes[route][200] != readers*each/2 {
			t.Errorf("cheap reads shed under projection pressure: %s %v", path, codes[route])
		}
	}
}

// Completion waves invalidate an in-flight projection. Successive discarded
// responses do not coexist and must not exhaust a single request's allowance.
func TestCheapReadInvalidationBudget(t *testing.T) {
	for _, path := range []string{"/api/status?jobs=active", "/api/jobs/selected-queued"} {
		t.Run(path, func(t *testing.T) {
			q, clock := fixture(t)
			for i := range 30 {
				id := fmt.Sprintf("completion-%02d", i)
				q.s.Jobs[id] = &Job{ID: id, State: "queued", Queued: clock.Now(), Spec: spec("experiment")}
			}
			q.s.Jobs["selected-queued"] = &Job{ID: "selected-queued", State: "queued", Queued: clock.Now(), Spec: spec("experiment")}
			if err := q.update(func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			budgetHost(q, 8<<20, 3)
			completed := 0
			var observedCharge int64
			minRetained := int64(8 << 20)
			q.observeRead = func(_ string, _, _ time.Duration) {
				q.requestBudgets.mu.Lock()
				observedCharge = max(observedCharge, q.requestBudgets.held)
				if completed > 0 {
					minRetained = min(minRetained, q.requestBudgets.held)
				}
				q.requestBudgets.mu.Unlock()
				if completed < 20 {
					id := fmt.Sprintf("completion-%02d", completed)
					completed++
					if err := q.update(func() error {
						j := q.touchJob(id)
						j.State, j.ReleaseConfirmed = "succeeded", true
						return nil
					}); err != nil {
						t.Error(err)
					}
				}
			}
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			(API{Queue: q, Token: "test"}).Handler().ServeHTTP(w, r)
			t.Logf("completions=%d observed in-flight charge=%d bytes minimum retained across retries=%d", completed, observedCharge, minRetained)
			if w.Code != 200 || completed != 20 {
				t.Fatalf("discarded projections consumed live capacity: completions=%d status=%d %s", completed, w.Code, w.Body.String())
			}
			if q.requestBudgets.held != 0 {
				t.Fatal("finished request retained projection capacity")
			}
			if minRetained <= 0 {
				t.Fatal("discarding a response released still-resident cold decode charges")
			}
		})
	}
}
