package myce

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func clockRequest(handler http.Handler, body, token string) *httptest.ResponseRecorder {
	request := httptest.NewRequest("POST", "/api/dev/clock/advance", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestDevClockProductionRouteAbsent(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		// An injected clock alone does not enable a dev endpoint (including fake clocks).
		for _, clock := range []Clock{RealClock{}} {
			q.clock = clock
			response := clockRequest(API{Queue: q, Token: "test"}.Handler(), `{"seconds":1}`, "test")
			if response.Code != 404 {
				t.Fatalf("production status = %d", response.Code)
			}
		}

	})

	t.Run("DevClockValidationAndAuthentication", func(t *testing.T) {
		q, _ := fixture(t)
		clock := &devClock{}
		q.clock = clock
		handler := API{Queue: q, Token: "test"}.Handler()
		if response := clockRequest(handler, `{"seconds":1}`, "wrong"); response.Code != 401 {
			t.Fatal(response.Code)
		}
		if clock.offset != 0 {
			t.Fatal("rejected request advanced clock")
		}
		before := clock.Now()
		if response := clockRequest(handler, `{"seconds":0.001}`, "test"); response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
		if clock.Now().Before(before.Add(time.Millisecond)) {
			t.Fatal("clock did not advance")
		}
	})
}

func TestDevClockQueueDecisions(t *testing.T) {
	t.Run("start deadline", func(t *testing.T) {
		q, _ := fixture(t)
		q.clock = &devClock{}
		s := spec("production")
		s.Needs.Platform = Platform{Required: []string{"xpu"}}
		j := submit(t, q, s)
		response := clockRequest(API{Queue: q, Token: "test"}.Handler(), fmt.Sprintf(`{"seconds":%g}`, ProductionStartTimeout.Seconds()+1), "test")
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
		requireState(t, q, j.ID, "failed")
	})
	t.Run("loss timeout", func(t *testing.T) {
		q, _ := fixture(t)
		q.clock = &devClock{}
		j := submit(t, q, spec("experiment"))
		if got := check(t, q, machine("lost", "fake"), nil).Job; got == nil {
			t.Fatal("not assigned")
		}
		// Keep a majority fresh so the normal partition guard does not mask loss.
		for _, id := range []string{"peer-one", "peer-two"} {
			m := machine(id, "fake")
			m.FreeMemoryGB = 0
			check(t, q, m, nil)
		}
		handler := API{Queue: q, Token: "test"}.Handler()
		for elapsed := time.Duration(0); elapsed <= LossAfter; elapsed += 2 * time.Second {
			response := clockRequest(handler, `{"seconds":2}`, "test")
			if response.Code != 200 {
				t.Fatal(response.Code, response.Body.String())
			}
			for _, id := range []string{"peer-one", "peer-two"} {
				m := machine(id, "fake")
				m.FreeMemoryGB = 0
				check(t, q, m, nil)
			}
		}
		requireState(t, q, j.ID, "queued")
		if job(t, q, j.ID).Losses != 1 {
			t.Fatal("loss did not follow dev clock")
		}
	})
}

type accountingInterleaveClock struct {
	base             Clock
	queue            *Queue
	once             sync.Once
	entered, release chan struct{}
}

func (c *accountingInterleaveClock) Now() time.Time {
	value := c.base.Now()
	// Contact, memory snapshots and mutex service instrumentation also read the
	// clock. Pause only the transaction's direct clock read for usage accounting,
	// after it owns q.mu and before the heartbeat updates LastSeen.
	pc, _, _, ok := runtime.Caller(1)
	if !ok || !strings.HasSuffix(runtime.FuncForPC(pc).Name(), ".(*Queue).updateTransaction") {
		return value
	}
	// Inspect physical ownership directly: the instrumented lock reads this
	// clock itself, so using it here would recursively enter service accounting.
	if c.queue.mu.mu.TryRLock() {
		c.queue.mu.mu.RUnlock()
		return value
	}
	c.once.Do(func() { close(c.entered); <-c.release })
	return value
}

func TestDevClockAdvanceDuringCheckin(t *testing.T) {
	q, _ := fixture(t)
	clock := &devClock{}
	q.clock = clock
	m := machine("clock-worker", "fake")
	check(t, q, m, nil)
	handler := API{Queue: q, Token: "test"}.Handler()
	gate := &accountingInterleaveClock{base: clock, queue: q, entered: make(chan struct{}), release: make(chan struct{})}
	q.clock = gate
	done := make(chan error, 1)
	go func() { _, err := q.Checkin(Checkin{Machine: m}); done <- err }()
	<-gate.entered
	// The next transaction has reached the availability seam, but the
	// in-flight heartbeat still owns the queue and its old accounting time.
	probeEntered := make(chan struct{})
	// Retention may perform a second transaction after advancing the clock.
	// Gate only the first arrival; the interleaving assertion stays unchanged.
	var probeOnce sync.Once
	q.checkpointProbe = func() bool { probeOnce.Do(func() { close(probeEntered) }); return true }
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() { response <- clockRequest(handler, `{"seconds":2592001}`, "test") }()
	<-probeEntered
	clock.mu.Lock()
	advancedDuringHeartbeat := clock.offset != 0
	clock.mu.Unlock()
	close(gate.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if r := <-response; r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	if advancedDuringHeartbeat {
		t.Error("clock advanced during an in-flight heartbeat")
	}
	var samples int
	var observed float64
	if err := q.db.QueryRow("SELECT COUNT(*), COALESCE(SUM(observed), 0) FROM usage_samples").Scan(&samples, &observed); err != nil {
		t.Fatal(err)
	}
	if observed < LossAfter.Seconds()/2 {
		t.Fatalf("clock advance dropped the last heartbeat's observation window: %f seconds", observed)
	}
	if samples > 10 {
		t.Fatalf("clock advance fabricated coverage across outage: %d samples", samples)
	}
}
