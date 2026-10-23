package myce

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	q, _ := fixture(t)
	// An injected clock alone does not enable a dev endpoint (including fake clocks).
	for _, clock := range []Clock{q.clock, RealClock{}} {
		q.clock = clock
		response := clockRequest(API{Queue: q, Token: "test"}.Handler(), `{"seconds":1}`, "test")
		if response.Code != 404 {
			t.Fatalf("production status = %d", response.Code)
		}
	}
}

func TestDevClockValidationAndAuthentication(t *testing.T) {
	q, _ := fixture(t)
	clock := &devClock{}
	q.clock = clock
	handler := API{Queue: q, Token: "test"}.Handler()
	for _, body := range []string{`{}`, `null`, `{"seconds":null}`, `{"seconds":0}`, `{"seconds":-1}`, `{"seconds":7776001}`, `{"seconds":1e100}`, `{"seconds":1e-10}`, `{"seconds":"1"}`, `{"seconds":1,"extra":true}`, `{"seconds":1} {}`} {
		if response := clockRequest(handler, body, "test"); response.Code != 400 {
			t.Fatalf("%s: %d", body, response.Code)
		}
	}
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
	minimumClock := &devClock{}
	q.clock = minimumClock
	if response := clockRequest(API{Queue: q, Token: "test"}.Handler(), `{"seconds":0.000000001}`, "test"); response.Code != 200 || minimumClock.offset != time.Nanosecond {
		t.Fatal("minimum valid advance", response.Code, minimumClock.offset)
	}
	for _, invalid := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := clock.advance(invalid); err == nil {
			t.Fatal("accepted nonfinite advance")
		}
	}
	limitClock := &devClock{}
	if _, err := limitClock.advance(devClockMaxAdvance.Seconds()); err != nil || limitClock.offset != devClockMaxAdvance {
		t.Fatal("maximum valid advance", err)
	}
	clock.offset = time.Duration(math.MaxInt64)
	if _, err := clock.advance(1); err == nil || clock.offset != time.Duration(math.MaxInt64) {
		t.Fatal("offset overflow")
	}
}

func TestDevClockConcurrentAdvances(t *testing.T) {
	q, _ := fixture(t)
	clock := &devClock{}
	q.clock = clock
	handler := API{Queue: q, Token: "test"}.Handler()
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if response := clockRequest(handler, `{"seconds":1}`, "test"); response.Code != 200 {
				t.Error(response.Code, response.Body.String())
			}
			_ = clock.Now()
		}()
	}
	wg.Wait()
	if clock.offset != 20*time.Second {
		t.Fatal(clock.offset)
	}
}

func TestDevClockOrdinaryElapsedTime(t *testing.T) {
	clock := &devClock{}
	if _, err := clock.advance(time.Hour.Seconds()); err != nil {
		t.Fatal(err)
	}
	// The timer supplies an elapsed-time observation; no process or scheduler
	// decision is synchronized by an arbitrary sleep.
	elapsed := <-time.After(time.Millisecond)
	now := clock.Now()
	if now.Before(elapsed.Add(time.Hour)) || now.After(RealClock{}.Now().Add(time.Hour)) {
		t.Fatal("dev time did not continue with ordinary elapsed time")
	}
}

func TestDevClockRestartReset(t *testing.T) {
	root := t.TempDir()
	config := DevConfig{Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "test", Workers: 1, MemoryGB: 8}
	d, err := StartDev(config)
	if err != nil {
		t.Fatal(err)
	}
	clock, ok := d.Queue.clock.(*devClock)
	if !ok {
		t.Fatal("StartDev did not install its private clock")
	}
	response := clockRequest(API{Queue: d.Queue, Token: "test"}.Handler(), `{"seconds":3600}`, "test")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	// Reopen the same SQLite queue at the same queue endpoint.
	config.Address = strings.TrimPrefix(d.URL, "http://")
	before := RealClock{}.Now()
	d, err = StartDev(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	restarted, ok := d.Queue.clock.(*devClock)
	if !ok || restarted == clock {
		t.Fatal("restart did not install a fresh private clock")
	}
	now := restarted.Now()
	if now.Before(before) || now.After(RealClock{}.Now()) {
		t.Fatal("restart retained the dev offset")
	}
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
	once             sync.Once
	entered, release chan struct{}
}

func (c *accountingInterleaveClock) Now() time.Time {
	value := c.base.Now()
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
	gate := &accountingInterleaveClock{base: clock, entered: make(chan struct{}), release: make(chan struct{})}
	q.clock = gate
	done := make(chan error, 1)
	go func() { _, err := q.Checkin(Checkin{Machine: m}); done <- err }()
	<-gate.entered
	// The next transaction has reached the availability seam, but the
	// in-flight heartbeat still owns the queue and its old accounting time.
	probeEntered := make(chan struct{})
	q.checkpointProbe = func() bool { close(probeEntered); return true }
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
	if err := q.db.QueryRow("SELECT COUNT(*) FROM usage_samples").Scan(&samples); err != nil {
		t.Fatal(err)
	}
	if samples > 10 {
		t.Fatalf("clock advance fabricated coverage across outage: %d samples", samples)
	}
}
