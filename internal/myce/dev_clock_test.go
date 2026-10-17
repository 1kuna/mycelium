package myce

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
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
	clock := &devClock{}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := clock.advance(1); err != nil {
				t.Error(err)
			}
			_ = clock.Now()
		}()
	}
	wg.Wait()
	if clock.offset != 20*time.Second {
		t.Fatal(clock.offset)
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
