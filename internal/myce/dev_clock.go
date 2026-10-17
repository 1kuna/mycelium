package myce

import (
	"errors"
	"math"
	"net/http"
	"sync"
	"time"
)

// devClockMaxAdvance bounds each request; repeated advances also cannot overflow
// the duration offset. The offset is process-local and resets on dev restart.
const devClockMaxAdvance = 90 * 24 * time.Hour

// devClock keeps real elapsed time for ordinary dev command execution, with an
// advance-only offset for exercising queue deadlines without waiting days.
// Only StartDev installs it; ordinary Open callers never get the dev route.
type devClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *devClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return RealClock{}.Now().Add(c.offset)
}

func (c *devClock) advance(seconds float64) (time.Time, error) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 1e-9 || seconds > devClockMaxAdvance.Seconds() {
		return time.Time{}, errors.New("advance must be positive, at least 1ns and at most 90d")
	}
	delta := time.Duration(seconds * float64(time.Second))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.offset > time.Duration(math.MaxInt64)-delta {
		return time.Time{}, errors.New("dev clock offset would overflow")
	}
	c.offset += delta
	return RealClock{}.Now().Add(c.offset), nil
}

func (a API) registerDevClock(mux *http.ServeMux) {
	clock, ok := a.Queue.clock.(*devClock)
	if !ok {
		return
	}
	mux.HandleFunc("POST /api/dev/clock/advance", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Seconds float64 `json:"seconds"`
		}
		if !decode(w, r, &request) {
			return
		}
		now, err := clock.advance(request.Seconds)
		if err == nil {
			// Apply expiry/loss/deadline decisions before acknowledging the advance.
			// A failed Tick cannot rewind a clock that other requests have observed.
			err = a.Queue.Tick()
		}
		respond(w, struct {
			Now time.Time `json:"now"`
		}{now}, err)
	})
}
