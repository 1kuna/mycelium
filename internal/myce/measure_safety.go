package myce

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"
)

// A positive acknowledgement is required: an empty or older-server reply
// cannot accidentally certify safety. No job payloads or host inventories cross
// this boundary, and it never uses the observational snapshot cache.
type measurementSafetyReply struct {
	Safe    bool   `json:"safe"`
	Refusal string `json:"refusal,omitempty"`
}

func (a API) registerMeasurementSafety(mux *requestMux) {
	mux.HandleFunc("GET /api/measure/safety", func(w http.ResponseWriter, r *http.Request) {
		out, err := a.Queue.measurementSafety(r.Context(), r.URL.Query().Get("worker"), r.URL.Query().Get("platform"))
		respondOperation(w, out, err, "status_unavailable")
	})
}

func (q *Queue) measurementSafety(ctx context.Context, worker, platform string) (measurementSafetyReply, error) {
	if worker == "" || platform == "" {
		return measurementSafetyReply{}, errors.New("measurement safety requires worker and platform")
	}
	unlock := requestLock(ctx, &q.mu)
	defer unlock()
	if err := ctx.Err(); err != nil {
		return measurementSafetyReply{}, err
	}
	if !q.simulation {
		if err := q.db.PingContext(ctx); err != nil {
			return measurementSafetyReply{}, err
		}
	}
	// The live index includes terminal attempts that still owe release custody;
	// immutable history and submission payloads need no copying or projection.
	jobs := q.s.Jobs
	if q.records != nil && !q.simulation {
		jobs = q.records.live
	}
	for _, j := range jobs {
		if (j.Spec.Priority == "production" || j.Spec.Class == "production") && (!terminal(j.State) || attemptReleasePending(j)) {
			return measurementSafetyReply{Refusal: "production work outstanding; measurement refused"}, nil
		}
	}
	for _, c := range q.calls {
		if c.Priority == "production" || c.Class == "production" || c.Machine == worker {
			return measurementSafetyReply{Refusal: "production model call or active call on selected worker; measurement refused"}, nil
		}
	}
	for _, p := range q.s.Pins {
		if p.Machine == worker || p.Platform == platform {
			return measurementSafetyReply{Refusal: "production pin targets this worker/platform; measurement refused"}, nil
		}
	}
	for _, m := range q.s.ModelSlots {
		if m.Machine == worker && (m.Priority == "production" || m.Class == "production" || m.InFlight > 0) {
			return measurementSafetyReply{Refusal: "production model or active calls on selected worker; measurement refused"}, nil
		}
	}
	return measurementSafetyReply{Safe: true}, nil
}

func (c Client) measurementSafety(ctx context.Context, worker, platform string) error {
	var out measurementSafetyReply
	path := "/api/measure/safety?worker=" + url.QueryEscape(worker) + "&platform=" + url.QueryEscape(platform)
	// The measurement loop owns retries. HTTP refusals, including an older
	// queue's 404, are permanent; never fall back to the wide inspection read.
	if err := c.callOnce(ctx, http.MethodGet, path, nil, &out); err != nil {
		return err
	}
	if !out.Safe || out.Refusal != "" {
		if out.Refusal == "" {
			return errors.New("queue did not certify measurement safety")
		}
		return errors.New(out.Refusal)
	}
	return nil
}

// LoadTimeout is also the maximum safety-read budget. The enclosing maintenance
// context may end it sooner. Backoff grows from the observed failed-read cost
// (at least the configured sampling interval), avoiding a hot retry loop without
// introducing another fixed polling rate or extending the maintenance window.
func waitMeasurementSafety(ctx context.Context, clock Clock, budget, interval time.Duration, check func(context.Context) error, wait func(context.Context, time.Duration) error) error {
	if wait == nil {
		wait = waitStatusPause
	}
	deadline := clock.Now().Add(budget)
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	delay := interval
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		remaining := deadline.Sub(clock.Now())
		if remaining <= 0 {
			return errors.Join(context.DeadlineExceeded, last)
		}
		started := clock.Now()
		attempt, stop := context.WithTimeout(ctx, remaining)
		err := check(attempt)
		stop()
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err)
		}
		if !clock.Now().Before(deadline) {
			return errors.Join(context.DeadlineExceeded, err)
		}
		if err == nil {
			return nil
		}
		var network net.Error
		if !errors.Is(err, context.DeadlineExceeded) && !(errors.As(err, &network) && network.Timeout()) {
			return err
		}
		last = err
		delay = min(max(delay, clock.Now().Sub(started)), deadline.Sub(clock.Now()))
		if err := wait(ctx, delay); err != nil {
			return errors.Join(err, last)
		}
		delay = min(delay, budget/2) * 2
	}
}
