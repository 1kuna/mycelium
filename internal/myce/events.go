package myce

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type WorkerEvent struct {
	Cursor string `json:"cursor"`
	Execution
	StopAt       *time.Time `json:"stop_at,omitempty"`
	deadline     time.Time
	GraceSeconds float64 `json:"grace_seconds"`
	Kill         bool    `json:"kill"`
}

func (a API) workerEvent(w http.ResponseWriter, r *http.Request) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		a.Queue.mu.Lock()
		wake := a.Queue.wake
		var event WorkerEvent
		for _, j := range a.Queue.s.Jobs {
			if j.Machine == r.PathValue("id") && j.Session == r.URL.Query().Get("session") && (j.State == "stopping" || terminal(j.State) && !j.ReleaseConfirmed) && j.StopAt != nil {
				deadline := *j.StopAt
				event = WorkerEvent{StopAt: &deadline, Cursor: fmt.Sprintf("%s:%d:%s", j.ID, j.Attempt, j.StopAt.Format(time.RFC3339Nano)), Execution: Execution{ID: j.ID, Attempt: j.Attempt}, GraceSeconds: max(0, j.StopAt.Sub(a.Queue.clock.Now()).Seconds()), Kill: expired(j.StopAt, a.Queue.clock.Now())}
				break
			}
		}
		a.Queue.mu.Unlock()
		if event.Cursor != "" && event.Cursor != r.URL.Query().Get("after") {
			respond(w, event, nil)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			respond(w, WorkerEvent{Cursor: r.URL.Query().Get("after")}, nil)
			return
		case <-wake:
		}
	}
}
func (w Worker) watchStops(ctx context.Context, events chan<- WorkerEvent) {
	if w.Clock == nil {
		w.Clock = RealClock{}
	}
	after := ""
	for ctx.Err() == nil {
		var event WorkerEvent
		e := w.Client.Call(ctx, "GET", "/api/workers/"+url.PathEscape(w.Machine.ID)+"/events?session="+url.QueryEscape(w.Machine.Session)+"&after="+url.QueryEscape(after), nil, &event)
		if e != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if event.ID != "" && event.Cursor != after {
			event.deadline = w.Clock.Now().Add(time.Duration(max(0, event.GraceSeconds) * float64(time.Second)))
			if event.StopAt != nil {
				event.deadline = *event.StopAt
			}
			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
		after = event.Cursor
	}
}

// Process control never takes the upload/store loop's locks or performs I/O.
// A stop received before command startup is retained for that exact attempt.
type processControl struct {
	availability *availabilityMonitor
	mu           sync.Mutex
	clock        Clock
	current      *process
	pending      WorkerEvent
	leaving      bool
	wake         chan struct{}
}

func (c *processControl) apply() {
	p := c.current
	if p == nil {
		return
	}
	now := c.clock.Now()
	if c.availability != nil && !c.availability.snapshot().Available {
		p.mu.Lock()
		p.polite = true
		p.mu.Unlock()
		grace := 10 * time.Second
		if !p.job.Spec.Resumable {
			grace = 0
		}
		p.stop(now, grace, false)
	}
	if c.leaving && p.job.Spec.Resumable {
		p.stop(now, StopGrace, false)
	}
	if p.job.ID == c.pending.ID && p.job.Attempt == c.pending.Attempt {
		p.stop(now, max(0, c.pending.deadline.Sub(now)), c.pending.Kill || !now.Before(c.pending.deadline))
	}
	p.enforceGrace(now)
}
func (c *processControl) set(p *process) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = p
	c.apply()
}
func (c *processControl) run(ctx, stop context.Context, events <-chan WorkerEvent) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	stopCh := stop.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			c.mu.Lock()
			c.pending = event
			c.apply()
			c.mu.Unlock()
			select {
			case c.wake <- struct{}{}:
			default:
			}
		case <-stopCh:
			c.mu.Lock()
			c.leaving = true
			c.apply()
			c.mu.Unlock()
			stopCh = nil
		case <-ticker.C:
			c.mu.Lock()
			c.apply()
			c.mu.Unlock()
		}
	}
}
