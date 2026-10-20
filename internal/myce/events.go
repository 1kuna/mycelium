package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

type WorkerEvent struct {
	PriorityStop *PriorityPreemption `json:"priority_stop,omitempty"`
	Events       []WorkerEvent       `json:"events,omitempty"`
	Session      string              `json:"session,omitempty"`
	Cursor       string              `json:"cursor"`
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
		now := a.Queue.clock.Now()
		entries := map[attemptKey]WorkerEvent{}
		add := func(id string, attempt int, session string, deadline time.Time) {
			if session != r.URL.Query().Get("session") {
				return
			}
			e := WorkerEvent{Session: session, StopAt: &deadline, Cursor: fmt.Sprintf("%s:%s:%d:%s:%t", session, id, attempt, deadline.Format(time.RFC3339Nano), !now.Before(deadline)), Execution: Execution{ID: id, Attempt: attempt}, GraceSeconds: max(0, deadline.Sub(now).Seconds()), Kill: !now.Before(deadline)}
			if j := a.Queue.s.Jobs[id]; j != nil {
				for _, copy := range j.Copies {
					if copy.Machine == r.PathValue("id") && copy.Session == session && copy.Attempt == attempt {
						e.PriorityStop = copy.PriorityStop
					}
				}
			}
			if e.PriorityStop != nil {
				e.Cursor += ":" + e.PriorityStop.Demand
			}
			if old, ok := entries[eventKey(e)]; ok && old.StopAt.Before(deadline) {
				return
			}
			entries[eventKey(e)] = e
		}
		for _, j := range a.Queue.s.Jobs {
			if j.Machine == r.PathValue("id") && (j.State == "stopping" || terminal(j.State) && !j.ReleaseConfirmed) && j.StopAt != nil {
				add(j.ID, j.Attempt, j.Session, *j.StopAt)
			}
		}
		for _, hold := range a.Queue.s.HostReleases {
			if hold.Machine == r.PathValue("id") && !hold.Until.IsZero() {
				add(hold.Job, hold.Attempt, hold.Session, hold.Until)
			}
		}
		var events []WorkerEvent
		for _, e := range entries {
			events = append(events, e)
		}
		slices.SortFunc(events, func(a, b WorkerEvent) int { return strings.Compare(a.Cursor, b.Cursor) })
		if len(events) > 0 {
			event = events[0] // Keep the bounded legacy single-event adapter.
			var cursor strings.Builder
			for _, e := range events {
				cursor.WriteString(e.Cursor)
				cursor.WriteByte(0)
			}
			event.Cursor = fmt.Sprintf("%x", sha256.Sum256([]byte(cursor.String())))
			event.Events = events
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
		if event.Cursor != after {
			batch := event.Events
			if len(batch) == 0 && event.ID != "" {
				batch = []WorkerEvent{event}
			}
			for _, e := range batch {
				e.deadline = w.Clock.Now().Add(time.Duration(max(0, e.GraceSeconds) * float64(time.Second)))
				if e.StopAt != nil {
					e.deadline = *e.StopAt
				}
				select {
				case events <- e:
				case <-ctx.Done():
					return
				}
			}
		}
		after = event.Cursor
	}
}

// Process control never waits for uploads or takes the worker-store loop's locks.
// Signal facts and daemon intent use their existing small local state writes.
// A stop received before command startup is retained for that exact attempt.
type processControl struct {
	availability  *availabilityMonitor
	mu            sync.Mutex
	clock         Clock
	processes     map[attemptKey]*process
	pendingEvents map[attemptKey]WorkerEvent
	leaving       bool
	wake          chan struct{}
}

func eventKey(e WorkerEvent) attemptKey { return attemptKey{e.ID, e.Attempt, e.Session} }
func (c *processControl) apply() {
	now := c.clock.Now()
	for key, p := range c.processes {
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
		if event, ok := c.pendingEvents[key]; ok {
			p.mu.Lock()
			if p.recordPriorityStopLocked(event.PriorityStop) == nil {
				p.stopLocked(now, max(0, event.deadline.Sub(now)), event.Kill || !now.Before(event.deadline))
			}
			p.mu.Unlock()
		}
		p.enforceGrace(now)
	}
}
func (c *processControl) set(p *process) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p == nil {
		return
	}
	if c.processes == nil {
		c.processes = map[attemptKey]*process{}
	}
	c.processes[jobKey(p.job)] = p
	c.apply()
}
func (c *processControl) remove(j Job) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.processes, jobKey(j))
	delete(c.pendingEvents, jobKey(j))
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
			if c.pendingEvents == nil {
				c.pendingEvents = map[attemptKey]WorkerEvent{}
			}
			key := eventKey(event)
			if old, ok := c.pendingEvents[key]; !ok || event.deadline.Before(old.deadline) || old.PriorityStop == nil && event.PriorityStop != nil {
				c.pendingEvents[key] = event
			}
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
