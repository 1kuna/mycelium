package myce

import (
	"context"
	"crypto/sha256"
	"errors"
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
	UserCancel   bool    `json:"user_cancel,omitempty"`
	GraceSeconds float64 `json:"grace_seconds"`
	Kill         bool    `json:"kill"`
}

func workerStopEventJob(j *Job) bool {
	return j.StopAt != nil && (j.State == "stopping" || terminal(j.State) && !j.ReleaseConfirmed)
}

func (a API) workerEvent(w http.ResponseWriter, r *http.Request) {
	q := a.Queue
	machine := r.PathValue("id")
	query := r.URL.Query()
	session, after := query.Get("session"), query.Get("after")
	defer q.trackWorkerPoll(r.Context(), machine, session)()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		requestUnlock1 := requestLock(r.Context(), &q.mu)
		wake := q.wake
		var event WorkerEvent
		now := q.clock.Now()
		entries := map[attemptKey]WorkerEvent{}
		add := func(id string, attempt int, ownerSession string, deadline time.Time) {
			if ownerSession != session {
				return
			}
			e := WorkerEvent{Session: session, StopAt: &deadline, Cursor: fmt.Sprintf("%s:%s:%d:%s:%t", session, id, attempt, deadline.Format(time.RFC3339Nano), !now.Before(deadline)), Execution: Execution{ID: id, Attempt: attempt}, GraceSeconds: max(0, deadline.Sub(now).Seconds()), Kill: !now.Before(deadline)}
			if j := q.s.Jobs[id]; j != nil {
				e.UserCancel = cancelled(j)
				for _, copy := range j.Copies {
					if copy.Machine == machine && copy.Session == session && copy.Attempt == attempt {
						e.PriorityStop = copy.PriorityStop
					}
				}
			}
			if scarceYieldStop(e.PriorityStop) && !e.UserCancel {
				j := q.s.Jobs[id]
				if j != nil && yieldMatches(j) && !j.ScarceHostYield.Authorized {
					return
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
		jobs := q.s.Jobs
		if q.records != nil && !q.simulation && q.records.jobCount == len(jobs) {
			jobs = q.records.stopEvents
		}
		for _, j := range jobs {
			if j.Machine == machine && workerStopEventJob(j) {
				add(j.ID, j.Attempt, j.Session, *j.StopAt)
			}
		}
		for _, hold := range q.s.HostReleases {
			if hold.Machine == machine && !hold.Until.IsZero() {
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
		requestUnlock1()
		if event.Cursor != "" && event.Cursor != after {
			respond(w, event, nil)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			respond(w, WorkerEvent{Cursor: after}, nil)
			return
		case <-wake:
		}
	}
}
func (w Worker) watchStops(ctx context.Context, events chan<- WorkerEvent) {
	if w.Clock == nil {
		w.Clock = RealClock{}
	}
	// This loop owns reconnect pacing with the worker's injected Clock. A
	// nested client retry would conceal failures and add a second backoff.
	client := w.Client.WithoutRetries()
	after := ""
	var retry workerRetry
	wake := time.NewTicker(10 * time.Millisecond)
	defer wake.Stop()
	for ctx.Err() == nil {
		if !retry.ready(w.Clock.Now()) {
			select {
			case <-ctx.Done():
				return
			case <-wake.C:
			}
			continue
		}
		var event WorkerEvent
		e := client.Call(ctx, "GET", "/api/workers/"+url.PathEscape(w.Machine.ID)+"/events?session="+url.QueryEscape(w.Machine.Session)+"&after="+url.QueryEscape(after), nil, &event)
		if e != nil {
			delay := time.Second
			var response *HTTPError
			if errors.As(e, &response) && response.RetryAfter > 0 {
				delay = response.RetryAfter
			}
			retry.succeeded(w.Clock.Now(), delay)
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

// Process control never takes the actor store locks or performs network I/O.
// Its small stop receipt is independent of the actor's mutable state.
// A stop received before command startup is retained for that exact attempt.
type processControl struct {
	availability  *availabilityMonitor
	mu            sync.Mutex
	clock         Clock
	processes     map[attemptKey]*process
	pendingEvents map[attemptKey]WorkerEvent
	retired       []teardownMark // Released accelerator attempts' teardown evidence.
	leaving       bool
	service       bool            // Installed shutdown must release even non-resumable CPU work.
	serviceStop   context.Context // A foreground restart releases it the same way.
	wake          chan struct{}
}

func eventKey(e WorkerEvent) attemptKey { return attemptKey{e.ID, e.Attempt, e.Session} }
func (c *processControl) apply() {
	now := c.clock.Now()
	for key, p := range c.processes {
		if c.availability != nil && !c.availability.snapshot().Available {
			p.cancelStarting()
			if p.stopAdmission != nil {
				p.stopAdmission()
			}
			p.mu.Lock()
			p.polite = true
			p.mu.Unlock()
			grace := 10 * time.Second
			if !p.job.Spec.Resumable {
				grace = 0
			}
			p.stop(now, grace, false)
		}
		service := c.service || c.serviceStop != nil && c.serviceStop.Err() != nil
		if c.leaving && (service || p.job.Spec.Resumable || !p.job.Spec.Needs.cpuOnly()) {
			grace := StopGrace
			if !p.job.Spec.Needs.cpuOnly() {
				grace = defaultGPUStopGrace
			}
			if service {
				p.stopService(now, grace)
			} else {
				p.stop(now, grace, false)
			}
		}
		if event, ok := c.pendingEvents[key]; ok {
			p.cancelStarting()
			// Publish before receipt persistence or signaling can wait on p.mu.
			if p.stopAdmission != nil {
				p.stopAdmission()
			}
			if err := p.recordPriorityStop(nil, event.PriorityStop); err != nil {
				continue // Retry persistence before delivering the priority stop.
			}
			p.stopDirective(now, max(0, event.deadline.Sub(now)), event.Kill || !now.Before(event.deadline), event.UserCancel)
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
	if p := c.processes[jobKey(j)]; p != nil && !p.job.Spec.Needs.cpuOnly() {
		c.pruneTeardowns()
		mark := p.teardownMark()
		if len(mark.times) > 0 {
			mark.until = c.clock.Now().Add(teardownRetention)
			c.retired = append(c.retired, mark)
		}
	}
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
			if p := c.processes[key]; p != nil && p.stopAdmission != nil {
				p.stopAdmission()
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
