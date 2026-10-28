package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The event stream can beat (or outlive a blocked) heartbeat. A checkpoint
// handler exiting zero must retain the stop's cause without an actor directive.
func TestPriorityStopEventBeforeHeartbeat(t *testing.T) {
	for _, group := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "native-group"}[group], func(t *testing.T) {
			w, store, p := d5NativeStart(t, group, nil, []string{"sh", "-c", `trap 'echo PAUSED; exit 0' TERM; touch "$1"; while :; do sleep .01; done`, "main", "{work}/ready"})
			nativeFile(t, p.job.Spec.Run.Workdir, "ready")
			data, err := json.Marshal(map[string]any{"id": p.job.ID, "attempt": p.job.Attempt, "session": p.job.Session, "priority_stop": &PriorityPreemption{Cause: priorityPreemptionCause, Demand: "deadline-model", Victim: p.job.ID}})
			if err != nil {
				t.Fatal(err)
			}
			var event WorkerEvent
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatal(err)
			}
			event.deadline = w.Clock.Now().Add(time.Minute)
			control := processControl{clock: w.Clock, processes: map[attemptKey]*process{jobKey(p.job): p}, pendingEvents: map[attemptKey]WorkerEvent{eventKey(event): event}}
			persist := p.savePriorityStop
			p.savePriorityStop = func(*PriorityPreemption) error { return errors.New("receipt unavailable") }
			control.apply()
			p.mu.Lock()
			stopped := p.stopped || p.killed || p.priorityStop != nil
			p.mu.Unlock()
			if stopped {
				t.Fatal("stop delivered before intent was durable")
			}
			p.savePriorityStop = persist
			control.apply()
			r := d5Finish(t, p)
			if r.Success || !r.Interrupted || r.PriorityStop == nil || r.PriorityStop.Demand != "deadline-model" {
				t.Fatalf("event stop lost its cause before heartbeat: %+v", r)
			}
			// The event does not write the actor's mutable state. Reopen recovers
			// its independent receipt even if no heartbeat ever arrived.
			if store.state.PriorityStop != nil {
				t.Fatal("event mutated actor custody")
			}
			reopened := &workerStore{dir: store.dir, state: store.state}
			if err := reopened.restorePriorityStop(); err != nil {
				t.Fatal(err)
			}
			if reopened.state.PriorityStop == nil || reopened.state.PriorityStop.Demand != "deadline-model" {
				t.Fatal("restart lost event stop intent")
			}
		})
	}
}

func TestWorkerEventCarriesPriorityStop(t *testing.T) {
	q, _ := fixture(t)
	m := machine("event-worker", "fake")
	j := submit(t, q, spec("experiment"))
	if check(t, q, m, nil).Job == nil {
		t.Fatal("missing assignment")
	}
	if err := q.update(func() error {
		q.stopBatchFor(q.s.Jobs[j.ID], &Job{ID: "deadline-model", Spec: spec("deadline")})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "event-test"}.Handler())
	defer server.Close()
	var event WorkerEvent
	client := Client{URL: server.URL, Token: "event-test"}
	if err := client.Call(context.Background(), "GET", "/api/workers/"+m.ID+"/events?session="+m.Session, nil, &event); err != nil {
		t.Fatal(err)
	}
	if event.ID != j.ID || event.Attempt != 1 || event.Session != m.Session || event.UserCancel || event.PriorityStop == nil || event.PriorityStop.Demand != "deadline-model" || event.PriorityStop.Victim != j.ID {
		t.Fatalf("event lost exact priority stop: %+v", event)
	}
}

type observedEventClock struct {
	manualWorkerClock
	reads chan struct{}
}

func (c *observedEventClock) Now() time.Time {
	now := c.manualWorkerClock.Now()
	select {
	case c.reads <- struct{}{}:
	default:
	}
	return now
}
func TestWorkerEventReconnectUsesClock(t *testing.T) {
	clock := &observedEventClock{reads: make(chan struct{}, 16)}
	clock.now = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	requests := make(chan struct{}, 4)
	transport := clientTestTransport(func(r *http.Request) (*http.Response, error) {
		requests <- struct{}{}
		return nil, errors.New("event stream unavailable")
	})
	w := Worker{Clock: clock, Machine: machine("event-worker", "fake"), Client: Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: transport}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.watchStops(ctx, make(chan WorkerEvent)) }()
	defer func() { cancel(); <-done }()
	// Observe the initial readiness check, failure timestamp, and backoff check.
	for i := 0; i < 3; i++ {
		select {
		case <-clock.reads:
		case <-time.After(testWaitTimeout):
			t.Fatal("retry did not consult Clock")
		}
	}
	select {
	case <-requests:
	default:
		t.Fatal("initial request missing")
	}
	select {
	case <-requests:
		t.Fatal("retried before fake-clock deadline")
	default:
	}
	clock.Advance(time.Second)
	select {
	case <-requests:
	case <-time.After(testWaitTimeout):
		t.Fatal("fake-clock advance did not release reconnect")
	}
}
