package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #171: two acknowledged starts and zero losses are not two command failures.
func TestAttemptHistoryRequeueThenExit(t *testing.T) {
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	fakeCheckpointAvailability(t, q, true)
	m := machine("one", "fake")
	submit(t, q, spec("experiment"))
	first := check(t, q, m, nil).Job
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := classifyAttemptOutcome(result(first, false), attemptCause{memberOutcome: memberOutcome{ObservationLost: true}})
	r.OperatorStop = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	second := check(t, q, m, nil).Job
	if second == nil || second.Attempt != 2 {
		t.Fatal("missing second attempt", second)
	}
	if err := q.Started(result(second, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	exit := 1
	r = classifyAttemptOutcome(result(second, false), attemptCause{memberOutcome: memberOutcome{Exit: &memberExit{Status: exit, ObservedAt: clock.Now()}}})
	r.ExecutionCause = &RuntimeCause{Operation: "command", Class: "command_exit", ExitCode: &exit}
	r.Error = "private-command-path: exit status 1"
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(result(first, true)); !errors.Is(err, ErrStale) {
		t.Fatal("stale result accepted", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	got, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "failed" || got.Starts != 2 || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatal("charging/state changed", got)
	}
	// Decode the supported wire shape so this regression fails before the field exists.
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "private-command-path") {
		t.Fatal("attempt history disclosed command error text", string(data))
	}
	var wire struct {
		History []struct {
			Attempt        int           `json:"attempt"`
			Machine        string        `json:"machine"`
			Session        string        `json:"session"`
			State          string        `json:"state"`
			EndedAt        time.Time     `json:"ended_at"`
			OperatorStop   bool          `json:"operator_stop"`
			ExecutionCause *RuntimeCause `json:"execution_cause"`
		} `json:"attempt_history"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.History) != 2 {
		t.Fatalf("want both attempt causes, got %s", data)
	}
	a, b := wire.History[0], wire.History[1]
	if a.Attempt != 1 || a.State != "queued" || !a.OperatorStop || a.Machine != m.ID || a.Session != m.Session || !a.EndedAt.Equal(clock.Now().Add(-time.Second)) {
		t.Fatal("first requeue cause lost", a)
	}
	if b.Attempt != 2 || b.State != "failed" || b.ExecutionCause == nil || b.ExecutionCause.Class != "command_exit" || b.ExecutionCause.ExitCode == nil || *b.ExecutionCause.ExitCode != 1 || !b.EndedAt.Equal(clock.Now()) {
		t.Fatal("terminal exit cause lost", b)
	}
}

func TestAttemptHistoryBoundedRequeues(t *testing.T) {
	q, clock := fixture(t)
	m := machine("one", "fake")
	submit(t, q, spec("experiment"))
	var current *Job
	for attempt := 1; attempt <= attemptHistoryLimit+2; attempt++ {
		current = check(t, q, m, nil).Job
		if current == nil || current.Attempt != attempt {
			t.Fatal("missing attempt", attempt, current)
		}
		if current.ExecutionCause != nil {
			t.Fatal("previous execution cause leaked into assignment", current)
		}
		clock.Advance(time.Second)
		r := result(current, false)
		r.Interrupted, r.PoliteStop = true, true
		r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	got := job(t, q, current.ID)
	if got.Losses != 0 || got.InfrastructureRetries != 0 || len(got.AttemptHistory) != attemptHistoryLimit {
		t.Fatal("unbounded history or changed charging", got)
	}
	for i, a := range got.AttemptHistory {
		if a.Attempt != i+3 || a.State != "queued" || !a.PoliteStop || a.ExecutionCause == nil || a.ExecutionCause.Class != launchRefused {
			t.Fatal("wrong retained cause", a)
		}
	}
}

// Queue stop provenance must survive every requeue path without a full result.
func TestAttemptHistoryQueueStopWithoutResult(t *testing.T) {
	for _, tc := range []struct {
		path, reason string
	}{
		{"release-only", "hypha waiting: on battery"},
		{"heartbeat-other", "machine leaving"},
		{"heartbeat-empty", "making room for production job"},
		{"stop-expiry", "making room for production job"},
		{"machine-quiet", "hypha waiting: on battery"},
		{"priority-preemption", "making room for production job"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			s := spec("experiment")
			heartbeat := strings.HasPrefix(tc.path, "heartbeat-")
			if heartbeat {
				// Docker release-only ack settles custody; heartbeat omission
				// then advances the stop, exercising both omission branches.
				m = dockerMachine(m.ID, m.GPU)
				s.Run = dockerJob().Spec.Run
			}
			submit(t, q, s)
			first := check(t, q, m, nil).Job
			if first == nil {
				t.Fatal("missing assignment")
			}
			if err := q.update(func() error {
				live := q.s.Jobs[first.ID]
				if tc.path == "priority-preemption" {
					q.stopBatchFor(live, &Job{ID: "demand", Spec: spec("production")})
				} else if strings.HasPrefix(tc.reason, "making room for ") {
					q.stopFor(live, "production", "job")
				} else {
					q.stop(live, tc.reason)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if tc.path == "release-only" || tc.path == "priority-preemption" || heartbeat {
				r := result(first, false)
				r.ReleaseOnly = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			switch tc.path {
			case "heartbeat-other", "heartbeat-empty":
				c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Recovering: true}
				if tc.path == "heartbeat-other" {
					c.Attempts = []AttemptReport{{Execution: Execution{ID: "unknown", Attempt: 1}, Session: m.Session}}
				}
				if _, err := q.Checkin(c); err != nil {
					t.Fatal(err)
				}
			case "stop-expiry":
				clock.Advance(job(t, q, first.ID).StopAt.Sub(clock.Now()))
				check(t, q, observer(), nil)
			case "machine-quiet":
				clock.Advance(LossAfter)
				check(t, q, observer(), nil)
			}
			got := job(t, q, first.ID)
			if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || len(got.AttemptHistory) != 1 {
				t.Fatal("stop did not requeue once without a counted loss", got)
			}
			a := got.AttemptHistory[0]
			priority := tc.path == "priority-preemption"
			if a.Attempt != first.Attempt || a.Machine != first.Machine || a.Session != first.Session || a.State != "queued" || !a.Interrupted || a.PoliteStop == priority || !a.EndedAt.Equal(clock.Now()) {
				t.Fatalf("incorrect queue-stop provenance: %+v", a)
			}
			if priority && (a.PriorityStop == nil || a.PriorityStop.Cause != priorityPreemptionCause || a.PriorityStop.Demand != "demand" || a.PriorityStop.Victim != first.ID) {
				t.Fatalf("priority preemption provenance lost: %+v", a)
			}
		})
	}
}
