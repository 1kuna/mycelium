package myce

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestAttemptHistoryQueueFinals(t *testing.T) {
	for _, path := range []string{"cancel-release", "cancel-expiry", "cancel-loss", "cancel-rejoin", "start-deadline"} {
		t.Run(path, func(t *testing.T) {
			q, clock := fixture(t)
			s := spec("experiment")
			if path == "start-deadline" {
				s.Priority = "production"
				at := clock.Now().Add(time.Minute)
				s.StartBy = &at
			}
			submit(t, q, s)
			m := machine("one", "fake")
			attempt := check(t, q, m, nil).Job
			if path != "start-deadline" {
				cancelAck(t, q, attempt.ID)
			}
			switch path {
			case "cancel-release":
				r := result(attempt, false)
				r.ReleaseOnly = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			case "cancel-expiry":
				clock.Advance(StopGrace)
				check(t, q, observer(), nil)
			case "cancel-loss":
				clock.Advance(LossAfter)
				check(t, q, observer(), nil)
			case "cancel-rejoin":
				fresh := m
				fresh.Session = "fresh"
				if _, err := q.Checkin(Checkin{Machine: fresh, Fresh: true, PreviousSession: m.Session}); err != nil {
					t.Fatal(err)
				}
			case "start-deadline":
				clock.Advance(time.Minute)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
			}
			q = reopenCancellationQueue(t, q, clock)
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			got, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), attempt.ID)
			if err != nil || len(got.AttemptHistory) != 1 {
				t.Fatalf("missing queue final: %+v %v", got, err)
			}
			h := got.AttemptHistory[0]
			state, reason := "cancelled", "cancel requested"
			if path == "start-deadline" {
				state, reason = "failed", "production start deadline exceeded"
			}
			if h.Attempt != attempt.Attempt || h.Machine != attempt.Machine || h.Session != attempt.Session || h.State != state || h.Reason != reason || !h.Interrupted || !h.EndedAt.Equal(clock.Now()) || h.ExecutionCause != nil || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("wrong queue final: %+v job=%+v", h, got)
			}
			r := result(attempt, false)
			r.ReleaseOnly = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(h, job(t, q, attempt.ID).AttemptHistory[0]) {
				t.Fatal("late cleanup rewrote ended outcome")
			}
		})
	}
}
