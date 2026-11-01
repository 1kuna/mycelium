package myce

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDeadlineGuidancePolicyAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tiers  []PriorityTier
		change string
	}{
		{"default", nil, "higher"},
		{"catchall", []PriorityTier{{"shared", []string{"*/*"}}}, "same"},
		{"custom", []PriorityTier{{"preferred", []string{"test/experiment"}}, {"rest", []string{"*/*"}}}, "lower"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			if tc.tiers != nil {
				if _, err := q.SetPriorityPolicy(0, PriorityPolicy{Version: 1, Tiers: tc.tiers}); err != nil {
					t.Fatal(err)
				}
			}
			s := spec("experiment")
			deadline := clock.Now().Add(time.Hour)
			s.Deadline, s.SubmissionKey = &deadline, "deadline-advice"
			server := httptest.NewServer(API{Queue: q, Token: "test-only"}.Handler())
			defer server.Close()
			client := Client{URL: server.URL, Token: "test-only"}
			submitted, err := client.Submit(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := client.Submit(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			for _, got := range []*Job{submitted, replayed} {
				d := got.DeadlineGuidance
				if d == nil || d.PrecedenceChange != tc.change || d.DeadlinePolicy != q.priorityMatch(Spec{Project: s.Project, Class: "deadline"}) || got.PriorityPolicy != q.priorityMatch(s) {
					t.Fatalf("wrong match: %+v", got)
				}
				if got.ID != submitted.ID || got.Spec.Class != "experiment" || got.Spec.SubmissionKey != s.SubmissionKey {
					t.Fatal("advice changed accepted identity")
				}
				if !strings.Contains(d.Example, "class: deadline\ndeadline: ") || !strings.Contains(d.ETA, "unknown") || len(d.Blockers) == 0 {
					t.Fatalf("incomplete advice: %+v", d)
				}
			}
			fetched, err := client.GetJob(context.Background(), submitted.ID)
			if err != nil || fetched.DeadlineGuidance == nil || fetched.DeadlineGuidance.PrecedenceChange != tc.change {
				t.Fatal(fetched, err)
			}
			// Advice agrees with actual dispatch ordering under each policy.
			urgentSpec := spec("deadline")
			earlier := clock.Now().Add(30 * time.Minute)
			urgentSpec.Deadline = &earlier
			urgent := submit(t, q, urgentSpec)
			want := urgent.ID
			if tc.change == "lower" {
				want = submitted.ID
			}
			m := machine("worker", "fake")
			if got := check(t, q, m, nil).Job; got == nil || got.ID != want {
				t.Fatal("dispatch disagrees with deadline guidance", got)
			}
		})
	}
	q, _ := fixture(t)
	s := spec("deadline")
	s.Deadline = nil
	if _, err := q.Submit(s); err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatal("missing deadline accepted", err)
	}
}

func TestDeadlineGuidanceNonResumableAndOverrun(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.Resumable = false
	running := submit(t, q, s)
	m := machine("worker", "fake")
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != running.ID {
		t.Fatal(assigned)
	}
	if err := q.Started(Result{Machine: assigned.Machine, Session: assigned.Session, Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}}); err != nil {
		t.Fatal(err)
	}
	demand := submit(t, q, spec("deadline"))
	assertBlocker := func(want string) {
		t.Helper()
		got, err := q.jobSnapshot(demand.ID)
		if err != nil {
			t.Fatal(err)
		}
		blockers := strings.Join(got.DeadlineGuidance.Blockers, "; ")
		if !strings.Contains(blockers, "non-resumable command "+running.ID) || !strings.Contains(blockers, want) {
			t.Fatal(blockers)
		}
	}
	assertBlocker("ETA unknown")
	// Synthetic comparable timing evidence; clocks advance without wall sleeps.
	predicted := 10.0
	q.s.Jobs[running.ID].Timing = &WorkTiming{PredictedSeconds: &predicted}
	assertBlocker("10s remaining")
	clock.Advance(11 * time.Second)
	assertBlocker("overrun")
	if reply := check(t, q, m, assigned); reply.Job != nil || reply.Stop {
		t.Fatalf("deadline interrupted non-resumable work: %+v", reply)
	}
}
