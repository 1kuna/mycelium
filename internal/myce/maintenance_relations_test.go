package myce

import (
	"errors"
	"testing"
	"time"
)

// A report arriving after the single planning pass must propagate through all
// fan-out/dependency edges before the same transaction becomes durable.
func TestQueueMaintenanceClosureRollbackRestart(t *testing.T) {
	q, clock := fixture(t)
	makeJob := func(id string) *Job {
		return &Job{ID: id, State: "queued", Spec: spec("experiment"), Queued: clock.Now(), WaitingSince: clock.Now()}
	}
	producer, wave, sibling, downstream, gather := makeJob("producer"), makeJob("wave"), makeJob("sibling"), makeJob("downstream"), makeJob("gather")
	producer.Spec.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "piece"}, {Key: "other"}}, MaxConcurrency: 1}
	producer.Spec.Outputs = map[string]string{"out": "results"}
	producer.FanOut = &FanOutProgress{Next: 2, Children: []string{wave.ID, sibling.ID}}
	wave.Batch = &BatchWork{Parent: producer.ID, Pieces: []FanOutPiece{{Key: "piece"}}, Concurrency: 1}
	sibling.Batch = &BatchWork{Parent: producer.ID, Pieces: []FanOutPiece{{Key: "other"}}, Concurrency: 1}
	downstream.Spec.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "piece"}}, MaxConcurrency: 1}
	downstream.Spec.Outputs = map[string]string{"out": "results"}
	downstream.FanOut = &FanOutProgress{}
	downstream.Spec.After = &PieceDependency{Job: producer.ID, Outputs: map[string]string{"input": "piece/out"}}
	gather.Spec.After = &PieceDependency{Job: downstream.ID, Outputs: map[string]string{"input": "piece/out"}}
	ids := []string{producer.ID, wave.ID, downstream.ID, gather.ID}
	if err := q.update(func() error {
		for _, j := range []*Job{producer, wave, sibling, downstream, gather} {
			q.putJob(j.ID, j)
		}
		q.maintain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	drainAdmissionWait(t, q)
	assertStates := func(want string) {
		t.Helper()
		wantSibling := "queued"
		if want == "failed" {
			wantSibling = "cancelled"
		}
		if got := q.s.Jobs[sibling.ID]; got.State != wantSibling || wantSibling == "cancelled" && !cancelled(got) {
			t.Fatalf("sibling cancellation lost: %+v", got)
		}
		for _, id := range ids {
			if got := q.s.Jobs[id].State; got != want {
				t.Fatalf("%s state %s, want %s", id, got, want)
			}
		}
	}
	clock.Advance(time.Second)
	calls := 0
	q.observeBodyPhase = func(name string, _ time.Duration) {
		if name == "maintain" {
			calls++
		}
	}
	abort := errors.New("reject propagated decision")
	if err := q.update(func() error {
		q.maintain()
		q.touchJob(wave.ID).State = "failed"
		q.maintain() // A repeated request settles only changed relations.
		assertStates("failed")
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("full maintenance ran %d times in one rejected transaction", calls)
	}
	assertStates("queued")
	q = reopenQueue(t, q, clock)
	assertStates("queued")
	calls = 0
	q.observeBodyPhase = func(name string, _ time.Duration) {
		if name == "maintain" {
			calls++
		}
	}
	if err := q.update(func() error { q.maintain(); q.touchJob(wave.ID).State = "failed"; return nil }); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("full maintenance ran %d times in one committed transaction", calls)
	}
	assertStates("failed")
	q = reopenQueue(t, q, clock)
	assertStates("failed")
	for _, id := range []string{downstream.ID, gather.ID} {
		j := q.s.Jobs[id]
		if j.DiagnosticCode != "upstream_failed" || j.FinishedAt == nil || !j.FinishedAt.Equal(clock.Now()) {
			t.Fatalf("reopen lost dependent decision: %+v", j)
		}
	}
	if q.s.Jobs[producer.ID].Reason != "declared piece failed" {
		t.Fatal("reopen lost parent failure")
	}
}

func TestQueueMaintenanceOncePerCheckin(t *testing.T) {
	for _, devices := range []bool{false, true} {
		t.Run(map[bool]string{false: "scalar inventory", true: "device inventory"}[devices], func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("worker", "fake")
			if devices {
				m = m3Machine()
			}
			calls := 0
			q.observeBodyPhase = func(name string, _ time.Duration) {
				if name == "maintain" {
					calls++
				}
			}
			checkin := func(reports []AttemptReport) Reply {
				t.Helper()
				calls = 0
				c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: reports}
				r, err := q.Checkin(c)
				if err != nil {
					t.Fatal(err)
				}
				if calls != 1 {
					t.Fatalf("full maintenance ran %d times in one checkin", calls)
				}
				return r
			}
			checkin(nil)
			j := submit(t, q, spec("experiment"))
			r := checkin(nil)
			var assigned *Job
			for _, directive := range r.Attempts {
				if directive.Job != nil {
					assigned = directive.Job
				}
			}
			if assigned == nil || assigned.ID != j.ID {
				t.Fatalf("checkin lost assignment: state=%s reason=%q admission=%q", q.s.Jobs[j.ID].State, q.s.Jobs[j.ID].Reason, q.s.Machines[m.ID].AdmissionReason)
			}
			checkin([]AttemptReport{{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Session: assigned.Session, Started: true}})
		})
	}
}
