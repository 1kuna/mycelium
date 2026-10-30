package myce

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func runtimeJob(t *testing.T, q *Queue, id string) *Job {
	t.Helper()
	snap, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range snap.Jobs {
		if j.ID == id {
			return j
		}
	}
	t.Fatal("job absent")
	return nil
}

func TestJobRuntimeElapsedRemainingAndNoHistory(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(10 * time.Second)
	r := runtimeJob(t, q, first.ID).Runtime
	if r == nil || *r.ElapsedSeconds != 10 || r.RemainingSeconds != nil || !strings.Contains(r.Reason, "no accepted matching measurement") {
		t.Fatalf("first run: %+v", r)
	}
	finish(t, q, &first, true)
	second := startObservation(t, q, m, s)
	clock.Advance(3 * time.Second)
	j := runtimeJob(t, q, second.ID)
	r = j.Runtime
	apiJob, err := q.jobSnapshot(j.ID)
	if err != nil || !reflect.DeepEqual(apiJob.Runtime, r) {
		t.Fatalf("job/status runtime mismatch: %+v / %+v, %v", apiJob, r, err)
	}
	if *r.ElapsedSeconds != 3 || r.RemainingSeconds == nil || *r.RemainingSeconds != 7 || r.Samples != 1 || r.Unit != "records" || r.Source == "" || *r.HistoryAgeSeconds != 3 {
		t.Fatalf("measured ETA: %+v", r)
	}
	if !strings.Contains(JobTiming(j), "remaining ~7.00s") {
		t.Fatal(JobTiming(j))
	}
	// Projection must not persist advancing elapsed time or change frozen timing.
	if q.s.Jobs[j.ID].Runtime != nil || *q.s.Jobs[j.ID].Timing.PredictedSeconds != 10 {
		t.Fatal("projection mutated state")
	}
	clock.Advance(7 * time.Second)
	r = runtimeJob(t, q, j.ID).Runtime
	if r.RemainingSeconds != nil || !strings.Contains(r.Reason, "estimate exceeded") {
		t.Fatalf("overrun promised completion: %+v", r)
	}
}

func TestJobRuntimeStaleAndNoncomparableHistory(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	finish(t, q, &first, true)
	// Keep the worker live rather than allowing loss/recovery to obscure history.
	for key, old := range q.s.WorkObservations {
		old.RecordedAt = clock.Now().Add(-31 * 24 * time.Hour)
		q.s.WorkObservations[key] = old
	}
	second := startObservation(t, q, m, s)
	r := runtimeJob(t, q, second.ID).Runtime
	if r.RemainingSeconds != nil || !strings.Contains(r.Reason, "older than 30 days") {
		t.Fatalf("stale history: %+v", r)
	}
	q.s.Jobs[second.ID].Timing.Reason = "mixed or unknown host occupancy"
	r = runtimeJob(t, q, second.ID).Runtime
	if !strings.Contains(r.Reason, "mixed or unknown host occupancy") {
		t.Fatalf("incompatible history: %+v", r)
	}
}

func TestJobRuntimePlainCommandAndStartedBoundary(t *testing.T) {
	q, clock := fixture(t)
	m := machine("a", "fake")
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	reply := check(t, q, m, nil)
	if reply.Job == nil {
		t.Fatal("not assigned")
	}
	r := runtimeJob(t, q, j.ID).Runtime
	if r.ElapsedSeconds != nil || !strings.Contains(r.Reason, "Started") {
		t.Fatalf("preparation counted: %+v", r)
	}
	if err := q.Started(Result{Machine: reply.Job.Machine, Session: reply.Job.Session, Execution: Execution{ID: j.ID, Attempt: reply.Job.Attempt}}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	r = runtimeJob(t, q, j.ID).Runtime
	if *r.ElapsedSeconds != 2 || !strings.Contains(r.Reason, "metadata unavailable") {
		t.Fatalf("plain command: %+v", r)
	}
}
