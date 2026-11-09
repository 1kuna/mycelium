package myce

import (
	"errors"
	"testing"
	"time"
)

func observationFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: .1}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("synthetic", "synthetic-host", "cpu", 8, 32)
	m.ExtensionFingerprint = "synthetic-cpu-v1"
	r := newExtensionReport(e, &m, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "synthetic-check"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	s := Spec{Name: "count", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	return q, clock, m, s
}

func startObservation(t *testing.T, q *Queue, m Machine, s Spec) Job {
	t.Helper()
	j := submit(t, q, s)
	r := check(t, q, m, nil)
	if r.Job == nil || r.Job.ID != j.ID {
		t.Fatalf("not assigned: %+v", r)
	}
	if err := q.Started(Result{Machine: r.Job.Machine, Session: r.Job.Session, Execution: Execution{ID: j.ID, Attempt: r.Job.Attempt}}); err != nil {
		t.Fatal(err)
	}
	return *r.Job
}

func TestWorkObservationRejectsFailedStaleAndDuplicate(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	failed := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	finish(t, q, &failed, false)
	if len(q.s.WorkObservations) != 0 {
		t.Fatal("failed attempt learned")
	}
	j := startObservation(t, q, m, s)
	stale := Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt + 1}, Success: true}
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatal("stale result", err)
	}
	if len(q.s.WorkObservations) != 0 {
		t.Fatal("stale attempt learned")
	}
	clock.Advance(2 * time.Second)
	finish(t, q, &j, true)
	finish(t, q, &j, true)
	for _, o := range q.s.WorkObservations {
		if o.Samples != 1 || o.Seconds != 2 {
			t.Fatal("duplicate learned", o)
		}
	}
	if len(q.s.WorkObservations) != 1 {
		t.Fatal("accepted attempt missing")
	}
}
