package myce

import (
	"encoding/json"
	"errors"
	"strings"
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

func TestWorkObservationAcceptedTimingAndPersistence(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	clock.Advance(2 * time.Second)
	finish(t, q, &j, true)
	if len(q.s.WorkObservations) != 1 {
		t.Fatalf("observations: %+v; timing: %+v", q.s.WorkObservations, q.s.Jobs[j.ID].Timing)
	}
	var observed WorkObservation
	for _, o := range q.s.WorkObservations {
		observed = o
	}
	if observed.Key.Work != "extension:sample" || observed.Key.Target.Platform != "cpu" || observed.Key.Host != "synthetic-host" || observed.Key.Component != "cpu" || observed.Key.Reference.InputShape != "small-v1" || observed.Key.Concurrency != 1 || observed.Key.BatchSize != 1 || observed.Seconds != 2 || observed.Units != 10 || observed.Samples != 1 || observed.Job != j.ID || observed.Attempt != j.Attempt {
		t.Fatalf("wrong observation: %+v", observed)
	}
	if !observed.RecordedAt.Equal(clock.Now()) {
		t.Fatal("not queue Clock time")
	}
	var rows int
	if err := q.db.QueryRow("SELECT count(*) FROM queue_state WHERE key LIKE 'work_observations/%'").Scan(&rows); err != nil || rows != 1 {
		t.Fatal("per-record persistence", rows, err)
	}
	second := startObservation(t, q, m, s)
	timing := q.s.Jobs[second.ID].Timing
	if timing.PredictedSeconds == nil || *timing.PredictedSeconds != 2 || timing.Samples != 1 {
		t.Fatalf("prediction: %+v", timing)
	}
	clock.Advance(3 * time.Second)
	finish(t, q, &second, true)
	before := q.s.Jobs[second.ID].Timing
	if before.ActualSeconds == nil || *before.ActualSeconds != 3 {
		t.Fatalf("actual: %+v", before)
	}
	var changesBefore, changesAfter int
	q.db.QueryRow("SELECT total_changes()").Scan(&changesBefore)
	snap, err := q.snapshot(true)
	if err != nil {
		t.Fatal(err)
	}
	q.db.QueryRow("SELECT total_changes()").Scan(&changesAfter)
	if changesBefore != changesAfter {
		t.Fatal("status mutated SQLite")
	}
	found := false
	for _, job := range snap.Jobs {
		if job.ID == second.ID {
			found = strings.Contains(JobTiming(job), "prediction 2.00s (1 samples); actual 3.00s")
		}
	}
	if !found {
		t.Fatal("status lost prediction/actual")
	}
	var database string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(database, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(reopened.s.WorkObservations) != 1 || reopened.s.Jobs[second.ID].Timing.ActualSeconds == nil {
		t.Fatal("observations not restored")
	}
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

func TestWorkObservationUnknownAndInvalidation(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	var old CapabilityProfile
	if err := json.Unmarshal([]byte(`{"version":0,"os":"linux","arch":"amd64","logical_cpus":8,"memory_layout":"separate","system_memory_bytes":34359738368,"benchmark":{"state":"complete","measurements":[{"resource":"cpu","rate":1000,"unit":"records/s"}]}}`), &old); err != nil {
		t.Fatal(err)
	}
	m.CapabilityProfile = &old
	legacy := s
	legacy.Workload = nil
	j := startObservation(t, q, m, legacy)
	clock.Advance(time.Second)
	finish(t, q, &j, true)
	if JobTiming(q.s.Jobs[j.ID]) != "prediction unknown; actual unknown" || len(q.s.WorkObservations) != 0 {
		t.Fatal("old profile invented comparable evidence")
	}
	j = startObservation(t, q, m, s)
	clock.Advance(time.Second)
	finish(t, q, &j, true)
	if len(q.s.WorkObservations) != 1 {
		t.Fatal("seed missing")
	}
	m.ExtensionFingerprint = "changed-cpu"
	check(t, q, m, nil)
	waiting := submit(t, q, s)
	if got := check(t, q, m, nil); got.Job != nil {
		t.Fatal("changed fingerprint reused qualification", waiting.ID)
	}
	e := q.s.Extensions[s.Extension]
	changed, err := extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	changed.Variants[0].References = map[string]WorkReference{"extension:sample": {InputShape: "small-v2", Unit: "records", Units: 10}}
	changed = freezeComposition(t, changed)
	if changed.Digest == e.Digest || extensionQualificationReason(changed, m.ExtensionReports[0], &m) == "" {
		t.Fatal("reference change kept qualification")
	}
}

func TestWorkObservationMixedHostIntervalExcluded(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	other := cpuSpec(1, .1)
	other.Name = "competing"
	competing := submit(t, q, other)
	// Use machine inventory so ordinary concurrent CPU admission is enabled.
	// Direct assign is the existing queue transaction seam, no fake timing API.
	if err := q.update(func() error { q.assign(q.s.Jobs[competing.ID], q.s.Machines[m.ID]); return nil }); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	finish(t, q, q.s.Jobs[competing.ID], true)
	clock.Advance(time.Second)
	finish(t, q, &j, true)
	if len(q.s.WorkObservations) != 0 || q.s.Jobs[j.ID].Timing.Reason != "mixed or unknown host occupancy" {
		t.Fatal("mixed interval learned", q.s.Jobs[j.ID].Timing)
	}
}
