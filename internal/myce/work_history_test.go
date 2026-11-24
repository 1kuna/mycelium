package myce

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func workHistorySnapshot(t *testing.T, q *Queue) Snapshot {
	t.Helper()
	s, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

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
	m.MemoryEnforcement = cgroupMemoryEnforcement
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
		if o.Samples != 1 || o.LastSeconds != 2 {
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
	// Placement metadata can explain the host without inventing a measured
	// prediction or observation for an old profile.
	timing := q.s.Jobs[j.ID].Timing
	if !strings.HasPrefix(JobTiming(q.s.Jobs[j.ID]), "last accepted duration unknown; actual unknown") || len(q.s.WorkObservations) != 0 || timing != nil && (timing.LastAcceptedSeconds != nil || timing.ActualSeconds != nil) {
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
	e := registeredExtension(t, q, s.Extension)
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

func TestWorkObservationResolvedMemoryIdentity(t *testing.T) {
	m := cpuMachine("synthetic", "synthetic-host", "cpu", 8, 32)
	needs := ResourceNeeds{CPUCores: 1, Memory: "auto", MemoryGB: 4, HostMemoryGB: 8}
	key := func(n ResourceNeeds, args []string) string {
		k, reason := referenceObservationKey(Extension{}, &m, WorkReference{}, "extension:sample", n, args, nil, "cpu", "synthetic")
		if k == nil {
			t.Fatal(reason)
		}
		return identityHash(k)
	}
	first := key(needs, nil)
	// Different free room on a busy host changes the exploratory reservation.
	needs.MemoryGB, needs.HostMemoryGB = 2, 3
	if key(needs, nil) != first {
		t.Error("resolved memory split comparable timing evidence")
	}
	needs.CPUCores = 2
	if key(needs, nil) == first {
		t.Error("CPU requirement did not invalidate timing evidence")
	}
	needs.CPUCores = 1
	if key(needs, []string{"different-recipe"}) == first {
		t.Error("arguments did not invalidate timing evidence")
	}
}

func TestWorkObservationRetainsAcceptedTimingAfterOOM(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	accepted := startObservation(t, q, m, s)
	clock.Advance(2 * time.Second)
	finish(t, q, &accepted, true)
	key := q.s.Jobs[accepted.ID].Timing.Key
	failed := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&failed, false)
	r.OutOfMemory = true
	peak := .2
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if elapsed, ok := q.lastAcceptedSeconds(key); !ok || elapsed != 2 {
		t.Fatalf("OOM invalidated an earlier accepted duration: %g (valid=%t)", elapsed, ok)
	}
}
