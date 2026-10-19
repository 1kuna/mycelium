package myce

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func projectSpec(project string) Spec { s := spec("experiment"); s.Project = project; return s }
func TestFairSharingUsesClockAndExplains(t *testing.T) {
	q, c := fixture(t)
	a := submit(t, q, projectSpec("verso"))
	c.Advance(time.Second)
	b := submit(t, q, projectSpec("atlas"))
	m := machine("fake", "fake")
	if e := q.update(func() error {
		q.s.FairUsage = map[string]FairUsage{fairUsageKey("experiment", "gpu", "verso"): {Seconds: 100, At: c.Now()}}
		q.s.Machines[m.ID] = &m
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	s, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	for _, j := range s.Jobs {
		if j.ID == a.ID && !strings.Contains(j.Reason, "atlas goes first (verso is ahead of its fair share)") {
			t.Fatal(j.Reason)
		}
	}
	if got := check(t, q, m, nil).Job; got == nil || got.ID != b.ID {
		t.Fatalf("wanted atlas, got %+v", got)
	}
	c.Advance(24 * time.Hour)
	if got := decayed(FairUsage{Seconds: 100, At: c.Now().Add(-24 * time.Hour)}, c.Now()); math.Abs(got-50) > 1e-9 {
		t.Fatal(got)
	}
}
func TestFairWeightsDeficitAndFit(t *testing.T) {
	q, c := fixture(t)
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"verso": {Weight: 3}, "atlas": {Weight: 1}}); e != nil {
		t.Fatal(e)
	}
	a := submit(t, q, projectSpec("verso"))
	b := submit(t, q, projectSpec("atlas"))
	m := machine("fake", "fake")
	q.s.FairUsage = map[string]FairUsage{fairUsageKey("experiment", "gpu", "verso"): {Seconds: 90, At: c.Now()}, fairUsageKey("experiment", "gpu", "atlas"): {Seconds: 60, At: c.Now()}}
	if got := q.pick(&m); got.ID != a.ID {
		t.Fatal("weighted deficit should prefer verso", got)
	}
	q.s.Jobs[a.ID].Spec.Needs.MemoryGB = 100
	if got := q.pick(&m); got.ID != b.ID {
		t.Fatal("non-fitting project must not block fitting one", got)
	}
	q.s.Jobs[b.ID].State = "succeeded"
	q.s.Jobs[a.ID].Spec.Needs.MemoryGB = 8
	if got := q.pick(&m); got.ID != a.ID {
		t.Fatal("sole project may consume all capacity")
	}
}
func TestFairnessNeverOverridesProductionOrDeadline(t *testing.T) {
	q, c := fixture(t)
	a := submit(t, q, projectSpec("atlas"))
	p := spec("production")
	p.Project = "verso"
	production := submit(t, q, p)
	d := spec("deadline")
	d.Project = "verso"
	deadline := submit(t, q, d)
	q.s.FairUsage = map[string]FairUsage{fairUsageKey("experiment", "gpu", "verso"): {Seconds: 1e9, At: c.Now()}}
	m := machine("fake", "fake")
	if q.pick(&m).ID != production.ID {
		t.Fatal("production lost")
	}
	q.s.Jobs[production.ID].State = "succeeded"
	if q.pick(&m).ID != deadline.ID {
		t.Fatal("deadline lost")
	}
	q.s.Jobs[deadline.ID].State = "succeeded"
	if q.pick(&m).ID != a.ID {
		t.Fatal("experiment missing")
	}
}
func TestExperimentCapsAndProductionBypass(t *testing.T) {
	q, _ := fixture(t)
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"verso": {Weight: 1, MaxConcurrentExperiments: 1}}); e != nil {
		t.Fatal(e)
	}
	a := submit(t, q, projectSpec("verso"))
	b := submit(t, q, projectSpec("verso"))
	m := machine("one", "fake")
	got := check(t, q, m, nil).Job
	if got == nil {
		t.Fatal("no assignment")
	}
	other := a
	if got.ID == a.ID {
		other = b
	}
	if !strings.Contains(q.capReason(q.s.Jobs[other.ID]), "concurrent experiment cap") {
		t.Fatal("missing cap reason")
	}
	n := machine("two", "fake")
	if check(t, q, n, nil).Job != nil {
		t.Fatal("cap exceeded")
	}
	p := spec("production")
	p.Project = "verso"
	production := submit(t, q, p)
	if check(t, q, n, nil).Job.ID != production.ID {
		t.Fatal("production capped")
	}
}
func TestMemoryShareCap(t *testing.T) {
	q, _ := fixture(t)
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 1, MaxMemoryShare: .25}}); e != nil {
		t.Fatal(e)
	}
	j := submit(t, q, projectSpec("atlas"))
	m := machine("one", "fake")
	m.TotalMemoryGB = 16
	check(t, q, m, nil)
	if !strings.Contains(q.capReason(q.s.Jobs[j.ID]), "25%") {
		t.Fatal("memory cap not explained")
	}
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 1, MaxMemoryShare: .5}}); e != nil {
		t.Fatal(e)
	}
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("boundary share must fit")
	}
}

func TestModelCallMemoryCapCountsSharedModelOnce(t *testing.T) {
	q, _, m := modelFixture(t)
	m.TotalMemoryGB = 8
	q.s.Machines[m.ID].TotalMemoryGB = 8
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 1, MaxMemoryShare: 1}}); e != nil {
		t.Fatal(e)
	}
	q.calls["active"] = &ModelCall{ID: "active", Project: "atlas", Priority: "experiment", Model: "test", Machine: m.ID, Session: m.Session}
	next := callJob(&ModelCall{ID: "next", Project: "atlas", Priority: "experiment", Model: "test"})
	if reason := q.capReasonFor(next, &m); reason != "" {
		t.Fatal("same reservation counted twice", reason)
	}
	other := machine("two", "fake")
	if reason := q.capReasonFor(next, &other); reason == "" {
		t.Fatal("new device reservation bypassed memory cap")
	}
}
func TestStatusIncludesQueuedModelCalls(t *testing.T) {
	q, _, m := modelFixture(t)
	call := addModelCall(t, q, "experiment")
	s, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, j := range s.Jobs {
		if j.ID == call.ID {
			found = true
			if j.Spec.Project != "test" || j.Reason == "" {
				t.Fatal(j)
			}
		}
	}
	if !found {
		t.Fatal("queued call omitted", m.ID)
	}
}

func TestProjectPolicyValidation(t *testing.T) {
	q, _ := fixture(t)
	for _, p := range []ProjectPolicy{{Weight: -1}, {Weight: math.NaN()}, {Weight: 1, MaxConcurrentExperiments: -1}, {Weight: 1, MaxMemoryShare: 1.1}} {
		if e := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": p}); e == nil {
			t.Fatal("invalid policy accepted", p)
		}
	}
	path := filepath.Join(t.TempDir(), "projects.yaml")
	if e := os.WriteFile(path, []byte("atlas:\n  weight: 2\n  max_concurrent_experiments: 1\n"), 0600); e != nil {
		t.Fatal(e)
	}
	p, e := ReadProjectPolicies(path)
	if e != nil || p["atlas"].Weight != 2 {
		t.Fatal(p, e)
	}
	if e = os.WriteFile(path, []byte("atlas:\n  weight: 2\n---\nverso:\n  weight: 3\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ReadProjectPolicies(path); e == nil {
		t.Fatal("multiple configs silently accepted")
	}
}

func TestBusyStatusUsesDispatchOrderAndCombinedReasons(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake")
	prod := submit(t, q, spec("production"))
	running := check(t, q, m, nil).Job
	if running == nil || running.ID != prod.ID {
		t.Fatal("production not running")
	}
	a := submit(t, q, projectSpec("verso"))
	c.Advance(time.Second)
	b := submit(t, q, projectSpec("atlas"))
	c.Advance(time.Second)
	d := submit(t, q, projectSpec("atlas"))
	q.s.FairUsage = map[string]FairUsage{fairUsageKey("experiment", "gpu", "verso"): {Seconds: 100, At: c.Now()}}
	s, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	if len(s.Jobs) != 4 || s.Jobs[0].ID != prod.ID || s.Jobs[1].ID != b.ID || s.Jobs[2].ID != d.ID || s.Jobs[3].ID != a.ID {
		t.Fatalf("order: %+v", s.Jobs)
	}
	if s.Jobs[1].Reason != "waiting for a free machine; next in line" {
		t.Fatal(s.Jobs[1].Reason)
	}
	if s.Jobs[3].Reason != "waiting for a free machine; atlas goes first (verso is ahead of its fair share)" {
		t.Fatal(s.Jobs[3].Reason)
	}
	c.Advance(75 * time.Second)
	if e = q.Complete(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: prod.ID, Attempt: running.Attempt}, Success: true}); e != nil {
		t.Fatal(e)
	}
	got := check(t, q, m, nil).Job
	if got == nil || got.ID != b.ID {
		t.Fatalf("status winner differs from dispatch: %+v", got)
	}
}

func TestProjectionUsesAffinityDeadlineAndDoesNotMutateJobs(t *testing.T) {
	q, c := fixture(t)
	a := submit(t, q, projectSpec("atlas"))
	c.Advance(time.Second)
	s := projectSpec("atlas")
	s.Needs.Model = "cached"
	b := submit(t, q, s)
	m := machine("one", "fake")
	m.CachedModels = []string{"cached"}
	q.s.Machines[m.ID] = &m
	order, _ := q.queueProjection()
	if order[b.ID] != 1 || order[a.ID] != 2 || q.s.Jobs[b.ID].State != "queued" || q.pick(&m).ID != b.ID {
		t.Fatal(order)
	}
}

func TestProjectionIncludesEligibleModelCalls(t *testing.T) {
	q, c, m := modelFixture(t)
	q.s.Machines[m.ID] = &m
	q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: "ready", Priority: "production"}
	batch := submit(t, q, projectSpec("verso"))
	call := &ModelCall{ID: "call", Model: "test", Project: "atlas", Priority: "production", Queued: c.Now()}
	q.calls[call.ID] = call
	order, reasons := q.queueProjection()
	if order[call.ID] != 1 || order[batch.ID] != 2 || !strings.Contains(reasons[batch.ID], "production work goes first") {
		t.Fatal(order, reasons)
	}
	if q.s.Jobs[batch.ID].State != "queued" || call.Machine != "" {
		t.Fatal("projection assigned work")
	}
}
