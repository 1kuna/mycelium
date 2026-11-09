package myce

import (
	"context"

	"testing"
	"time"
)

func TestProcessMemoryGrowthSustainedAdmissionAndPhysicalBound(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 16)
	m.CapabilityProfile.OS = "darwin"
	m.CapabilityProfile.SystemMemoryBytes = 16 << 30
	m.MemoryEnforcement = darwinRSSGuardStatus
	m.HostMemoryBudgetGB = 16
	j := Job{ID: "holder", State: "running", Attempt: 1, Machine: m.ID, Session: m.Session,
		Spec: cpuSpec(2, 2), MemoryEnvelope: &JobMemoryEnvelope{ProbeFloorGB: 2}}
	j.Spec.Needs = ResourceNeeds{Memory: "auto", CPUCores: 2, HostMemoryGB: 2, Platform: Platform{Required: []string{"cpu"}}}
	if err := j.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	var g processMemoryGrowth
	g.observe(clock.Now(), .9)
	clock.Advance(100 * time.Millisecond)
	margin := g.observe(clock.Now(), 1)
	// RSS supplies a footprint, never anonymous-resident credit. Exercise the
	// actual wire conversion: flat 0/0 must remain valid too.
	footprint := 1.0
	s := &CurrentHostMemory{FootprintGB: &footprint, GrowthObserved: g.horizon > 0, GrowthGB: margin, GrowthRateGB: g.rate}
	r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: s}
	sample := currentHostMemorySamples([]AttemptReport{r}, clock.Now())[jobKey(j)]
	clock.Advance(4 * time.Second)
	q.s.Machines[m.ID], q.s.Jobs[j.ID] = &m, &j
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: {jobKey(j): sample}}
	q.refreshHostMemory()
	if m.FreeHostMemoryGB >= 12 {
		t.Fatal("queue admitted unsafe neighbor", m.FreeHostMemoryGB)
	}
	w := Worker{Machine: m, Clock: clock, hostMemorySamples: map[attemptKey]hostMemorySample{jobKey(j): sample}, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
	next := Job{Spec: cpuSpec(2, 12)}
	if err := next.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := localAttemptAdmission(w, []Job{j}, next, nil); err == nil {
		t.Fatal("worker admitted unsafe neighbor")
	}
}

func TestProcessMemoryGrowthPhysicalBoundPreservesEvidenceAndLinux(t *testing.T) {
	m := cpuMachine("worker", "host", "cpu", 16, 16)
	m.CapabilityProfile.OS = "darwin"
	m.CapabilityProfile.SystemMemoryBytes = 16 << 30
	if n := boundedProcessMemoryCharge(&m, 2, 20, 120); n != 20 {
		t.Fatal("lost actual over-physical RSS", n)
	}
	if n := boundedProcessMemoryCharge(&m, 24, 4, 120); n != 24 {
		t.Fatal("lost known floor", n)
	}
}
