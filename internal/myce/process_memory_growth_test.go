package myce

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestProcessMemoryGrowthReplaysDelayedFlatInflation(t *testing.T) {
	// Synthetic timing reproduces the scale of retained summaries, not their
	// historical cause. Each trace grows 1 GiB in one second, then stays flat
	// across a delayed observation. The original estimator crosses the burst
	// rate with that unrelated flat interval.
	for _, tc := range []struct{ peak, delay float64 }{{4.35, 107.6}, {2.6, 19.9}, {4.4, 15.5}} {
		_, clock := fixture(t)
		var old memoryGrowth
		var fixed processMemoryGrowth
		old.observe(clock.Now(), tc.peak-1)
		fixed.observe(clock.Now(), tc.peak-1)
		clock.Advance(time.Second)
		old.observe(clock.Now(), tc.peak)
		burst := fixed.observe(clock.Now(), tc.peak)
		peak := sampledMemoryPeak(nil, &AttemptMemoryPeak{HostGB: &tc.peak, GrowthGB: &burst})
		clock.Advance(time.Duration(tc.delay * float64(time.Second)))
		inflated := old.observe(clock.Now(), tc.peak)
		flat := fixed.observe(clock.Now(), tc.peak)
		peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{HostGB: &tc.peak, GrowthGB: &flat})
		if math.Abs(inflated-tc.delay) > 1e-8 || flat != 0 || fixed.rate != 0 || fixed.horizon == 0 || memoryGrowthGB(peak) != 1 {
			t.Fatalf("old=%g live=%g rate=%g summary=%g", inflated, flat, fixed.rate, memoryGrowthGB(peak))
		}
		t.Logf("established synthetic replay: peak %.2f GiB, old growth %.1f GiB, fixed flat growth/rate 0, retained observed increment 1 GiB", tc.peak, inflated)
	}
}

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
	charge, growth := jobHostMemoryCharge(&j, &m, 2, sample, clock.Now())
	if math.Abs(charge-(5.1+exploratoryDriftMarginGB(&m))) > 1e-8 || growth != charge || charge <= 2 {
		t.Fatalf("lost sustained growth or clipped at estimate: charge=%g growth=%g", charge, growth)
	}
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
	// A still-positive extreme slope may consume all physical room, but cannot
	// invent hundreds of GiB of physical growth on this 16 GiB host.
	sample.rateGB = 100
	charge, growth = jobHostMemoryCharge(&j, &m, 2, sample, clock.Now())
	if charge != 16 || growth != 16 {
		t.Fatalf("physical bound credited room: charge=%g growth=%g", charge, growth)
	}
	g.observe(clock.Now(), 1)
	s.GrowthGB, s.GrowthRateGB = 0, g.rate
	sample = currentHostMemorySamples([]AttemptReport{r}, clock.Now())[jobKey(j)]
	if !sample.growthObserved || sample.gb != 0 {
		t.Fatal("flat sample invalidated or RSS credited", sample)
	}
	charge, growth = jobHostMemoryCharge(&j, &m, 2, sample, clock.Now())
	if charge != 2 || growth != 2 {
		t.Fatal("flat sample did not preserve known floor", charge, growth)
	}
	clock.Advance(hostMemorySampleFreshFor)
	charge, growth = jobHostMemoryCharge(&j, &m, 16, sample, clock.Now())
	if charge != 16 || growth != 16 {
		t.Fatal("stale sample released conservative charge", charge, growth)
	}
}

func TestProcessMemoryGrowthPhysicalBoundPreservesEvidenceAndLinux(t *testing.T) {
	_, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 16)
	m.CapabilityProfile.OS = "darwin"
	m.CapabilityProfile.SystemMemoryBytes = 16 << 30
	if n := boundedProcessMemoryCharge(&m, 2, 20, 120); n != 20 {
		t.Fatal("lost actual over-physical RSS", n)
	}
	if n := boundedProcessMemoryCharge(&m, 24, 4, 120); n != 24 {
		t.Fatal("lost known floor", n)
	}
	m.CapabilityProfile.SystemMemoryBytes = 0
	if n := boundedProcessMemoryCharge(&m, 2, 4, 120); n != 120 {
		t.Fatal("fabricated physical bound", n)
	}
	m.CapabilityProfile.SystemMemoryBytes = 16 << 30
	m.CapabilityProfile.OS = "linux"
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	if n := boundedProcessMemoryCharge(&m, 2, 4, 120); n != 120 {
		t.Fatal("changed hard-cgroup forecast", n)
	}
	// The existing Linux estimator deliberately keeps its original semantics.
	var linux memoryGrowth
	linux.observe(clock.Now(), 1)
	clock.Advance(time.Second)
	linux.observe(clock.Now(), 2)
	clock.Advance(100 * time.Second)
	if n := linux.observe(clock.Now(), 2); n != 100 || linux.rate != 1 {
		t.Fatal("changed Linux estimator", n, linux.rate)
	}
}
