package myce

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// burstGapSample drives the worker's real per-check-in cgroup sampler: use
// grows from 0.5 to 3.5 GiB within one 1 s check-in, then one check-in
// arrives 30 s later at 3.06 GiB. It returns the queue's sample of the last
// report.
func burstGapSample(t *testing.T, clock *fakeClock, job *Job) hostMemorySample {
	t.Helper()
	dir := t.TempDir()
	a := &memoryAttempt{fs: osMemoryFS{}, path: dir}
	var samples map[attemptKey]hostMemorySample
	report := func(gb float64) {
		bytes := int64(gb * (1 << 30))
		if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte(fmt.Sprintf("anon %d\nkernel 0\nshmem 0\nunevictable 0\nfile_dirty 0\nfile_writeback 0\n", bytes)), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte(fmt.Sprint(bytes)), 0600); err != nil {
			t.Fatal(err)
		}
		r := AttemptReport{Execution: Execution{ID: job.ID, Attempt: job.Attempt}, Session: job.Session, Started: true, CurrentHostMemory: a.currentHostMemory(clock)}
		samples = updateHostMemorySamples([]AttemptReport{r}, clock.Now(), samples)
	}
	report(.5)
	clock.Advance(time.Second)
	report(3.5)
	clock.Advance(30 * time.Second)
	report(3.0625)
	return samples[jobKey(*job)]
}

// Live incident, 2026-11-12, on a unified CUDA host with a 113.064 GiB
// allowance and 121.06 GiB physical RAM: a bounded auto gate (9.5 GiB
// envelope, about 3 GiB used) was charged 113.064 GiB, and a declared unified
// device job (48.125 GiB) about 121 GB. The ledger showed 0 GiB free with
// 82.8 GiB live, and another project's job waited. One burst and one long
// check-in gap reproduce both figures exactly. The worker reported growth as
// the burst's rate times the gap (90 GiB), and the queue projected that rate
// across a freshness window the gap had stretched to 60 s. Growth measured
// within one interval (3 GiB) now bounds both, so each job is charged its
// envelope.
func TestForecastBurstGapKeepsEnvelopeCharge(t *testing.T) {
	_, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 20, 82.8)
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB, m.MemoryEnforcement = 113.0640640258789, .125, cgroupMemoryEnforcement
	m.CapabilityProfile.SystemMemoryBytes = 129992077312
	started := clock.Now()
	j := &Job{ID: "gate", Attempt: 1, Session: m.Session, Machine: m.ID, State: "running", StartedAt: &started, Spec: cpuSpec(2, 0)}
	j.Spec.Needs = ResourceNeeds{CPUCores: 2, Memory: "auto", HostMemoryGB: 9.371604919433594, Platform: Platform{Required: []string{"cpu"}}}
	j.MemoryEnvelope = &JobMemoryEnvelope{Basis: "exploratory", Evidence: 212, HostGB: 9.496604919433594, Enforcement: learnedMemoryGuardEnforcement}
	s := burstGapSample(t, clock, j)
	if !near(s.growthGB, 3) || !near(s.rateGB, 3) {
		t.Fatalf("reported growth %g GiB at %g GiB/s, want the 3 GiB measured within one interval", s.growthGB, s.rateGB)
	}
	unified := m
	unified.UnifiedMemory = true
	d := &Job{ID: "device", Attempt: 1, Session: m.Session, Machine: m.ID, State: "running", StartedAt: &started, Spec: cpuSpec(2, 0)}
	d.Spec.Needs = ResourceNeeds{CPUCores: 2, MemoryGB: 32, HostMemoryGB: 16, Platform: Platform{Required: []string{"cuda"}}}
	d.MemoryEnvelope = &JobMemoryEnvelope{Basis: "declared", HostGB: 48.125, DeviceGB: 32}
	ds := burstGapSample(t, clock, d)
	for _, age := range []time.Duration{0, 5 * time.Second, 20 * time.Second, 59 * time.Second} {
		now := clock.Now().Add(age)
		if charge, _ := memoryCharge(j, &m, hostNeed(&m, 0, j.Spec.Needs.HostMemoryGB), s, now); !near(charge, j.MemoryEnvelope.HostGB) {
			t.Fatalf("auto job charged %g GiB at sample age %s, want its %g GiB envelope", charge, age, j.MemoryEnvelope.HostGB)
		}
		if charge, _ := memoryCharge(d, &unified, hostNeed(&unified, 32, 16), ds, now); !near(charge, 48.125) {
			t.Fatalf("device job charged %g GiB at sample age %s, want its 48.125 GiB envelope", charge, age)
		}
	}
}

// A job that keeps growing is still forecast at its measured rate: until its
// sample expires (two intervals), the forecast covers what it really reaches.
func TestForecastSteadyGrowthStillCovered(t *testing.T) {
	_, clock := fixture(t)
	dir := t.TempDir()
	a := &memoryAttempt{fs: osMemoryFS{}, path: dir}
	j := &Job{ID: "grow", Attempt: 1, Session: "s"}
	var samples map[attemptKey]hostMemorySample
	gb := 1.0
	for range 4 {
		bytes := int64(gb * (1 << 30))
		if err := os.WriteFile(filepath.Join(dir, "memory.stat"), []byte(fmt.Sprintf("anon %d\nkernel 0\nshmem 0\nunevictable 0\nfile_dirty 0\nfile_writeback 0\n", bytes)), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte(fmt.Sprint(bytes)), 0600); err != nil {
			t.Fatal(err)
		}
		r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: a.currentHostMemory(clock)}
		samples = updateHostMemorySamples([]AttemptReport{r}, clock.Now(), samples)
		clock.Advance(time.Second)
		gb += .5 // 0.5 GiB/s, sampled every second.
	}
	s := samples[jobKey(*j)]
	last := clock.Now().Add(-time.Second)
	for _, age := range []time.Duration{0, time.Second, 2*time.Second - time.Millisecond} {
		reached := 2.5 + .5*age.Seconds()
		if forecast := s.hostForecast(last.Add(age)); forecast < reached {
			t.Fatalf("forecast %g GiB at age %s is below the %g GiB the job reaches", forecast, age, reached)
		}
	}
}
