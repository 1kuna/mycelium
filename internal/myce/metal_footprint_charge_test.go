package myce

import (
	"testing"
	"time"
)

// A Darwin Metal worker whose guard samples phys_footprint.
func metalFootprintMachine() Machine {
	m := cpuMachine("mac", "mac", "metal", 12, 40)
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
	m.UnifiedMemory, m.MemoryEnforcement, m.MemoryCeilingSlackGB = true, darwinRSSGuardStatus, .125
	m.Capabilities = append(m.Capabilities, metalFootprintGuardCapability)
	return m
}

// A started Metal attempt declaring 8 GiB device plus 2 GiB host, whose work
// has accepted peaks on this target of 1 GiB RSS plus 5 GiB of footprint
// beyond it.
func metalFootprintJob(at time.Time) Job {
	j := Job{ID: "metal", Attempt: 1, Session: "s", State: "running", StartedAt: &at, Spec: spec("experiment")}
	j.Spec.Needs = ResourceNeeds{MemoryGB: 8, HostMemoryGB: 2, Platform: Platform{Required: []string{"metal"}}}
	j.MemoryEnvelope = &JobMemoryEnvelope{Basis: "declared", HostGB: 2, Learned: &LearnedMemory{HostGB: 9, Evidence: 3, Source: "test", PeakHostGB: 1, PeakDeviceGB: 5}}
	return j
}

// Metal buffers are invisible to RSS but counted in phys_footprint (#1044).
// With a fresh footprint sample and a footprint-learned peak, Metal work is
// charged max(peak, high water, forecast) plus drift and the guard's measured
// overshoot, bounded by its unified envelope, and its footprint is credited
// once. Without any of those it keeps its envelope.
func TestMetalUsedMemoryCharge(t *testing.T) {
	_, clock := fixture(t)
	at := clock.Now()
	m := metalFootprintMachine()
	ceiling := hostNeed(&m, 8, 2) // 10.125 GiB: device + host + slack.
	drift := exploratoryDriftMarginGB(&m)
	for _, tc := range []struct {
		name           string
		sample         hostMemorySample
		charge, growth float64
	}{
		{"below accepted peak", hostMemorySample{footprintGB: 3, peakGB: 3, at: at}, 6 + drift, 3 + drift},
		{"growing past peak", hostMemorySample{footprintGB: 7, peakGB: 7, growthGB: .5, rateGB: 1, at: at},
			7.5 + drift + darwinGuardReaction.Seconds(), .5 + drift + darwinGuardReaction.Seconds()},
		{"held at envelope plus overshoot", hostMemorySample{footprintGB: 10, peakGB: 10, growthGB: 2, rateGB: 2, at: at},
			ceiling + 2*darwinGuardReaction.Seconds(), ceiling + 2*darwinGuardReaction.Seconds() - 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := metalFootprintJob(at)
			charge, growth := memoryCharge(&j, &m, ceiling, tc.sample, at)
			if !near(charge, tc.charge) || !near(growth, tc.growth) {
				t.Fatalf("charge=%g growth=%g, want %g/%g", charge, growth, tc.charge, tc.growth)
			}
		})
	}

	sample := hostMemorySample{footprintGB: 3, peakGB: 3, at: at}
	for _, tc := range []struct {
		name string
		edit func(*Job, *Machine, *hostMemorySample)
	}{
		{"worker without footprint guard", func(_ *Job, m *Machine, _ *hostMemorySample) { m.Capabilities = nil }},
		{"no accepted footprint peak", func(j *Job, _ *Machine, _ *hostMemorySample) { j.MemoryEnvelope.Learned.PeakDeviceGB = 0 }},
		{"no host declaration", func(j *Job, _ *Machine, _ *hostMemorySample) { j.Spec.Needs.HostMemoryGB = 0 }},
		{"unmeasured host", func(_ *Job, m *Machine, _ *hostMemorySample) { m.HostMemoryMeasured = false }},
		{"unguarded host", func(_ *Job, m *Machine, _ *hostMemorySample) {
			m.MemoryEnforcement = "memory ceiling not enforced on this host (requires Linux cgroup v2)"
		}},
		{"stale sample", func(_ *Job, _ *Machine, s *hostMemorySample) { s.at = at.Add(-2 * hostMemorySampleFreshFor) }},
		{"no footprint", func(_ *Job, _ *Machine, s *hostMemorySample) { s.footprintGB = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j, mm, s := metalFootprintJob(at), metalFootprintMachine(), sample
			tc.edit(&j, &mm, &s)
			if _, lend := lendingFloor(&j, &mm, s, at); lend != lendNone {
				t.Fatal("charged by use without footprint containment")
			}
		})
	}
}

// Metal growth into lent memory is caught by the #1296 backstop: a growing
// experiment yields before production when free memory cannot absorb it.
func TestMetalFootprintOvercommitYields(t *testing.T) {
	q, clock := fixture(t)
	m := metalFootprintMachine()
	m.HostMemoryBudgetGB, m.FreeMemoryGB, m.TotalMemoryGB = 40, 40, 40
	m.Devices = []MachineDevice{{ID: "0", Platform: "metal", Identity: "Apple Silicon", Fingerprint: "Apple Silicon", Selector: "0", MemoryMode: "unified", UnifiedMemory: true, TotalMemoryGB: 40, BudgetGB: 40, FreeMemoryGB: 40, MemoryMeasured: true}}
	prod := cpuSpec(2, 12)
	prod.Priority, prod.Project = "production", "prod"
	gpu := spec("experiment")
	gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 16, 2
	gpu.Needs.Platform = Platform{Required: []string{"metal"}}
	var jobs []*Job
	for _, s := range []Spec{prod, gpu} {
		j := submit(t, q, s)
		a := assignedJob(machineCheck(t, q, m, jobs...))
		if a == nil {
			t.Fatal("job not assigned", job(t, q, j.ID).Reason)
		}
		if err := q.Started(result(a, false)); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		e := *q.s.Jobs[a.ID].MemoryEnvelope
		e.Learned = &LearnedMemory{HostGB: 4, Evidence: 4, Source: "test", PeakHostGB: 1, PeakDeviceGB: 3}
		q.s.Jobs[a.ID].MemoryEnvelope = &e
		q.mu.Unlock()
		jobs = append(jobs, job(t, q, a.ID))
		clock.Advance(time.Second)
	}
	report := func(metal, rate, available float64) {
		t.Helper()
		clock.Advance(time.Second)
		c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
		c.Machine.HostAvailableMemoryGB = available
		for i, j := range jobs {
			fp, growth := 4.0, 0.0
			if i == 1 {
				fp, growth = metal, rate
			}
			s := &CurrentHostMemory{FootprintGB: &fp, GrowthGB: growth, GrowthRateGB: rate, GrowthObserved: true, sampledAt: clock.Now()}
			c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: s})
		}
		if _, err := q.Checkin(c); err != nil {
			t.Fatal(err)
		}
	}
	report(4, 0, 30)
	q.mu.Lock()
	owners := q.hostMemoryLedgers[m.ID].Owners
	q.mu.Unlock()
	charged := false
	for _, o := range owners {
		charged = charged || o.Execution != nil && o.Execution.ID == jobs[1].ID && o.ChargeGB < hostNeed(&m, 16, 2)
	}
	if !charged {
		t.Fatalf("Metal job not charged by use: %+v", owners)
	}
	for _, j := range jobs {
		if got := job(t, q, j.ID); got.State != "running" {
			t.Fatal("steady work yielded", got.ID, got.State, got.Reason)
		}
	}
	// Metal buffers grow at 1 GiB/s while free memory falls to 3 GiB.
	report(12, 1, 3)
	if got := job(t, q, jobs[0].ID); got.State != "running" {
		t.Fatal("production stopped", got.State, got.Reason)
	}
	victim := job(t, q, jobs[1].ID)
	if stop := priorityStop(victim); victim.State != "stopping" || stop == nil || stop.Cause != memoryOvercommitCause {
		t.Fatalf("growing Metal experiment did not yield: %s %+v %q", victim.State, stop, victim.Reason)
	}
}

// A Metal attempt's footprint split (RSS host share, footprint beyond it as
// the device share) is accepted unified evidence, and the rerun records its
// peaks as the by-use floor. An RSS-only peak, which never saw the buffers, is
// not accepted for unified GPU work.
func TestMetalFootprintPeakLearned(t *testing.T) {
	for _, footprint := range []bool{true, false} {
		t.Run(map[bool]string{true: "footprint", false: "rss only"}[footprint], func(t *testing.T) {
			q, clock := fixture(t)
			m := metalFootprintMachine()
			m.HostMemoryBudgetGB, m.FreeMemoryGB, m.TotalMemoryGB = 40, 40, 40
			m.ExtensionFingerprint = "metal-v1"
			m.Devices = []MachineDevice{{ID: "0", Platform: "metal", Identity: "Apple Silicon", Fingerprint: "Apple Silicon", Selector: "0", MemoryMode: "unified", UnifiedMemory: true, TotalMemoryGB: 40, BudgetGB: 40, FreeMemoryGB: 40, MemoryMeasured: true, ExtensionFingerprint: "metal-v1"}}
			machineCheck(t, q, m)
			s := spec("experiment")
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = 8, 2
			s.Needs.Platform = Platform{Required: []string{"metal"}}
			submit(t, q, s)
			first := assignedJob(machineCheck(t, q, m))
			if first == nil {
				t.Fatal("first run not assigned")
			}
			if err := q.Started(result(first, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			host, device := 1.25, 5.5
			r := result(first, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, Source: processGroupMemorySource}
			if footprint {
				r.MemoryPeak.DeviceGB, r.MemoryPeak.DeviceSource = &device, deviceFootprintSource
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			submit(t, q, s)
			next := assignedJob(machineCheck(t, q, m))
			if next == nil {
				t.Fatal("rerun not assigned")
			}
			l := next.MemoryEnvelope.Learned
			if !footprint {
				if l != nil && l.PeakDeviceGB > 0 {
					t.Fatalf("RSS-only peak accepted: %+v", l)
				}
				return
			}
			if l == nil || l.Evidence < 1 || l.PeakHostGB != host || l.PeakDeviceGB != device {
				t.Fatalf("learned %+v, want peaks %g/%g", l, host, device)
			}
		})
	}
}
