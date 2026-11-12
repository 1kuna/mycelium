package myce

import (
	"math"
	"testing"
	"time"
)

const cgroupEnforced = "memory ceilings enforced (cgroup v2)"

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// A started, declared native CPU attempt with learned evidence on this target.
func usedChargeRunning(declared, learned float64) Job {
	j := Job{ID: "j", Attempt: 1, Session: "s", State: "running", Spec: cpuSpec(2, declared)}
	j.Spec.Needs = ResourceNeeds{CPUCores: 2, HostMemoryGB: declared, Platform: Platform{Required: []string{"cpu"}}}
	started := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	j.StartedAt = &started
	j.MemoryEnvelope = &JobMemoryEnvelope{Basis: "declared", HostGB: declared, Learned: &LearnedMemory{HostGB: learned, Evidence: 3, Source: "test"}}
	return j
}

// Audit fixture (#1044, MY-43): a dedicated host with a measured 100.06 GiB
// allowance and 89.6 GiB live availability after its reserve, running one job
// that reserved 16.125 GiB (16 declared plus 0.125 slack) and uses 12.7 GiB.
// Before: the budget arm charges the whole envelope, leaving 83.935 GiB. After:
// the job is charged its use plus the drift margin, leaving 87.235 GiB. A
// first run with no learned evidence keeps the envelope.
func TestUsedMemoryChargeAuditFixture(t *testing.T) {
	for _, tc := range []struct {
		name         string
		learned      bool
		charge, free float64
	}{
		{"first run keeps envelope", false, 16.125, 83.935},
		{"learned run charged by use", true, 12.825, 87.235},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 32, 89.6)
			m.Devices = []MachineDevice{}
			m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB, m.MemoryEnforcement = 100.06, .125, cgroupEnforced
			submit(t, q, cpuSpec(2, 16))
			a := assignedJob(machineCheck(t, q, m))
			if a == nil {
				t.Fatal("declared job not assigned")
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			if tc.learned {
				q.mu.Lock()
				e := *q.s.Jobs[a.ID].MemoryEnvelope
				e.Learned = &LearnedMemory{HostGB: 12.5, Evidence: 4, Source: "test"}
				q.s.Jobs[a.ID].MemoryEnvelope = &e
				q.mu.Unlock()
			}
			clock.Advance(time.Second)
			footprint := 12.7
			report := AttemptReport{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session, Started: true,
				CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 12.7, FootprintGB: &footprint, GrowthObserved: true, sampledAt: clock.Now()}}
			if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
				t.Fatal(err)
			}
			q.mu.Lock()
			l := q.hostMemoryLedgers[m.ID]
			free := q.s.Machines[m.ID].FreeHostMemoryGB
			q.mu.Unlock()
			if len(l.Owners) != 1 || !near(l.Owners[0].ChargeGB, tc.charge) || !near(free, tc.free) {
				t.Fatalf("charge %+v free %g, want %g/%g", l.Owners, free, tc.charge, tc.free)
			}
		})
	}
}

// Growth after admission raises the charge with each sample, credits current
// resident memory exactly once, and stops at the enforced containment bound.
func TestUsedMemoryChargeGrowthContainedNotDoubleCharged(t *testing.T) {
	_, clock := fixture(t)
	linux := cpuMachine("worker", "host", "cpu", 16, 64)
	linux.MemoryEnforcement, linux.MemoryCeilingSlackGB = cgroupEnforced, .125
	j := usedChargeRunning(16, 4)
	ceiling := hostNeed(&linux, 0, 16)
	at := clock.Now()
	for _, tc := range []struct {
		name           string
		sample         hostMemorySample
		charge, growth float64
		age            time.Duration
	}{
		// Learned need (4.125) bounds a job that has not yet reached it.
		{"below learned need", hostMemorySample{gb: 2, footprintGB: 2, peakGB: 2, at: at}, 4.25, 2.25, 0},
		// 8 resident + 1 measured increment + 0.5 GiB/s over 2 s age + drift.
		{"growing", hostMemorySample{gb: 8, footprintGB: 8, peakGB: 8, growthGB: 1, rateGB: .5, at: at}, 10.125, 2.125, 2 * time.Second},
		// A fast grower is held at the kernel ceiling, not charged beyond it.
		{"contained at ceiling", hostMemorySample{gb: 15, footprintGB: 15, peakGB: 15, growthGB: 2, rateGB: 4, at: at}, ceiling, ceiling - 15, 2 * time.Second},
		// The attempt's high water stays charged after use falls back.
		{"high water retained", hostMemorySample{gb: 3, footprintGB: 3, peakGB: 11, at: at}, 11.125, 8.125, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := at.Add(tc.age)
			charge, growth := memoryCharge(&j, &linux, ceiling, tc.sample, now)
			if !near(charge, tc.charge) || !near(growth, tc.growth) {
				t.Fatalf("charge=%g growth=%g, want %g/%g", charge, growth, tc.charge, tc.growth)
			}
			if !near(growth, charge-tc.sample.gb) {
				t.Fatal("resident memory not credited exactly once", charge, growth, tc.sample.gb)
			}
		})
	}

	// Darwin: the RSS guard may overshoot by rate × reaction window, so the
	// bound and the margin both carry that overshoot. RSS is credited once.
	darwin := cpuMachine("mac", "mac", "cpu", 8, 16)
	darwin.CapabilityProfile.OS, darwin.MemoryEnforcement = "darwin", darwinRSSGuardStatus
	dceiling := hostNeed(&darwin, 0, 16)
	overshoot := 2 * darwinGuardReaction.Seconds() // 2 GiB/s
	sample := hostMemorySample{footprintGB: 6, peakGB: 6, rateGB: 2, at: at}
	charge, growth := memoryCharge(&j, &darwin, dceiling, sample, at)
	margin := exploratoryDriftMarginGB(&darwin) + overshoot
	if !near(charge, 6+margin) || !near(growth, margin) {
		t.Fatalf("darwin charge=%g growth=%g, want %g/%g", charge, growth, 6+margin, margin)
	}
	sample = hostMemorySample{footprintGB: 16.2, peakGB: 16.2, rateGB: 2, at: at}
	if charge, _ := memoryCharge(&j, &darwin, dceiling, sample, at); !near(charge, dceiling+overshoot) {
		t.Fatal("darwin charge not bounded by ceiling plus guard overshoot", charge, dceiling+overshoot)
	}
}

// Work without a guard on its growth, or without learned evidence or fresh
// samples, keeps its full envelope.
func TestUsedMemoryChargeKeepsEnvelopeWithoutContainment(t *testing.T) {
	_, clock := fixture(t)
	at := clock.Now()
	sample := hostMemorySample{gb: 3, footprintGB: 3, peakGB: 3, at: at}
	for _, tc := range []struct {
		name string
		edit func(*Job, *Machine, *hostMemorySample)
	}{
		{"no learned evidence", func(j *Job, _ *Machine, _ *hostMemorySample) { j.MemoryEnvelope.Learned = nil }},
		{"accelerator job", func(j *Job, _ *Machine, _ *hostMemorySample) { j.Spec.Needs.Platform = Platform{Any: true} }},
		{"auto sizing", func(j *Job, _ *Machine, _ *hostMemorySample) { j.Spec.Needs.Memory = "auto" }},
		{"unenforced host", func(_ *Job, m *Machine, _ *hostMemorySample) {
			m.MemoryEnforcement = "memory ceiling not enforced on this host (x)"
		}},
		{"interactive host", func(_ *Job, m *Machine, _ *hostMemorySample) { m.SharedHost = &SharedHostProfile{} }},
		{"not started", func(j *Job, _ *Machine, _ *hostMemorySample) { j.StartedAt = nil }},
		{"stale sample", func(_ *Job, _ *Machine, s *hostMemorySample) { s.at = at.Add(-2 * hostMemorySampleFreshFor) }},
		{"missing sample", func(_ *Job, _ *Machine, s *hostMemorySample) { *s = hostMemorySample{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := cpuMachine("worker", "host", "cpu", 16, 64)
			m.MemoryEnforcement, m.MemoryCeilingSlackGB = cgroupEnforced, .125
			j, s := usedChargeRunning(16, 4), sample
			tc.edit(&j, &m, &s)
			if _, lend := lendingFloor(&j, &m, s, at); lend != lendNone {
				t.Fatal("charged by use without containment")
			}
		})
	}
}

// Several lenders grow at once into memory lent to their neighbours. The
// lowest tier yields first through the planned-stop path; production keeps
// running; one yield is in flight at a time; free memory never reaches zero.
func TestUsedMemoryOvercommitYieldsLowestTierFirst(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 32, 46)
	m.Devices = []MachineDevice{}
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB, m.MemoryEnforcement = 48, .125, cgroupEnforced
	prod := cpuSpec(2, 15)
	prod.Priority, prod.Project = "production", "prod"
	var jobs []*Job
	for _, s := range []Spec{prod, cpuSpec(2, 15), cpuSpec(2, 15)} {
		submit(t, q, s)
		a := assignedJob(machineCheck(t, q, m, jobs...))
		if a == nil {
			t.Fatal("job not assigned")
		}
		if err := q.Started(result(a, false)); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		e := *q.s.Jobs[a.ID].MemoryEnvelope
		e.Learned = &LearnedMemory{HostGB: 4, Evidence: 2, Source: "test"}
		q.s.Jobs[a.ID].MemoryEnvelope = &e
		q.mu.Unlock()
		jobs = append(jobs, job(t, q, a.ID))
		clock.Advance(time.Second)
	}
	report := func(gb, rate, available float64) {
		t.Helper()
		clock.Advance(time.Second)
		c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
		c.Machine.HostAvailableMemoryGB = available
		for _, j := range jobs {
			fp := gb
			c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true,
				CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: gb, FootprintGB: &fp, GrowthObserved: true, GrowthGB: rate, GrowthRateGB: rate, sampledAt: clock.Now()}})
		}
		if _, err := q.Checkin(c); err != nil {
			t.Fatal(err)
		}
	}
	// Steady state at the learned need: nothing yields.
	report(4, 0, 34)
	for _, j := range jobs {
		if got := job(t, q, j.ID); got.State != "running" {
			t.Fatal("steady by-use work yielded", got.ID, got.State, got.Reason)
		}
	}
	// All three grow toward their ceilings while free memory falls.
	report(14, .5, 3)
	if got := job(t, q, jobs[0].ID); got.State != "running" {
		t.Fatal("production yielded while lower tiers could", got.State, got.Reason)
	}
	if got := job(t, q, jobs[1].ID); got.State != "running" {
		t.Fatal("older experiment yielded before the newest", got.State)
	}
	victim := job(t, q, jobs[2].ID)
	stop := priorityStop(victim)
	if victim.State != "stopping" || stop == nil || stop.Cause != memoryOvercommitCause {
		t.Fatalf("newest lowest-tier job did not yield: %s %+v %q", victim.State, stop, victim.Reason)
	}
	// While that stop is in flight, no further work is stopped.
	report(14.5, .5, 2)
	if got := job(t, q, jobs[1].ID); got.State != "running" {
		t.Fatal("second yield before the first released", got.State)
	}
	// The planned stop requeues with no retry charge.
	finishInterrupted(t, q, victim)
	if got := job(t, q, victim.ID); got.State != "queued" || got.InfrastructureRetries != 0 || got.Losses != 0 {
		t.Fatalf("yield was not a free requeue: %s retries=%d losses=%d %q", got.State, got.InfrastructureRetries, got.Losses, got.Reason)
	}
	if got := job(t, q, jobs[0].ID); got.State != "running" {
		t.Fatal("production stopped", got.State)
	}
}

// Production yields only to protect other production work, and only when it
// can resume. A lone production job, or non-resumable production, never does.
func TestUsedMemoryOvercommitProductionRules(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resumable []bool
		victim    int
	}{
		{"lone production never yields", []bool{true}, -1},
		{"non-resumable production never yields", []bool{false, false}, -1},
		{"resumable production yields for production", []bool{false, true}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 32, 46)
			m.Devices = []MachineDevice{}
			m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB, m.MemoryEnforcement = 48, .125, cgroupEnforced
			var jobs []*Job
			for _, resumable := range tc.resumable {
				s := cpuSpec(2, 22)
				s.Priority, s.Project, s.Resumable = "production", "prod", resumable
				submit(t, q, s)
				a := assignedJob(machineCheck(t, q, m, jobs...))
				if a == nil {
					t.Fatal("job not assigned")
				}
				if err := q.Started(result(a, false)); err != nil {
					t.Fatal(err)
				}
				q.mu.Lock()
				e := *q.s.Jobs[a.ID].MemoryEnvelope
				e.Learned = &LearnedMemory{HostGB: 4, Evidence: 2, Source: "test"}
				q.s.Jobs[a.ID].MemoryEnvelope = &e
				q.mu.Unlock()
				jobs = append(jobs, job(t, q, a.ID))
				clock.Advance(time.Second)
			}
			clock.Advance(time.Second)
			c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
			c.Machine.HostAvailableMemoryGB = 1
			for _, j := range jobs {
				fp := 21.0
				c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true,
					CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 21, FootprintGB: &fp, GrowthObserved: true, GrowthGB: .5, GrowthRateGB: .5, sampledAt: clock.Now()}})
			}
			if _, err := q.Checkin(c); err != nil {
				t.Fatal(err)
			}
			for i, j := range jobs {
				got := job(t, q, j.ID)
				if want := i == tc.victim; (got.State == "stopping") != want {
					t.Fatalf("job %d state %s, want yield=%t (%q)", i, got.State, want, got.Reason)
				}
			}
		})
	}
}

// A unified CUDA host with a measured allowance, cgroup ceilings and room for
// one naming-sized envelope.
func unifiedUsedChargeMachine() Machine {
	m := unifiedCUDAAdmissionMachine()
	m.MemoryEnforcement, m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = cgroupEnforced, 100.06, 100
	m.ReservedMemoryGB = 16 // The measured unified host's explicit reserve: its physical floor.
	m.FreeMemoryGB, m.TotalMemoryGB = 100, 100
	// This audit uses the measured host's 16 GiB reserve (#1044), not the
	// admission fixture's 25 GiB floor, which leaves only 81 GiB here.
	m.ReservedMemoryGB = 16
	return m
}

// Accepted peaks of the work on this target, as recordLearnedMemory stores them.
func unifiedPeaks(host, device float64) *LearnedMemory {
	return &LearnedMemory{HostGB: host, Evidence: 7, Source: "test", PeakHostGB: host, PeakDeviceGB: device}
}

// The live #1044 naming stage on the unified CUDA host: envelope host
// 38.731987 (peak 20.391857 + spread) and device 44.519424 (peak 22.318359 +
// growth) plus 0.125 slack = 83.376411 GiB. Running, it uses 2.290314 GiB in
// its cgroup and 22.318359 GiB of device memory (24.608673 GiB). Before, it
// is charged the envelope, 3.39x its use. After, it is charged its accepted
// peaks plus the drift margin, 42.835216 GiB (1.74x), so the reserved
// GiB-hours of a run fall by 49%. A job without peaks, or without a fresh
// per-process device sample, keeps the envelope.
func TestUnifiedUsedMemoryChargeAuditFixture(t *testing.T) {
	const host, device, cgroup, used = 38.731987, 44.519424, 2.290314, 22.318359
	for _, tc := range []struct {
		name         string
		peaks        bool
		deviceSample bool
		charge       float64
	}{
		{"before: envelope", false, true, 83.376411},
		{"no device sample keeps envelope", true, false, 83.376411},
		{"after: charged by use", true, true, 20.391857 + 22.318359 + .125},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := unifiedUsedChargeMachine()
			s := spec("experiment")
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = device, host
			pending := submit(t, q, s)
			a := assignedJob(check(t, q, m, nil))
			if a == nil {
				t.Fatal("naming-sized job not assigned:", job(t, q, pending.ID).Reason)
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			q.mu.Lock()
			e := *q.s.Jobs[a.ID].MemoryEnvelope
			e.Learned = &LearnedMemory{HostGB: host, Evidence: 7, Source: "test"}
			if tc.peaks {
				e.Learned = unifiedPeaks(20.391857, 22.318359)
				e.Learned.HostGB = host
			}
			q.s.Jobs[a.ID].MemoryEnvelope = &e
			q.mu.Unlock()
			clock.Advance(time.Second)
			fp, dev := cgroup, used
			sample := &CurrentHostMemory{NonreclaimableGB: cgroup, FootprintGB: &fp, GrowthObserved: true, sampledAt: clock.Now()}
			if tc.deviceSample {
				sample.DeviceGB = &dev
			}
			c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session, Started: true, CurrentHostMemory: sample}}}
			c.Machine.HostAvailableMemoryGB = 100 - cgroup - used
			if _, err := q.Checkin(c); err != nil {
				t.Fatal(err)
			}
			q.mu.Lock()
			l := q.hostMemoryLedgers[m.ID]
			q.mu.Unlock()
			if len(l.Owners) != 1 || !near(l.Owners[0].ChargeGB, tc.charge) {
				t.Fatalf("owners %+v, want charge %g", l.Owners, tc.charge)
			}
		})
	}
}

// Each share is charged its accepted peak, high water or forecast; growth past
// the peaks raises the charge with each sample up to the envelope; observed
// use above the envelope is kept; only the cgroup's host vector earns credit.
func TestUnifiedUsedMemoryChargeGrowth(t *testing.T) {
	_, clock := fixture(t)
	m := unifiedUsedChargeMachine()
	j := Job{ID: "j", Attempt: 1, Session: "s", State: "running", Spec: spec("experiment")}
	j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB = 40, 20
	started := clock.Now()
	j.StartedAt = &started
	j.MemoryEnvelope = &JobMemoryEnvelope{Basis: "learned", HostGB: 60.125, DeviceGB: 40, Learned: unifiedPeaks(10, 20)}
	ceiling := hostNeed(&m, 40, 20)
	at := clock.Now()
	for _, tc := range []struct {
		name           string
		sample         hostMemorySample
		age            time.Duration
		charge, growth float64
	}{
		// Model not loaded yet: the accepted peaks hold its share.
		{"below peaks", hostMemorySample{gb: 1, footprintGB: 1, peakGB: 1, deviceGB: .5, at: at, deviceAt: at}, 0, 30.125, 29.125},
		// Device 26 + 2 increment + 1 GiB/s over 2 s; host at its peak.
		{"device past peak", hostMemorySample{gb: 2, footprintGB: 2, peakGB: 2, deviceGB: 26, deviceGrowthGB: 2, deviceRateGB: 1, at: at, deviceAt: at}, 2 * time.Second, 40.125, 38.125},
		// A falling device share keeps its attempt high water.
		{"device high water", hostMemorySample{gb: 2, footprintGB: 2, peakGB: 2, deviceGB: 5, devicePeakGB: 33, at: at, deviceAt: at}, 0, 43.125, 41.125},
		// A forecast never reserves past the envelope.
		{"forecast bounded", hostMemorySample{gb: 2, footprintGB: 2, peakGB: 2, deviceGB: 39, deviceGrowthGB: 9, deviceRateGB: 5, at: at, deviceAt: at}, time.Second, ceiling, ceiling - 2},
		// cudaMalloc is not contained: real use above the envelope stays charged.
		{"observed above envelope", hostMemorySample{gb: 3, footprintGB: 3, peakGB: 3, deviceGB: 70, at: at, deviceAt: at}, 0, 73, 70},
	} {
		t.Run(tc.name, func(t *testing.T) {
			charge, growth := memoryCharge(&j, &m, ceiling, tc.sample, at.Add(tc.age))
			if !near(charge, tc.charge) || !near(growth, tc.growth) {
				t.Fatalf("charge=%g growth=%g, want %g/%g", charge, growth, tc.charge, tc.growth)
			}
		})
	}

	// Without per-process device evidence, or off unified CUDA, GPU work keeps
	// the envelope.
	fresh := hostMemorySample{gb: 2, footprintGB: 2, peakGB: 2, deviceGB: 20, at: at, deviceAt: at}
	for _, tc := range []struct {
		name string
		edit func(*Job, *Machine, *hostMemorySample)
	}{
		{"no accepted peaks", func(j *Job, _ *Machine, _ *hostMemorySample) { j.MemoryEnvelope.Learned.PeakDeviceGB = 0 }},
		{"RSS fallback estimate", func(_ *Job, _ *Machine, s *hostMemorySample) { s.source = deviceUnifiedResidentSource }},
		{"exclusive attribution", func(_ *Job, _ *Machine, s *hostMemorySample) { s.exclusive = true }},
		{"stale device sample", func(_ *Job, _ *Machine, s *hostMemorySample) { s.deviceAt = at.Add(-deviceMemorySampleFreshFor) }},
		{"missing device sample", func(_ *Job, _ *Machine, s *hostMemorySample) { s.deviceAt = time.Time{} }},
		{"stale host sample", func(_ *Job, _ *Machine, s *hostMemorySample) { s.at = at.Add(-2 * hostMemorySampleFreshFor) }},
		{"multi-card attempt", func(j *Job, _ *Machine, _ *hostMemorySample) { j.AllocatedDevices = []string{"0", "1"} }},
		{"no cgroup ceilings", func(_ *Job, m *Machine, _ *hostMemorySample) {
			m.MemoryEnforcement = "memory ceiling not enforced on this host (x)"
		}},
		{"discrete CUDA", func(_ *Job, m *Machine, _ *hostMemorySample) { m.UnifiedMemory = false }},
		{"Metal", func(_ *Job, m *Machine, _ *hostMemorySample) {
			m.GPU, m.CapabilityProfile.OS, m.MemoryEnforcement = "metal", "darwin", darwinRSSGuardStatus
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jj, mm, s := j, m, fresh
			l := *j.MemoryEnvelope.Learned
			e := *j.MemoryEnvelope
			e.Learned = &l
			jj.MemoryEnvelope = &e
			cp := *m.CapabilityProfile
			mm.CapabilityProfile = &cp
			tc.edit(&jj, &mm, &s)
			if _, lend := lendingFloor(&jj, &mm, s, at); lend != lendNone {
				t.Fatal("GPU work charged by use without measured device memory")
			}
		})
	}
}

// Device growth alone, with flat host samples, trips the backstop: the
// experiment GPU job yields before its uncontained device allocations exhaust
// memory that production work on the host still needs.
func TestUnifiedUsedMemoryOvercommitYieldsOnDeviceGrowth(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedUsedChargeMachine()
	m.Devices = []MachineDevice{{ID: "0", Platform: "cuda", Identity: "GPU-test", Fingerprint: "cuda-test", Selector: "0", MemoryMode: "unified", UnifiedMemory: true, TotalMemoryGB: 100, BudgetGB: 100, FreeMemoryGB: 100, MemoryMeasured: true}}
	prod := cpuSpec(2, 30)
	prod.Priority, prod.Project = "production", "prod"
	gpu := spec("experiment")
	gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 40, 4
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
		e.Learned = unifiedPeaks(4, 10)
		q.s.Jobs[a.ID].MemoryEnvelope = &e
		q.mu.Unlock()
		jobs = append(jobs, job(t, q, a.ID))
		clock.Advance(time.Second)
	}
	report := func(device, rate, available float64) {
		t.Helper()
		clock.Advance(time.Second)
		c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
		c.Machine.HostAvailableMemoryGB = available
		for i, j := range jobs {
			fp := 4.0
			s := &CurrentHostMemory{NonreclaimableGB: 4, FootprintGB: &fp, GrowthObserved: true, sampledAt: clock.Now()}
			if i == 1 {
				d := device
				s.DeviceGB, s.DeviceGrowthGB, s.DeviceGrowthRateGB = &d, rate, rate
			}
			c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: s})
		}
		if _, err := q.Checkin(c); err != nil {
			t.Fatal(err)
		}
	}
	// At its accepted device peak with ample free memory, nothing yields.
	report(10, 0, 60)
	for _, j := range jobs {
		if got := job(t, q, j.ID); got.State != "running" {
			t.Fatal("steady work yielded", got.ID, got.State, got.Reason)
		}
	}
	// The device share grows at 1 GiB/s while free memory falls to 6 GiB.
	report(30, 1, 6)
	if got := job(t, q, jobs[0].ID); got.State != "running" {
		t.Fatal("production stopped", got.State, got.Reason)
	}
	victim := job(t, q, jobs[1].ID)
	if stop := priorityStop(victim); victim.State != "stopping" || stop == nil || stop.Cause != memoryOvercommitCause {
		t.Fatalf("growing GPU experiment did not yield: %s %+v %q", victim.State, stop, victim.Reason)
	}
}
