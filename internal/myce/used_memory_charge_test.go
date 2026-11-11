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
			charge, growth := jobHostMemoryCharge(&j, &linux, ceiling, tc.sample, now)
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
	charge, growth := jobHostMemoryCharge(&j, &darwin, dceiling, sample, at)
	margin := exploratoryDriftMarginGB(&darwin) + overshoot
	if !near(charge, 6+margin) || !near(growth, margin) {
		t.Fatalf("darwin charge=%g growth=%g, want %g/%g", charge, growth, 6+margin, margin)
	}
	sample = hostMemorySample{footprintGB: 16.2, peakGB: 16.2, rateGB: 2, at: at}
	if charge, _ := jobHostMemoryCharge(&j, &darwin, dceiling, sample, at); !near(charge, dceiling+overshoot) {
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
		{"docker", func(j *Job, _ *Machine, _ *hostMemorySample) { j.Spec.Run.Docker = true }},
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
			if _, _, ok := usedMemoryCharge(&j, &m, hostNeed(&m, j.Spec.Needs.MemoryGB, 16), s, at); ok {
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
