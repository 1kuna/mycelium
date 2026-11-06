package myce

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func sharedCPUReport(t *testing.T, q *Queue, m Machine, run *Job, elapsed, usage float64) Reply {
	t.Helper()
	r := &AttemptResources{ElapsedSeconds: elapsed, CPU: AttemptCPUTime{Source: "sampled_process_tree", UsageSeconds: &usage, LowerBound: true}}
	reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: m.Session, Started: true, Resources: r}}})
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

func TestSharedHostMeasuredCPUAdmissionAndResume(t *testing.T) {
	for _, cores := range []int{4, 128} {
		t.Run(strconv.Itoa(cores), func(t *testing.T) {
			q, clock, m, run := sharedRunning(t, true)
			m.CapabilityProfile.OS = "darwin"
			m.CapabilityProfile.LogicalCPUs = cores
			m.CPUBudgetCores = cores
			if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
				t.Fatal(err)
			}
			if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "budget", CPUBudgetCores: &cores}); err != nil {
				t.Fatal(err)
			}
			if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			sharedCPUReport(t, q, m, run, 1, 100)
			if got := q.s.Machines[m.ID].CPUInUseCores; got != 1 {
				t.Fatal("first cumulative counter is not an interval rate", got)
			}
			clock.Advance(time.Second)
			// One declared core consumes three during the second interval.
			sharedCPUReport(t, q, m, run, 2, 103)
			observed := job(t, q, run.ID).CPUObservation
			if observed == nil || observed.PeakCores != 3 || q.s.Machines[m.ID].CPUInUseCores != 3 {
				t.Fatalf("actual overuse not charged: %+v %+v", observed, q.s.Machines[m.ID])
			}
			probe := Job{Spec: cpuSpec(cores-2, 1)}
			if cpuFits(&probe, q.s.Machines[m.ID]) {
				t.Fatal("admission used declaration instead of measured occupancy")
			}
			// Repeated and reordered reports cannot erase demonstrated need.
			sharedCPUReport(t, q, m, run, 2, 103)
			sharedCPUReport(t, q, m, run, 1, 0)
			if got := job(t, q, run.ID).CPUObservation.PeakCores; got != 3 {
				t.Fatal("replay changed learning", got)
			}
			m.SharedHostPressure.MemoryLevel = 2
			r := check(t, q, m, run)
			if !r.Stop || r.Kill {
				t.Fatal("no checkpoint yield", r)
			}
			finishInterrupted(t, q, run)
			if r = check(t, q, m, nil); r.Job != nil {
				t.Fatal("admitted during pressure")
			}
			m.SharedHostPressure.MemoryLevel = 1
			clock.Advance(time.Second)
			r = check(t, q, m, nil)
			if r.Job == nil || r.Job.ID != run.ID || r.Job.Attempt <= run.Attempt || r.Job.CPUObservation.PeakCores != 3 {
				t.Fatal("resume lost learned CPU need", r)
			}
			if q.s.Machines[m.ID].CPUInUseCores != 3 {
				t.Fatal("resumed attempt charged only declaration")
			}
		})
	}
}

func TestSharedHostOwnerPressureRetainsNonresumableAndProduction(t *testing.T) {
	for _, class := range []string{"experiment", "production"} {
		t.Run(class, func(t *testing.T) {
			q, _, m, run := sharedRunning(t, class == "production")
			q.mu.Lock()
			q.s.Jobs[run.ID].Spec.Class, q.s.Jobs[run.ID].Spec.Priority = class, class
			q.mu.Unlock()
			m.SharedHostPressure.MemoryLevel = 4
			r := check(t, q, m, run)
			j := job(t, q, run.ID)
			if r.Stop || r.Kill || j.State != "running" || !strings.Contains(j.Reason, "; retained ") {
				t.Fatalf("protected work stopped or unreported: %+v %+v", j, r)
			}
			if q.s.Machines[m.ID].SharedHostReason == "" {
				t.Fatal("pressure did not stop further admission")
			}
			m.SharedHostPressure.MemoryLevel = 1
			check(t, q, m, run)
			if strings.Contains(job(t, q, run.ID).Reason, "; retained ") {
				t.Fatal("recovery left a stale pressure warning")
			}
		})
	}
}

func TestSharedHostSwapGrowthYieldsFromExistingHealth(t *testing.T) {
	q, clock, m, run := sharedRunning(t, true)
	h := &HostHealth{}
	for i := 0; i < hostHealthWarmup; i++ {
		clock.Advance(time.Second)
		h.observe(HostHealthSample{At: clock.Now(), HostHealthReading: HostHealthReading{Pressure: 1, WakeSeconds: .001}})
	}
	for i := 1; i <= hostHealthConfirmation; i++ {
		clock.Advance(time.Second)
		h.observe(HostHealthSample{At: clock.Now(), HostHealthReading: HostHealthReading{Pressure: 1, SwapBytes: float64(i * (1 << 20)), WakeSeconds: 1}})
	}
	if h.DrainReason != "host_swap_growth" {
		t.Fatal("existing measured health did not confirm swap pressure", h.DrainReason)
	}
	m.HostHealth = h
	r := check(t, q, m, run)
	if !r.Stop || !strings.Contains(job(t, q, run.ID).Reason, "swap growth") {
		t.Fatal("swap evidence withheld admission without yielding resumable work", r)
	}
}

func TestSharedHostCPUPSILearnsLocalBaseline(t *testing.T) {
	for _, baseline := range []float64{0, 10} {
		q, clock, m, run := sharedRunning(t, true)
		m.SharedHostPressure.CPUKnown = true
		// A host with existing contention must first recover from its cold
		// observation. Seed only queue-owned established healthy evidence.
		q.mu.Lock()
		q.s.Machines[m.ID].SharedHostPressure.Learning.CPU.observe(baseline)
		q.s.Machines[m.ID].SharedHostPressure.Learning.CPUKnown = true
		q.mu.Unlock()
		m.SharedHostPressure.CPUPSI = baseline
		clock.Advance(time.Second)
		check(t, q, m, run)
		m.SharedHostPressure.CPUPSI = baseline + 20
		clock.Advance(time.Second)
		r := check(t, q, m, run)
		if !r.Stop || q.s.Machines[m.ID].SharedHostYieldReason != "shared host CPU PSI" {
			t.Fatal("CPU PSI did not yield against local baseline", r)
		}
	}
}

func TestSharedHostCPUFieldsStripForStrictOlderQueue(t *testing.T) {
	p := &SharedHostPressure{CPUKnown: true, CPUPSI: 2, MemoryKnown: true, WakeKnown: true}
	c := Checkin{Machine: Machine{SharedHostPressure: p}}
	legacy := legacyHostProbeCheckin(c)
	if legacy.Machine.SharedHostPressure.CPUKnown || legacy.Machine.SharedHostPressure.CPUPSI != 0 || !p.CPUKnown || !hasHostProbeFields(c) {
		t.Fatal("new pressure fields leaked or stripping mutated local evidence")
	}
}
