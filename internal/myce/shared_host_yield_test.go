package myce

import (
	"fmt"
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

func TestSharedHostOwnerPressureYieldsEveryTier(t *testing.T) {
	for _, tc := range []struct {
		class     string
		resumable bool
	}{{"experiment", false}, {"production", true}, {"production", false}} {
		t.Run(fmt.Sprint(tc.class, tc.resumable), func(t *testing.T) {
			q, _, m, run := sharedRunning(t, tc.resumable)
			q.mu.Lock()
			q.s.Jobs[run.ID].Spec.Class, q.s.Jobs[run.ID].Spec.Priority = tc.class, tc.class
			q.mu.Unlock()
			m.SharedHostPressure.MemoryLevel = 4
			r := check(t, q, m, run)
			if !r.Stop || r.Kill == tc.resumable || tc.resumable && r.GraceSeconds <= 0 {
				t.Fatalf("owner pressure did not yield %s work: %+v", tc.class, r)
			}
			if q.s.Machines[m.ID].SharedHostReason == "" {
				t.Fatal("pressure did not stop further admission")
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

func TestSharedHostColdCPUPressureCannotTrainHealthyBaseline(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	for i := 0; i < 4; i++ {
		clock.Advance(time.Second)
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, CPUKnown: true, CPUPSI: 12}
		check(t, q, m, nil)
		stored := q.s.Machines[m.ID]
		if stored.SharedHostYieldReason != "shared host CPU PSI" || stored.SharedHostPressure.Learning.CPUKnown {
			t.Fatal("cold contention became healthy", stored.SharedHostPressure)
		}
	}
	m.SharedHostPressure.CPUPSI = 0
	clock.Advance(time.Second)
	check(t, q, m, nil)
	if stored := q.s.Machines[m.ID]; stored.SharedHostYieldReason != "" || !stored.SharedHostPressure.Learning.CPUKnown {
		t.Fatal("affirmative quiet CPU evidence did not recover/train")
	}
}

func TestSharedHostFinalCPUChargesExactPendingRelease(t *testing.T) {
	q, clock, m, run := sharedRunning(t, true)
	sharedCPUReport(t, q, m, run, 1, 100)
	m.SharedHostPressure.MemoryLevel = 2
	if r := check(t, q, m, run); !r.Stop {
		t.Fatal("no owner yield")
	}
	clock.Advance(time.Second)
	usage := 104.0
	r := result(run, false)
	r.Interrupted, r.ReleasePending = true, true
	r.Resources = &AttemptResources{ElapsedSeconds: 2, CPU: AttemptCPUTime{Source: "sampled_process_tree", UsageSeconds: &usage}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	key := releaseKey(HostRelease{Job: run.ID, Machine: run.Machine, Session: run.Session, Attempt: run.Attempt})
	if receipt := q.s.HostReleases[key]; receipt.CPUCores != 4 {
		t.Fatal("final CPU learning lost at requeue with pending cleanup", receipt)
	}
	if q.s.Machines[m.ID].CPUInUseCores != 4 {
		t.Fatal("unreleased execution advertised free CPU")
	}
}

// Replay shape from the maintainer's retained 2026-11-02 incident, relayed on
// #389 on 2026-11-06: 16 GiB physical RAM, 2 GiB declared (2.125 resolved),
// 2,004 Python members, 64.07 GiB RSS-sum and 44.83 GiB swap-out activity.
// Intermediate values/times below are synthetic: the retained record does not
// establish their chronology. RSS-sum/footprint is not exclusive physical RAM,
// and swap allocation is not a swap-out counter. No real processes are spawned.
func TestSharedHostForkGrowthReplayYieldsBeforeCriticalAndArbitratesGuard(t *testing.T) {
	for _, ending := range []string{"checkpoint", "guard before yield", "guard races yield"} {
		t.Run(ending, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("interactive", "interactive", "cpu", 4, 16)
			m.CapabilityProfile.OS = "darwin"
			m.CapabilityProfile.SystemMemoryBytes = 16 << 30
			m.CPUBudgetCores, m.HostMemoryBudgetGB = 2, 12
			m.MemoryObservation = &HostMemoryObservation{TotalGB: 16, UsedGB: 4}
			m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
			check(t, q, m, nil)
			if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
				t.Fatal(err)
			}
			m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 1}
			if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "profile", SharedHost: m.SharedHost}); err != nil {
				t.Fatal(err)
			}
			check(t, q, m, nil)
			if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
				t.Fatal(err)
			}
			s := cpuSpec(2, 2)
			s.Resumable = true
			j := submit(t, q, s)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != j.ID {
				t.Fatal("incident fixture not admitted")
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			trace := []struct {
				level, members   int
				rssGB, swapOutGB float64
			}{
				{1, 2, 2.125, 0},
				{2, 128, 4, .25},
				{4, 2004, 64.07, 44.83},
			}
			stage := 0
			probe := processMemoryProbe{OwnedResident: func(map[int]string) (uint64, int, error) {
				return uint64(trace[stage].rssGB * (1 << 30)), trace[stage].members, nil
			}}
			readStage := func() (uint64, int) {
				t.Helper()
				bytes, members, err := probe.OwnedResident(map[int]string{1: "synthetic-owned-supervisor"})
				if err != nil {
					t.Fatal(err)
				}
				return bytes, members
			}
			readStage() // Origin before growth; summed RSS is not host usage.
			guardResult := func() Result {
				stage = 1
				bytes, _ := readStage()
				e := &RSSLimitEvidence{ObservedBytes: bytes, LimitBytes: int64(2.125 * (1 << 30)), Quality: "owned_session_rss"}
				return classifyAttemptOutcome(result(run, false), attemptCause{memberOutcome: memberOutcome{RSSLimit: e}})
			}
			if ending == "guard before yield" {
				if err := q.Complete(guardResult()); err != nil {
					t.Fatal(err)
				}
			}
			// The first OS warning, while well below the retained RSS peak,
			// must act; neither critical level nor swap exhaustion is needed.
			clock.Advance(time.Second)
			stage = 1
			readStage()
			m.SharedHostPressure.MemoryLevel = trace[stage].level
			m.MemoryObservation.UsedGB, m.MemoryObservation.MyceliumGB = 10, 4
			r := check(t, q, m, run)
			if ending == "guard before yield" {
				if got := job(t, q, run.ID); got.State != "failed" || priorityStop(got) != nil {
					t.Fatal("owner pressure reclassified an already stopped guard failure", got)
				}
				return
			}
			stopping := job(t, q, run.ID)
			if !r.Stop || r.Kill || stopping.State != "stopping" || priorityStop(stopping) == nil {
				t.Fatal("no early checkpoint yield at warning before critical", r)
			}
			deadline := *stopping.StopAt
			// If the payload does not exit yet, the later incident peak must
			// retain the same stop and deadline, not start a competing action.
			clock.Advance(time.Second)
			stage = 2
			bytes, members := readStage()
			m.SharedHostPressure.MemoryLevel = trace[stage].level
			// Feed the retained terminal shape through the synthetic sensor;
			// swap-out remains context, never substituted for swap allocation.
			if members != 2004 || bytes <= 64*(1<<30) || trace[stage].swapOutGB < 44 {
				t.Fatal("incident terminal shape lost")
			}
			check(t, q, m, run)
			if stopped := job(t, q, run.ID); !stopped.StopAt.Equal(deadline) || stopped.Attempt != run.Attempt {
				t.Fatal("critical pressure renewed/duplicated the earlier stop")
			}
			if ending == "guard races yield" {
				if err := q.Complete(guardResult()); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, run.ID)
				if got.State != "failed" || got.ExecutionCause == nil || got.ExecutionCause.Class != rssLimitExceeded || got.InfrastructureRetries != 0 || got.Losses != 0 {
					t.Fatal("owner yield hid/double-counted the explicit guard failure", got)
				}
				return
			}
			finishInterrupted(t, q, run)
			m.SharedHostPressure.MemoryLevel = 1
			m.MemoryObservation.UsedGB, m.MemoryObservation.MyceliumGB = 4, 0
			clock.Advance(time.Second)
			next := check(t, q, m, nil).Job
			if next == nil || next.ID != run.ID || next.Attempt != run.Attempt+1 {
				t.Fatal("early checkpoint yield did not resume same logical job")
			}
		})
	}
}
