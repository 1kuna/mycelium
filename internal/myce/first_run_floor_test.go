package myce

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"
)

// A 31 GiB Linux worker under ordinary load, as in #1110: 17 GiB available,
// a 28 GiB allowance, cgroup v2 ceilings and a worker that charges first runs
// their floor.
func floorFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker-host", "cpu", 16, 17)
	m.Devices, m.CPUExtensionFingerprint = []MachineDevice{}, "cpu-v1"
	m.HostMemoryBudgetGB, m.ExtensionFingerprint = 28, "cpu-v1"
	m.MemoryEnforcement, m.MemoryCeilingSlackGB = cgroupMemoryEnforcement, .125
	m.CapabilityProfile.SystemMemoryBytes = 31 << 30
	m.Capabilities = append(slices.Clone(m.Capabilities), learnedMemoryGuardCapability, firstRunFloorCapability)
	check(t, q, m, nil)
	return q, clock, m
}

func autoSpec(name string, command ...string) Spec {
	s := cpuSpec(1, 0)
	s.Name, s.Run.Command = name, command
	return s
}

// #1110's working load: memory PSI some at 1.3% of every sampling interval,
// 17 GiB available less the job's own use, swap static, no OOM kills.
type psiNoiseHost struct {
	availableGB float64
	stallUS     uint64
}

func (h *psiNoiseHost) sample(interval time.Duration, usedGB float64) *gpuHostState {
	h.stallUS += uint64(float64(interval/time.Microsecond) * .013)
	return &gpuHostState{available: h.availableGB - usedGB, swap: 3_700_000, swapOut: 925_000, psi: 1.3, psiTotalUS: h.stallUS}
}

// The cgroup guard as attempt_memory.go builds it for an assigned job, with
// the worker ledger others reports for everything else on the host.
func cgroupGuard(m Machine, j *Job, reserve float64, others func(attemptKey) (float64, float64)) learnedMemoryGuard {
	g := Worker{Machine: m, HostMemoryReserveGB: reserve, hostChargesExcept: others}.learnedGuard(*j)
	g.hard = g.ceiling
	return g
}

// The #1484 walkthrough: an echo was charged 17.3 GiB, the host's whole free
// room, and lost its first attempt when free memory moved 50–100 MB before
// the worker's strict launch check. Charged its floor, it launches with far
// less live RAM, is never stopped by PSI growth, and succeeds first time.
func TestFirstRunFloorEchoChargedSmall(t *testing.T) {
	q, clock, m := floorFixture(t)
	queued := submit(t, q, autoSpec("echo", "echo", "hello"))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID {
		t.Fatal("first run not assigned:", job(t, q, queued.ID).Reason)
	}
	e := run.MemoryEnvelope
	if e.Basis != "exploratory" || !strings.Contains(e.Source, firstRunFloorSource) || run.Spec.Needs.HostMemoryGB != minimumHostMemoryGB ||
		e.HostGB != minimumHostMemoryGB+m.MemoryCeilingSlackGB || e.LaunchDriftGB != 0 || e.Refusal != "" || e.Enforcement != learnedMemoryGuardEnforcement {
		t.Fatalf("echo not charged its floor: %+v needs=%+v", e, run.Spec.Needs)
	}
	if cap := adaptiveMemoryCeilingGB(run, &m); cap != 28 {
		t.Fatalf("kernel ceiling %g GiB, want the 28 GiB allowance", cap)
	}
	// The worker's launch check needs the floor, not the room it was offered.
	w := Worker{Machine: m, Clock: RealClock{}, HostMemorySource: func(context.Context) (float64, error) { return .5, nil }}
	if err := localAttemptAdmission(w, nil, *run, nil); err != nil {
		t.Fatal("worker refused a floor-charged first run with 0.5 GiB live:", err)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	g := cgroupGuard(m, run, 1, nil)
	host := &psiNoiseHost{availableGB: 17}
	for range 100 {
		if stop := g.observeMemory(clock.Now(), map[int]string{7: "birth"}, 3<<20, 1, nil, host.sample(attemptMemorySamplePeriod, 0)); stop != nil {
			t.Fatalf("echo stopped under PSI noise: %+v", stop)
		}
		clock.Advance(attemptMemorySamplePeriod)
	}
	r := result(run, true)
	peak := 3.0 / 1024
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if done := job(t, q, run.ID); done.State != "succeeded" || done.Attempt != 1 || done.OOMRetries != 0 {
		t.Fatalf("echo did not succeed first time: state=%s attempt=%d", done.State, done.Attempt)
	}
}

// Production, the shared interactive host, unguarded hosts and older
// workers keep the whole-room probe; OOM recovery keeps its measured sizing
// (#1484, MY-42, MY-45).
func TestFirstRunFloorKeepsWholeRoomProbe(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Machine, *Job)
	}{
		{"older worker", func(m *Machine, _ *Job) {
			m.Capabilities = slices.DeleteFunc(slices.Clone(m.Capabilities), func(c string) bool { return c == firstRunFloorCapability })
		}},
		{"production", func(_ *Machine, j *Job) { j.Spec.Priority = "production" }},
		{"shared interactive host", func(m *Machine, _ *Job) { m.SharedHost = &SharedHostProfile{} }},
		{"no kernel or sampled guard", func(m *Machine, _ *Job) { m.MemoryEnforcement = "admission-only" }},
		{"OOM retry", func(_ *Machine, j *Job) { j.OOMRetries = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _, m := floorFixture(t)
			s := autoSpec("echo", "echo", "hello")
			s.Project = "prod" // A production tier in the default policy.
			j := &Job{ID: "probe", State: "queued", Spec: s}
			if err := q.defaultMemory(&j.Spec); err != nil {
				t.Fatal(err)
			}
			if err := normalizeResources(&j.Spec); err != nil {
				t.Fatal(err)
			}
			q.mu.Lock()
			copy := *q.s.Machines[m.ID]
			q.mu.Unlock()
			if got := q.memoryCandidate(j, &copy); !floorCharged(got.MemoryEnvelope) {
				t.Fatalf("baseline not floor-charged: %+v", got.MemoryEnvelope)
			}
			tc.edit(&copy, j)
			got := q.memoryCandidate(j, &copy)
			kept := got.Spec.Needs.HostMemoryGB > 10 // The whole-room probe.
			if j.OOMRetries > 0 {
				kept = got.MemoryEnvelope.Source == measuredOOMRecoverySource
			}
			if floorCharged(got.MemoryEnvelope) || !kept {
				t.Fatalf("lost its sizing: %+v needs %+v", got.MemoryEnvelope, got.Spec.Needs)
			}
		})
	}
}

// A first run that grows is charged what it uses while room exists, and PSI
// growth alone never stops it. When it outgrows the room the guard stops it
// before the reserve is spent, the job requeues with its measured peak as a
// floor, and the second run succeeds.
func TestFirstRunFloorGrowsThenRelearns(t *testing.T) {
	q, clock, m := floorFixture(t)
	queued := submit(t, q, autoSpec("grow", "python3", "grow.py"))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID || !strings.Contains(run.MemoryEnvelope.Source, firstRunFloorSource) {
		t.Fatal("first run not floor-charged")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	// The ledger charges measured use as the run grows into the room.
	const rate = .5 // GiB/s
	used := 4.0
	typedCheckin(t, q, clock, m, 17-used, []*Job{run}, func(*Job) *CurrentHostMemory { return cgroupUse(clock, used, rate) })
	if charge := ledgerCharges(q, m)[run.ID]; charge < used || charge > used+1 {
		t.Fatalf("ledger charges %g GiB for %g GiB of use", charge, used)
	}

	const reserve = 1.0
	g := cgroupGuard(m, run, reserve, nil)
	host := &psiNoiseHost{availableGB: 17}
	step := attemptMemorySamplePeriod
	var stop *RSSLimitEvidence
	var at float64
	for used = 0; used < 17 && stop == nil; used += rate * step.Seconds() {
		stop = g.observeMemory(clock.Now(), map[int]string{7: "birth"}, uint64(used*(1<<30)), 1, nil, host.sample(step, used))
		at = used
		clock.Advance(step)
	}
	if stop == nil {
		t.Fatal("run grew through the host's room without a stop")
	}
	// Room ran out: available had reached the reserve plus the growth the
	// guard's reaction allows, and the stop landed before the reserve went.
	if available := 17 - at; available < reserve || available > reserve+rate*(step+processMemoryGrace).Seconds()+rate*step.Seconds() {
		t.Fatalf("stopped at %g GiB used with %g GiB available; reserve %g", at, available, reserve)
	}
	r := classifyAttemptOutcome(result(run, false), attemptCause{memberOutcome: memberOutcome{RSSLimit: stop}})
	growth := rate * step.Seconds()
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &at, NonreclaimableGB: &at, CgroupGB: &at, GrowthGB: &growth, Constrained: true, Events: &MemoryEventCounts{}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	requeued := job(t, q, run.ID)
	if requeued.State != "queued" || requeued.MemoryOOMFloor == nil || math.Abs(requeued.MemoryOOMFloor.HostMemoryGB-(at+minimumHostMemoryGB)) > 1e-9 {
		t.Fatalf("not requeued at its measured peak: state=%s floor=%+v", requeued.State, requeued.MemoryOOMFloor)
	}
	next := check(t, q, m, nil).Job
	if next == nil || next.ID != run.ID || next.Spec.Needs.HostMemoryGB < at || strings.Contains(next.MemoryEnvelope.Source, firstRunFloorSource) {
		t.Fatalf("relearning run not charged its measured peak: %+v", next)
	}
	if err := q.Started(result(next, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	done := result(next, true)
	done.MemoryPeak = &AttemptMemoryPeak{HostGB: &at, NonreclaimableGB: &at, CgroupGB: &at, GrowthGB: &growth, Events: &MemoryEventCounts{}}
	if err := q.Complete(done); err != nil {
		t.Fatal(err)
	}
	if ended := job(t, q, run.ID); ended.State != "succeeded" || ended.Attempt != 2 {
		t.Fatalf("relearning run did not succeed: state=%s attempt=%d", ended.State, ended.Attempt)
	}
}

// Two first runs admitted on one host at their floors grow together. Each
// guard's room counts the other's ledger charge, published once a second as
// the worker's heartbeat does, and the host's measured loss of room through
// its own reaction time. Both stop while their joint use is still inside the
// host's room, short of the reserve. The same holds beside growth the ledger
// never sees, such as a persistent service (MY-45), which only the host's
// measured loss of room covers.
func TestFirstRunFloorJointGrowthStaysInRoom(t *testing.T) {
	for _, tc := range []struct {
		name    string
		runs    int
		service float64 // Unledgered growth, GiB/s.
	}{{"two first runs", 2, 0}, {"first run beside a growing service", 1, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m := floorFixture(t)
			var runs []*Job
			for i := range tc.runs {
				submit(t, q, autoSpec(fmt.Sprint("run-", i), "python3", "grow.py", fmt.Sprint(i)))
				a := assignNext(t, q, clock, m, 17, runs, nil)
				if a == nil || !floorCharged(a.MemoryEnvelope) {
					t.Fatalf("floor-charged first run %d not admitted: %+v", i, a)
				}
				runs = append(runs, a)
			}
			const reserve, rate, available = 1.0, .5, 17.0 // Each run grows .5 GiB/s.
			step := attemptMemorySamplePeriod
			used, service := make([]float64, len(runs)), 0.0
			// The heartbeat's published ledger: each run's charge from its last sample.
			ledger := map[attemptKey][2]float64{}
			publish := func() {
				for i, j := range runs {
					report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: cgroupUse(clock, used[i], rate)}
					sample := currentHostMemorySamples([]AttemptReport{report}, clock.Now())[jobKey(*j)]
					charge, pending := memoryCharge(j, &m, hostNeed(&m, 0, j.Spec.Needs.HostMemoryGB), sample, clock.Now())
					ledger[jobKey(*j)] = [2]float64{charge, pending}
				}
			}
			others := func(key attemptKey) (float64, float64) {
				var charge, pending float64
				for k, v := range ledger {
					if k != key {
						charge, pending = charge+v[0], pending+v[1]
					}
				}
				return charge, pending
			}
			var guards []learnedMemoryGuard
			for _, j := range runs {
				guards = append(guards, cgroupGuard(m, j, reserve, others))
			}
			host := &psiNoiseHost{availableGB: available}
			stopped := make([]bool, len(runs))
			total := func() float64 {
				sum := service
				for _, u := range used {
					sum += u
				}
				return sum
			}
			for n := range 400 {
				if n%int(time.Second/step) == 0 {
					publish()
				}
				state := host.sample(step, total())
				for i := range guards {
					if !stopped[i] && guards[i].observeMemory(clock.Now(), map[int]string{7 + i: "birth"}, uint64(used[i]*(1<<30)), 1, nil, state) != nil {
						stopped[i] = true
						t.Logf("run %d stopped at %.1f GiB; host use %.1f GiB, %.2f GiB available", i, used[i], total(), state.available)
					}
				}
				if !slices.Contains(stopped, false) {
					return
				}
				for i := range used {
					if !stopped[i] {
						used[i] += rate * step.Seconds()
					}
				}
				service += tc.service * step.Seconds()
				if available-total() < reserve {
					t.Fatalf("host use %.2f GiB reached the reserve; stops %v", total(), stopped)
				}
				clock.Advance(step)
			}
			t.Fatalf("runs grew without stopping: %v", stopped)
		})
	}
}

// The room a floor-charged run's guard reads is the worker's own launch
// ledger without that run: every other attempt's charge, published lock-free.
func TestFirstRunFloorRoomFromWorkerLedger(t *testing.T) {
	m := cpuMachine("worker", "worker-host", "cpu", 16, 17)
	m.HostMemoryBudgetGB, m.MemoryEnforcement, m.MemoryCeilingSlackGB = 28, cgroupMemoryEnforcement, .125
	rt := &modelRuntime{memoryMachine: m, clock: &fakeClock{now: time.Unix(100, 0)}}
	if charge, pending := rt.hostChargesExcept(attemptKey{"a", 1, m.Session}); charge != 0 || pending != 0 {
		t.Fatalf("unpublished ledger charged %g/%g", charge, pending)
	}
	var jobs []Job
	for _, s := range []struct {
		id string
		gb float64
	}{{"a", 4}, {"b", 6}} {
		j := Job{ID: s.id, Attempt: 1, Session: m.Session, State: "running", Spec: typedSpec(s.id, s.gb)}
		if err := normalizeResources(&j.Spec); err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, j)
	}
	rt.mu.Lock()
	rt.batchJobs = jobs
	rt.publishHostLedgerLocked()
	rt.mu.Unlock()
	charge, pending := rt.hostChargesExcept(jobKey(jobs[0]))
	if want := hostNeed(&m, 0, 6); charge != want || pending != want {
		t.Fatalf("others of a charged %g/%g, want b's unstarted %g", charge, pending, want)
	}
}
