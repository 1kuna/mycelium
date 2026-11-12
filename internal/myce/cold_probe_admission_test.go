package myce

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"
)

func coldProbeAdmissionFixture(t *testing.T, cpu bool) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "fake", 16, 19.49)
	m.HostMemoryBudgetGB = 24
	m.CapabilityProfile.SystemMemoryBytes = 32 << 30
	m.Devices = []MachineDevice{}
	s := cpuSpec(1, 0)
	if !cpu {
		e := compositionExtension(t, false)
		e.Kinds = nil
		v := &e.Variants[0]
		v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "fake"}
		v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
		delete(v.Baselines, "kind:sample")
		v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
		e = freezeComposition(t, e)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		m.ExtensionFingerprint = "synthetic-cold"
		m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "card", Fingerprint: "card-v1", Selector: "0", ExtensionFingerprint: m.ExtensionFingerprint, MemoryMode: "dedicated", MemoryMeasured: true, TotalMemoryGB: 32, BudgetGB: 32, FreeMemoryGB: 32}}
		target := m.Devices[0].machine(m)
		r := newExtensionReport(e, &target, "extension:sample")
		r.Setup, r.Qualified, r.CheckID = true, true, "synthetic-check"
		m.Devices[0].ExtensionReports = []ExtensionReport{r}
		s = Spec{Name: "cold-check", Project: "synthetic", Priority: "experiment", Extension: extensionKey(e), ExtensionDigest: e.Digest, RequireTarget: true, AllowedMachines: []string{m.ID}, Targets: []ExecutionTarget{v.Target}, Needs: ResourceNeeds{Memory: "auto"}, ExtensionCheck: &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}}
	}
	machineCheck(t, q, m)
	return q, clock, m, s
}

// The incident's drained-host comparisons were 19.4665/19.4525,
// 19.4527/19.4431 and 19.4874/19.4613 GiB. A single page of drift room is
// smaller than any of those declines. Replay the queue and all worker guards.
func TestColdProbeAdmissionUsesMeasuredHeadroomDrift(t *testing.T) {
	for _, cpu := range []bool{false, true} {
		t.Run(map[bool]string{false: "discrete extension check", true: "CPU live charge"}[cpu], func(t *testing.T) {
			q, clock, m, s := coldProbeAdmissionFixture(t, cpu)
			clock.Advance(time.Second)
			m.HostAvailableMemoryGB = 19.46
			machineCheck(t, q, m)
			clock.Advance(time.Second)
			m.HostAvailableMemoryGB = 19.48
			machineCheck(t, q, m)
			queued := submit(t, q, s)
			probe := assignedJob(machineCheck(t, q, m))
			if probe == nil || probe.ID != queued.ID {
				t.Fatal("cold probe not assigned", job(t, q, queued.ID).Reason)
			}
			// Inventory declines alone do not establish this command's
			// launch error. Observe one exact refused offer, then retry.
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 19.4525, nil }}
			launchErr := localAttemptAdmission(w, nil, *probe, nil)
			if !errors.Is(launchErr, errWorkerMemory) {
				t.Fatal("missing initial drift refusal", launchErr)
			}
			refused := result(probe, false)
			refused.Interrupted = true
			refused.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", PrelaunchRefused: true, Admission: admissionDetail(launchErr, *probe)}
			if err := q.Complete(refused); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			probe = assignedJob(machineCheck(t, q, m))
			if probe == nil {
				t.Fatal("no launch-error retry")
			}
			ceiling := probe.MemoryEnvelope.HostGB
			if ceiling <= 11.9 || ceiling >= 19.4525 || q.hostJobCharge(q.s.Jobs[probe.ID]) != ceiling {
				t.Fatalf("cold ceiling/charge did not retain measured headroom: %g", ceiling)
			}
			if err := localAttemptAdmission(w, nil, *probe, nil); err != nil {
				t.Fatal("drained host refused fitting cold probe", err)
			}
			if err := w.checkHostRAM(probe.Spec.Needs.MemoryGB, probe.Spec.Needs.HostMemoryGB); err != nil {
				t.Fatal("startContext RAM check refused cold probe", err)
			}
			a := &attemptSupervisor{job: *probe}
			rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: []Job{*probe}, batchSupervisors: map[attemptKey]*attemptSupervisor{jobKey(*probe): a}, hostMemorySource: w.HostMemorySource}
			if err := rt.checkBatchLaunchMemory(context.Background(), HostLaunch{Machine: m.ID, Session: probe.Session, Key: probe.ID, Attempt: strconv.Itoa(probe.Attempt)}); err != nil {
				t.Fatal("final launch gate refused cold probe", err)
			}
			bytes, err := w.ceilingBytes(probe.Spec.Needs.MemoryGB, probe.Spec.Needs.HostMemoryGB)
			if err != nil {
				t.Fatal(err)
			}
			contained, err := fakeCeilings(t).create("probe", bytes, 0)
			if err != nil || math.Abs(float64(contained.bytes)/(1<<30)-ceiling) > minimumHostMemoryGB {
				t.Fatal("native containment differs from full reserved ceiling", err)
			}
			// A known 20 GiB demand remains a strict refusal with numeric proof.
			if err := w.checkHostRAM(0, 20); !errors.Is(err, errWorkerMemory) || admissionDetail(err, *probe).Required != 20 {
				t.Fatal("too-large declared demand was admitted", err)
			}
			if cpu {
				footprint := 11.9
				report := AttemptReport{Execution: Execution{ID: probe.ID, Attempt: probe.Attempt}, Session: probe.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: footprint, FootprintGB: &footprint, GrowthObserved: true}}
				if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
					t.Fatal(err)
				}
				live := q.s.Jobs[probe.ID]
				if live.MemoryEnvelope.HostGB != ceiling || q.hostJobCharge(live) >= 12 {
					t.Fatal("live footprint did not reduce charge while retaining containment")
				}
				clock.Advance(hostMemorySampleFreshFor)
				if q.hostJobCharge(live) != ceiling {
					t.Fatal("expired sample retained reduced charge")
				}
			}
		})
	}
}

func TestColdProbeAdmissionLearnsExactRefusalAcrossRestart(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	submit(t, q, s)
	probe := assignedJob(machineCheck(t, q, m))
	free := 19.46
	w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return free, nil }}
	err := localAttemptAdmission(w, nil, *probe, nil)
	if !errors.Is(err, errWorkerMemory) {
		t.Fatal("cold page-only margin did not reproduce refusal", err)
	}
	r := result(probe, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", PrelaunchRefused: true, Admission: admissionDetail(err, *probe)}
	stale := r
	stale.Attempt++
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatal("stale refusal accepted", err)
	}
	if q.coldProbeDriftMarginGB(&m) != minimumHostMemoryGB {
		t.Fatal("stale refusal changed margin")
	}
	// The worker reports this same decline before it completes the refusal.
	// Availability and the exact refusal are two observations of one drop.
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB = free
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: r.Execution, Session: r.Session}}}); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	margin := q.coldProbeDriftMarginGB(&m)
	if margin < .03 || margin > .03+2*minimumHostMemoryGB {
		t.Fatal("strict refusal failed to retain one snapshot-to-launch drop", margin)
	}
	q = memoryFactsRestart(t, q, clock)
	clock.Advance(time.Second)
	probe = assignedJob(machineCheck(t, q, m))
	if probe == nil || probe.Attempt != 2 {
		t.Fatal("cold probe did not retry", probe)
	}
	if err := localAttemptAdmission(w, nil, *probe, nil); err != nil {
		t.Fatal("retry chased headroom instead of reserving measured launch drift", err)
	}
	// Known containment and OOM lower bounds cannot be shrunk by the reserve.
	q.mu.Lock()
	defer q.mu.Unlock()
	queued := *q.s.Jobs[probe.ID]
	queued.State = "queued"
	queued.MemoryOOMFloor = &SizeHint{HostMemoryGB: 20}
	candidate := q.memoryCandidate(&queued, q.s.Machines[m.ID])
	if candidate.Spec.Needs.HostMemoryGB < 20 || hostFits(q.s.Machines[m.ID], 0, candidate.Spec.Needs.HostMemoryGB) {
		t.Fatal("launch drift lowered a retained OOM requirement")
	}
}

func TestColdProbeAdmissionBoundedExplorationUsesSameDrift(t *testing.T) {
	q, clock, linux, darwin, s := memoryExplorationFixture(t)
	memoryExplorationSeed(t, q, clock, linux, darwin, s, 3, foreignPeakAboveDarwinAllowance)
	clock.Advance(time.Second)
	darwin.HostAvailableMemoryGB -= .5
	check(t, q, darwin, nil)
	if q.coldProbeDriftMarginGB(&darwin) <= exploratoryDriftMarginGB(&darwin) {
		t.Fatal("fixture has no learned drift")
	}
	s.Targets = []ExecutionTarget{machineTarget(&darwin)}
	s.Needs.HostMemoryGB = 1
	queued := submit(t, q, s)
	probe := check(t, q, darwin, nil).Job
	if probe == nil || probe.ID != queued.ID || probe.MemoryEnvelope.Source != boundedMemoryExploration {
		t.Fatal("learned launch drift blocked bounded exploration on idle host", job(t, q, queued.ID).Reason)
	}
	want := darwin.HostMemoryBudgetGB - darwin.MemoryCeilingSlackGB - q.coldProbeDriftMarginGB(&darwin)
	if math.Abs(probe.Spec.Needs.HostMemoryGB-want) > 1e-9 {
		t.Fatal("bounded and physical budgets used different drift reserves", probe.Spec.Needs.HostMemoryGB, want)
	}
}

type coldProbeAdvancingClock struct{ *fakeClock }

func (c coldProbeAdvancingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Nanosecond)
	return c.now
}

func TestColdProbeAdmissionRefusalRetainsReserveAtHistoryRollover(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	m.HostAvailableMemoryGB -= .5
	// The next check-in fills the history; another sample during assignment
	// would evict the oldest pair, which contains the only availability drop.
	for i := 1; i < hostHealthHistorySize-1; i++ {
		clock.Advance(time.Second)
		machineCheck(t, q, m)
	}
	submit(t, q, s)
	q.clock = coldProbeAdvancingClock{clock}
	probe := assignedJob(machineCheck(t, q, m))
	if probe == nil || math.Abs(probe.MemoryEnvelope.HostGB-(m.HostAvailableMemoryGB-minimumHostMemoryGB)) > 1e-9 {
		t.Fatal("cold offer did not reserve the observed drop", probe)
	}
	free := probe.MemoryEnvelope.HostGB - .1
	w := Worker{Machine: m, Clock: q.clock, HostMemorySource: func(context.Context) (float64, error) { return free, nil }}
	err := localAttemptAdmission(w, nil, *probe, nil)
	if !errors.Is(err, errWorkerMemory) {
		t.Fatal("additional launch drop did not refuse the offer", err)
	}
	r := result(probe, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", PrelaunchRefused: true, Admission: admissionDetail(err, *probe)}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	margin := q.jobProbeDriftMarginGB(q.s.Jobs[probe.ID], &m)
	if margin < .1 || margin > .1+3*minimumHostMemoryGB {
		t.Fatal("refusal forgot the reserve used for admission", margin)
	}
	probe = assignedJob(machineCheck(t, q, m))
	if probe == nil || probe.Attempt != 2 {
		t.Fatal("cold probe did not retry", probe)
	}
	if err := localAttemptAdmission(w, nil, *probe, nil); err != nil {
		t.Fatal("history rollover allowed another oversized retry", err)
	}
}

// A first run held only by the cold-probe drift margin reports its own hold
// kind, with the host-memory diagnostic code it always had; a mixed refusal
// stays ordinary memory.
func TestColdProbeMarginHold(t *testing.T) {
	q, _, m, s := coldProbeAdmissionFixture(t, true)
	queued := submit(t, q, s)
	q.mu.Lock()
	target := *q.s.Machines[m.ID]
	j := *q.s.Jobs[queued.ID]
	q.mu.Unlock()
	// Room at or below the ceiling slack plus the drift margin leaves no launch headroom.
	q.mu.Lock()
	target.EffectiveHostMemoryBudgetGB = target.MemoryCeilingSlackGB + q.jobProbeDriftMarginGB(&j, &target)
	_, why, temporary := q.exploratoryMemoryStatus(&j, &target)
	q.mu.Unlock()
	if !temporary || memoryRefusalKind(&JobMemoryEnvelope{Refusal: why}, "memory") != "cold_probe_margin" || holdDiagnosticCodes["cold_probe_margin"] != "host_memory_headroom" {
		t.Fatalf("cold-probe margin refusal %q", why)
	}
	if memoryRefusalKind(&JobMemoryEnvelope{Refusal: memoryCapacityRemedies}, "memory") != "memory" {
		t.Fatal("another envelope refusal took the cold-probe kind")
	}
}
