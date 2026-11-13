package myce

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestAdaptiveMemoryPhysicalProbeLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hosts int
		ram   float64
	}{{"tiny", 1, .5}, {"large", 4, 512}} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			var m Machine
			for i := 0; i < tc.hosts; i++ {
				host := fmt.Sprintf("worker-%d", i)
				target := cpuMachine(host, host, "cpu", 1, tc.ram)
				target.CapabilityProfile.SystemMemoryBytes = uint64(tc.ram * (1 << 30))
				target.HostMemoryBudgetGB, target.ExtensionFingerprint = tc.ram, "cpu-v1"
				target.HostAvailableMemoryGB = tc.ram * .75
				check(t, q, target, nil)
				if i == 0 {
					m = target
				}
			}
			s := cpuSpec(1, 0)
			s.AllowedMachines = []string{m.ID}
			first := runMemory(t, q, clock, m, s, tc.ram/8, nil)
			room := m.HostAvailableMemoryGB - exploratoryDriftMarginGB(&m)
			if e := first.MemoryEnvelope; e.Basis != "exploratory" || e.Evidence != 0 || math.Abs(e.HostGB-room) > 1e-8 || strings.Contains(e.Source, "25%") {
				t.Fatalf("physical cold probe: %+v", e)
			}
			growth := tc.ram / 64
			second := runMemory(t, q, clock, m, s, tc.ram/8, func(r *Result) { r.MemoryPeak.GrowthGB = &growth })
			if e := second.MemoryEnvelope; e.Basis != "learned" || e.Evidence != 1 || e.HostGB >= first.MemoryEnvelope.HostGB {
				t.Fatalf("first accepted sample not used: %+v", e)
			}
			third := runMemory(t, q, clock, m, s, tc.ram/8, nil)
			if third.Spec.Needs.HostMemoryGB < tc.ram/8+growth {
				t.Fatal("lost measured growth horizon", third.MemoryEnvelope)
			}
			s.Run.Command = []string{"echo", "constrained-work"}
			runMemory(t, q, clock, m, s, tc.ram/4, func(r *Result) { r.MemoryPeak.Constrained = true })
			q = memoryFactsRestart(t, q, clock)
			check(t, q, m, nil)
			probe := submit(t, q, s)
			next := check(t, q, m, nil).Job
			if next == nil || next.ID != probe.ID || next.MemoryEnvelope.Basis != "exploratory" || next.Spec.Needs.HostMemoryGB < tc.ram/4 {
				t.Fatalf("constrained lower bound lost: %+v", next)
			}
			if err := q.Started(result(next, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			r := result(next, false)
			r.OutOfMemory = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, next.ID)
			if got.MemoryOOMFloor == nil || got.MemoryOOMFloor.HostMemoryGB != next.Spec.Needs.HostMemoryGB+minimumHostMemoryGB || got.MemoryOOMFloor.MemoryGB != 0 || got.OOMRetries != 1 {
				t.Fatalf("OOM floor lost: %+v", got.MemoryOOMFloor)
			}
			// More physical room permits another probe. An OOM at the full
			// physical ceiling must terminate without a count-based limit.
			m.HostAvailableMemoryGB = tc.ram
			retry := check(t, q, m, nil).Job
			if retry == nil || retry.ID != next.ID || retry.Spec.Needs.HostMemoryGB < got.MemoryOOMFloor.HostMemoryGB {
				t.Fatal("retry lost its retained floor", retry)
			}
			if err := q.Started(result(retry, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			r = result(retry, false)
			r.OutOfMemory = true
			peak := tc.ram
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if final := job(t, q, retry.ID); final.State != "failed" || final.OOMRetries != 2 {
				t.Fatal("physical probe bypassed capacity refusal", final.State, final.OOMRetries)
			}
		})
	}
}

func TestAdaptiveMemoryTransferUsesPairedError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hosts int
		ram   float64
	}{{"two small hosts", 2, .5}, {"large installation", 4, 512}} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			var source, destination Machine
			for i := 0; i < tc.hosts; i++ {
				id := fmt.Sprintf("worker-%d", i)
				m := cpuMachine(id, id, "cpu", 1, tc.ram)
				m.HostMemoryBudgetGB, m.ExtensionFingerprint = tc.ram, id+"-v1"
				m.CapabilityProfile.SystemMemoryBytes = uint64(tc.ram * (1 << 30))
				if i == 1 {
					m.CapabilityProfile.OS = "darwin"
				}
				check(t, q, m, nil)
				if i == 0 {
					source = m
				}
				if i == 1 {
					destination = m
				}
			}
			unit := tc.ram / 64
			s := cpuSpec(1, 0)
			s.Name = "calibration"
			// Different accepted workloads expose transfer variation on these identities.
			for i, pair := range [][2]float64{{1, 2}, {2, 5}} {
				s.Run.Command = []string{"echo", fmt.Sprint(i)}
				s.AllowedMachines = []string{source.ID}
				runMemory(t, q, clock, source, s, pair[0]*unit, nil)
				s.AllowedMachines = []string{destination.ID}
				runMemory(t, q, clock, destination, s, pair[1]*unit, nil)
			}
			s.Run.Command = []string{"echo", "new-work"}
			s.AllowedMachines = []string{source.ID}
			runMemory(t, q, clock, source, s, 3*unit, nil)
			s.AllowedMachines = nil
			queued := submit(t, q, s)
			candidate := q.memoryCandidate(queued, q.s.Machines[destination.ID])
			low := (2*unit + minimumHostMemoryGB) / unit
			high := (5*unit + minimumHostMemoryGB) / (2 * unit)
			want := (3*unit + minimumHostMemoryGB) * (high + (high - low))
			if e := candidate.MemoryEnvelope; e.Source != "accepted transfer + measured error" || e.Basis != "learned" || math.Abs(e.HostGB-want) > 1e-8 {
				t.Fatalf("did not use measured transfer errors: %+v want %v", e, want)
			}
			// Unknown identities and expired pairs cannot inherit calibration. The
			// source's peak was measured on another OS, so it floors this first run
			// instead of a whole-host probe (#1527).
			changed := destination
			changed.ExtensionFingerprint = "changed"
			q.s.Machines[destination.ID].ExtensionFingerprint = "changed"
			if e := q.memoryCandidate(queued, &changed).MemoryEnvelope; e.Basis != "exploratory" || math.Abs(e.HostGB-3*unit) > 1e-8 {
				t.Fatal("unknown transfer did not explore from the foreign peak floor", e)
			}
			// A constrained target observation is a requirement even if foreign
			// accepted evidence exists. It cannot be hidden by the probe budget.
			lower := tc.ram + unit
			queued.MemoryAttempts = []AttemptMemoryFacts{{MemoryPeak: &AttemptMemoryPeak{HostGB: &lower, Constrained: true}}}
			if e := q.memoryCandidate(queued, &changed).MemoryEnvelope; e.HostGB < lower {
				t.Fatal("unknown transfer discarded constrained requirement", e)
			}
			queued.MemoryAttempts = nil
			clock.Advance(workCurveMaxAge + time.Second)
			if e := q.memoryCandidate(queued, q.s.Machines[destination.ID]).MemoryEnvelope; e.Basis != "exploratory" {
				t.Fatal("expired transfer became accepted", e)
			}
		})
	}
}

func TestAdaptiveMemoryGrowthUsesClockAndLiveCredit(t *testing.T) {
	_, clock := fixture(t)
	var g memoryGrowth
	if n := g.observe(clock.Now(), .125); n != 0 {
		t.Fatal("invented first growth", n)
	}
	clock.Advance(time.Second)
	if n := g.observe(clock.Now(), .25); n != .125 {
		t.Fatal(n)
	}
	clock.Advance(3 * time.Second)
	if n := g.observe(clock.Now(), .3125); n != .375 {
		t.Fatal("delayed sensor did not expand horizon", n)
	}
	sample := hostMemorySample{gb: .25, growthGB: .125, at: clock.Now()}
	// The .125 forecast is already covered by the .25 unreached envelope.
	if reservedGrowth(nil, 0, .5, sample, clock.Now()) != .25 {
		t.Fatal("growth counted again inside the unreached envelope")
	}
	clock.Advance(hostMemorySampleFreshFor)
	if reservedGrowth(nil, 0, .5, sample, clock.Now()) != .5 {
		t.Fatal("unknown sample earned credit")
	}
}

// A learned CPU family must not inflate an experiment's RAM by an arbitrary
// transfer multiplier and thereby strand a large declared GPU workload. This
// isolates memory admission; input/cache readiness remains a separate predicate.
func TestAdaptiveMemoryCPUFamilyLeavesMeasuredTrainingHeadroom(t *testing.T) {
	q, clock := fixture(t)
	cpu := cpuMachine("cpu-worker", "mixed-host", "cpu", 8, 64)
	gpu := cpuMachine("gpu-worker", "mixed-host", "cuda", 8, 64)
	cpu.HostMemoryBudgetGB, gpu.HostMemoryBudgetGB = 64, 64
	cpu.ExtensionFingerprint, gpu.ExtensionFingerprint = "cpu-v1", "gpu-v1"
	check(t, q, gpu, nil)
	check(t, q, cpu, nil)
	s := cpuSpec(1, 0)
	s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
	s.AllowedMachines = []string{cpu.ID}
	const acceptedPeak = 5.3572
	for i := 0; i < 37; i++ {
		runMemory(t, q, clock, cpu, s, acceptedPeak, nil)
	}
	s.Run.Command = []string{"echo", "next-family-work"}
	pending := submit(t, q, s)
	experiment := check(t, q, cpu, nil).Job
	if experiment == nil || experiment.ID != pending.ID || experiment.Spec.Needs.HostMemoryGB != acceptedPeak+minimumHostMemoryGB || experiment.MemoryEnvelope.Evidence != 37 {
		t.Fatal("family evidence acquired an invented multiplier", experiment)
	}
	if err := q.Started(result(experiment, false)); err != nil {
		t.Fatal(err)
	}
	training := cpuSpec(1, 0)
	training.Name = "declared-training"
	training.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1, HostMemoryGB: 52.1, Platform: Platform{Required: []string{"cuda"}}}
	queued := submit(t, q, training)
	run := check(t, q, gpu, nil).Job
	if run == nil || run.ID != queued.ID || run.Spec.Needs.HostMemoryGB != 52.1 || run.MemoryEnvelope.Basis != "declared" {
		t.Fatal("measured CPU experiment stranded fitting declared training", job(t, q, queued.ID).Reason)
	}
	if got := q.s.Machines[gpu.ID].HostMemoryInUseGB; math.Abs(got-(acceptedPeak+minimumHostMemoryGB+52.1)) > 1e-8 || got > 64 {
		t.Fatal("shared host ledger lost a reservation", got)
	}
}

func TestAdaptiveMemoryTransferDoesNotDivideBySourceUncertainty(t *testing.T) {
	q, clock, source := learnedMemoryFixture(t)
	destination := cpuMachine("destination", "destination-host", "cpu", 8, 32)
	destination.HostMemoryBudgetGB, destination.ExtensionFingerprint = 32, "destination-v1"
	check(t, q, destination, nil)
	s := cpuSpec(1, 0)
	for i := 0; i < 2; i++ {
		s.Run.Command = []string{"echo", fmt.Sprint(i)}
		for _, target := range []Machine{source, destination} {
			s.AllowedMachines = []string{target.ID}
			for _, gb := range []float64{1, 8} {
				if target.ID == destination.ID {
					gb = 8
				}
				runMemory(t, q, clock, target, s, gb, func(r *Result) {
					r.MemoryPeak.NonreclaimableGB = &gb
					r.MemoryPeak.Events = &MemoryEventCounts{}
				})
			}
		}
	}
	s.Run.Command = []string{"echo", "steady-new-work"}
	s.AllowedMachines = []string{source.ID}
	runMemory(t, q, clock, source, s, 8, nil)
	s.AllowedMachines = []string{destination.ID}
	queued := submit(t, q, s)
	candidate := q.memoryCandidate(queued, q.s.Machines[destination.ID])
	if e := candidate.MemoryEnvelope; e.Basis != "learned" || e.HostGB < 8 {
		t.Fatal("source variation reduced target demand below measured peak", e)
	}
}

func TestAdaptiveMemoryFirstConstrainedCompletionRetainsFloor(t *testing.T) {
	for _, ram := range []float64{.5, 512} {
		t.Run(fmt.Sprint(ram), func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 8, ram)
			m.HostMemoryBudgetGB, m.ExtensionFingerprint = ram, "cpu-v1"
			m.CapabilityProfile.SystemMemoryBytes = uint64(ram * (1 << 30))
			check(t, q, m, nil)
			s := cpuSpec(1, 0)
			lower := ram / 2
			runMemory(t, q, clock, m, s, lower, func(r *Result) { r.MemoryPeak.Constrained = true })
			q = memoryFactsRestart(t, q, clock)
			m.HostAvailableMemoryGB = ram / 4
			check(t, q, m, nil)
			queued := submit(t, q, s)
			if run := check(t, q, m, nil).Job; run != nil {
				t.Fatal("first constrained result shrank to currently free memory", run.MemoryEnvelope)
			}
			candidate := q.memoryCandidate(queued, q.s.Machines[m.ID])
			if candidate.Spec.Needs.HostMemoryGB < lower || candidate.MemoryEnvelope.Basis != "exploratory" {
				t.Fatal("physical fallback dropped retained requirement", candidate.MemoryEnvelope)
			}
			m.HostAvailableMemoryGB = ram
			if run := check(t, q, m, nil).Job; run == nil || run.ID != queued.ID || run.Spec.Needs.HostMemoryGB < lower {
				t.Fatal("physical recovery lost the requirement", run)
			}
		})
	}
}
