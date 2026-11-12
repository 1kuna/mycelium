package myce

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestHostMemoryRemainingGrowthAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		used, available, budget, age          float64
		missing, wrongAttempt, wrongJob, want bool
	}{
		{name: "fresh admits production", used: 2.75, available: 14, budget: 24, want: true},
		// Exactly 2 GiB cannot admit .36: 14 - (16.356244 - 2) < 0.
		{name: "exact two GiB still needs growth", used: 2, available: 14, budget: 24},
		{name: "near envelope under pressure", used: 16.2, available: .4, budget: 24},
		{name: "stale", used: 2.75, available: 14, budget: 24, age: 5},
		{name: "missing", available: 14, budget: 24, missing: true},
		{name: "wrong attempt", used: 2.75, available: 14, budget: 24, wrongAttempt: true},
		{name: "wrong job", used: 2.75, available: 14, budget: 24, wrongJob: true},
		{name: "full budget cap", used: 16, available: 14, budget: 16.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("growth-host", "growth-host", "cpu", 16, 24)
			m.Devices = []MachineDevice{}
			m.HostMemoryBudgetGB = tc.budget
			first := submit(t, q, cpuSpec(4, 16.35624408721924))
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != first.ID {
				t.Fatal("holder not assigned")
			}
			clock.Advance(time.Second)
			report := AttemptReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Session: m.Session, Started: true}
			if !tc.missing {
				report.CurrentHostMemory = &CurrentHostMemory{NonreclaimableGB: tc.used, AgeSeconds: tc.age}
			}
			if tc.wrongAttempt {
				report.Attempt++
			}
			m.HostAvailableMemoryGB = tc.available
			if tc.wrongJob {
				report.ID = "old-job"
			}
			if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
				t.Fatal(err)
			}
			// Production is deliberately submitted after a competing same-size job.
			experiment := submit(t, q, cpuSpec(2, .36))
			s := cpuSpec(2, .36)
			s.Priority = "production"
			prod := submit(t, q, s)
			reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}})
			if err != nil {
				t.Fatal(err)
			}
			got := assignedJob(reply)
			if tc.want {
				if got == nil || got.ID != prod.ID {
					t.Fatalf("production not admitted: %+v; reason=%s", got, job(t, q, prod.ID).Reason)
				}
				requireState(t, q, experiment.ID, "queued")
			} else if got != nil {
				t.Fatalf("unsafe admission: %+v", got)
			}

			// The worker must independently recheck growth and the full static budget.
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return tc.available, nil }, hostMemorySamples: currentHostMemorySamples([]AttemptReport{report}, clock.Now())}
			next := *job(t, q, prod.ID)
			if err := localAttemptAdmission(w, []Job{*assigned}, next, nil); (err == nil) != tc.want {
				t.Fatalf("worker admission=%v want %v", err, tc.want)
			}
		})
	}
}

func TestHostMemoryRemainingGrowthExpiresWithoutRenewal(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("growth", "growth", "cpu", 16, 24)
	m.Devices = []MachineDevice{}
	m.HostMemoryBudgetGB = 24
	submit(t, q, cpuSpec(4, 16.35624408721924))
	holder := check(t, q, m, nil).Job
	if holder == nil {
		t.Fatal("no holder")
	}
	m.HostAvailableMemoryGB = 14
	report := AttemptReport{Execution: Execution{ID: holder.ID, Attempt: holder.Attempt}, Session: m.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2.75}}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	q.refreshHostMemory()
	room := q.s.Machines[m.ID].FreeHostMemoryGB
	q.mu.Unlock()
	if room < .36 {
		t.Fatal("fresh growth room missing", room)
	}
	w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 14, nil }, hostMemorySamples: currentHostMemorySamples([]AttemptReport{report}, clock.Now())}
	next := Job{Spec: Spec{Needs: ResourceNeeds{CPUCores: 2, HostMemoryGB: .36, Platform: Platform{Required: []string{"cpu"}}}}}
	if err := localAttemptAdmission(w, []Job{*holder}, next, nil); err != nil {
		t.Fatal("worker refused fresh growth room", err)
	}
	clock.Advance(hostMemorySampleFreshFor)
	if err := localAttemptAdmission(w, []Job{*holder}, next, nil); err == nil {
		t.Fatal("worker credited expired telemetry")
	}

	q.mu.Lock()
	q.refreshHostMemory()
	room = q.s.Machines[m.ID].FreeHostMemoryGB
	q.mu.Unlock()
	if room != 0 {
		t.Fatal("stale sample still credited", room)
	}
	// A fresh heartbeat carrying an aged actor sample cannot renew credit.
	report.CurrentHostMemory.AgeSeconds = hostMemorySampleFreshFor.Seconds()
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	room = q.s.Machines[m.ID].FreeHostMemoryGB
	q.mu.Unlock()
	if room != 0 {
		t.Fatal("cached sample renewed", room)
	}
}

func TestHostMemoryCurrentCreditsOnlyAnonymousResident(t *testing.T) {
	_, clock := fixture(t)
	dir := t.TempDir()
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a := &memoryAttempt{fs: osMemoryFS{}, path: dir}
	write("memory.stat", "anon 2952790016\nfile 10737418240\nkernel 1073741824\n")
	write("memory.current", "14763950080")
	sample := a.currentHostMemory(clock)
	if sample == nil || sample.NonreclaimableGB != 2.75 {
		t.Fatal("credited reclaimable cache or peak", sample)
	}
	write("memory.current", "1073741824")
	if sample = a.currentHostMemory(clock); sample == nil || sample.NonreclaimableGB != 1 {
		t.Fatal("credit exceeded current charge", sample)
	}
	write("memory.stat", "file 10737418240\n")
	if a.currentHostMemory(clock) != nil {
		t.Fatal("missing anon accepted")
	}
	// Darwin/admission-only workers have no cgroup and must keep full growth.
	if (*memoryAttempt)(nil).currentHostMemory(clock) != nil {
		t.Fatal("unsupported telemetry fabricated")
	}
	for _, sample := range []hostMemorySample{{gb: math.NaN(), at: clock.Now()}, {gb: math.Inf(1), at: clock.Now()}, {gb: 2.75, at: clock.Now().Add(time.Second)}} {
		if reservedGrowth(nil, 0, 16.356, sample, clock.Now()) != 16.356 {
			t.Fatal("invalid telemetry credited")
		}
	}
	reports := []AttemptReport{{Execution: Execution{ID: "holder", Attempt: 1}, Session: "old", Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2.75}}}
	samples := currentHostMemorySamples(reports, clock.Now())
	if reservedGrowth(nil, 0, 16.356, samples[attemptKey{"holder", 1, "new"}], clock.Now()) != 16.356 {
		t.Fatal("stale session credited")
	}
}

func TestHostMemoryProbeForecastRequiresCompleteTimedFootprint(t *testing.T) {
	_, clock := fixture(t)
	dir := t.TempDir()
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a := &memoryAttempt{fs: osMemoryFS{}, path: dir}
	write("memory.current", "8589934592")
	stats := "kernel 134217728\nshmem 134217728\nunevictable 0\nfile_dirty 67108864\nfile_writeback 0\nfile 6442450944\n"
	write("memory.stat", "anon 1073741824\n"+stats)
	first := a.currentHostMemory(clock)
	if first == nil || first.GrowthObserved || first.FootprintGB == nil || *first.FootprintGB != 1.3125 {
		t.Fatal("invented first horizon or counted reclaimable cache", first)
	}
	clock.Advance(time.Second)
	write("memory.stat", "anon 2147483648\n"+stats)
	second := a.currentHostMemory(clock)
	if second == nil || !second.GrowthObserved || second.GrowthGB != 1 || second.GrowthRateGB != 1 || second.FootprintGB == nil || *second.FootprintGB != 2.3125 || second.NonreclaimableGB != 2 {
		t.Fatal("missing measured footprint/growth", second)
	}
	clock.Advance(time.Second)
	write("memory.stat", "anon 2147483648\n")
	missing := a.currentHostMemory(clock)
	if missing == nil || missing.GrowthObserved || missing.FootprintGB != nil {
		t.Fatal("partial counters authorize forecast", missing)
	}
	clock.Advance(time.Second)
	write("memory.stat", "anon 2147483648\n"+stats)
	if a.currentHostMemory(clock).GrowthObserved {
		t.Fatal("bridged missing observation interval")
	}
}

func TestHostMemoryProbeForecastAgesBeforeExpiry(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 16)
	m.HostMemoryBudgetGB = 16
	j := Job{State: "running", Spec: cpuSpec(2, 0), MemoryEnvelope: &JobMemoryEnvelope{ProbeFloorGB: minimumHostMemoryGB}}
	j.Spec.Needs.Memory, j.Spec.Needs.HostMemoryGB = "auto", 16
	j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
	if err := j.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	sample := hostMemorySample{gb: 1, footprintGB: 1, growthGB: .1, rateGB: 1, growthObserved: true, at: clock.Now()}
	clock.Advance(4 * time.Second)
	charge, _ := memoryCharge(&j, &m, 16, sample, clock.Now())
	if charge < 5.1 {
		t.Fatalf("aged forecast charges %g GiB; measured rate predicts at least 5.1", charge)
	}
	// The same aged measurement must prevent a 12 GiB neighbor on a 16 GiB
	// allowance, independently of raw physical availability.
	q.s.Machines[m.ID] = &m
	j.ID, j.Attempt, j.Machine, j.Session = "probe", 1, m.ID, m.Session
	q.s.Jobs[j.ID] = &j
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: {jobKey(j): sample}}
	q.refreshHostMemory()
	if q.s.Machines[m.ID].FreeHostMemoryGB >= 12 {
		t.Fatal("queue admits predictable growth beyond allowance")
	}
	w := Worker{Machine: m, Clock: clock, hostMemorySamples: map[attemptKey]hostMemorySample{jobKey(j): sample}, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
	next := Job{Spec: cpuSpec(2, 12)}
	next.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
	next.Spec.Needs.HostMemoryGB, next.Spec.Needs.MemoryGB = 12, 0
	if err := next.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := localAttemptAdmission(w, []Job{j}, next, nil); err == nil {
		t.Fatal("worker admits predictable growth beyond allowance")
	}
}

// Replay a small contained CPU probe blocking a declared neighbor despite
// ample live RAM: an aged burst forecast must not exceed memory.max.
func TestHostMemoryProbeForecastCannotExceedEnforcedCeiling(t *testing.T) {
	for _, enforced := range []bool{true, false} {
		t.Run(fmt.Sprint(enforced), func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 16, 18.8)
			m.HostMemoryBudgetGB = 24
			m.Devices = []MachineDevice{}
			if enforced {
				m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
			}
			machineCheck(t, q, m)
			const ceiling = 2.6227991325787627
			j := Job{ID: "probe", State: "running", Attempt: 1, Machine: m.ID, Session: m.Session, Spec: cpuSpec(2, ceiling), MemoryEnvelope: &JobMemoryEnvelope{HostGB: ceiling, ProbeFloorGB: minimumHostMemoryGB, Source: "exploratory first run", Basis: "exploratory", Enforcement: m.MemoryEnforcement}}
			j.Spec.Needs.Memory = "auto"
			j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB = 0, ceiling
			j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
			sample := hostMemorySample{gb: .3, footprintGB: .5, growthGB: 6, rateGB: 2, growthObserved: true, at: clock.Now()}
			clock.Advance(4 * time.Second)
			q.s.Jobs[j.ID] = &j
			q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: {jobKey(j): sample}}
			q.refreshHostMemory()
			charge, growth := memoryCharge(&j, &m, ceiling, sample, clock.Now())
			if enforced && (charge != ceiling || math.Abs(growth-(ceiling-.3)) > 1e-10) {
				t.Fatalf("contained probe charged forecast above ceiling: charge=%g growth=%g ceiling=%g", charge, growth, ceiling)
			}
			if !enforced && charge <= ceiling {
				t.Fatal("unenforced forecast lost overrun protection")
			}
			if got := hostFits(q.s.Machines[m.ID], 0, 6); got != enforced {
				t.Fatalf("queue neighbor fits=%v, want %v (charge %g)", got, enforced, charge)
			}
			w := Worker{Machine: m, Clock: clock, hostMemorySamples: map[attemptKey]hostMemorySample{jobKey(j): sample}, HostMemorySource: func(context.Context) (float64, error) { return 18.8, nil }}
			next := Job{Spec: cpuSpec(2, 6)}
			next.Spec.Needs.MemoryGB, next.Spec.Needs.HostMemoryGB = 0, 6
			next.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
			if err := localAttemptAdmission(w, []Job{j}, next, nil); (err == nil) != enforced {
				t.Fatalf("strict worker neighbor admission=%v, enforced=%v", err, enforced)
			}
			if enforced {
				over := sample
				over.footprintGB = ceiling + 1
				actual, _ := memoryCharge(&j, &m, ceiling, over, clock.Now())
				if actual < over.footprintGB {
					t.Fatal("forecast bound hid measured overuse", actual)
				}
				// New native guards enforce physical capacity, not the smaller
				// predicted envelope. Preserve their negotiated growth contract.
				adaptive := j
				envelope := *j.MemoryEnvelope
				adaptive.MemoryEnvelope = &envelope
				adaptive.MemoryEnvelope.Enforcement = learnedMemoryGuardEnforcement
				grown, _ := memoryCharge(&adaptive, &m, ceiling, sample, clock.Now())
				if grown <= ceiling || grown > m.HostMemoryBudgetGB {
					t.Fatal("adaptive execution ceiling replaced by its estimate", grown)
				}
				burst := sample
				burst.rateGB = 100
				bounded, _ := memoryCharge(&adaptive, &m, ceiling, burst, clock.Now())
				if bounded != m.HostMemoryBudgetGB {
					t.Fatal("adaptive forecast escaped physical containment", bounded)
				}
			}
			// Cancellation cannot discard custody or acquire reduced probe credit.
			j.State = "stopping"
			stopped, pending := memoryCharge(&j, &m, ceiling, sample, clock.Now())
			if stopped < ceiling || enforced && pending > ceiling {
				t.Fatal("stopping probe lost full charge", stopped)
			}
			clock.Advance(hostMemorySampleFreshFor)
			stopped, pending = memoryCharge(&j, &m, ceiling, sample, clock.Now())
			if stopped < ceiling || pending < ceiling {
				t.Fatal("stale stopping probe lost custody", stopped, pending)
			}
		})
	}
}

// Two declared CPU attempts reserve 9 GiB while burst margins predict more
// growth than their combined memory.max. The room reproduces the incident's
// shape; the synthetic forecasts exercise current nonadditive accounting.
func TestHostMemoryDeclaredGrowthBoundedByHardContainment(t *testing.T) {
	for _, enforced := range []bool{true, false} {
		t.Run(fmt.Sprint(enforced), func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 16, 24)
			m.HostMemoryBudgetGB = 24
			m.Devices = []MachineDevice{}
			if enforced {
				m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
			}
			machineCheck(t, q, m)
			submit(t, q, cpuSpec(2, 3))
			first := assignedJob(machineCheck(t, q, m))
			if first == nil {
				t.Fatal("first holder missing")
			}
			submit(t, q, cpuSpec(2, 6))
			second := assignedJob(machineCheck(t, q, m, first))
			if second == nil || second.ID == first.ID {
				t.Fatal("second holder missing")
			}
			clock.Advance(time.Second)
			m.HostAvailableMemoryGB = 18.939971923828125
			reports := []AttemptReport{
				{Execution: Execution{ID: first.ID, Attempt: first.Attempt}, Session: first.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{SampleID: 1, NonreclaimableGB: .4, GrowthGB: 10}},
				{Execution: Execution{ID: second.ID, Attempt: second.Attempt}, Session: second.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{SampleID: 1, NonreclaimableGB: .6, GrowthGB: 14}},
			}
			if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports}); err != nil {
				t.Fatal(err)
			}
			next := submit(t, q, cpuSpec(2, 1.1))
			ledger := q.s.Machines[m.ID]
			if enforced {
				if ledger.HostMemoryInUseGB != 9 || ledger.FreeHostMemoryGB < 9.9 || ledger.hostReservationDebtGB != 0 {
					t.Fatalf("hard ceilings left phantom debt: room=%g debt=%g", ledger.FreeHostMemoryGB, ledger.hostReservationDebtGB)
				}
			} else if ledger.HostMemoryInUseGB <= 9 || ledger.hostReservationDebtGB <= 0 {
				t.Fatal("unenforced forecast lost its protection", ledger.hostReservationDebtGB)
			}
			w := Worker{Machine: m, Clock: clock, hostMemorySamples: currentHostMemorySamples(reports, clock.Now()), HostMemorySource: func(context.Context) (float64, error) { return m.HostAvailableMemoryGB, nil }}
			if err := localAttemptAdmission(w, []Job{*first, *second}, *job(t, q, next.ID), nil); (err == nil) != enforced {
				t.Fatalf("strict worker admission=%v, containment=%v", err, enforced)
			}
			reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports})
			if err != nil {
				t.Fatal(err)
			}
			placed := assignedJob(reply)
			if (placed != nil && placed.ID == next.ID) != enforced {
				t.Fatalf("queue placement=%v, containment=%v", placed, enforced)
			}
			// The physical bound cannot grant missing-sample credit or ignore
			// real pressure. Full 9 GiB custody still binds after expiry.
			clock.Advance(hostMemorySampleFreshFor)
			w.Machine.HostAvailableMemoryGB = .5
			w.HostMemorySource = func(context.Context) (float64, error) { return .5, nil }
			if err := localAttemptAdmission(w, []Job{*first, *second}, *job(t, q, next.ID), nil); err == nil {
				t.Fatal("bound bypassed stale samples and physical pressure")
			}
		})
	}
}

// #1487: on a cgroup v2 worker a GPU attempt runs under memory.max at its host
// envelope, and its host forecast is read from that cgroup. A startup burst
// forecast past the envelope was bounded only by physical RAM, so one card's
// job was charged the whole host and the idle second card was withheld.
func TestHostMemoryGPUGrowthBoundedByAttemptCeiling(t *testing.T) {
	for _, enforced := range []bool{true, false} {
		t.Run(fmt.Sprint(enforced), func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			m.GPU, m.CapabilityProfile.Accelerators[0].Type = "xpu", "xpu"
			for i := range m.Devices {
				m.Devices[i].Platform = "xpu"
			}
			m.CapabilityProfile.SystemMemoryBytes = 31 << 30
			m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 24, 18
			if enforced {
				m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
			}
			s := spec("experiment")
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = 6, 7
			submit(t, q, s)
			first := assignedJob(machineCheck(t, q, m))
			if first == nil || !slices.Equal(first.AllocatedDevices, []string{"0"}) {
				t.Fatal("first card holder missing", first)
			}
			clock.Advance(time.Second)
			busy := m
			busy.Devices = slices.Clone(m.Devices)
			busy.Devices[0].FreeMemoryGB -= 6
			device := 6.0
			// Loading: 3.3 GiB resident, and the sampled rate predicts 28 GiB more.
			reports := []AttemptReport{{Execution: Execution{ID: first.ID, Attempt: first.Attempt}, Session: first.Session, Started: true,
				CurrentHostMemory: &CurrentHostMemory{SampleID: 1, NonreclaimableGB: 3.3, GrowthGB: 28, GrowthObserved: true, DeviceGB: &device}}}
			if _, err := q.Checkin(Checkin{Machine: busy, AttemptProtocol: keyedAttemptProtocol, Attempts: reports, period: time.Second}); err != nil {
				t.Fatal(err)
			}
			if charge := q.hostJobCharge(job(t, q, first.ID)); enforced != (charge == 7) {
				t.Fatalf("charge %g GiB, containment=%v", charge, enforced)
			}
			next := submit(t, q, s)
			w := Worker{Machine: busy, Clock: clock, hostMemorySamples: currentHostMemorySamples(reports, clock.Now()), HostMemorySource: func(context.Context) (float64, error) { return busy.HostAvailableMemoryGB, nil }}
			if err := localAttemptAdmission(w, []Job{*job(t, q, first.ID)}, *job(t, q, next.ID), nil); (err == nil) != enforced {
				t.Fatalf("worker admission=%v, containment=%v", err, enforced)
			}
			reply, err := q.Checkin(Checkin{Machine: busy, AttemptProtocol: keyedAttemptProtocol, Attempts: reports, period: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			placed := assignedJob(reply)
			if got := placed != nil && placed.ID == next.ID && slices.Equal(placed.AllocatedDevices, []string{"1"}); got != enforced {
				t.Fatalf("second card placement=%v, containment=%v (%s)", placed, enforced, job(t, q, next.ID).Reason)
			}
		})
	}
}
