package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func seedPoolReplay(q *Queue, job *Job, m *Machine, pool string, typical, charge float64) {
	key := pooledEvidenceKey(job, m)
	key.Pool = pool
	e := q.memoryEvidence.stage()
	for i := 0; i < 9; i++ {
		family := key
		family.Family, family.Exact = fmt.Sprintf("family-%d", i), fmt.Sprintf("exact-%d", i)
		peak := typical
		if i == 8 {
			peak = charge
		}
		e.observe(memoryEvidencePoint{Key: family, Peak: peak})
	}
	q.memoryEvidence = e
}

func TestMemoryPoolChargeAboveAllowance(t *testing.T) {
	for _, tc := range []struct {
		name, backend           string
		typical, charge, budget float64
	}{
		{"cold Metal", "metal", 3, 5.651, 5.618},
		{"Darwin CPU", "cpu", 6, 14.560, 13.212},
		{"typical exceeds allowance", "metal", 6, 7, 5.618},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			s := cpuSpec(1, 0)
			s.Needs.Memory = "auto"
			m := cpuMachine("small", "small", tc.backend, 8, 2)
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
			m.HostMemoryBudgetGB = tc.budget
			m.Capabilities = append(m.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
			if tc.backend == "metal" {
				m.UnifiedMemory = true
				m.CapabilityProfile.MemoryLayout = "unified"
				s.Needs.Platform = Platform{Required: []string{"metal"}}
			} else {
				s.Needs.Platform = Platform{Required: []string{"cpu"}}
			}
			seedPoolReplay(q, &Job{Spec: s}, &m, "ram", tc.typical, tc.charge)
			machineCheck(t, q, m)
			demand := submit(t, q, s)
			if got := assignedJob(machineCheck(t, q, m)); got != nil {
				t.Fatal("admitted below the typical estimate", got.ID)
			}
			stored, err := q.jobSnapshot(demand.ID)
			if err != nil || stored.Hold == nil || stored.Starts != 0 {
				t.Fatalf("queued receipt = %+v, %v", stored, err)
			}
			if tc.typical > tc.budget {
				if stored.Hold.Kind != "memory_capacity" || stored.Hold.Owner != ownerConsumer ||
					!strings.Contains(stored.Hold.Detail, "typical 6.000 GiB") ||
					!strings.Contains(stored.Hold.Detail, "host allowance 5.618 GiB") {
					t.Fatalf("permanent pool shortage = %+v", stored.Hold)
				}
				return
			}
			if stored.Hold.Kind != memoryWaitReason || stored.Hold.Owner != ownerQueue ||
				!strings.Contains(stored.Hold.Detail, "available 2.000 GiB") {
				t.Fatalf("temporary physical shortage = %+v", stored.Hold)
			}
			// Room reaches the typical estimate but remains below the original
			// platform charge. Both the queue and worker must admit it now.
			m.HostAvailableMemoryGB = tc.typical + 0.25
			offered := assignedJob(machineCheck(t, q, m))
			if offered == nil || offered.ID != demand.ID || offered.Attempt != 1 ||
				offered.Memory.RAM.ChargeGB != tc.typical || offered.Memory.RAM.Evidence.Level != "platform" {
				t.Fatalf("admission with typical room = %+v", offered)
			}
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) {
				return m.HostAvailableMemoryGB, nil
			}}
			if err := localAttemptAdmission(w, nil, *offered, nil); err != nil {
				t.Fatal(err)
			}
			if limit, err := w.jobCeilingBytes(*offered); limit != 0 || err != nil {
				t.Fatalf("estimate set a kill limit: %d, %v", limit, err)
			}
		})
	}
}

func TestMemoryPoolUnifiedAcceleratorShares(t *testing.T) {
	t.Run("without prior", testMemoryPoolUnifiedAcceleratorSharesWithoutPrior)
	const peak = 20.622669219970703
	const peerPeak = 24.35720443725586
	const ratio = 46.95225911511311 / peak
	const coldCharge = 38.30582046508789
	for _, tc := range []struct {
		name              string
		lower, familyHigh float64
		coldCharge        float64
		wantCharge        float64
		peerHint          float64
		wantPeer          bool
	}{
		{name: "one clean run", coldCharge: coldCharge, wantCharge: coldCharge, wantPeer: true},
		{name: "larger comparable peak", coldCharge: coldCharge, familyHigh: 44, wantCharge: 44, wantPeer: true},
		{name: "measured shortage", coldCharge: coldCharge, lower: 27, wantCharge: 54},
		{name: "cold charge has no headroom", coldCharge: 20, wantCharge: ratio * peak},
		{name: "insufficient cold evidence", wantCharge: ratio * peak},
		{name: "unmeasured peer uses hint", coldCharge: coldCharge, wantCharge: coldCharge, peerHint: 60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			m := sharedUnifiedMachine()
			m.HostMemoryBudgetGB = 96
			cpu := submit(t, q, cpuSpec(1, 5.5))
			cpuRun := assignedJob(machineCheck(t, q, m))
			if cpuRun == nil || cpuRun.ID != cpu.ID {
				t.Fatalf("missing CPU offer: %+v", cpuRun)
			}
			if err := q.Started(result(cpuRun, false)); err != nil {
				t.Fatal(err)
			}

			s := sharedSpec("experiment", 32)
			s.Needs.HostMemoryGB = 12
			queued := submit(t, q, s)
			target := allocationMachine(&m, []string{"0"})
			key := pooledEvidenceKey(queued, target)
			if tc.coldCharge > 0 {
				seedPoolReplay(q, queued, target, "ram", 20, tc.coldCharge)
			}
			e := q.memoryEvidence
			e.observe(memoryEvidencePoint{Key: key, Peak: peak})
			if tc.familyHigh > 0 {
				sibling := key
				sibling.Exact = "other-configuration"
				e.observe(memoryEvidencePoint{Key: sibling, Peak: tc.familyHigh})
			}
			e.observe(memoryEvidencePoint{Key: key, Peak: tc.lower, Limited: true})
			ratios := []float64{
				0.9, 0.95, 1, 1.01, 1.05, 1.1, 1.2, 1.3, ratio,
			}
			e.ratios[memoryRatioKey{key.Platform, key.Pool, "exact", "", 1}] = ratios
			e.ratios[memoryRatioKey{key.Platform, key.Pool, "family", "", 1}] = ratios

			run := assignedJob(machineCheck(t, q, m, cpuRun))
			if run == nil || run.ID != queued.ID || run.Memory == nil || run.Memory.Card != nil {
				t.Fatalf("missing unified-memory offer: %+v", run)
			}
			if got := run.Memory.RAM; math.Abs(got.ChargeGB-tc.wantCharge) > 1e-9 || got.TypicalGB != max(peak, tc.lower) ||
				got.Evidence != (memoryEstimateEvidence{Level: "exact", Runs: 1}) {
				t.Fatalf("RAM estimate = %+v, want %.3f GiB with one exact run", got, tc.wantCharge)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}

			m.HostAvailableMemoryGB = 92
			peerSpec := sharedSpec("experiment", 0)
			peerSpec.Needs.HostMemoryGB = tc.peerHint
			peer := submit(t, q, peerSpec)
			if tc.peerHint == 0 {
				// A different CPU configuration supplies family evidence.
				// At the same ratio, 24.357 GiB charged 55.455 GiB.
				peerKey := pooledEvidenceKey(peer, target)
				peerKey.Exact = "other-cpu-configuration"
				e.observe(memoryEvidencePoint{Key: peerKey, Peak: peerPeak})
			}
			offered := assignedJob(growthCheck(t, q, m, []*Job{run, cpuRun}, []float64{peak}, 0))
			if tc.wantPeer {
				if offered == nil || offered.ID != peer.ID || offered.Memory.RAM.ChargeGB != coldCharge ||
					offered.Memory.RAM.Evidence != (memoryEstimateEvidence{Level: "family", Runs: 1}) {
					t.Fatalf("peer offer = %+v, wait = %s", offered, job(t, q, peer.ID).Reason)
				}
			} else if offered != nil {
				t.Fatalf("peer used room held by measured demand: %+v", offered)
			}
			requireState(t, q, run.ID, "running")
		})
	}
}

func testMemoryPoolUnifiedAcceleratorSharesWithoutPrior(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	holder := submit(t, q, sharedSpec("experiment", 2))
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != holder.ID {
		t.Fatal("fixture did not admit holder")
	}
	cold := sharedSpec("experiment", 0)
	cold.Name, cold.Needs.HostMemoryGB, cold.Needs.Memory = "cold", 0, "auto"
	waiter := submit(t, q, cold)
	second := assignedJob(machineCheck(t, q, m, first))
	if second == nil || second.ID != waiter.ID || second.Memory.Card != nil || second.Memory.RAM.Alone {
		t.Fatalf("cold unified job did not share: %+v, %s", second, job(t, q, waiter.ID).Reason)
	}
	if err := localAttemptAdmission(sharedWorker(m, clock), []Job{*first}, *second, nil); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryPoolColdDiscreteCardAndSharedRAM(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 12, 12
	cold := func(name, id string) Spec {
		s := spec("experiment")
		s.Name, s.Needs.MemoryGB, s.Needs.Memory = name, 0, "auto"
		s.Needs.Platform = Platform{Required: []string{"fake"}}
		s.Needs.GPU = &GPUSelection{Devices: []string{id}}
		return s
	}
	s := cold("holder", "0")
	s.Needs.HostMemoryGB = 2
	holder := submit(t, q, s)
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != holder.ID || !first.Memory.Card.Alone {
		t.Fatalf("missing cold-card holder: %+v", first)
	}
	waiter := submit(t, q, cold("same-card", "0"))
	if offered := assignedJob(machineCheck(t, q, m, first)); offered != nil {
		t.Fatal("cold work joined an occupied discrete card", offered.ID)
	}
	stored, err := q.jobSnapshot(waiter.ID)
	if err != nil || stored.Hold == nil || stored.Hold.Kind != "capacity" || stored.Hold.Owner != ownerQueue ||
		!strings.Contains(stored.Hold.Detail, "device 0 held by "+first.ID) {
		t.Fatalf("occupied card receipt = %+v, %v", stored.Hold, err)
	}
	other := submit(t, q, cold("other-card", "1"))
	second := assignedJob(machineCheck(t, q, m, first))
	if second == nil || second.ID != other.ID || !slices.Equal(second.AllocatedDevices, []string{"1"}) ||
		!second.Memory.Card.Alone || second.Memory.RAM.Alone {
		t.Fatalf("cold card incorrectly required the host to itself: %+v", second)
	}
	finish(t, q, first, true)
	if offered := assignedJob(machineCheck(t, q, m, second)); offered == nil || offered.ID != waiter.ID {
		t.Fatalf("card waiter did not start when its holder released: %+v", offered)
	}
}

func TestMemoryPoolReplay1967(t *testing.T) {
	for _, seeded := range []bool{false, true} {
		t.Run(fmt.Sprint("history=", seeded), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("pooled-cpu", "cpu")
			m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
			m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 16, 24
			m.MemoryEnforcement = cgroupMemoryEnforcement
			s := cpuSpec(1, 0)
			s.Needs.Memory, s.Priority, s.Class = "auto", "production", "production"
			s.StartWindow = "2m"
			if seeded {
				seedPoolReplay(q, &Job{Spec: s}, &m, "ram", 1, 3)
			}
			machineCheck(t, q, m)
			queued := submit(t, q, s)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != queued.ID || run.Attempt != 1 || run.Memory == nil || run.Memory.RAM.Alone == seeded {
				t.Fatalf("first attempt = %+v, queued = %s", run, job(t, q, queued.ID).Reason)
			}
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
			if err := localAttemptAdmission(w, nil, *run, nil); err != nil {
				t.Fatal(err)
			}
			limit, err := w.jobCeilingBytes(*run)
			if err != nil || limit != 0 {
				t.Fatalf("estimated RAM became a ceiling: %d %v", limit, err)
			}
			a, err := fakeCeilings(t).createAttempt("short-cpu", limit, 0, true)
			if err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(filepath.Join(a.path, "memory.max"))
			if err != nil || string(b) != "max" {
				t.Fatalf("attempt memory.max = %q, %v", b, err)
			}
			r := result(run, true)
			if err := q.Started(r); err != nil {
				t.Fatal(err)
			}
			peak := 31.8 / 1024
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			key := pooledEvidenceKey(run, &m)
			if learned := q.memoryEvidence.exact[key].Peaks; len(learned) != 1 || learned[0] != peak {
				t.Fatalf("payload peak wasn't learned: %v", learned)
			}
		})
	}
}

func TestMemoryPoolReplay1970(t *testing.T) {
	for _, shortage := range []bool{false, true} {
		t.Run(fmt.Sprint("shortage=", shortage), func(t *testing.T) {
			q, clock, m, _ := gpuHostFixture(t)
			m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
			m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = 16.9, 24
			m.Devices[0].MemoryMode, m.Devices[0].MemoryMeasured = "fixed", false
			s := spec("production")
			s.Needs = ResourceNeeds{Memory: "auto", Platform: Platform{Required: []string{"xpu"}}}
			s.StartWindow = "2m"
			card := m.Devices[0].machine(m)
			seedPoolReplay(q, &Job{Spec: s}, &card, "ram", 6.3, 7)
			machineCheck(t, q, m)
			queued := submit(t, q, s)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != queued.ID || run.Memory == nil || run.Memory.RAM.ChargeGB != 7 {
				t.Fatalf("first-run XPU prior was not assigned: %+v; %s", run, job(t, q, queued.ID).Reason)
			}
			available := 16.9 - 2.0/1024
			if shortage {
				available = 4.9
			}
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return available, nil }}
			err := localAttemptAdmission(w, nil, *run, nil)
			if !shortage {
				if err != nil {
					t.Fatalf("small drift refused the launch: %v", err)
				}
				return
			}
			detail := admissionDetail(err, *run)
			if detail == nil || detail.Subcheck != memoryWaitReason || detail.Available == nil || *detail.Available != available || detail.Required != 6.3 {
				t.Fatalf("real shortage lacked typed launch evidence: %+v (%v)", detail, err)
			}
			r := capacityResult(run, false)
			r.ExecutionCause.Admission = detail
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			clock.Advance(3 * time.Minute)
			m.HostAvailableMemoryGB = available
			machineCheck(t, q, m)
			if got := job(t, q, queued.ID); got.State != "queued" || got.Starts != 0 || !got.StartCapacityWaiting {
				t.Fatalf("memory wait spent the start budget: %+v", got)
			}
		})
	}
}

func TestMemoryPoolReplay1962(t *testing.T) {
	q, clock := fixture(t)
	m := machine("pooled-cpu", "cpu")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 11, 24
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	s := cpuSpec(1, 0)
	s.Needs.Memory = "auto"
	seedPoolReplay(q, &Job{Spec: s}, &m, "ram", 8, 10)
	machineCheck(t, q, m)
	queued := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != queued.ID || run.Memory == nil {
		t.Fatal("pooled assignment missing")
	}
	free := 10.0
	actor := &attemptSupervisor{job: *run}
	runtime := &modelRuntime{machine: m.ID, clock: clock, batchSupervisors: map[attemptKey]*attemptSupervisor{jobKey(*run): actor},
		poolHostMemorySource: func(context.Context) (float64, error) { return free, nil }}
	launch := HostLaunch{Machine: m.ID, Session: run.Session, Kind: "batch", Key: run.ID, Attempt: "1"}
	for _, free = range []float64{9.99, 9.98, 9.97} {
		if err := runtime.checkBatchLaunchMemory(t.Context(), launch); err != nil {
			t.Fatalf("gate refused while typical fitted at %g GiB: %v", free, err)
		}
	}
	free = 7.5
	err := runtime.checkBatchLaunchMemory(t.Context(), launch)
	if err == nil {
		t.Fatal("gate accepted a physical shortage")
	}
	data, err := json.Marshal(custodianGateReceipt{Error: err.Error(), Admission: admissionDetail(err, *run)})
	if err != nil {
		t.Fatal(err)
	}
	var receipt custodianGateReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if d := admissionDetail(receipt.refusal(), *run); d == nil || d.Subcheck != memoryWaitReason || d.Required != 8 || d.Available == nil || *d.Available != free {
		t.Fatalf("detached gate dropped typed evidence: %+v", d)
	}
}

func TestMemoryPoolReplayBackfill(t *testing.T) {
	for _, tc := range []struct {
		issue                  string
		ram, holder, head, cpu float64
		accelerator, sameCard  bool
	}{
		{"787", 96, 56, 52, 1, false, false},
		{"1966", 24, 6, 16, 2, true, true},
		{"1717", 16, 11, 9, 8, true, false},
	} {
		t.Run(tc.issue, func(t *testing.T) {
			q, clock := fixture(t)
			m := backfillMachine("pooled", tc.ram)
			m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
			m.HostAvailableMemoryGB = tc.ram
			if tc.sameCard {
				m.CapabilityProfile.MemoryLayout = "separate"
				m.Devices[0].UnifiedMemory = false
				m.Devices[0].MemoryMeasured = true
			}
			gpuSpec := func(host float64) Spec {
				s := spec("experiment")
				s.Needs.Platform = Platform{Required: []string{"cuda"}}
				s.Needs.MemoryGB, s.Needs.HostMemoryGB = 4, host
				return s
			}
			machineCheck(t, q, m)
			s := cpuSpec(1, tc.holder)
			if tc.sameCard {
				s = gpuSpec(tc.holder)
			}
			original := submit(t, q, s)
			first := assignedJob(machineCheck(t, q, m))
			if first == nil || first.ID != original.ID {
				t.Fatal("holder was not assigned")
			}
			clock.Advance(time.Second)
			s = cpuSpec(1, tc.head)
			if tc.accelerator {
				s = gpuSpec(tc.head)
			}
			head := submit(t, q, s)
			if got := assignedJob(machineCheck(t, q, m, first)); got != nil {
				t.Fatal("head bypassed its current blocker", got.ID)
			}
			clock.Advance(time.Second)
			small := submit(t, q, cpuSpec(1, tc.cpu))
			admitted := assignedJob(machineCheck(t, q, m, first))
			if tc.issue == "1717" {
				if admitted != nil {
					t.Fatal("CPU demand consumed required RAM")
				}
				peer := machine("alternate", "cpu")
				peer.Capabilities = append(peer.Capabilities, pooledMemoryCapability)
				peer.HostMemoryMeasured, peer.HostAvailableMemoryGB = true, 16
				if got := assignedJob(machineCheck(t, q, peer)); got == nil || got.ID != small.ID {
					t.Fatal("portable CPU work did not take the alternative target")
				}
			} else if admitted == nil || admitted.ID != small.ID || admitted.Memory.RAM.ChargeGB != tc.cpu {
				t.Fatalf("surplus did not admit the expected small job: %+v", admitted)
			}
			if tc.sameCard {
				large := submit(t, q, cpuSpec(1, 7))
				if got := assignedJob(machineCheck(t, q, m, first, admitted)); got != nil && got.ID == large.ID {
					t.Fatal("backfill exceeded the head's surplus")
				}
			}
			finish(t, q, first, true)
			var remaining []*Job
			if admitted != nil {
				remaining = append(remaining, admitted)
			}
			if got := assignedJob(machineCheck(t, q, m, remaining...)); got == nil || got.ID != head.ID {
				t.Fatalf("head did not start on release: %+v, %s", got, job(t, q, head.ID).Reason)
			}
		})
	}
}

func TestMemoryPoolBackfillSkipsHeadAboveLiveHostCapacity(t *testing.T) {
	q, clock := fixture(t)
	tight := m3Machine()
	tight.ID, tight.HostID, tight.Session = "tight", "tight", "tight-session"
	tight.HostMemoryBudgetGB, tight.HostAvailableMemoryGB = 24, 16
	tight.Capabilities = append(tight.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
	wide := tight
	wide.ID, wide.HostID, wide.Session = "wide", "wide", "wide-session"
	wide.HostMemoryBudgetGB, wide.HostAvailableMemoryGB = 48, 48
	machineCheck(t, q, tight)
	machineCheck(t, q, wide)

	s := spec("production")
	s.Needs.Platform = Platform{Required: []string{"fake"}}
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	s.AllowedMachines = []string{wide.ID}
	s.Needs.HostMemoryGB, s.Needs.MemoryGB = 4, 8
	holder := submit(t, q, s)
	running := assignedJob(machineCheck(t, q, wide))
	if running == nil || running.ID != holder.ID {
		t.Fatalf("missing wide-host card holder: %+v", running)
	}
	if err := q.Started(result(running, false)); err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Second)
	s.Name = "larger head"
	s.AllowedMachines = []string{tight.ID, wide.ID}
	s.Needs.HostMemoryGB, s.Needs.MemoryGB = 18, 4
	head := submit(t, q, s)
	clock.Advance(time.Second)
	s.Name = "smaller job"
	s.AllowedMachines = []string{tight.ID}
	s.Needs.HostMemoryGB = 8
	small := submit(t, q, s)
	if got := assignedJob(machineCheck(t, q, tight)); got == nil || got.ID != small.ID {
		t.Fatalf("unfit head blocked the smaller job on the free card: %+v, %s", got, job(t, q, small.ID).Reason)
	}
	finish(t, q, running, true)
	if got := assignedJob(machineCheck(t, q, wide)); got == nil || got.ID != head.ID {
		t.Fatalf("head did not start on the wider host after release: %+v, %s", got, job(t, q, head.ID).Reason)
	}
}

func TestMemoryPoolReplay1813(t *testing.T) {
	q, clock := fixture(t)
	m := machine("deadline-host", "cpu")
	m.Devices = []MachineDevice{}
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 24, 24
	makeJob := func(name string, memory float64) Spec {
		s := cpuSpec(1, memory)
		s.Name, s.Priority, s.Class, s.StartWindow = name, "production", "production", "2m"
		return s
	}
	machineCheck(t, q, m)
	blocker := submit(t, q, makeJob("holder", 11))
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != blocker.ID {
		t.Fatal("initial production holder did not start")
	}
	if err := q.Started(result(first, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	head := submit(t, q, makeJob("head", 17))
	if got := assignedJob(machineCheck(t, q, m, first)); got != nil {
		t.Fatal("head should need the held RAM")
	}
	clock.Advance(time.Second)
	later := submit(t, q, makeJob("behind head", 8))
	if got := assignedJob(machineCheck(t, q, m, first)); got != nil {
		t.Fatal("later production work exhausted the protected surplus")
	}
	for i := 0; i < 13; i++ {
		clock.Advance(10 * time.Second)
		if got := assignedJob(machineCheck(t, q, m, first)); got != nil {
			t.Fatal("held work was dispatched before capacity released")
		}
	}
	if got := job(t, q, later.ID); got.State != "queued" || !got.StartCapacityWaiting {
		t.Fatalf("relative window expired while waiting for memory: %s, %s", got.State, got.Reason)
	}
	finish(t, q, first, true)
	lead := assignedJob(machineCheck(t, q, m))
	if lead == nil || lead.ID != head.ID {
		t.Fatal("head did not receive the release")
	}
	finish(t, q, lead, true)
	got := assignedJob(machineCheck(t, q, m))
	if got == nil || got.ID != later.ID || got.Spec.StartBy == nil || !got.Spec.StartBy.Equal(clock.Now().Add(2*time.Minute)) {
		t.Fatalf("waiting job did not receive a fresh start budget: %+v", got)
	}
}

func TestMemoryPoolMixedFleetKeepsRunningEnvelope(t *testing.T) {
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	legacy := machine("legacy", "cpu")
	legacy.HostMemoryMeasured, legacy.HostAvailableMemoryGB, legacy.HostMemoryBudgetGB = true, 24, 24
	pooled := machine("pooled", "cpu")
	pooled.HostMemoryMeasured, pooled.HostAvailableMemoryGB, pooled.HostMemoryBudgetGB = true, 24, 24
	pooled.Capabilities = append(pooled.Capabilities, pooledMemoryCapability)
	pooled.Release = &RunningRelease{Version: "candidate"}
	pooled.MemoryContainment = &MemoryContainment{State: "available"}
	s := cpuSpec(1, 4)
	s.AllowedMachines = []string{legacy.ID}
	old := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, legacy))
	if a == nil || a.ID != old.ID {
		t.Fatal("could not seed running custody")
	}
	envelope := &JobMemoryEnvelope{Basis: "declared", HostGB: 4, LimitHostGB: 4}
	a.Memory, a.MemoryEnvelope = nil, envelope
	if err := q.update(func() error {
		running := q.touchJob(a.ID)
		running.Memory, running.MemoryEnvelope = nil, envelope
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := result(a, true)
	if err := q.Started(r); err != nil {
		t.Fatal(err)
	}
	s.AllowedMachines = []string{pooled.ID}
	current := submit(t, q, s)
	b := assignedJob(machineCheck(t, q, pooled))
	if b == nil || b.ID != current.ID || b.Memory == nil || b.Memory.RAM.ChargeGB != 4 {
		t.Fatal("pooled worker did not receive an estimate")
	}
	q.Close()
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	legacy.Release = &RunningRelease{Version: "v3.1.0-rc.20261123.s087r6.35"}
	submit(t, q, cpuSpec(1, 2))
	reply := machineCheck(t, q, legacy, a)
	if assignedJob(reply) != nil || workerMemoryHold(&legacy).Kind != workerMemoryReleaseReason {
		t.Fatal("previous-release worker received new work")
	}
	uncontained := pooled
	uncontained.MemoryContainment = nil
	if hold := workerMemoryHold(&uncontained); hold == nil || hold.Kind != workerMemoryContainmentReason {
		t.Fatalf("uncontained Linux worker did not receive a typed refusal: %+v", hold)
	}
	stored := job(t, q, old.ID)
	if stored.State != "running" || stored.Attempt != a.Attempt || stored.Session != a.Session || stored.Memory != nil || stored.MemoryEnvelope == nil || stored.MemoryEnvelope.HostGB != 4 {
		t.Fatalf("restart rewrote a running legacy envelope: %+v", stored)
	}
	peak := 2.4
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, old.ID); got.State != "succeeded" {
		t.Fatalf("legacy result was not accepted: %s", got.State)
	}
	evidence, err := readMemoryEvidence(q.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := evidence.exact[pooledEvidenceKey(a, &legacy)].Peaks; len(got) != 1 || got[0] != peak {
		t.Fatalf("legacy completion was not projected durably: %v", got)
	}
	w := Worker{Machine: pooled, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 24, nil }}
	offered := *a
	w.acceptMemoryOffer(&offered)
	if offered.Memory.RAM.ChargeGB != 4 || offered.Memory.LimitGB != 0 {
		t.Fatalf("old envelope was not interpreted as a soft estimate: %+v", offered.Memory)
	}
	if err := localAttemptAdmission(w, nil, offered, nil); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryPoolOverrunWithoutShortage(t *testing.T) {
	q, _ := fixture(t)
	m := sharedUnifiedMachine()
	j := submit(t, q, sharedSpec("experiment", 17))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != j.ID {
		t.Fatal("single tenant not assigned")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	growthCheck(t, q, m, []*Job{run}, []float64{50}, 0)
	requireState(t, q, run.ID, "running")
}
