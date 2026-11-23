package myce

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func backfillMachine(id string, ram float64) Machine {
	m := cpuMachine(id, id, "cuda", 8, ram)
	m.CapabilityProfile.MemoryLayout = "unified"
	m.CapabilityProfile.SystemMemoryBytes = uint64(2 * ram * (1 << 30))
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 2 * ram}
	m.HostMemoryBudgetGB = ram
	m.Devices = []MachineDevice{{ID: "0", Platform: "cuda", Identity: "cuda-0", Fingerprint: "cuda", Selector: "0", UnifiedMemory: true, MemoryMode: "fixed", TotalMemoryGB: ram, BudgetGB: ram, FreeMemoryGB: ram}}
	return m
}

// Backfill tests exercise placement over an existing fleet, not enrollment
// throughput. Enroll one representative peer through Checkin, then load identical
// idle peers in one durable fixture transaction. Every distinct physical host
// remains visible to every real submission/check-in/completion below; four hosts
// cover the multi-host placement fence as fully as 200 did (#1086 coverage check).
func seedBackfillPeers(t *testing.T, q *Queue, exemplar string, hosts int) {
	t.Helper()
	accepted := q.s.Machines[exemplar]
	if err := q.update(func() error {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		for i := 2; i < hosts; i++ {
			// Keep queue-owned fields too (including CPU-ledger mode). Nested
			// public inventory is independent of the accepted representative.
			peer := *accepted
			inventory := cloneMachine(accepted)
			peer.CapabilityProfile, peer.Devices = inventory.CapabilityProfile, inventory.Devices
			peer.ID = fmt.Sprintf("peer-%03d", i)
			peer.HostID, peer.Session = peer.ID, "session-"+peer.ID
			q.touchMachine(peer.ID)
			q.s.Machines[peer.ID] = &peer
			// These peers have the same accepted cadence/session evidence,
			// rather than becoming cold workers that placement cannot use.
			contact := *q.contacts[exemplar]
			contact.session, contact.pending = peer.Session, nil
			q.editContact(peer.ID)
			q.contacts[peer.ID] = &contact
			evidence := q.s.ContactEvidence[exemplar]
			evidence.Session = peer.Session
			q.s.ContactEvidence[peer.ID] = evidence
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(q.s.Machines) != hosts {
		t.Fatalf("backfill fixture has %d hosts, want %d", len(q.s.Machines), hosts)
	}
	for _, peer := range q.s.Machines {
		if q.workerLossAllowance(peer) <= 0 {
			t.Fatalf("backfill fixture peer %s has no qualified cadence", peer.ID)
		}
	}
}

func TestHostBackfillAccumulatesForAccelerator(t *testing.T) {
	for _, tc := range []struct {
		hosts int
		ram   float64
		cores bool
	}{{1, 16, false}, {4, 1024, false}, {1, 64, true}} {
		t.Run(fmt.Sprintf("hosts=%d/ram=%g/cores=%v", tc.hosts, tc.ram, tc.cores), func(t *testing.T) {
			q, clock := fixture(t)
			m := backfillMachine("accelerator", tc.ram)
			machineCheck(t, q, m)
			// Other hosts cannot run these Linux commands, as in the incident.
			if tc.hosts > 1 {
				peer := valueMachine("peer-001", "cpu", tc.ram/8)
				peer.CapabilityProfile.OS = "darwin"
				check(t, q, peer, nil)
				seedBackfillPeers(t, q, peer.ID, tc.hosts)
			}
			cpu := cpuSpec(1, tc.ram/4)
			if tc.cores {
				cpu = cpuSpec(3, 1)
			}
			cpu.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
			a := submit(t, q, cpu)
			first := assignedJob(machineCheck(t, q, m))
			if first == nil || first.ID != a.ID {
				t.Fatal("first CPU assignment", first, q.waitReason(q.s.Jobs[a.ID]), q.s.Machines[m.ID])
			}
			b := submit(t, q, cpu)
			second := assignedJob(machineCheck(t, q, m, first))
			if second == nil || second.ID != b.ID {
				t.Fatal("second CPU assignment", second)
			}
			gpu := spec("experiment")
			gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
			gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = tc.ram/4, tc.ram/2
			if tc.cores {
				gpu.Needs.CPUCores, gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 4, 1, 1
			}
			demand := submit(t, q, gpu)
			clock.Advance(time.Second)
			cpu.Needs.MemoryGB = tc.ram / 8
			cpu.Needs.CPUCores = 1
			later := submit(t, q, cpu)
			if got := assignedJob(machineCheck(t, q, m, first, second)); got != nil {
				t.Fatalf("later CPU backfill %s consumed RAM needed by waiting accelerator %s (later=%s)", got.ID, demand.ID, later.ID)
			}
			status, err := q.jobSnapshot(later.ID, context.Background())
			if err != nil || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demand.ID) {
				t.Fatalf("missing capacity accumulation diagnostic: %v, %v", status, err)
			}
			for _, running := range []*Job{first, second} {
				if q.s.Jobs[running.ID].StopAt != nil {
					t.Fatal("same-tier running work was stopped")
				}
			}
			finish(t, q, first, true)
			if got := assignedJob(machineCheck(t, q, m, second)); got == nil || got.ID != demand.ID {
				t.Fatal("accelerator did not start after natural CPU completion", got)
			}
		})
	}
}

// #1717: a partial CPU release must accumulate toward older native checks,
// rather than immediately admitting a new CPU attempt. Check first-run sizing
// uses physical headroom, so a check can remain unfitting while CPU work fits.
func TestHostBackfillAccumulatesForExtensionChecks(t *testing.T) {
	for _, tc := range []struct {
		name               string
		bounded, alternate bool
	}{
		{"bounded release", true, false},
		{"alternate host", true, true},
		{"unknown release", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m, probe := gpuHostFixture(t)
			m.HostAvailableMemoryGB = 20
			machineCheck(t, q, m)
			var holders []*Job
			for i := range 2 {
				s := cpuSpec(1, 8)
				s.Name = fmt.Sprintf("holder-%d", i)
				if tc.bounded {
					s.WallTime = "1h"
				}
				j := submit(t, q, s)
				run := assignedJob(machineCheck(t, q, m, holders...))
				if run == nil || run.ID != j.ID {
					t.Fatal("missing initial CPU holder", run)
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				holders = append(holders, run)
			}
			probe.Workload = nil
			probe.ExtensionDigest = registeredExtension(t, q, probe.Extension).Digest
			probe.Project = "extension-check"
			probe.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			probe.AllowedMachines = []string{m.ID}
			probe.SizeHint = SizeHint{HostMemoryGB: 18}
			var demands []*Job
			for _, device := range []string{"0", "1"} {
				probe.Needs.GPU = &GPUSelection{Devices: []string{device}}
				demands = append(demands, submit(t, q, probe))
				clock.Advance(time.Second)
			}
			later := submit(t, q, cpuSpec(1, 8))
			finish(t, q, holders[0], true)
			got := assignedJob(machineCheck(t, q, m, holders[1]))
			if tc.bounded {
				if got != nil {
					t.Fatalf("partial release admitted %s before older extension checks", got.ID)
				}
				status, err := q.jobSnapshot(later.ID, context.Background())
				if err != nil || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demands[0].ID) {
					t.Fatalf("missing check accumulation hold: %v, %v", status, err)
				}
			} else {
				if got == nil || got.ID != later.ID {
					t.Fatal("native check with no evidenced release blocked fitting CPU work", job(t, q, later.ID).Reason)
				}
				finish(t, q, got, true)
			}
			if tc.alternate {
				peer := cpuMachine("general", "general", "cpu", 8, 32)
				peer.Devices = []MachineDevice{}
				if got := assignedJob(machineCheck(t, q, peer)); got == nil || got.ID != later.ID {
					t.Fatal("portable CPU work did not use another eligible host", got)
				}
			}
			if current := job(t, q, holders[1].ID); current.State != "running" || current.StopAt != nil || current.ScarceHostYield != nil {
				t.Fatal("same-tier holder was preempted")
			}
			finish(t, q, holders[1], true)
			for _, demand := range demands {
				run := assignedJob(machineCheck(t, q, m))
				if run == nil || run.ID != demand.ID {
					t.Fatal("older extension check did not acquire accumulated memory", run, job(t, q, demand.ID).Reason)
				}
				finish(t, q, run, true)
			}
		})
	}
}

func TestHostBackfillExtensionCheckBeforeMaintenanceCPU(t *testing.T) {
	q, clock, m, probe := gpuHostFixture(t)
	m.HostAvailableMemoryGB = 20
	cpu := submit(t, q, cpuSpec(1, 8))
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.ID != cpu.ID {
		t.Fatal("missing CPU execution", running)
	}
	if err := q.Started(result(running, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	probe.Workload = nil
	probe.ExtensionDigest = registeredExtension(t, q, probe.Extension).Digest
	probe.AllowedMachines = []string{m.ID}
	probe.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
	demand := submit(t, q, probe)
	if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, running)
	r := result(running, true)
	r.ReleaseOnly = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if !maintenanceResume(job(t, q, cpu.ID)) {
		t.Fatal("fixture lost maintenance resume priority")
	}
	if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != demand.ID {
		t.Fatal("CPU maintenance resume took queued accelerator check's memory", got)
	}
}

func TestHostBackfillExtensionCheckRespectsRetainedCardCustody(t *testing.T) {
	q, clock, m, probe := gpuHostFixture(t)
	m.HostAvailableMemoryGB = 20
	var holders []*Job
	for _, ram := range []float64{2, 10} {
		s := sharedSpec("experiment", 2)
		s.Needs.HostMemoryGB, s.WallTime = ram, "1h"
		s.Needs.Platform = Platform{Required: []string{"xpu"}}
		s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
		queued := submit(t, q, s)
		run := assignedJob(machineCheck(t, q, m, holders...))
		if run == nil || run.ID != queued.ID {
			t.Fatal("missing sharing attempt", job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		holders = append(holders, run)
	}
	pending := result(holders[0], true)
	pending.ReleasePending = true
	if err := q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, holders[0].ID, "succeeded")
	clock.Advance(time.Second)
	probe.Workload = nil
	probe.ExtensionDigest = registeredExtension(t, q, probe.Extension).Digest
	probe.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
	probe.AllowedMachines = []string{m.ID}
	probe.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	probe.SizeHint = SizeHint{HostMemoryGB: 15}
	demand := submit(t, q, probe)
	backfill := submit(t, q, cpuSpec(1, 4))
	cpu := assignedJob(machineCheck(t, q, m, holders[1]))
	if cpu == nil || cpu.ID != backfill.ID {
		t.Fatal("check fenced fitting CPU work behind retained card custody", job(t, q, backfill.ID).Reason)
	}
	finish(t, q, cpu, true)
	finish(t, q, holders[1], true)
	if got := assignedJob(machineCheck(t, q, m)); got != nil {
		t.Fatal("check ran before exact card release", got.ID)
	}
	pending.ReleasePending, pending.ReleaseOnly = false, true
	if err := q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != demand.ID {
		t.Fatal("check did not start after exact card release", job(t, q, demand.ID).Reason)
	}
}

// A fitting GPU job also consumes the shared host pool. Explain the same
// accumulation fence as CPU backfill, then place it beside CPU holders once
// the preceding demand clears (#1701).
func TestHostBackfillCUDAWithCPUOnlyHolders(t *testing.T) {
	q, clock := fixture(t)
	m := backfillMachine("accelerator", 96)
	m.CapabilityProfile.LogicalCPUs = 20
	m.MemoryCeilingSlackGB = .125
	var holders []*Job
	for range 2 {
		queued := submit(t, q, cpuSpec(3, 24))
		run := assignedJob(machineCheck(t, q, m, holders...))
		if run == nil || run.ID != queued.ID {
			t.Fatal("CPU holder not assigned", job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		holders = append(holders, run)
	}
	large := sharedSpec("experiment", 72)
	large.Project, large.Needs.HostMemoryGB = "large-gpu", 8
	large.Needs.Platform = Platform{Required: []string{"cuda"}}
	demand := submit(t, q, large)
	clock.Advance(time.Second)
	small := sharedSpec("experiment", 2)
	small.Needs.HostMemoryGB, small.Needs.CPUCores = 9, 2
	small.Needs.Platform = Platform{Required: []string{"cuda"}}
	later := submit(t, q, small)
	if got := assignedJob(machineCheck(t, q, m, holders...)); got != nil {
		t.Fatal("GPU backfill deepened the preceding demand's host RAM deficit", got.ID)
	}
	status, err := q.jobSnapshot(later.ID, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Hold == nil || status.Hold.Kind != "host_backfill" || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demand.ID) {
		t.Fatalf("GPU accumulation wait not explained: hold=%+v code=%q reason=%q", status.Hold, status.DiagnosticCode, status.Reason)
	}
	if !strings.Contains(status.Hold.Reason, "RAM") || status.Attempt != 0 {
		t.Fatalf("hold omitted RAM or refusal consumed an attempt: hold=%+v attempt=%d", status.Hold, status.Attempt)
	}
	// Cancellation here is a local fixture action, never an operator workaround.
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	got := assignedJob(machineCheck(t, q, m, holders...))
	if got == nil || got.ID != later.ID || len(got.AllocatedDevices) != 1 || got.AllocatedDevices[0] != "0" {
		t.Fatal("fitting GPU demand did not place beside CPU holders", got, job(t, q, later.ID).Reason)
	}
	for _, run := range holders {
		current := job(t, q, run.ID)
		if current.State != "running" || current.StopAt != nil {
			t.Fatal("GPU demand disturbed CPU holder", current.State, current.Reason)
		}
	}
}

func TestHostBackfillNeedsReadyDemand(t *testing.T) {
	for _, reason := range []string{"cold", "impossible", "cancelled", "higher-tier"} {
		t.Run(reason, func(t *testing.T) {
			q, clock := fixture(t)
			m := backfillMachine("accelerator", 64)
			m.EnvironmentBuilder = true
			machineCheck(t, q, m)
			first := submit(t, q, cpuSpec(1, 32))
			running := assignedJob(machineCheck(t, q, m))
			if running == nil || running.ID != first.ID {
				t.Fatal("missing initial CPU work", running)
			}
			gpu := spec("experiment")
			gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
			gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 16, 32
			if reason == "cold" {
				r := binaryRecipe(t, "cuda")
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"cold": {"cuda": r}}}); err != nil {
					t.Fatal(err)
				}
				gpu.Environment.Name = "cold"
			}
			if reason == "impossible" {
				gpu.Needs.HostMemoryGB = 64
			}
			demand := submit(t, q, gpu)
			if reason == "cancelled" {
				if err := q.Cancel(demand.ID); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			cpu := cpuSpec(1, 4)
			if reason == "higher-tier" {
				cpu.Priority = "production"
			}
			later := submit(t, q, cpu)
			if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != later.ID {
				t.Fatal("backfill withheld without ready same/higher-tier demand", got)
			}
		})
	}
}

func TestHostBackfillProtectsOnlyOneTarget(t *testing.T) {
	q, _ := fixture(t)
	a, b := backfillMachine("a", 64), backfillMachine("b", 64)
	machineCheck(t, q, a)
	machineCheck(t, q, b)
	cpu := cpuSpec(1, 32)
	cpu.AllowedMachines = []string{a.ID}
	initial := submit(t, q, cpu)
	running := assignedJob(machineCheck(t, q, a))
	if running == nil || running.ID != initial.ID {
		t.Fatal("missing initial CPU work", running)
	}
	gpu := spec("experiment")
	gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
	gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 16, 32
	demand := submit(t, q, gpu)
	cpu.Needs.MemoryGB = 4
	later := submit(t, q, cpu)
	if got := assignedJob(machineCheck(t, q, a, running)); got == nil || got.ID != later.ID {
		t.Fatal("busy host withheld although demand fits another host", got)
	}
	if got := assignedJob(machineCheck(t, q, b)); got == nil || got.ID != demand.ID {
		t.Fatal("demand did not start on its immediately fitting target", got)
	}
}

// A completely occupied host needs no backfill projection yet. After a partial
// natural release, the new pass must still protect a demand that cannot fit.
func TestHostBackfillFullyOccupiedThenPartialRelease(t *testing.T) {
	q, clock := fixture(t)
	m := backfillMachine("accelerator", 64)
	machineCheck(t, q, m)
	firstJob := submit(t, q, cpuSpec(3, 1))
	first := assignedJob(machineCheck(t, q, m))
	secondJob := submit(t, q, cpuSpec(5, 1))
	second := assignedJob(machineCheck(t, q, m, first))
	if first == nil || second == nil || first.ID != firstJob.ID || second.ID != secondJob.ID {
		t.Fatal("missing occupied-host fixture", first, second)
	}
	gpu := spec("experiment")
	gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
	gpu.Needs.CPUCores, gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 4, 1, 1
	demand := submit(t, q, gpu)
	clock.Advance(time.Second)
	later := submit(t, q, cpuSpec(1, 1))
	if got := assignedJob(machineCheck(t, q, m, first, second)); got != nil {
		t.Fatal("assigned work on a fully occupied host", got)
	}
	finish(t, q, first, true)
	if got := assignedJob(machineCheck(t, q, m, second)); got != nil {
		t.Fatal("partial release admitted backfill ahead of the waiting demand", got)
	}
	status, err := q.jobSnapshot(later.ID, context.Background())
	if err != nil || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demand.ID) {
		t.Fatal("partial release lost the capacity blocker", status, err)
	}
	finish(t, q, second, true)
	if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != demand.ID {
		t.Fatal("demand did not start after full release", got)
	}
}

// CPU backfill can use surplus beside a finite device holder. The next GPU
// execution still needs its full host vector when the card releases.
func TestHostBackfillProtectsNextGPUWhileDeviceBusy(t *testing.T) {
	for _, tc := range []struct {
		name              string
		background        bool
		tooLarge, surplus Spec
	}{
		{"RAM", false, cpuSpec(1, 8), cpuSpec(1, 7)},
		{"cores", false, cpuSpec(6, 1), cpuSpec(5, 1)},
		{"unrelated CPU holder", true, cpuSpec(1, 6), cpuSpec(1, 5)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			m.Devices = m.Devices[:1]
			m.HostMemoryBudgetGB = 16
			machineCheck(t, q, m)
			gpu := spec("experiment")
			gpu.Needs.Platform = Platform{Required: []string{"fake"}}
			gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB, gpu.Needs.CPUCores = 8, 8, 2
			gpu.WallTime = "1h"
			first := submit(t, q, gpu)
			running := assignedJob(machineCheck(t, q, m))
			if running == nil || running.ID != first.ID {
				t.Fatal("initial GPU execution missing", running)
			}
			holders := []*Job{running}
			if tc.background {
				cpu := submit(t, q, cpuSpec(1, 2))
				background := assignedJob(machineCheck(t, q, m, holders...))
				if background == nil || background.ID != cpu.ID {
					t.Fatal("background CPU execution missing", background)
				}
				holders = append(holders, background)
			}
			gpu.Needs.HostMemoryGB, gpu.Needs.CPUCores = 9, 3
			demand := submit(t, q, gpu)
			clock.Advance(time.Second)
			cpu := submit(t, q, tc.tooLarge)
			if got := assignedJob(machineCheck(t, q, m, holders...)); got != nil {
				t.Fatal("CPU backfill took capacity needed by the next GPU execution", got.ID)
			}
			status, err := q.jobSnapshot(cpu.ID, context.Background())
			if err != nil || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demand.ID) {
				t.Fatal("missing next-GPU capacity fence", status, err)
			}
			light := submit(t, q, tc.surplus)
			backfill := assignedJob(machineCheck(t, q, m, holders...))
			if backfill == nil || backfill.ID != light.ID {
				t.Fatalf("surplus CPU capacity withheld: %v (%s)", backfill, job(t, q, light.ID).Reason)
			}
			finish(t, q, running, true)
			remaining := append(holders[1:], backfill)
			if got := assignedJob(machineCheck(t, q, m, remaining...)); got == nil || got.ID != demand.ID {
				t.Fatal("waiting GPU execution stranded after device release", got)
			}
		})
	}
}

func TestHostBackfillDeviceReleaseRequiresDeviceWait(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.Devices = m.Devices[:1]
	m.HostMemoryBudgetGB = 16
	gpu := spec("experiment")
	gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 4, 8
	first := submit(t, q, gpu)
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.ID != first.ID {
		t.Fatal("initial GPU execution missing", running)
	}
	cpu := submit(t, q, cpuSpec(1, 2))
	background := assignedJob(machineCheck(t, q, m, running))
	if background == nil || background.ID != cpu.ID {
		t.Fatal("background CPU execution missing", background)
	}
	// The GPU can share the card as soon as the CPU holder releases 2 GiB.
	// Borrowing from the first GPU's future release would delay that start.
	demand := submit(t, q, gpu)
	later := submit(t, q, cpuSpec(1, 1))
	if got := assignedJob(machineCheck(t, q, m, running, background)); got != nil {
		t.Fatal("CPU work borrowed RAM from a GPU that can still share its card", got.ID)
	}
	status, err := q.jobSnapshot(later.ID, context.Background())
	if err != nil || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demand.ID) {
		t.Fatal("missing next-GPU capacity fence", status, err)
	}
	finish(t, q, background, true)
	if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != demand.ID {
		t.Fatal("GPU demand did not share the card after CPU release", got)
	}
}
