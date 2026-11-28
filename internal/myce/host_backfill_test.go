package myce

import (
	"fmt"
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

func TestHostBackfillExtensionCheckRespectsRetainedCardCustody(t *testing.T) {
	q, clock, m, probe := gpuHostFixture(t)
	m.HostAvailableMemoryGB = 20
	var holders []*Job
	for _, ram := range []float64{2, 10} {
		s := sharedSpec("experiment", 2)
		s.Needs.HostMemoryGB, s.WallTime = ram, "1h"
		s.Needs.Platform = Platform{Required: []string{"xpu"}}
		s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
		queued := submitMeasuredCardJob(t, q, m, s)
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
	stale := pending
	stale.Session += "-stale"
	// An unrelated session's report must leave the retained holder in the
	// next candidate pass, even though the terminal result already exists.
	if err := q.Complete(stale); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m)); got != nil {
		t.Fatal("check ran after a release from the wrong session", got.ID)
	}
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

func TestHostBackfillAccumulatesCoresForXPU(t *testing.T) {
	t.Run("ordinary stream", func(t *testing.T) { replayXPUCoreBackfill(t, false) })
	t.Run("CPU maintenance resume", func(t *testing.T) { replayXPUCoreBackfill(t, true) })
}

func replayXPUCoreBackfill(t *testing.T, withResume bool) {
	t.Helper()
	q, clock, m, _ := gpuHostFixture(t)
	m.CapabilityProfile.LogicalCPUs = 6
	machineCheck(t, q, m)

	small := cpuSpec(1, 0.5)
	small.WallTime = "1h"
	var running []*Job
	for i := 0; i < 6; i++ {
		queued := submit(t, q, small)
		got := assignedJob(machineCheck(t, q, m, running...))
		if got == nil || got.ID != queued.ID {
			t.Fatalf("filling core %d: %v", i, job(t, q, queued.ID).Reason)
		}
		running = append(running, got)
	}

	clock.Advance(time.Second)
	// A queued CPU maintenance resume uses the same host cores as the
	// incoming stream and must also leave room for the accelerator head.
	if withResume {
		resume := submit(t, q, small)
		if err := q.update(func() error {
			now := clock.Now()
			q.touchJob(resume.ID).MaintenanceResume = &now
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
	}
	xpu := spec("experiment")
	xpu.Project = "accelerator-project"
	xpu.Needs = ResourceNeeds{CPUCores: 4, HostMemoryGB: 1, MemoryGB: 2,
		Platform: Platform{Required: []string{"xpu"}}, GPU: &GPUSelection{Devices: []string{"1"}}}
	head := submit(t, q, xpu)
	for release := 1; release <= 4; release++ {
		clock.Advance(time.Second)
		submit(t, q, small)
		finish(t, q, running[0], true)
		running = running[1:]
		if release == 1 {
			// A higher tier may use the same partial release. Once that
			// attempt finishes, the core still accumulates for the XPU head.
			urgent := small
			urgent.Priority = "deadline"
			wanted := submit(t, q, urgent)
			got := assignedJob(machineCheck(t, q, m, running...))
			if got == nil || got.ID != wanted.ID {
				t.Fatal("XPU fence blocked the deadline tier")
			}
			finish(t, q, got, true)
		}
		got := assignedJob(machineCheck(t, q, m, running...))
		if release < 4 && got != nil {
			t.Fatalf("release %d was backfilled by %s", release, got.ID)
		}
		if release == 4 && (got == nil || got.ID != head.ID) {
			t.Fatalf("XPU demand missed its fourth release: %v", job(t, q, head.ID).Reason)
		}
		ledger := q.s.Machines[m.ID]
		if ledger.CPUInUseCores > ledger.EffectiveCPUCores {
			t.Fatalf("charged %d above %d admitted", ledger.CPUInUseCores, ledger.EffectiveCPUCores)
		}
	}
}

// CPU backfill can use surplus beside a finite device holder. The next GPU
// execution still needs its full host vector when the card releases.
