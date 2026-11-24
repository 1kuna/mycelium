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

// #1717: a partial CPU release must accumulate toward older native checks,
// rather than immediately admitting a new CPU attempt. Check first-run sizing
// uses physical headroom, so a check can remain unfitting while CPU work fits.

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

// A completely occupied host needs no backfill projection yet. After a partial
// natural release, the new pass must still protect a demand that cannot fit.

// CPU backfill can use surplus beside a finite device holder. The next GPU
// execution still needs its full host vector when the card releases.
