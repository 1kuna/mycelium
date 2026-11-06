package myce

import (
	"testing"
	"time"
)

func TestFleetUpgradeBusyHostAdmitsUntilExactIdleMoment(t *testing.T) {
	q, c, _ := fleetFixture(t, "host")
	m := cpuBackfillMachine("worker", 16)
	m.HostID = "host"
	m.Fleet = fleetTestProof(c, m.Session, 200, false)
	machineCheck(t, q, m)
	s := cpuSpec(1, 1)
	first := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID {
		t.Fatal("first job not assigned")
	}
	machineCheck(t, q, m, a)
	operation, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// An hours-long execution does not reserve the whole host for maintenance.
	c.Advance(2 * time.Hour)
	m.Fleet.ObservedAt, q.fleetQueue.Proof.ObservedAt = c.Now(), c.Now()
	machineCheck(t, q, m, a)
	second := submit(t, q, s)
	b := assignedJob(machineCheck(t, q, m, a))
	if b == nil || b.ID != second.ID || q.maintenanceBlocked("host") || q.fleetActiveHost() != nil {
		t.Fatal("pending update held spare capacity", q.fleetUpgradeStatus())
	}
	for _, r := range []*Job{a, b} {
		if got := job(t, q, r.ID); got.State != "running" || got.StopAt != nil || got.Attempt != 1 || got.Losses != 0 {
			t.Fatal("pending upgrade interrupted execution", got)
		}
	}
	finish(t, q, a, true)
	// A completion retaining its payload's custody is still busy.
	r := result(b, true)
	r.ReleasePending = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	third := submit(t, q, s)
	if q.maintenanceBlocked("host") || q.fleetActiveHost() != nil {
		t.Fatal("release-pending payload acquired a drain")
	}
	r.ReleasePending, r.ReleaseOnly = false, true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// The exact release and next placement share the serialized queue boundary.
	if got := assignedJob(machineCheck(t, q, m)); got != nil {
		t.Fatal("queued work refilled the cutover gap", got.ID)
	}
	h := q.fleetActiveHost()
	if h == nil || h.Phase != "queue_update" || !h.Owned || q.s.HostMaintenance["host"].DrainOwner != operation.Operation || job(t, q, third.ID).State != "queued" {
		t.Fatal("fresh idle moment did not acquire exact drain", q.fleetUpgradeStatus())
	}
	if i := fleetInstructionFor(t, q); i.Machine != "@queue" {
		t.Fatal("colocated queue must update first")
	}
}

func TestFleetUpgradePendingSurvivesRestartAndRefreshesIdleSession(t *testing.T) {
	q, c, workers := fleetFixture(t, "host", "host")
	m := workers[0]
	j := submit(t, q, spec("experiment"))
	a := check(t, q, m, nil).Job
	if a == nil || a.ID != j.ID {
		t.Fatal("job not assigned")
	}
	if err := q.Started(result(a, true)); err != nil {
		t.Fatal(err)
	}
	operation, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	q = reopenCancellationQueue(t, q, c)
	fakeCheckpointAvailability(t, q, true)
	q.fleetQueue = &fleetQueueRuntime{Host: "host", OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-reopened", 101, false)}
	fleetTick(t, q)
	if q.s.FleetUpgrade.Operation != operation.Operation || q.fleetActiveHost() != nil || q.maintenanceBlocked("host") {
		t.Fatal("restart converted pending intent into a drain", q.fleetUpgradeStatus())
	}
	// The same returning attempt remains exact and running, without a new try.
	check(t, q, m, a)
	if got := job(t, q, a.ID); got.State != "running" || got.Attempt != 1 || got.Losses != 0 {
		t.Fatal("queue restart changed running attempt", got)
	}
	completion := result(a, true)
	completion.ReleasePending = true
	if err := q.Complete(completion); err != nil {
		t.Fatal(err)
	}
	old := m.Session
	m.Session = "worker-restarted-before-cutover"
	m.Fleet = fleetTestProof(c, m.Session, 201, false)
	if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: old}); err != nil {
		t.Fatal(err)
	}
	h := q.fleetActiveHost()
	if h == nil || h.Before.Session != m.Session || h.QueueBefore.Session != "queue-reopened" {
		t.Fatal("idle cutover retained obsolete pending identities", h)
	}
}

func TestFleetUpgradeIdleDeviceEngineMustReleaseBeforeInstruction(t *testing.T) {
	q, _, workers := fleetFixture(t, "host", "host")
	m := workers[0]
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	s := &ModelSlot{Machine: m.ID, Session: m.Session, Device: "0", Model: "test", Attempt: "engine", State: "ready", Recovering: true}
	q.s.ModelSlots = map[string]*ModelSlot{slotKey(m.ID, s.Model, s.Device): s}
	// Use the upgrade transaction alone: the model's own reconciliation is
	// covered by the host maintenance tests, including exact unload reports.
	if err := q.update(func() error { q.maintainFleetUpgrade(); return nil }); err != nil {
		t.Fatal(err)
	}
	if q.maintenanceBlocked("host") || q.fleetActiveHost() != nil {
		t.Fatal("unknown calls after restart proved idle")
	}
	s = q.s.ModelSlots[slotKey(m.ID, "test", "0")]
	s.Recovering = false
	if err := q.update(func() error { q.maintainFleetUpgrade(); return nil }); err != nil {
		t.Fatal(err)
	}
	h := q.fleetActiveHost()
	if h == nil || h.Phase != "draining" || !h.Owned {
		t.Fatal("ready idle device engine did not enter drain", h)
	}
	if i, err := q.fleetInstruction("host", "@queue", "queue-old"); err != nil || i.Operation != "" {
		t.Fatal("resident engine authorized restart", i, err)
	}
	delete(q.s.ModelSlots, slotKey(m.ID, "test", "0")) // Exact unload confirmation.
	if err := q.update(func() error { q.maintainFleetUpgrade(); return nil }); err != nil {
		t.Fatal(err)
	}
	if i := fleetInstructionFor(t, q); i.Machine != "@queue" {
		t.Fatal("released engine did not permit queue update")
	}
}
