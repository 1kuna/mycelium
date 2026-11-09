package myce

import (
	"testing"
	"time"
)

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
	if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: old, period: time.Second}); err != nil {
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

func TestFleetUpgradePendingRestartNeedsExactZeroEngineCalls(t *testing.T) {
	q, c, m := modelFixture(t, "host")
	config := q.models
	signed, key, _ := signedTestRelease(t)
	if err := q.ConfigureEnrollmentRelease(signed, key, "https://publisher.invalid/release"); err != nil {
		t.Fatal(err)
	}
	m.Fleet = fleetTestProof(c, m.Session, 200, false)
	check(t, q, m, nil)
	q.fleetQueue = &fleetQueueRuntime{Host: "host", OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-old", 100, false)}
	call := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	if call.Machine != m.ID {
		t.Fatal("model call not admitted")
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if q.fleetActiveHost() != nil {
		t.Fatal("live inference acquired upgrade drain")
	}
	q = reopenCancellationQueue(t, q, c)
	fakeCheckpointAvailability(t, q, true)
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	q.fleetQueue = &fleetQueueRuntime{Host: "host", OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-reopened", 101, false)}
	slot := q.s.ModelSlots[slotKey(m.ID, "test")]
	attempt := slot.Attempt
	if slot.InFlight != 0 || slot.Recovering || q.s.Hosts["host"] != nil && q.s.Hosts["host"].Policy.ProductionHold {
		t.Fatal("fixture must exercise reset calls without production-hold recovery")
	}
	for _, report := range []ModelReport{
		{Name: "test", Attempt: "stale-engine", State: "ready", CallsMeasured: true},
		{Name: "test", Attempt: attempt, State: "ready"},
		{Name: "test", Attempt: attempt, State: "ready", CallsMeasured: true, InFlight: 1},
	} {
		if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, ModelReports: []ModelReport{report}}); err != nil {
			t.Fatal(err)
		}
		if q.maintenanceBlocked("host") || q.fleetPausesCalls() || q.fleetActiveHost() != nil || q.s.ModelSlots[slotKey(m.ID, "test")].State != "ready" {
			t.Fatal("unknown or live engine calls acquired drain", q.fleetUpgradeStatus())
		}
	}
	if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, ModelReports: []ModelReport{{Name: "test", Attempt: attempt, State: "ready", CallsMeasured: true}}}); err != nil {
		t.Fatal(err)
	}
	if h := q.fleetActiveHost(); h == nil || h.Phase != "draining" || !h.Owned {
		t.Fatal("exact zero call report did not select idle cutover", h)
	}
	if i, err := q.fleetInstruction("host", "@queue", "queue-reopened"); err != nil || i.Operation != "" {
		t.Fatal("resident engine bypassed unload custody", i, err)
	}
}
