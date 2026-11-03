package myce

import (
	"testing"
	"time"
)

func TestReservationSharedHostCPUDrainCompletes(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	// Shared interactive hosts can retain GPUs in inventory, disabled by profile.
	m.GPU, m.Device, m.Devices = "fake", "0", m3Machine().Devices
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, nil)
	started := submit(t, q, cpuSpec(1, 1))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != started.ID {
		t.Fatal("missing draining work", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	bound := submit(t, q, cpuSpec(1, 1))
	r, err := q.CreateReservation(reservationRequest("shared-check", "host="+hostID(&m), bound.ID))
	if err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, r.ID, "draining")
	clock.Advance(time.Second)
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, r.ID, "ready")
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != bound.ID {
		t.Fatal("CPU validation never admitted", next)
	}
	if err := q.Started(result(next, false)); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, r.ID, "in_use")
}

func TestReservationNeverReadyExpiryYieldsAdmission(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
		t.Fatal(err)
	}
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	r, err := q.CreateReservation(reservationRequest("never-ready", "host="+hostID(&m), bound.ID))
	if err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, r.ID, "draining")
	clock.Advance(2 * time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	expired := reservationState(t, q, r.ID, "expired")
	if expired.ReadyAt != nil || expired.ExpiredAt == nil || !expired.ExpiredAt.Equal(clock.Now()) {
		t.Fatal("missing expiry evidence", expired)
	}
	if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	other := submit(t, q, exclusionSpec(m.ID, "0"))
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != other.ID {
		t.Fatal("expired reservation still blocks others", next)
	}
	if got := job(t, q, bound.ID); got.State != "queued" || got.Attempt != 0 {
		t.Fatal("expired binding ran or was cancelled", got)
	}
	replay, err := q.CreateReservation(reservationRequest("never-ready", "host="+hostID(&m), bound.ID))
	if err != nil || replay.ExpiredAt == nil {
		t.Fatal("replay resurrected admission", replay, err)
	}
}
