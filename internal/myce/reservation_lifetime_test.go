package myce

import (
	"testing"
	"time"
)

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
