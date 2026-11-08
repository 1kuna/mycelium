package myce

import (
	"strconv"
	"testing"
	"time"
)

// #946: acquiring a reservation between native custody and Started must not
// withdraw its own assignment while another Started command keeps it draining.
func TestReservationAcquisitionPreservesOwnPendingStart(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	submit(t, q, cpuSpec(1, 1))
	neighbor := assignedJob(machineCheck(t, q, m))
	if neighbor == nil {
		t.Fatal("missing neighboring CPU command")
	}
	if err := q.Started(result(neighbor, false)); err != nil {
		t.Fatal(err)
	}
	bound := submit(t, q, exclusionSpec(m.ID, "1"))
	run := assignedJob(machineCheck(t, q, m, neighbor))
	if run == nil || run.ID != bound.ID {
		t.Fatal("missing bound assignment", run)
	}
	clock.Advance(2 * time.Second)
	ledgerCustody(t, q, run, "owned-custody", "reserved")
	ledgerCustody(t, q, run, "owned-custody", "started")
	if got := job(t, q, bound.ID); got.StartedAt != nil {
		t.Fatal("custody was mistaken for Started")
	}
	clock.Advance(time.Second)
	r, err := q.CreateReservation(reservationRequest("own-pending-start", "host="+m.HostID, bound.ID))
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "draining" || !r.CreatedAt.Equal(clock.Now()) {
		t.Fatal("neighbor should keep the reservation draining", r)
	}
	got := job(t, q, bound.ID)
	if got.State != "running" || got.Attempt != run.Attempt || got.StopAt != nil || got.StartedAt != nil || got.Copies[0].StopCause != "" || got.Losses != 0 {
		t.Fatalf("reservation stopped its own attempt: state=%s attempt=%d stop=%v started=%v cause=%q losses=%d", got.State, got.Attempt, got.StopAt, got.StartedAt, got.Copies[0].StopCause, got.Losses)
	}
	launch := HostLaunch{Machine: run.Machine, Session: run.Session, Kind: "batch", Key: run.ID, Attempt: strconv.Itoa(run.Attempt)}
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal("reservation revoked its own launch", err)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal("reservation revoked its own Started handoff", err)
	}
	clock.Advance(time.Second)
	for _, directive := range machineCheck(t, q, m, neighbor, run).Attempts {
		if (directive.ID == run.ID || directive.ID == neighbor.ID) && (directive.Stop || directive.Kill) {
			t.Fatal("reservation stopped existing work", directive)
		}
	}
	if got := job(t, q, bound.ID); got.State != "running" || got.Attempt != 1 || got.StartedAt == nil || got.Losses != 0 {
		t.Fatal("bound attempt churned instead of starting", got)
	}
}
