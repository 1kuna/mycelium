package myce

import (
	"context"
	"testing"
	"time"
)

func TestReservationColdCheckAfterCancelledMemoryRelease(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, false)
	m.HostAvailableMemoryGB = 21.3
	m.CapabilityProfile.SystemMemoryBytes = 31 << 30
	machineCheck(t, q, m)
	oldSpec := s
	oldSpec.ExtensionCheck = nil
	oldSpec.Workload = &Workload{InputShape: "small-v1", Units: 10}
	oldSpec.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	oldSpec.Needs.Memory = "auto"
	oldSpec.Needs.MemoryGB, oldSpec.Needs.HostMemoryGB = 0, 0
	oldSpec.SizeHint = SizeHint{MemoryGB: 30, HostMemoryGB: 17}
	old := submit(t, q, oldSpec)
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.ID != old.ID {
		t.Fatal("missing initial memory holder")
	}
	if err := q.Started(result(running, false)); err != nil {
		t.Fatal(err)
	}
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	bound := submit(t, q, s)
	if _, err := q.CreateReservation(reservationRequest("cold-check", "host="+m.HostID, bound.ID)); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(running.ID); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, "cold-check", "draining")
	if len(q.s.HostReleases) == 0 {
		t.Fatal("cancellation failed to retain exact release custody")
	}
	clock.Advance(time.Second)
	if next := assignedJob(machineCheck(t, q, m)); next != nil {
		t.Fatal("omitted cancelled attempt granted capacity before exact release", next.ID)
	}
	r := result(running, false)
	r.ReleaseOnly, r.Interrupted = true, true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, "cold-check", "ready")
	if len(q.s.HostReleases) != 0 {
		t.Fatal("confirmed cancellation retained a charge")
	}
	memory := q.s.Machines[m.ID]
	if memory.HostMemoryInUseGB != 0 || memory.hostReservationDebtGB != 0 || memory.FreeHostMemoryGB != m.HostAvailableMemoryGB {
		t.Fatalf("drained ledger: used=%g debt=%g free=%g", memory.HostMemoryInUseGB, memory.hostReservationDebtGB, memory.FreeHostMemoryGB)
	}
	probe := assignedJob(machineCheck(t, q, m))
	if probe == nil || probe.ID != bound.ID {
		t.Fatal("ready reservation did not admit cold check", job(t, q, bound.ID).Reason)
	}
	w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return m.HostAvailableMemoryGB, nil }}
	if err := localAttemptAdmission(w, nil, *probe, nil); err != nil {
		t.Fatal("cold check failed strict worker admission", err)
	}
}
