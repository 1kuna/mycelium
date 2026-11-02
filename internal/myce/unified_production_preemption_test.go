package myce

import (
	"testing"
	"time"
)

// Production on a unified-memory CUDA host waited behind a running,
// non-resumable deadline command that held most of the host RAM allowance.
func TestUnifiedProductionPreemptsDeadlineHostMemory(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.MemoryObservation.UsedGB = 6
	check(t, q, m, nil)
	deadline := spec("deadline")
	deadline.Project, deadline.Resumable = "editor", false
	deadline.Needs.MemoryGB, deadline.Needs.HostMemoryGB, deadline.Needs.CPUCores = 64, 8, 2
	deadline.AllowedMachines = []string{m.ID}
	victim := submit(t, q, deadline)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != victim.ID {
		t.Fatal("missing deadline holder", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	// The holder's live use (~41 GiB) already left the measured headroom while
	// its unknown unified-device growth still reserves the full envelope.
	m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 49.3, 46.6
	check(t, q, m, run)
	prod := spec("production")
	prod.Project, prod.Resumable = "naming", false
	prod.Needs.MemoryGB, prod.Needs.HostMemoryGB, prod.Needs.CPUCores = 27.9, 25.5, 4
	prod.AllowedMachines = []string{m.ID}
	demand := submit(t, q, prod)
	clock.Advance(time.Second)
	reply := check(t, q, m, run)
	if !reply.Stop {
		t.Fatalf("production did not preempt the deadline holder: reason=%q", job(t, q, demand.ID).Reason)
	}
	if reply.Job != nil {
		t.Fatal("production admitted before the holder released", reply.Job)
	}
	// The holder exits; its unified allocation returns to live headroom.
	if err := q.Complete(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 90, 6
	if got := check(t, q, m, nil).Job; got == nil || got.ID != demand.ID {
		t.Fatalf("production not admitted after release: %v reason=%q", got, job(t, q, demand.ID).Reason)
	}
}
