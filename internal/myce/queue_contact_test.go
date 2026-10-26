package myce

import (
	"testing"
	"time"
)

// #223: a check-in waiting behind a slow queue update is queue time, not worker
// silence; the worker is still lost once it is genuinely quiet afterwards.
func TestCheckinWaitingOnQueueLockIsNotWorkerSilence(t *testing.T) {
	q, c := fixture(t)
	worker, peer := machine("worker", "fake"), machine("peer", "fake")
	s := spec("production")
	s.AllowedMachines = []string{worker.ID}
	j := submit(t, q, s)
	assigned := check(t, q, worker, nil).Job
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	c.Advance(4 * time.Second)
	check(t, q, peer, nil)

	q.mu.Lock() // A slow update holds the queue lock for 6 s.
	done := make(chan error, 1)
	go func() {
		_, err := q.Checkin(Checkin{Machine: worker, Running: &Execution{ID: assigned.ID, Attempt: assigned.Attempt}})
		done <- err
	}()
	await(t, func() bool {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		contact := q.contacts[worker.ID][worker.Session]
		return contact != nil && contact.pending > 0
	})
	c.Advance(6 * time.Second)
	q.maintain() // Another update's maintenance runs while the check-in waits.
	gone, state := q.s.Machines[worker.ID].Gone, q.s.Jobs[j.ID].State
	q.mu.Unlock()
	if gone || state != "running" {
		t.Fatalf("queue lock wait judged a reporting worker quiet: gone=%v state=%s", gone, state)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, "running")

	c.Advance(ProductionLossAfter)
	check(t, q, peer, nil)
	requireState(t, q, j.ID, "queued")
	if !q.s.Machines[worker.ID].Gone {
		t.Fatal("quiet worker stayed joined after its last processed check-in")
	}
}
