package myce

import (
	"context"
	"errors"
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
		contact := q.contacts[worker.ID]
		return contact != nil && len(contact.pending) > 0
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

func TestCheckinAbandonedContactExpires(t *testing.T) {
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
	q.beginCheckin(worker.ID, worker.Session) // Abandoned: never calls settle.
	c.Advance(29 * time.Second)
	check(t, q, peer, nil)
	requireState(t, q, j.ID, "running")
	c.Advance(time.Second) // Exactly arrival + 30 s: no more pending credit.
	check(t, q, peer, nil)
	requireState(t, q, j.ID, "queued")
	if !q.s.Machines[worker.ID].Gone || job(t, q, j.ID).Losses != 1 {
		t.Fatal("abandoned check-in hid a dead worker after 30 s")
	}
}

// #343: a client deadline after 10 s behind the queue mutex must not remove
// arrival protection and retroactively turn queue time into worker silence.
func TestCheckinCancelledWhileWaitingRetainsBoundedContact(t *testing.T) {
	q, c := fixture(t)
	worker, peer := machine("worker", "fake"), machine("peer", "fake")
	s := spec("production")
	s.AllowedMachines = []string{worker.ID}
	j := submit(t, q, s)
	assigned := check(t, q, worker, nil).Job
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	last := q.s.Machines[worker.ID].LastSeen
	c.Advance(4 * time.Second)
	check(t, q, peer, nil)
	arrived := c.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := q.CheckinContext(ctx, Checkin{Machine: worker})
		done <- err
	}()
	await(t, func() bool {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		return len(q.contacts[worker.ID].pending) == 1
	})
	c.Advance(12 * time.Second)
	cancel()
	q.maintain()
	gone, state := q.s.Machines[worker.ID].Gone, q.s.Jobs[j.ID].State
	q.mu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled check-in: %v", err)
	}
	if gone || state != "running" {
		t.Fatalf("cancelled queue wait became worker silence: gone=%v state=%s", gone, state)
	}
	if !q.s.Machines[worker.ID].LastSeen.Equal(last) {
		t.Fatal("cancelled check-in refreshed LastSeen")
	}
	// Even after the cancelled handler has returned, keep the original window
	// for its retry to arrive. Never grant a fresh 30 s from cancellation.
	c.Advance(arrived.Add(pendingCheckinContactLimit - time.Nanosecond).Sub(c.Now()))
	check(t, q, peer, nil)
	requireState(t, q, j.ID, "running")
	c.Advance(time.Nanosecond)
	check(t, q, peer, nil)
	requireState(t, q, j.ID, "queued")
	if !q.s.Machines[worker.ID].Gone || job(t, q, j.ID).Losses != 1 {
		t.Fatal("cancelled arrival hid a dead worker beyond its original 30 s bound")
	}
}

func TestCheckinRejectedReportDoesNotRefreshContact(t *testing.T) {
	q, c := fixture(t)
	worker := machine("worker", "fake")
	check(t, q, worker, nil)
	last := q.s.Machines[worker.ID].LastSeen
	c.Advance(time.Second)
	stale := q.beginCheckin(worker.ID, "stale-session")
	if got := q.lastContact(q.s.Machines[worker.ID], c.Now()); !got.Equal(last) {
		t.Fatal("stale session received pending contact")
	}
	stale(false)
	worker.GPU = "invalid"
	if _, err := q.Checkin(Checkin{Machine: worker}); err == nil {
		t.Fatal("invalid GPU report accepted")
	}
	if !q.s.Machines[worker.ID].LastSeen.Equal(last) || !q.lastContact(q.s.Machines[worker.ID], c.Now()).Equal(last) {
		t.Fatal("rejected check-in refreshed contact")
	}
}
