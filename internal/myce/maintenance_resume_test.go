package myce

import (
	"slices"
	"testing"
	"time"
)

// A long run that Mycelium stopped for planned maintenance resumes ahead of
// same-tier work submitted later, even when the later work's project has the
// better fair turn. Its host drains toward it: later work may not take the
// room it needs unless it is expected to finish before the resume could start
// anyway. Higher tiers still go first, and production still preempts it.
func TestMaintenanceResumeKeepsItsPlace(t *testing.T) {
	q, clock := fixture(t)
	m := backfillMachine("node", 64)
	machineCheck(t, q, m)

	long := spec("experiment")
	long.Project, long.Name = "long-project", "long-run"
	long.Needs.Platform = Platform{Required: []string{"cuda"}}
	long.Needs.MemoryGB, long.Needs.HostMemoryGB = 1, 40
	run := submit(t, q, long)
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != run.ID {
		t.Fatal("long run not placed", first)
	}
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	submitted := job(t, q, run.ID).Queued
	clock.Advance(time.Hour) // Its project now has the worse fair turn.
	machineCheck(t, q, m, first)

	// Planned maintenance stops it at a checkpoint; it is requeued.
	if _, err := q.ChangeHostMaintenance(hostID(q.s.Machines[m.ID]), HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, first)
	r := result(first, true)
	r.ReleaseOnly = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, run.ID); got.State != "queued" || got.MaintenanceResume == nil || !got.Queued.Equal(submitted) {
		t.Fatal("maintenance stop did not requeue it in its original place", got.State, got.MaintenanceResume, got.Queued, submitted)
	}

	// A higher tier still goes first. Its accelerator work holds part of the
	// host's RAM, which only the drain projects as released.
	clock.Advance(time.Minute)
	holder := spec("deadline")
	holder.Name, holder.Project, holder.WallTime = "holder", "deadline-project", "1h"
	holder.Needs.Platform = Platform{Required: []string{"cuda"}}
	holder.Needs.MemoryGB, holder.Needs.HostMemoryGB = 1, 32
	submit(t, q, holder)
	if _, err := q.ChangeHostMaintenance(hostID(q.s.Machines[m.ID]), HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	held := assignedJob(machineCheck(t, q, m))
	if held == nil || held.Spec.Name != "holder" {
		t.Fatal("higher tier did not go first", held)
	}
	if err := q.Started(result(held, false)); err != nil {
		t.Fatal(err)
	}

	// Smaller, later work from a fresh project would fit beside the holder.
	// The project's accelerator job shares the resume's fair-turn family, where
	// the fresh project has the better turn.
	clock.Advance(time.Minute)
	small := cpuSpec(1, 16)
	small.Name, small.Project = "small", "small-project"
	smallGPU := spec("experiment")
	smallGPU.Name, smallGPU.Project = "small-gpu", "small-project"
	smallGPU.Needs.Platform = Platform{Required: []string{"cuda"}}
	smallGPU.Needs.MemoryGB, smallGPU.Needs.HostMemoryGB = 1, 16
	smalls := []*Job{submit(t, q, small), submit(t, q, small), submit(t, q, smallGPU)}
	if got := assignedJob(machineCheck(t, q, m, held)); got != nil {
		t.Fatal("later small work took the room the maintenance resume needs", got.Spec.Name)
	}

	// Work bounded to finish before the holder's wall time ends backfills.
	short := small
	short.Name, short.WallTime = "short", "10m"
	submit(t, q, short)
	backfill := assignedJob(machineCheck(t, q, m, held))
	if backfill == nil || backfill.Spec.Name != "short" {
		t.Fatal("bounded short work did not backfill ahead of the drain", backfill)
	}
	if err := q.Started(result(backfill, false)); err != nil {
		t.Fatal(err)
	}

	// Room frees: the resume starts first, and later work gets only the excess.
	clock.Advance(5 * time.Minute)
	finish(t, q, held, true)
	resumed := assignedJob(machineCheck(t, q, m, backfill))
	if resumed == nil || resumed.ID != run.ID {
		t.Fatal("maintenance resume did not start first when room freed", resumed)
	}
	if err := q.Started(result(resumed, false)); err != nil {
		t.Fatal(err)
	}
	if job(t, q, run.ID).MaintenanceResume != nil {
		t.Fatal("started resume kept its maintenance place")
	}
	if got := assignedJob(machineCheck(t, q, m, backfill, resumed)); got != nil {
		t.Fatal("small work overcommitted the host after the resume started", got.Spec.Name)
	}
	for _, s := range smalls {
		if job(t, q, s.ID).State != "queued" {
			t.Fatal("small work left the queue", s.ID)
		}
	}

	// Production still preempts the resumed run.
	prod := spec("production")
	prod.Name, prod.Project = "prod", "prod-project"
	prod.Needs.Platform = Platform{Required: []string{"cuda"}}
	prod.Needs.MemoryGB, prod.Needs.HostMemoryGB = 1, 40
	submit(t, q, prod)
	reply := machineCheck(t, q, m, backfill, resumed)
	if !slices.ContainsFunc(reply.Attempts, func(a AttemptDirective) bool { return a.ID == run.ID && a.Stop && a.PriorityStop != nil }) {
		t.Fatal("production did not preempt the resumed run", reply.Attempts)
	}
}
