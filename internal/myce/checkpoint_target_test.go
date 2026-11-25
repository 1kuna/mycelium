package myce

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #1437: a planned stop of checkpointed multi-target work resumes on the target
// that wrote the checkpoint, waiting while that host is busy. Declared tested
// resume_compatibility, or that target leaving the fleet, permits another.
func TestPlannedStopResumesOnCheckpointTarget(t *testing.T) {
	for _, mode := range []string{"bound", "portable", "left", "unreported", "unknown-loss"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			a, b := machine("a", "fake", "host-a"), machine("b", "fake", "host-b")
			a.CapabilityProfile.Arch = "arm64"
			s := spec("experiment")
			s.Targets = []ExecutionTarget{{OS: "linux", Arch: "arm64"}, {OS: "linux", Arch: "amd64"}}
			if mode == "portable" {
				s.ResumeCompatibility = &ResumeCompatibility{CheckpointFormat: "counter-v1", ResultContract: "counter-result-v1", Targets: []ResumeTarget{
					{Target: ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "fake"}, Environment: "none", ResultCheck: "counter-resume-test"},
					{Target: ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "fake"}, Environment: "none", ResultCheck: "counter-resume-test"},
				}}
			}
			submit(t, q, s)
			j := check(t, q, a, nil).Job
			if j == nil || j.Machine != a.ID {
				t.Fatal("not placed on A", j)
			}
			if err := q.Started(result(j, false)); err != nil {
				t.Fatal(err)
			}
			progress := int64(3)
			if mode == "unreported" || mode == "unknown-loss" {
				progress = -1
			}
			if _, err := q.Checkin(Checkin{Machine: a, Running: &Execution{ID: j.ID, Attempt: j.Attempt, Progress: progress}, Started: true}); err != nil {
				t.Fatal(err)
			}
			if mode == "unknown-loss" {
				if err := q.update(func() error { q.retry(q.s.Jobs[j.ID], "machine quiet for loss timeout"); return nil }); err != nil {
					t.Fatal(err)
				}
				if got := job(t, q, j.ID).CheckpointTarget; got == nil || got.Generation != 0 || got.Target != (ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "fake"}) {
					t.Fatal("unknown writer target discarded on loss", got)
				}
				if got := check(t, q, b, nil).Job; got != nil {
					t.Fatal("unknown checkpoint moved across targets", got)
				}
				return
			}

			if mode == "unreported" {
				if got := job(t, q, j.ID); got.Progress != -1 || got.CheckpointTarget != nil {
					t.Fatal("unknown progress not preserved", got)
				}
				saveYieldGeneration(t, job(t, q, j.ID), 3)
			}
			if got := job(t, q, j.ID).CheckpointTarget; mode != "unreported" && (got == nil || got.Target != (ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "fake"}) || got.Generation != 3) {
				t.Fatal("checkpoint target not recorded", got)
			}

			// Planned maintenance stop, acknowledged and released.
			if _, err := q.ChangeHostMaintenance("host-a", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
				t.Fatal(err)
			}
			finishInterrupted(t, q, j)
			r := result(j, true)
			r.ReleaseOnly = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); got.State != "queued" || got.InfrastructureRetries != 0 {
				t.Fatal("planned stop did not requeue", got.State, got.InfrastructureRetries)
			}

			if got := job(t, q, j.ID).CheckpointTarget; got == nil || got.Generation != 3 {
				t.Fatal("finalization missed checkpoint target", got)
			}
			switch mode {
			case "portable":
				if got := check(t, q, b, nil).Job; got == nil || got.ID != j.ID {
					t.Fatal("declared portable checkpoint did not resume on B", got)
				}
				return
			case "left":
				if err := q.Leave(a.ID, false); err != nil {
					t.Fatal(err)
				}
				if got := check(t, q, b, nil).Job; got == nil || got.ID != j.ID {
					t.Fatal("checkpoint target left the fleet but B did not resume it", got)
				}
				return
			}

			// A host of the checkpoint's target exists: B never takes it, during
			// maintenance or while A is busy with other work.
			if got := check(t, q, b, nil).Job; got != nil {
				t.Fatal("resumed across targets during maintenance", got.ID)
			}
			if h := waitFor(t, q, j.ID); h == nil || h.Kind != checkpointTargetHold {
				t.Fatal("status does not explain the checkpoint-target wait", h)
			}
			filler := spec("deadline")
			filler.Name, filler.AllowedMachines = "filler", []string{a.ID}
			submit(t, q, filler)
			if _, err := q.ChangeHostMaintenance("host-a", HostMaintenanceRequest{Action: "resume"}); err != nil {
				t.Fatal(err)
			}
			busy := check(t, q, a, nil).Job
			if busy == nil || busy.Spec.Name != "filler" {
				t.Fatal("A did not take the filler", busy)
			}
			if got := check(t, q, b, nil).Job; got != nil {
				t.Fatal("resumed across targets while A was busy", got.ID)
			}
			if h := waitFor(t, q, j.ID); h == nil || h.Kind != checkpointTargetHold {
				t.Fatal("busy wait not explained", h)
			}
			finish(t, q, busy, true)
			got := check(t, q, a, nil).Job
			if got == nil || got.ID != j.ID || got.Machine != a.ID {
				t.Fatal("did not resume on the checkpoint's target", got)
			}
		})
	}
}

func TestInterruptedPortableResumePreservesCheckpointFloor(t *testing.T) {
	q, clock, a, b, first := yieldFixture(t, false)
	// The second worker uses a different, explicitly compatible target.
	b.CapabilityProfile.Arch = "arm64"
	if err := q.update(func() error {
		j := q.touchJob(first.ID)
		j.Spec.ResumeCompatibility.Targets = append(j.Spec.ResumeCompatibility.Targets, ResumeTarget{
			Target: ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "cpu"}, Environment: "none", ResultCheck: "counter-resume-test",
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	saveYieldGeneration(t, job(t, q, first.ID), 7)
	first.Progress = 7
	machineCheck(t, q, a, first)
	origin := job(t, q, first.ID).CheckpointTarget
	if origin == nil || origin.Generation != 7 {
		t.Fatal("first generation not accepted", origin)
	}
	if _, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, first)
	release := result(first, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	second := assignedJob(machineCheck(t, q, b))
	if second == nil || second.ID != first.ID || second.Attempt != first.Attempt+1 {
		t.Fatal("compatible worker did not resume", second)
	}
	if err := q.Started(result(second, false)); err != nil {
		t.Fatal(err)
	}
	second.Progress = -1
	machineCheck(t, q, b, second)
	clock.Advance(time.Second)
	if _, err := q.ChangeHostMaintenance(b.HostID, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	// No second-attempt save is visible to the queue during finalization.
	finishInterrupted(t, q, second)
	release = result(second, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	current := job(t, q, first.ID)
	if current.State != "queued" {
		t.Fatal("interrupted attempt was not requeued", current.State, current.Attempt, current.Reason)
	}
	if got := current.CheckpointTarget; got == nil || *got != *origin {
		t.Fatalf("unreported attempt erased accepted checkpoint: got %+v, want %+v", got, origin)
	}
	if _, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	third := assignedJob(machineCheck(t, q, a))
	if third == nil || third.ID != first.ID || third.Attempt != second.Attempt+1 {
		t.Fatal("next worker did not receive the interrupted job", third)
	}
	third.Checkpoint = filepath.Join(t.TempDir(), "job")
	writeSave(t, filepath.Join(third.Checkpoint, "attempt-1"), "6")
	if _, err := prepareCheckpoint(*third); err == nil || !strings.Contains(err.Error(), "below accepted generation 7") {
		t.Fatal("worker prepared resume below the accepted floor", err)
	}
	writeSave(t, filepath.Join(third.Checkpoint, "attempt-1"), "7")
	if _, err := prepareCheckpoint(*third); err != nil {
		t.Fatal("worker refused to resume the accepted generation", err)
	}
}

func waitFor(t *testing.T, q *Queue, id string) *Hold {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.waitHold(q.s.Jobs[id])
}
