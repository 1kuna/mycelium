package myce

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMissingCheckpointFloorHoldsAndReleasesAssignment(t *testing.T) {
	q, _ := fixture(t)
	w := recoveryWorker(t)
	w.control = &processControl{}
	w.Machine.HostID = "resume-host"
	w.Machine.Devices = inventoryReviewDevices()[:1]
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return w.Machine.Devices, nil }
	w.checkpointPrepare = prepareCheckpoint
	w.Client = Client{URL: "http://queue.invalid", Token: "test", HTTP: &http.Client{
		Transport: handlerTestTransport((API{Queue: q, Token: "test"}).Handler()),
	}}
	first := submit(t, q, spec("experiment"))
	old := check(t, q, machine("first", "fake"), nil).Job
	if old == nil || old.ID != first.ID {
		t.Fatal("missing original assignment")
	}
	writeSave(t, attemptCheckpoint(*old), "7")
	if err := q.Started(result(old, false)); err != nil {
		t.Fatal(err)
	}
	if err := q.Leave(old.Machine, false); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, old)
	writeSave(t, attemptCheckpoint(*old), "6")
	resumed := check(t, q, w.Machine, nil).Job
	if resumed == nil || resumed.CheckpointTarget == nil || resumed.CheckpointTarget.Generation != 7 {
		t.Fatal("retry did not receive accepted generation 7", resumed)
	}
	writeSave(t, attemptCheckpoint(*resumed), "6")

	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	store, err := newAttemptStore(root, *resumed)
	if err != nil {
		t.Fatal(err)
	}
	actor := &attemptSupervisor{store: store, job: *resumed, worker: w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	go actor.run(ctx)
	defer func() { cancel(); <-actor.done }()
	await(t, func() bool {
		_, diagnostic, finished, err := actor.snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if diagnostic.CheckpointUnavailable {
			t.Fatal("missing generation marked the healthy checkpoint root unavailable")
		}
		return finished
	})

	var held Job
	if err := w.Client.Call(t.Context(), "GET", "/api/jobs/"+first.ID, nil, &held); err != nil {
		t.Fatal(err)
	}
	if held.State != "paused" || held.Hold == nil || held.Hold.Kind != "checkpoint_floor_missing" ||
		!strings.Contains(held.Hold.Detail, "generation 6 is below accepted generation 7") ||
		!strings.Contains(held.Hold.Next, "myce resume "+first.ID) || !strings.Contains(held.Hold.Next, "new submission key") {
		t.Fatalf("missing checkpoint status: state=%s hold=%+v", held.State, held.Hold)
	}
	if held.Attempt != resumed.Attempt || held.ExecutionRelease != "confirmed" || held.Losses != resumed.Losses ||
		held.InfrastructureRetries != resumed.InfrastructureRetries || held.Starts != resumed.Starts-1 ||
		held.CheckpointTarget == nil || held.CheckpointTarget.Generation != 7 {
		t.Fatal("hold did not preserve the accepted floor and release the exact attempt", held.State, held.ExecutionRelease, held.CheckpointTarget)
	}
	if got, err := checkpointProgress(attemptCheckpoint(*resumed)); err != nil || got != 6 {
		t.Fatalf("hold lost the prepared checkpoint: generation=%d, err=%v", got, err)
	}
	if err := q.Complete(result(resumed, true)); !errors.Is(err, ErrStale) {
		t.Fatal("held attempt still accepted completion", err)
	}
	if offer := check(t, q, w.Machine, nil).Job; offer != nil {
		t.Fatal("held job churned through another assignment", offer.ID)
	}
	other := submit(t, q, spec("experiment"))
	unrelated := check(t, q, w.Machine, nil).Job
	if unrelated == nil || unrelated.ID != other.ID {
		t.Fatal("missing checkpoint kept other checkpoint work from the worker")
	}
	finish(t, q, unrelated, true)

	writeSave(t, attemptCheckpoint(*old), "7")
	if _, err := q.ResumeJob(first.ID, held.PauseRevision); err != nil {
		t.Fatal(err)
	}
	retry := check(t, q, w.Machine, nil).Job
	if retry == nil || retry.ID != first.ID || retry.Attempt <= resumed.Attempt {
		t.Fatal("restored checkpoint did not get a new attempt", retry)
	}
	dir, err := prepareCheckpoint(*retry)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := checkpointProgress(dir); err != nil || got != 7 {
		t.Fatalf("restored checkpoint generation=%d, err=%v", got, err)
	}
}

func TestStalePreparedCheckpointUsesPriorGenerationAtFloor(t *testing.T) {
	j := Job{Attempt: 3, Checkpoint: filepath.Join(t.TempDir(), "job"), CheckpointTarget: &CheckpointOrigin{Generation: 7}}
	writeSave(t, attemptCheckpoint(j), "6")
	writeSave(t, filepath.Join(j.Checkpoint, "attempt-1"), "7")
	dir, err := prepareCheckpoint(j)
	if err != nil || dir != attemptCheckpoint(j) {
		t.Fatalf("failed to refresh stale prepared copy: dir=%q err=%v", dir, err)
	}
	if got, err := checkpointProgress(dir); err != nil || got != 7 {
		t.Fatalf("prepared checkpoint generation=%d, err=%v", got, err)
	}
	retired, err := filepath.Glob(filepath.Join(j.Checkpoint, ".myce-stale-*.tmp", "checkpoint"))
	if err != nil || len(retired) != 1 {
		t.Fatalf("stale copy custody: found %d directories, err=%v", len(retired), err)
	}
	if got, err := checkpointProgress(retired[0]); err != nil || got != 6 {
		t.Fatalf("retired checkpoint generation=%d, err=%v", got, err)
	}
}

func TestLateReturnBelowAcceptedCheckpointFloor(t *testing.T) {
	for _, replacementLost := range []bool{false, true} {
		t.Run("replacementLost="+stringBool(replacementLost), func(t *testing.T) {
			q, clock := fixture(t)
			submit(t, q, spec("experiment"))
			oldWorker := machine("first", "fake")
			old := check(t, q, oldWorker, nil).Job
			writeSave(t, attemptCheckpoint(*old), "6")
			clock.Advance(LossAfter)
			check(t, q, observer(), nil)
			newWorker := machine("replacement", "fake")
			replacement := check(t, q, newWorker, nil).Job
			if replacement == nil || replacement.Attempt <= old.Attempt {
				t.Fatal("missing replacement assignment")
			}
			writeSave(t, attemptCheckpoint(*replacement), "7")
			_, err := q.CheckinKeyed(Checkin{Machine: newWorker, AttemptProtocol: keyedAttemptProtocol,
				Attempts: []AttemptReport{{Execution: Execution{ID: replacement.ID, Attempt: replacement.Attempt, Progress: 7}, Session: replacement.Session, Started: true}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(attemptCheckpoint(*replacement)); err != nil {
				t.Fatal(err)
			}
			wantState := "running"
			if replacementLost {
				advanceReportingPeer(t, q, clock, observer(), LossAfter)
				wantState = "queued"
			}
			before := job(t, q, old.ID)
			if before.State != wantState || before.CheckpointTarget == nil || before.CheckpointTarget.Generation != 7 {
				t.Fatal("replacement did not establish a floor", before.State, before.CheckpointTarget)
			}
			reply, err := q.CheckinKeyed(Checkin{Machine: oldWorker, AttemptProtocol: keyedAttemptProtocol,
				Attempts: []AttemptReport{{Execution: Execution{ID: old.ID, Attempt: old.Attempt, Progress: 6}, Session: old.Session, Started: true}},
			})
			if err != nil {
				t.Fatal(err)
			}
			err = q.Complete(result(old, true))
			after := job(t, q, old.ID)
			if len(reply.Attempts) != 1 || !reply.Attempts[0].Kill || !errors.Is(err, ErrStale) ||
				after.Attempt != before.Attempt || after.State != before.State || *after.CheckpointTarget != *before.CheckpointTarget {
				t.Fatalf("below-floor return: directives=%+v completion=%v state=%s attempt=%d origin=%+v",
					reply.Attempts, err, after.State, after.Attempt, after.CheckpointTarget)
			}
		})
	}
}
