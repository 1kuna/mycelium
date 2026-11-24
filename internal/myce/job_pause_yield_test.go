package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func yieldRequest(j *Job) PauseJobRequest {
	attempt := j.Attempt
	return PauseJobRequest{Reason: "free capacity", Project: j.Spec.Project, Yield: true, IfAttempt: &attempt}
}

func TestJobPauseYieldReleaseAndResume(t *testing.T) {
	for _, mode := range []string{"complete", "pending", "restart", "lost"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("yield-worker", "fake")
			original := submit(t, q, spec("experiment"))
			run := check(t, q, m, nil).Job
			if run == nil {
				t.Fatal("missing assignment")
			}
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal(err)
			}
			writeSave(t, attemptCheckpoint(*run), "7")
			stopping, err := q.PauseJob(run.ID, yieldRequest(run))
			if err != nil {
				t.Fatal(err)
			}
			if stopping.State != "stopping" || stopping.Pause != nil || stopping.YieldPause == nil || stopping.Losses != 0 || !plannedMaintenanceStop(stopping) {
				t.Fatalf("premature pause or wrong stop: %+v", stopping)
			}
			if _, err := q.ResumeJob(run.ID, stopping.PauseRevision); err == nil {
				t.Fatal("resume released a still-running attempt")
			}
			directive := check(t, q, m, run)
			if !directive.Stop || directive.Kill || directive.GraceSeconds <= 0 || stopping.StopAt.Sub(clock.Now()) != q.gpuStopGrace() {
				t.Fatalf("missing planned stop/grace: %+v", directive)
			}
			if mode == "restart" {
				q = reopenCancellationQueue(t, q, clock)
			}
			if mode == "lost" {
				other := machine("observer-worker", "fake")
				check(t, q, other, nil)
				clock.Advance(LossAfter + StopGrace)
				check(t, q, other, nil)
				if got := job(t, q, run.ID); got.State != "stopping" || got.ReleaseConfirmed || got.Pause != nil || got.Machine != run.Machine {
					t.Fatal("silence discharged custody", got)
				}
			}
			r := result(run, false)
			r.Interrupted = true
			if mode == "pending" {
				r.ReleasePending = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				clock.Advance(StopGrace + time.Second)
				check(t, q, m, run)
				if got := job(t, q, run.ID); got.State != "stopping" || got.Pause != nil || got.ReleaseConfirmed || got.Machine != run.Machine {
					t.Fatal("pending cleanup surrendered custody", got)
				}
				r.ReleasePending, r.ReleaseOnly = false, true
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			paused := job(t, q, run.ID)
			if paused.ID != original.ID || paused.State != "paused" || paused.Pause == nil || paused.YieldPause != nil || !paused.ReleaseConfirmed || paused.Progress != 7 || paused.Losses != 0 || paused.InfrastructureRetries != 0 {
				t.Fatalf("release lost pause/progress: %+v", paused)
			}
			if data, err := os.ReadFile(filepath.Join(attemptCheckpoint(*run), "state.json")); err != nil || len(data) == 0 {
				t.Fatal("save removed", err)
			}
			if _, err := q.PauseJob(run.ID, yieldRequest(run)); err == nil {
				t.Fatal("late yield accepted")
			}
			if err := q.Complete(result(run, true)); !errors.Is(err, ErrStale) {
				t.Fatal("late result accepted", err)
			}
			q = reopenCancellationQueue(t, q, clock)
			if got := job(t, q, run.ID); got.State != "paused" || got.PauseRevision != paused.PauseRevision {
				t.Fatal("pause not durable", got)
			}
			if _, err := q.ResumeJob(run.ID, paused.PauseRevision); err != nil {
				t.Fatal(err)
			}
			next := check(t, q, m, nil).Job
			if next == nil || next.ID != original.ID || next.Attempt <= run.Attempt {
				t.Fatal("resume did not restore placement", next)
			}
			// A new attempt resets live progress until the worker reports it.
			// Prove the worker's actual restore path still receives generation 7.
			restored, err := prepareCheckpoint(*next)
			if err != nil {
				t.Fatal("restore saved checkpoint", err)
			}
			if generation, err := checkpointProgress(restored); err != nil || generation != 7 {
				t.Fatal("resume lost saved generation", generation, err)
			}
			if _, err := q.PauseJob(run.ID, yieldRequest(run)); !errors.Is(err, ErrStale) {
				t.Fatal("stale yield stopped replacement", err)
			}
			if got := job(t, q, run.ID); got.State != "running" {
				t.Fatal("stale request changed attempt", got)
			}
		})
	}
}

func TestJobPauseYieldHTTPRefusals(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Resumable = false
	submit(t, q, s)
	m := machine("yield-worker", "fake")
	run := check(t, q, m, nil).Job
	if run == nil {
		t.Fatal("missing assignment")
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "fixture"}
	request := yieldRequest(run)
	err := client.Call(context.Background(), "POST", "/api/jobs/"+run.ID+"/pause", request, nil)
	pauseError(t, err, "job_yield_confirmation_required")
	request.ConfirmRestart, request.Project = true, "different-project"
	err = client.Call(context.Background(), "POST", "/api/jobs/"+run.ID+"/pause", request, nil)
	var api *HTTPError
	if !errors.As(err, &api) || api.Status != 403 || api.Code != "job_project_mismatch" {
		t.Fatal("foreign project accepted", err)
	}
	if got := job(t, q, run.ID); got.State != "running" || got.YieldPause != nil {
		t.Fatal("refusal changed job", got)
	}
	request.Project = run.Spec.Project
	var stopped Job
	if err := client.Call(context.Background(), "POST", "/api/jobs/"+run.ID+"/pause", request, &stopped); err != nil || stopped.State != "stopping" {
		t.Fatal("explicit restart refused", err)
	}
	r := result(run, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, run.ID); got.State != "paused" || got.Losses != 0 {
		t.Fatal("confirmed restart did not pause", got)
	}
}
