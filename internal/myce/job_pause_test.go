package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func pauseForTest(t *testing.T, q *Queue, id string, until *time.Time) *Job {
	t.Helper()
	j, err := q.PauseJob(id, PauseJobRequest{Reason: "waiting for input review", Until: until})
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func pauseError(t *testing.T, err error, code string) {
	t.Helper()
	var api *HTTPError
	if !errors.As(err, &api) || api.Status != 409 || api.Code != code {
		t.Fatalf("wanted %s conflict, got %v", code, err)
	}
}
func TestJobPauseResumeReopen(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "keep-identity"
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "store"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	client := Client{URL: server.URL, Token: "test"}
	path, ref := policyInput(t, "immutable input")
	if err := client.UploadFile(context.Background(), path, ref); err != nil {
		t.Fatal(err)
	}
	s.Files = FileManifest{"input": ref}
	first := submit(t, q, s)
	clock.Advance(time.Second)
	next := submit(t, q, spec("experiment"))
	until := clock.Now().Add(time.Hour)
	var paused Job
	if err := client.Call(context.Background(), "POST", "/api/jobs/"+first.ID+"/pause", PauseJobRequest{Reason: "review input", Until: &until}, &paused); err != nil {
		t.Fatal(err)
	}
	var observed Job
	if err := client.Call(context.Background(), "GET", "/api/jobs/"+first.ID, nil, &observed); err != nil || observed.State != "paused" || observed.Pause.Reason != "review input" || !observed.Pause.Until.Equal(until) {
		t.Fatalf("GET missing pause: %+v %v", observed.Pause, err)
	}
	server.Close()
	q = reopenCancellationQueue(t, q, clock)
	got := job(t, q, first.ID)
	if got.State != "paused" || got.PauseRevision != paused.PauseRevision || !got.Pause.Until.Equal(until) {
		t.Fatal("reopen lost pause", got)
	}
	replay := submit(t, q, s)
	if replay.ID != first.ID || replay.State != "paused" || !reflect.DeepEqual(replay.Spec, first.Spec) || replay.Queued != first.Queued || replay.Front != first.Front || replay.WaitingSince != first.WaitingSince || replay.Attempt != first.Attempt {
		t.Fatal("pause changed immutable submission/age", replay)
	}
	if got := check(t, q, machine("one", "fake"), nil).Job; got == nil || got.ID != next.ID {
		t.Fatal("paused job remained runnable", got)
	}
	resumed, err := q.ResumeJob(first.ID, paused.PauseRevision)
	if err != nil || resumed.State != "queued" || resumed.Pause != nil || resumed.Queued != first.Queued || resumed.WaitingSince != first.WaitingSince || resumed.Front != first.Front {
		t.Fatal("resume lost position", resumed, err)
	}
	q = reopenCancellationQueue(t, q, clock)
	if got := job(t, q, first.ID); got.State != "queued" || got.PauseRevision <= paused.PauseRevision {
		t.Fatal("reopen lost resume", got)
	}
	if got := check(t, q, machine("two", "fake"), nil).Job; got == nil || got.ID != first.ID {
		t.Fatal("resumed job not runnable", got)
	}
}
func TestJobPauseExpiryStaleResumeAndCancel(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("experiment"))
	until := clock.Now().Add(time.Minute)
	first := pauseForTest(t, q, j.ID, &until)
	clock.Advance(time.Minute - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if job(t, q, j.ID).State != "paused" {
		t.Fatal("expired early")
	}
	q = reopenCancellationQueue(t, q, clock)
	clock.Advance(time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if job(t, q, j.ID).State != "queued" {
		t.Fatal("expiry did not resume")
	}
	second := pauseForTest(t, q, j.ID, nil)
	_, err := q.ResumeJob(j.ID, first.PauseRevision)
	pauseError(t, err, "job_pause_revision_conflict")
	if got := job(t, q, j.ID); got.State != "paused" || got.PauseRevision != second.PauseRevision {
		t.Fatal("stale resume cleared newer pause", got)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, clock)
	if got := job(t, q, j.ID); got.State != "cancelled" || !cancelled(got) {
		t.Fatal("paused cancellation lost", got)
	}
	_, err = q.ResumeJob(j.ID, second.PauseRevision)
	pauseError(t, err, "job_pause_revision_conflict")
}
func TestJobPauseDeadlinesContinue(t *testing.T) {
	for _, kind := range []string{"give-up", "start-by"} {
		t.Run(kind, func(t *testing.T) {
			q, clock := fixture(t)
			s := spec("experiment")
			until := clock.Now().Add(time.Minute)
			if kind == "give-up" {
				s.GiveUpAt = &until
			} else {
				s = spec("production")
				s.StartBy = &until
			}
			j := submit(t, q, s)
			pauseForTest(t, q, j.ID, nil)
			clock.Advance(time.Minute)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); got.State != "failed" {
				t.Fatal("pause froze deadline", got)
			}
		})
	}
}
func TestJobPauseAssignmentRace(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	var reply Reply
	var pauseErr, assignErr error
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; _, pauseErr = q.PauseJob(j.ID, PauseJobRequest{Reason: "review"}) }()
	go func() { defer wg.Done(); <-start; reply, assignErr = q.Checkin(Checkin{Machine: m}) }()
	close(start)
	wg.Wait()
	if assignErr != nil {
		t.Fatal(assignErr)
	}
	if pauseErr == nil {
		if reply.Job != nil || job(t, q, j.ID).State != "paused" {
			t.Fatal("pause and assignment both won")
		}
	} else {
		pauseError(t, pauseErr, "job_pause_conflict")
		if reply.Job == nil || reply.Job.ID != j.ID {
			t.Fatal("assignment did not win", reply.Job)
		}
		if err := q.Started(result(reply.Job, true)); err != nil {
			t.Fatal(err)
		}
		_, err := q.PauseJob(j.ID, PauseJobRequest{Reason: "review"})
		pauseError(t, err, "job_pause_conflict")
	}
}
func TestJobPauseColdPreparation(t *testing.T) {
	e := compositionExtension(t, false)
	q, w := prepareTestWorker(t, e)
	a := submit(t, q, Spec{Name: "demand", Project: "test", Extension: extensionKey(e)})
	offer := check(t, q, w.Machine, nil).EnvironmentPlan
	if len(offer) != 1 {
		t.Fatal("no cold preparation", offer)
	}
	paused := pauseForTest(t, q, a.ID, nil)
	if err := q.AuthorizeLaunch(*offer[0].Launch); !errors.Is(err, ErrStale) {
		t.Fatal("paused cold offer authorized", err)
	}
	if reply := check(t, q, w.Machine, nil); len(reply.EnvironmentPlan) != 0 || len(reply.ExtensionQualificationPlan) != 0 || reply.Job != nil {
		t.Fatal("paused job retained admission", reply)
	}
	// Shared preparation is recomputed, with fresh authority, for the remaining job.
	submit(t, q, Spec{Name: "demand", Project: "test", Extension: extensionKey(e)})
	fresh := check(t, q, w.Machine, nil).EnvironmentPlan
	if len(fresh) != 1 {
		t.Fatal("shared preparation lost")
	}
	if err := q.AuthorizeLaunch(*fresh[0].Launch); err != nil {
		t.Fatal("shared preparation refused", err)
	}
	if _, err := q.ResumeJob(a.ID, paused.PauseRevision); err != nil {
		t.Fatal(err)
	}
}
func TestJobPauseSharedQualification(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "resume"
		if shared {
			name = "shared"
		}
		t.Run(name, func(t *testing.T) {
			q, w, e, a, _, plans := qualificationFixture(t)
			plan := plans[0]
			reconcileQualification(t, w, plan)
			original := qualificationJob(t, q, plan.Report.CheckID)
			if shared {
				submit(t, q, Spec{Name: "demand", Project: "test", Extension: extensionKey(e)})
			}
			paused := pauseForTest(t, q, a.ID, nil)
			w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
			reply := check(t, q, w.Machine, nil)
			if !shared {
				if reply.Job != nil {
					t.Fatal("paused demand launched check", reply.Job)
				}
				q = reopenCancellationQueue(t, q, q.clock)
				if _, err := q.ResumeJob(a.ID, paused.PauseRevision); err != nil {
					t.Fatal(err)
				}
				reply = check(t, q, w.Machine, nil)
			}
			if reply.Job == nil || reply.Job.ID != original.ID {
				t.Fatalf("lost shared/resumed qualification: assignment=%+v check=%+v", reply.Job, qualificationJob(t, q, plan.Report.CheckID))
			}
			if err := q.Started(result(reply.Job, true)); err != nil {
				t.Fatal(err)
			}
			finish(t, q, reply.Job, true)
			accepted := job(t, q, original.ID)
			if accepted.State != "succeeded" || cancelled(accepted) {
				t.Fatal("reversible pause poisoned qualification", accepted)
			}
		})
	}
}
func TestJobPauseFanOut(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	parent := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil {
		t.Fatal("no child")
	}
	paused := pauseForTest(t, q, parent.ID, nil)
	if job(t, q, run.ID).State != "running" {
		t.Fatal("parent pause stopped assigned child")
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	// A genuine interrupted child returns to the parent's non-runnable state.
	r := result(run, false)
	r.Interrupted = true
	r.PoliteStop = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	child := job(t, q, run.ID)
	if child.State != "paused" || child.Pause.Parent != parent.ID {
		t.Fatal("requeued child escaped parent", child)
	}
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatal("paused child accepted superseded result", err)
	}
	if reply := check(t, q, m, run); !reply.Stop || job(t, q, run.ID).State != "paused" {
		t.Fatal("late attempt escaped pause", reply)
	}
	q = reopenCancellationQueue(t, q, clock)
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("paused parent dispatched child", got)
	}
	if _, err := q.ResumeJob(parent.ID, paused.PauseRevision); err != nil {
		t.Fatal(err)
	}
	if got := check(t, q, m, nil).Job; got == nil {
		t.Fatal("resumed parent did not dispatch")
	}
}

func TestJobPauseModelCopyDemand(t *testing.T) {
	q, _, m := syncFixture(t)
	s := spec("experiment")
	s.Needs.Model, s.Environment.Name = "a", "not-ready"
	a, b := submit(t, q, s), submit(t, q, s)
	initial := check(t, q, m, nil).CopyPlan
	if len(initial) != 1 {
		t.Fatal("missing copy", initial)
	}
	pauseForTest(t, q, a.ID, nil)
	if err := q.AuthorizeLaunch(*initial[0].Launch); !errors.Is(err, ErrStale) {
		t.Fatal("old copy authority retained", err)
	}
	fresh := check(t, q, m, nil).CopyPlan
	if len(fresh) != 1 || fresh[0].Score != 1 {
		t.Fatal("shared copy demand lost", fresh)
	}
	pauseForTest(t, q, b.ID, nil)
	if err := q.AuthorizeLaunch(*fresh[0].Launch); !errors.Is(err, ErrStale) {
		t.Fatal("copy authorized without runnable demand", err)
	}
	if got := check(t, q, m, nil).CopyPlan; len(got) != 0 {
		t.Fatal("paused model demand offered", got)
	}
}
