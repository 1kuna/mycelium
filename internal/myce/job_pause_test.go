package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
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

func TestJobPauseHTTPAuthorizationAndErrors(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	j := submit(t, q, spec("experiment"))
	client := Client{URL: server.URL, Token: "wrong"}
	for _, action := range []string{"pause", "resume"} {
		err := client.Call(context.Background(), "POST", "/api/jobs/"+j.ID+"/"+action, PauseJobRequest{Reason: "review"}, nil)
		var api *HTTPError
		if !errors.As(err, &api) || api.Status != 401 || api.Code != "authentication_required" {
			t.Fatalf("%s bypassed authentication: %v", action, err)
		}
	}
	client.Token = "test"
	err := client.Call(context.Background(), "POST", "/api/jobs/"+j.ID+"/pause", PauseJobRequest{}, nil)
	var api *HTTPError
	if !errors.As(err, &api) || api.Status != 400 || api.Code != "job_pause_invalid" {
		t.Fatalf("lost pause validation error: %v", err)
	}
	paused := pauseForTest(t, q, j.ID, nil)
	err = client.Call(context.Background(), "POST", "/api/jobs/"+j.ID+"/resume", ResumeJobRequest{IfRevision: paused.PauseRevision + 1}, nil)
	pauseError(t, err, "job_pause_revision_conflict")
	if _, err := q.ResumeJob(j.ID, paused.PauseRevision); err != nil {
		t.Fatal(err)
	}
	assigned := check(t, q, machine("review-worker", "fake"), nil).Job
	if assigned == nil {
		t.Fatal("missing assignment")
	}
	for _, started := range []bool{false, true} {
		if started {
			if err := q.Started(result(assigned, true)); err != nil {
				t.Fatal(err)
			}
		}
		err := client.Call(context.Background(), "POST", "/api/jobs/"+j.ID+"/pause", PauseJobRequest{Reason: "review"}, nil)
		pauseError(t, err, "job_pause_conflict")
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

func TestJobPauseRestartsProductionStartWindow(t *testing.T) {
	const window = 600 * time.Second
	for _, dependent := range []bool{false, true} {
		name := "queued"
		if dependent {
			name = "producer completes during pause"
		}
		t.Run(name, func(t *testing.T) {
			q, clock, m, s := fanOutFixture(t, 1)
			s.FanOut = nil
			s.Outputs = map[string]string{"out": "results"}
			var producer *Job
			if dependent {
				upstream := submit(t, q, s)
				producer = check(t, q, m, nil).Job
				if producer == nil || producer.ID != upstream.ID {
					t.Fatal("producer was not assigned", producer)
				}
				s = pieceConsumer(s, upstream.ID, "")
			}
			s.Priority, s.StartWindow, s.SubmissionKey = "production", "600s", "paused-production"
			original := submit(t, q, s)
			paused := pauseForTest(t, q, original.ID, nil)
			clock.Advance(time.Minute)
			if producer != nil {
				finish(t, q, producer, true)
			}
			clock.Advance(29 * time.Minute)
			if got := check(t, q, m, nil).Job; got != nil {
				t.Fatal("assigned paused work", got.ID)
			}
			if held := job(t, q, original.ID); held.State != "paused" || !dependencyReady(held) || held.Spec.SubmissionKey != s.SubmissionKey || held.Spec.StartBy != nil {
				t.Fatal("pause did not suspend the ready job's cutoff", held.State, held.Reason, held.Spec.StartBy)
			}
			resumed, err := q.ResumeJob(original.ID, paused.PauseRevision)
			if err != nil {
				t.Fatal(err)
			}
			if resumed.ID != original.ID || resumed.Spec.SubmissionKey != s.SubmissionKey || resumed.Spec.StartBy == nil || !resumed.Spec.StartBy.Equal(clock.Now().Add(window)) {
				t.Fatal("resume lost identity or did not grant a fresh window", resumed)
			}
			if replay := submit(t, q, s); replay.ID != original.ID {
				t.Fatal("submission key no longer resolves to the original job", replay.ID)
			}
			clock.Advance(window - time.Second)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != original.ID || run.Spec.SubmissionKey != s.SubmissionKey {
				t.Fatal("resumed job was not assigned", run)
			}
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJobPausePreservesAbsoluteStartDeadline(t *testing.T) {
	q, clock := fixture(t)
	s := spec("production")
	deadline := clock.Now().Add(10 * time.Minute)
	s.StartBy, s.SubmissionKey = &deadline, "absolute-start"
	original := submit(t, q, s)
	paused := pauseForTest(t, q, original.ID, nil)
	if paused.Spec.StartBy == nil || !paused.Spec.StartBy.Equal(deadline) {
		t.Fatal("pause changed the explicit deadline", paused.Spec.StartBy)
	}
	clock.Advance(10 * time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, original.ID); got.State != "failed" || !strings.Contains(got.Reason, "production start deadline exceeded") || got.Spec.SubmissionKey != s.SubmissionKey {
		t.Fatal("caller deadline did not expire the paused job", got.State, got.Reason)
	}
	_, err := q.ResumeJob(original.ID, paused.PauseRevision)
	pauseError(t, err, "job_pause_revision_conflict")
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
func TestJobPauseFanOut(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Priority, s.StartWindow, s.SubmissionKey = "production", "600s", "paused-fan-out"
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
	if child.State != "paused" || child.Pause.Parent != parent.ID || child.Spec.StartBy != nil {
		t.Fatal("requeued child escaped parent", child)
	}
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatal("paused child accepted superseded result", err)
	}
	if reply := check(t, q, m, run); !reply.Stop || job(t, q, run.ID).State != "paused" {
		t.Fatal("late attempt escaped pause", reply)
	}
	q = reopenCancellationQueue(t, q, clock)
	clock.Advance(30 * time.Minute)
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("paused parent dispatched child", got)
	}
	resumed, err := q.ResumeJob(parent.ID, paused.PauseRevision)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ID != parent.ID || resumed.Spec.SubmissionKey != s.SubmissionKey {
		t.Fatal("resume changed fan-out parent identity", resumed)
	}
	child = job(t, q, run.ID)
	if child.State != "queued" || child.Spec.StartBy == nil || !child.Spec.StartBy.Equal(clock.Now().Add(600*time.Second)) {
		t.Fatal("parent resume did not restart the child's window", child)
	}
	if got := check(t, q, m, nil).Job; got == nil || got.ID != run.ID || got.Attempt != run.Attempt+1 {
		t.Fatal("resumed parent did not retry its child", got)
	}
}
