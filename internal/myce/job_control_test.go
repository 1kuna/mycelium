package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Run the same ordered interleavings through SQLite queue transactions and
// the authenticated HTTP routes. No sleep determines which write wins.
type fencedControlTest struct {
	queue  *Queue
	client *Client
}

func newFencedControlTest(t *testing.T, q *Queue, wire bool) fencedControlTest {
	t.Helper()
	h := fencedControlTest{queue: q}
	if wire {
		s := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
		t.Cleanup(s.Close)
		h.client = &Client{URL: s.URL, Token: "test"}
	}
	return h
}

func (h fencedControlTest) pause(id string, r FencedPauseRequest) (*Job, error) {
	if h.client == nil {
		return h.queue.FencedPause(context.Background(), id, r)
	}
	var j Job
	err := h.client.Call(context.Background(), "POST", "/api/jobs/"+id+"/control/pause", r, &j)
	return &j, err
}

func (h fencedControlTest) resume(id string, r FencedResumeRequest) (*Job, error) {
	if h.client == nil {
		return h.queue.FencedResume(context.Background(), id, r)
	}
	var j Job
	err := h.client.Call(context.Background(), "POST", "/api/jobs/"+id+"/control/resume", r, &j)
	return &j, err
}

func (h fencedControlTest) read(t *testing.T, id string) *Job {
	t.Helper()
	if h.client == nil {
		j, err := h.queue.jobSnapshot(id)
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	var j Job
	if err := h.client.Call(context.Background(), "GET", "/api/jobs/"+id, nil, &j); err != nil {
		t.Fatal(err)
	}
	return &j
}

func (h fencedControlTest) cancel(t *testing.T, j *Job) Cancellation {
	t.Helper()
	var ack Cancellation
	var err error
	if h.client == nil {
		ack, err = h.queue.CancelSubmission(j.Spec.Project, j.Spec.SubmissionKey)
	} else {
		r := CancelSubmissionRequest{Project: j.Spec.Project, SubmissionKey: j.Spec.SubmissionKey}
		err = h.client.Call(context.Background(), "POST", "/api/jobs/control/cancel-by-key", r, &ack)
	}
	if err != nil || !ack.WillNotRun || ack.JobID == nil || *ack.JobID != j.ID || ack.AcknowledgedAt == nil {
		t.Fatalf("keyed cancellation ack = %+v, err = %v", ack, err)
	}
	return ack
}

func assertFencedControl(t *testing.T, j *Job, err error, state, action string, generation, revision uint64) {
	t.Helper()
	want := JobControl{Generation: generation, Action: action, Revision: revision}
	if err != nil || j == nil || j.State != state || j.PauseRevision != revision || j.Control == nil || *j.Control != want {
		t.Fatalf("want %s with control %+v, got %+v, err = %v", state, want, j, err)
	}
}

func TestFencedJobControlOrdering(t *testing.T) {
	for _, transport := range []string{"queue", "http"} {
		t.Run(transport, func(t *testing.T) {
			q, clock := fixture(t)
			h := newFencedControlTest(t, q, transport == "http")
			original := submit(t, q, spec("production"))
			id := original.ID
			queued := h.read(t, id) // Prime exact GET before any controls commit.
			deadline := queued.Spec.StartBy
			if deadline == nil {
				t.Fatal("fixture requires a relative production start window")
			}
			firstPause := FencedPauseRequest{Project: original.Spec.Project, Reason: "first hold", IfAttempt: &original.Attempt, ControlGeneration: 1}
			firstResume := FencedResumeRequest{IfRevision: &queued.PauseRevision, ControlGeneration: 1}
			clock.Advance(time.Second)
			resumed, err := h.resume(id, firstResume)
			assertFencedControl(t, resumed, err, "queued", "resume", 1, 1)
			if resumed.Spec.StartBy == nil || !resumed.Spec.StartBy.Equal(*deadline) {
				t.Fatal("runnable resume restarted the start window", resumed.Spec.StartBy, deadline)
			}
			_, err = h.pause(id, firstPause) // The earlier pause arrives after its resume.
			pauseError(t, err, "job_control_generation_conflict")
			replay, err := h.resume(id, firstResume)
			assertFencedControl(t, replay, err, "queued", "resume", 1, 1)

			secondPause := firstPause
			secondPause.ControlGeneration = 2
			second, err := h.pause(id, secondPause)
			assertFencedControl(t, second, err, "paused", "pause", 2, 2)
			secondResume := FencedResumeRequest{IfRevision: &second.PauseRevision, ControlGeneration: 2}
			thirdPause := secondPause
			thirdPause.ControlGeneration, thirdPause.Reason = 3, "newer hold"
			clock.Advance(time.Second)
			third, err := h.pause(id, thirdPause)
			assertFencedControl(t, third, err, "paused", "pause", 3, 3)
			_, err = h.resume(id, secondResume)
			pauseError(t, err, "job_control_generation_conflict")
			secondResume.ControlGeneration = 3 // A current generation still needs a current token.
			_, err = h.resume(id, secondResume)
			pauseError(t, err, "job_pause_revision_conflict")
			_, err = q.ResumeJob(id, second.PauseRevision)
			pauseError(t, err, "job_pause_revision_conflict")
			_, err = h.pause(id, FencedPauseRequest{Project: thirdPause.Project, Reason: "different request", IfAttempt: thirdPause.IfAttempt, ControlGeneration: 3})
			pauseError(t, err, "job_control_generation_conflict")
			assertFencedControl(t, h.read(t, id), nil, "paused", "pause", 3, 3)

			// The real hold freezes the relative budget. Resume gets the full window.
			clock.Advance(30 * time.Minute)
			lastResume := FencedResumeRequest{IfRevision: &third.PauseRevision, ControlGeneration: 4}
			last, err := h.resume(id, lastResume)
			assertFencedControl(t, last, err, "queued", "resume", 4, 4)
			if last.Spec.StartBy == nil || !last.Spec.StartBy.After(clock.Now()) || last.ID != id {
				t.Fatal("resume lost the job or its fresh start window", last)
			}
			m := machine("fenced-control-worker", "fake")
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != id {
				t.Fatal("resumed work was not assigned", assigned)
			}
			thirdPause.ControlGeneration = 5
			if _, err := h.pause(id, thirdPause); !errors.Is(err, ErrStale) && !stale(err) {
				t.Fatalf("pause from before assignment accepted: %v", err)
			}
			thirdPause.IfAttempt = &assigned.Attempt
			_, err = h.pause(id, thirdPause)
			pauseError(t, err, "job_pause_conflict")
			replay, err = h.resume(id, lastResume)
			assertFencedControl(t, replay, err, "running", "resume", 4, 4)
		})
	}
}

func TestFencedJobControlPauseRevision(t *testing.T) {
	for _, transport := range []string{"queue", "http"} {
		t.Run(transport, func(t *testing.T) {
			q, clock := fixture(t)
			h := newFencedControlTest(t, q, transport == "http")
			j := submit(t, q, spec("experiment"))
			firstRead := h.read(t, j.ID)
			pause := FencedPauseRequest{
				Project: j.Spec.Project, Reason: "batch hold", IfAttempt: &firstRead.Attempt,
				IfRevision: &firstRead.PauseRevision, ControlGeneration: 9,
			}
			until := clock.Now().Add(time.Hour)
			foreign := pauseForTest(t, q, j.ID, &until)

			_, err := h.pause(j.ID, pause)
			pauseError(t, err, "job_pause_revision_conflict")
			current := h.read(t, j.ID)
			if current.Control != nil || current.PauseRevision != foreign.PauseRevision ||
				current.Pause == nil || current.Pause.Reason != foreign.Pause.Reason ||
				current.Pause.Until == nil || !current.Pause.Until.Equal(until) {
				t.Fatal("stale pause changed the intervening hold", current)
			}

			pause.IfRevision = &current.PauseRevision
			accepted, err := h.pause(j.ID, pause)
			assertFencedControl(t, accepted, err, "paused", "pause", 9, 2)
			replayed, err := h.pause(j.ID, pause)
			assertFencedControl(t, replayed, err, "paused", "pause", 9, 2)
			changed := pause
			changed.IfRevision = &accepted.PauseRevision
			_, err = h.pause(j.ID, changed)
			pauseError(t, err, "job_control_generation_conflict")

			resume := FencedResumeRequest{IfRevision: &accepted.PauseRevision, ControlGeneration: 9}
			resumed, err := h.resume(j.ID, resume)
			assertFencedControl(t, resumed, err, "queued", "resume", 9, 3)
			changed.ControlGeneration = 10
			_, err = h.pause(j.ID, changed)
			pauseError(t, err, "job_pause_revision_conflict")
			assertFencedControl(t, h.read(t, j.ID), nil, "queued", "resume", 9, 3)
		})
	}
}

func TestFencedJobControlExecutionRelease(t *testing.T) {
	q, _ := fixture(t)
	h := newFencedControlTest(t, q, true)
	j := submit(t, q, spec("experiment"))
	if got := h.read(t, j.ID); got.ExecutionRelease != "not_applicable" {
		t.Fatal("unattempted job release =", got.ExecutionRelease)
	}
	worker := machine("release-worker", "fake")
	attempt := check(t, q, worker, nil).Job
	if attempt == nil || attempt.ID != j.ID {
		t.Fatal("missing assignment", attempt)
	}
	if got := h.read(t, j.ID); got.ExecutionRelease != "pending" {
		t.Fatal("assigned job release =", got.ExecutionRelease)
	}
	// Model a previously released copy with an independent stage obligation.
	// The fixture changes custody only, so the exact GET cache must invalidate
	// even when the durable job and copies stay byte-for-byte the same.
	if err := q.update(func() error {
		stored := q.touchJob(j.ID)
		q.confirmCopyRelease(result(attempt, false), true)
		q.resumeJob(stored)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	paused := pauseForTest(t, q, j.ID, nil)
	assertRelease := func(want string) {
		t.Helper()
		got := h.read(t, j.ID)
		if got.State != "paused" || got.PauseRevision != paused.PauseRevision ||
			len(got.Copies) != 1 || !got.Copies[0].ReleaseConfirmed ||
			got.Cancellation != nil || got.ExecutionRelease != want {
			t.Fatalf("paused release = %q, want %q (state %s, copies %+v)", got.ExecutionRelease, want, got.State, got.Copies)
		}
	}
	assertRelease("confirmed")

	stage := HostRelease{Job: j.ID, Host: worker.ID, Machine: worker.ID, Session: worker.Session, Preparation: true}
	if err := q.update(func() error {
		q.s.HostReleases = map[string]HostRelease{releaseKey(stage): stage}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertRelease("pending")

	release := Result{Execution: Execution{ID: j.ID}, Machine: stage.Machine, Session: "wrong-session", ReleaseOnly: true}
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	assertRelease("pending")
	release.Session = stage.Session
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	assertRelease("confirmed")
}

func TestFencedJobControlLostReplyAndReopen(t *testing.T) {
	for _, transport := range []string{"queue", "http"} {
		t.Run(transport, func(t *testing.T) {
			q, clock := fixture(t)
			h := newFencedControlTest(t, q, transport == "http")
			j := submit(t, q, spec("experiment"))
			old := h.read(t, j.ID) // Exact GET must invalidate even without a clock tick.
			pause := FencedPauseRequest{Project: j.Spec.Project, Reason: "review", IfAttempt: &old.Attempt, IfRevision: &old.PauseRevision, ControlGeneration: 7}
			if transport == "http" {
				dropFencedPauseReply(t, q, j.ID, pause)
			} else {
				if _, err := h.pause(j.ID, pause); err != nil { // Discard the successful reply.
					t.Fatal(err)
				}
			}
			observed := h.read(t, j.ID)
			assertFencedControl(t, observed, nil, "paused", "pause", 7, 1)
			if observed.Pause == nil || observed.Pause.Reason != pause.Reason {
				t.Fatal("GET lost the committed hold", observed)
			}
			clock.Advance(time.Second)
			q = reopenCancellationQueue(t, q, clock)
			h = newFencedControlTest(t, q, transport == "http")
			retry, err := h.pause(j.ID, pause)
			assertFencedControl(t, retry, err, "paused", "pause", 7, 1)
			resume := FencedResumeRequest{IfRevision: &observed.PauseRevision, ControlGeneration: 7}
			if _, err := h.resume(j.ID, resume); err != nil { // Discard this reply too.
				t.Fatal(err)
			}
			assertFencedControl(t, h.read(t, j.ID), nil, "queued", "resume", 7, 2)
			q = reopenCancellationQueue(t, q, clock)
			h = newFencedControlTest(t, q, transport == "http")
			retry, err = h.resume(j.ID, resume)
			assertFencedControl(t, retry, err, "queued", "resume", 7, 2)
			_, err = h.pause(j.ID, pause)
			pauseError(t, err, "job_control_generation_conflict")
		})
	}
}

func dropFencedPauseReply(t *testing.T, q *Queue, id string, pause FencedPauseRequest) {
	t.Helper()
	api := API{Queue: q, Token: "test"}.Handler()
	status := make(chan int, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, r)
		status <- response.Code
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer server.Close()
	client := Client{URL: server.URL, Token: "test", singleAttempt: true}
	err := client.Call(context.Background(), "POST", "/api/jobs/"+id+"/control/pause", pause, nil)
	if err == nil || <-status != http.StatusOK {
		t.Fatalf("expected a committed pause followed by a lost response, err = %v", err)
	}
}

func TestFencedJobControlCancelAndAssignment(t *testing.T) {
	for _, transport := range []string{"queue", "http"} {
		t.Run(transport, func(t *testing.T) {
			for _, cancelFirst := range []bool{true, false} {
				t.Run(map[bool]string{true: "cancel-first", false: "offer-in-flight"}[cancelFirst], func(t *testing.T) {
					testFencedControlCancelAssignment(t, transport == "http", cancelFirst)
				})
			}
		})
	}
}

func testFencedControlCancelAssignment(t *testing.T, wire, cancelFirst bool) {
	t.Helper()
	q, clock := fixture(t)
	h := newFencedControlTest(t, q, wire)
	s := spec("experiment")
	s.SubmissionKey = "control-cancel"
	j := submit(t, q, s)
	m := machine("control-assignment", "fake")
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second}
	var ack Cancellation
	if cancelFirst {
		ack = h.cancel(t, j)
	}
	committed, deliver := make(chan struct{}), make(chan struct{})
	deliverReply := sync.OnceFunc(func() { close(deliver) })
	defer deliverReply()
	type checkinResult struct {
		reply Reply
		err   error
	}
	done := make(chan checkinResult, 1)
	if wire {
		api := API{Queue: q, Token: "test"}.Handler()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			buffer := httptest.NewRecorder()
			api.ServeHTTP(buffer, r)
			close(committed)
			<-deliver
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(buffer.Code)
			_, _ = w.Write(buffer.Body.Bytes())
		}))
		t.Cleanup(server.Close)
		go func() {
			client := Client{URL: server.URL, Token: "test", singleAttempt: true}
			var reply Reply
			err := client.Call(context.Background(), "POST", "/api/workers/checkin", c, &reply)
			done <- checkinResult{reply, err}
		}()
	} else {
		go func() {
			reply, err := q.checkinResponseContext(context.Background(), c, func(Reply, error) {
				close(committed)
				<-deliver
			})
			done <- checkinResult{reply, err}
		}()
	}
	select {
	case <-committed:
	case <-time.After(testWaitTimeout):
		t.Fatal("check-in did not reach the response boundary")
	}
	clock.Advance(time.Second)
	if !cancelFirst {
		if assigned := h.read(t, j.ID); assigned.State != "running" || assigned.Attempt != 1 || assigned.StartedAt != nil {
			t.Fatal("fixture requires an assigned, unstarted attempt", assigned)
		}
		ack = h.cancel(t, j)
	}
	deliverReply()
	var checked checkinResult
	select {
	case checked = <-done:
	case <-time.After(testWaitTimeout):
		t.Fatal("check-in response did not finish")
	}
	if checked.err != nil {
		t.Fatal(checked.err)
	}
	if cancelFirst {
		if len(checked.reply.Attempts) != 0 || ackRelease(ack) != "not_applicable" || ack.State != "cancelled" {
			t.Fatal("cancel-first assigned work", checked.reply.Attempts, ack)
		}
	} else {
		if len(checked.reply.Attempts) != 1 || checked.reply.Attempts[0].Job == nil || ackRelease(ack) != "pending" || ack.State != "stopping" {
			t.Fatal("in-flight offer did not retain exact release custody", checked.reply.Attempts, ack)
		}
		offer := checked.reply.Attempts[0].Job
		l := HostLaunch{Kind: "batch", Key: offer.ID, Attempt: strconv.Itoa(offer.Attempt), Machine: offer.Machine, Session: offer.Session}
		r := result(offer, true)
		if wire {
			if err := h.client.Call(context.Background(), "POST", "/api/workers/launch", l, nil); !stale(err) {
				t.Fatalf("HTTP launch accepted cancelled assignment: %v", err)
			}
			if err := h.client.Call(context.Background(), "POST", "/api/workers/started", r, nil); !stale(err) {
				t.Fatalf("HTTP Started accepted cancelled assignment: %v", err)
			}
		} else {
			if err := q.AuthorizeLaunch(l); !errors.Is(err, ErrStale) {
				t.Fatalf("launch accepted cancelled assignment: %v", err)
			}
			if err := q.Started(r); !errors.Is(err, ErrStale) {
				t.Fatalf("Started accepted cancelled assignment: %v", err)
			}
		}
		// A stale release must not finish the cancellation or free the charge.
		complete := q.Complete
		if wire {
			complete = func(r Result) error {
				return h.client.Call(context.Background(), "POST", "/api/workers/complete", r, nil)
			}
		}
		r.Success, r.ReleaseOnly = false, true
		r.Attempt++
		if err := complete(r); err != nil || ackRelease(h.cancel(t, j)) != "pending" {
			t.Fatalf("stale attempt changed cancellation custody: %v", err)
		}
		r.Attempt--
		if err := complete(r); err != nil {
			t.Fatal(err)
		}
		if final := h.cancel(t, j); ackRelease(final) != "confirmed" || final.State != "cancelled" || !final.AcknowledgedAt.Equal(*ack.AcknowledgedAt) {
			t.Fatal("exact release did not settle the original cancellation", final)
		}
	}
	got := h.read(t, j.ID)
	pause := FencedPauseRequest{Project: s.Project, Reason: "late hold", IfAttempt: &got.Attempt, ControlGeneration: 100}
	resume := FencedResumeRequest{IfRevision: &got.PauseRevision, ControlGeneration: 101}
	_, err := h.pause(j.ID, pause)
	pauseError(t, err, "job_control_cancelled")
	_, err = h.resume(j.ID, resume)
	pauseError(t, err, "job_control_cancelled")
	assertNoCancellationAttempt(t, q, got)
}

func TestFencedJobControlHTTPCapabilityAndValidation(t *testing.T) {
	q, _ := fixture(t)
	h := newFencedControlTest(t, q, true)
	ctx := context.Background()
	var status Snapshot
	if err := h.client.Call(ctx, "GET", "/api/status", nil, &status); err != nil {
		t.Fatal(err)
	}
	for _, capability := range []string{"fenced_job_control", "fenced_job_control_if_revision", "paused_start_window_frozen"} {
		if !slices.Contains(status.QueueCapabilities, capability) {
			t.Fatal("missing queue capability", capability, status.QueueCapabilities)
		}
	}
	j := submit(t, q, spec("experiment"))
	_, err := h.pause(j.ID, FencedPauseRequest{Project: j.Spec.Project, Reason: "missing attempt", ControlGeneration: 1})
	submissionError(t, err, 400, "job_pause_invalid")
	_, err = h.resume(j.ID, FencedResumeRequest{ControlGeneration: 1})
	submissionError(t, err, 400, "job_resume_invalid")
	_, err = h.pause(j.ID[:8], FencedPauseRequest{Project: j.Spec.Project, Reason: "prefix", IfAttempt: &j.Attempt, ControlGeneration: 1})
	submissionError(t, err, 404, "job_unknown")
}

func TestFencedJobControlConflictWireSchema(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "schema-conflicts"
	j := submit(t, q, s)
	h := newFencedControlTest(t, q, false)
	pause := FencedPauseRequest{Project: j.Spec.Project, Reason: "hold", IfAttempt: &j.Attempt, ControlGeneration: 1}
	if _, err := h.pause(j.ID, pause); err != nil {
		t.Fatal(err)
	}
	pause.Reason = "different hold"
	handler := API{Queue: q, Token: "test"}.Handler()
	schema := compileWireSchema(t, "error-v1.schema.json")
	assertConflict := func(code string) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/api/jobs/"+j.ID+"/control/pause", strings.NewReader(string(wireJSON(t, pause))))
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var reply map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
			t.Fatal(err)
		}
		if w.Code != http.StatusConflict || reply["code"] != code {
			t.Fatalf("expected 409 %s, got %d %v", code, w.Code, reply)
		}
		validateWireValue(t, schema, reply)
	}
	assertConflict("job_control_generation_conflict")
	h.cancel(t, j)
	assertConflict("job_control_cancelled")
}
