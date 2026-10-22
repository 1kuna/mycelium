package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func cancelAck(t *testing.T, q *Queue, id string) Cancellation {
	t.Helper()
	a, err := q.CancelJob(id)
	if err != nil {
		t.Fatal(err)
	}
	if !a.WillNotRun || a.JobID == nil || *a.JobID != id {
		t.Fatalf("ack: %+v", a)
	}
	return a
}

func TestCancellationStates(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"queued", "preparing", "running", "stopping", "succeeded", "failed", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			q, c := fixture(t)
			s := spec("experiment")
			s.SubmissionKey = "cancel-key"
			j := submit(t, q, s)
			if state != "queued" && state != "cancelled" {
				m := machine("one", "fake")
				m.MaxJobs = 1
				j = check(t, q, m, nil).Job
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if state != "preparing" {
					if err := q.Started(result(j, true)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if state == "stopping" {
				if err := q.Leave("one", true); err != nil {
					t.Fatal(err)
				}
			}
			if state == "succeeded" || state == "failed" {
				finish(t, q, j, state == "succeeded")
			}
			if state == "cancelled" {
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneJob(q.s.Jobs[j.ID])
			a := cancelAck(t, q, j.ID)
			wantState, release := state, "pending"
			if state == "queued" {
				wantState, release = "cancelled", "not_applicable"
			}
			if state == "preparing" || state == "running" {
				wantState = "stopping"
			}
			if state == "cancelled" {
				release = "not_applicable"
			}
			if state == "succeeded" || state == "failed" {
				release = "confirmed"
			}
			if a.State != wantState || ackRelease(a) != release || a.Project != s.Project || a.SubmissionKey == nil || *a.SubmissionKey != s.SubmissionKey || !a.AcknowledgedAt.Equal(c.Now()) {
				t.Fatalf("ack: %+v", a)
			}
			after := cloneJob(q.s.Jobs[j.ID])
			after.CancelledAt = before.CancelledAt // Exclusion metadata may be added; state/results stay unchanged.
			if terminal(before.State) && !reflect.DeepEqual(before, after) {
				t.Fatal("terminal job changed")
			}
			stopAt := q.s.Jobs[j.ID].StopAt
			c.Advance(time.Second)
			repeat := cancelAck(t, q, j.ID)
			if !reflect.DeepEqual(stopAt, q.s.Jobs[j.ID].StopAt) {
				t.Fatal("repeat renewed stop grace")
			}
			if repeat.State != a.State || ackRelease(repeat) != ackRelease(a) {
				t.Fatalf("repeat: %+v", repeat)
			}
			if release == "pending" {
				if len(q.s.HostReleases) == 0 {
					t.Fatal("release uncharged")
				}
				if err := q.Started(result(j, true)); !errors.Is(err, ErrStale) {
					t.Fatalf("permit = %v", err)
				}
				if err := q.Started(result(j, true)); !errors.Is(err, ErrStale) {
					t.Fatalf("started = %v", err)
				}
				finish(t, q, j, true) // Success/OOM/interruption cannot override cancellation.
				if a := cancelAck(t, q, j.ID); a.State != "cancelled" || ackRelease(a) != "confirmed" {
					t.Fatalf("released: %+v", a)
				}
			}
		})
	}
}

func TestCancellationKeyRecords(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "never-seen"
	a, err := q.CancelSubmission(s.Project, s.SubmissionKey)
	if err != nil || a.JobID != nil || a.State != "absent" || !a.WillNotRun || ackRelease(a) != "not_applicable" {
		t.Fatalf("reservation: %+v %v", a, err)
	}
	c.Advance(time.Second)
	again, err := q.CancelSubmission(s.Project, s.SubmissionKey)
	if err != nil || again.State != "absent" || *again.SubmissionKey != s.SubmissionKey {
		t.Fatalf("repeat: %+v %v", again, err)
	}
	j := submit(t, q, s)
	if j.State != "cancelled" || j.CancelledAt == nil {
		t.Fatal("reservation became runnable")
	}
	if err := q.update(func() error { delete(q.s.Jobs, j.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	for _, byKey := range []bool{false, true} {
		var a Cancellation
		if byKey {
			a, err = q.CancelSubmission(s.Project, s.SubmissionKey)
		} else {
			a, err = q.CancelJob(j.ID)
		}
		if err != nil || a.State != "removed" || !a.WillNotRun {
			t.Fatalf("removed: %+v %v", a, err)
		}
	}
	c.Advance(submissionRetention)
	a, err = q.CancelSubmission(s.Project, s.SubmissionKey)
	if err != nil || a.State != "unknown" || a.JobID != nil || !a.WillNotRun {
		t.Fatalf("expired: %+v %v", a, err)
	}
	_, err = q.CancelJob(j.ID)
	submissionError(t, err, 404, "job_unknown")
	_, err = q.CancelJob("missing")
	submissionError(t, err, 404, "job_unknown")
	_, err = q.Submit(s)
	submissionError(t, err, 410, "submission_key_expired")
	for _, pair := range [][2]string{{"", "key"}, {"project", ""}} {
		if _, err = q.CancelSubmission(pair[0], pair[1]); err == nil {
			t.Fatal("empty identity accepted")
		}
	}
}

func TestCancellationSubmitOrders(t *testing.T) {
	t.Parallel()
	for _, cancelFirst := range []bool{true, false} {
		q, _ := fixture(t)
		s := spec("experiment")
		s.SubmissionKey = "race"
		if cancelFirst {
			if _, err := q.CancelSubmission(s.Project, s.SubmissionKey); err != nil {
				t.Fatal(err)
			}
		}
		j := submit(t, q, s)
		if !cancelFirst {
			if _, err := q.CancelSubmission(s.Project, s.SubmissionKey); err != nil {
				t.Fatal(err)
			}
		}
		if a := cancelAck(t, q, j.ID); a.State != "cancelled" {
			t.Fatal(a)
		}
		if check(t, q, machine("one", "fake"), nil).Job != nil {
			t.Fatal("cancelled job assigned")
		}
	}
	// Concurrent transactions must have one identity and no runnable result.
	for range 12 {
		q, _ := fixture(t)
		s := spec("experiment")
		s.SubmissionKey = "concurrent"
		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, err := q.Submit(s); errs <- err }()
		go func() {
			defer wg.Done()
			<-start
			_, err := q.CancelSubmission(s.Project, s.SubmissionKey)
			errs <- err
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if len(q.s.Jobs) != 1 || len(q.s.Submissions) != 1 {
			t.Fatal("non-atomic key cancellation")
		}
		for _, j := range q.s.Jobs {
			if j.State != "cancelled" {
				t.Fatal("runnable job")
			}
		}
	}
}

func TestCancellationHTTP(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test-only"}
	ctx := context.Background()
	a, err := client.CancelSubmission(ctx, "test", "key")
	if err != nil || a.State != "absent" {
		t.Fatalf("client: %+v %v", a, err)
	}
	s := spec("experiment")
	s.SubmissionKey = "key"
	j := submit(t, q, s)
	a, err = client.Cancel(ctx, j.ID)
	if err != nil || a.State != "cancelled" {
		t.Fatalf("client: %+v %v", a, err)
	}
	h := API{Queue: q, Token: "test-only"}.Handler()
	for _, path := range []string{"/api/jobs/" + j.ID + "/cancel", "/api/jobs/missing/cancel"} {
		r := httptest.NewRequest("POST", path, strings.NewReader("{}"))
		r.Header.Set("Authorization", "Bearer test-only")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(path, "missing") {
			if w.Code != 404 || body["code"] != "job_unknown" || body["will_not_run"] != nil {
				t.Fatalf("unknown: %d %v", w.Code, body)
			}
		} else if len(body) != 7 || body["will_not_run"] != true {
			t.Fatalf("envelope: %v", body)
		}
	}
	_, err = client.Cancel(ctx, "missing")
	submissionError(t, err, 404, "job_unknown")
}

func reopenCancellationQueue(t *testing.T, q *Queue, c Clock) *Queue {
	t.Helper()
	var path string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

func assertNoCancellationAttempt(t *testing.T, q *Queue, j *Job) {
	t.Helper()
	for _, id := range []string{"elsewhere-a", "elsewhere-b"} {
		if reply := check(t, q, machine(id, "fake"), nil); reply.Job != nil {
			t.Fatalf("future assignment: %+v", reply.Job)
		}
	}
	got := q.s.Jobs[j.ID]
	if got.Attempt != j.Attempt || !cancelled(got) || got.State == "queued" || got.State == "running" {
		t.Fatalf("resurrected: %+v", got)
	}
}

func TestCancellationNoFutureExecution(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"loss", "leave", "rejoin", "queue-restart", "engine-restart", "stop-grace", "operator-stop", "polite-stop", "oom", "extension-unavailable", "terminal-refund"} {
		t.Run(path, func(t *testing.T) {
			q, c := fixture(t)
			s := spec("experiment")
			s.SubmissionKey = "cancel-running"
			m := machine("one", "fake")
			j := submit(t, q, s)
			j = check(t, q, m, nil).Job
			if j == nil {
				t.Fatal("no assignment")
			}
			if err := q.Started(result(j, true)); err != nil {
				t.Fatal(err)
			}
			if path == "terminal-refund" {
				if err := q.update(func() error {
					live := q.s.Jobs[j.ID]
					live.State = "failed"
					live.Losses = 3
					live.Reason = "failed after 3 infrastructure losses: test"
					live.Copies[0].LossCharged = true
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
				t.Fatal(a)
			}
			r := result(j, false)
			switch path {
			case "loss":
				check(t, q, machine("healthy-a", "fake"), nil)
				check(t, q, machine("healthy-b", "fake"), nil)
				c.Advance(LossAfter)
				if err := q.update(func() error {
					q.s.Machines["healthy-a"].LastSeen = c.Now()
					q.s.Machines["healthy-b"].LastSeen = c.Now()
					q.maintain()
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if !q.s.Machines[m.ID].Gone {
					t.Fatal("loss was not simulated")
				}
				reply := check(t, q, m, j)
				if !reply.Stop || !reply.Kill {
					t.Fatal("late copy was adopted")
				}
			case "leave":
				if err := q.Leave(m.ID, true); err != nil {
					t.Fatal(err)
				}
				r.Interrupted = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			case "rejoin":
				fresh := m
				fresh.Session = "new-session"
				reply, err := q.Checkin(Checkin{Machine: fresh, Fresh: true, PreviousSession: m.Session})
				if err != nil || reply.Job != nil {
					t.Fatalf("rejoin: %+v %v", reply, err)
				}
				if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" {
					t.Fatal(a)
				}
			case "queue-restart":
				q = reopenCancellationQueue(t, q, c)
				c.Advance(RestartGrace + time.Second)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
					t.Fatal(a)
				}
				if reply := check(t, q, m, j); !reply.Stop {
					t.Fatal("restart adopted cancelled copy")
				}
			case "engine-restart":
				r.Interrupted = true
				r.ReleasePending = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
					t.Fatal(a)
				}
			case "stop-grace":
				c.Advance(StopGrace + time.Second)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
					t.Fatal("elapsed grace proved release", a)
				}
			case "operator-stop", "terminal-refund":
				r.Interrupted = true
				r.OperatorStop = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			case "polite-stop":
				r.Interrupted = true
				r.PoliteStop = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			case "oom":
				r.OutOfMemory = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			case "extension-unavailable":
				r.ExtensionUnavailable = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			if path == "terminal-refund" && q.s.Jobs[j.ID].State != "failed" {
				t.Fatal("terminal state changed")
			}
			assertNoCancellationAttempt(t, q, j)
			if err := q.Started(result(j, true)); !errors.Is(err, ErrStale) {
				t.Fatalf("permit: %v", err)
			}
			if err := q.Started(result(j, true)); !errors.Is(err, ErrStale) {
				t.Fatalf("started: %v", err)
			}
		})
	}
}

func TestCancellationReleaseCustody(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "copy-key"
	one, two := machine("one", "fake"), machine("two", "fake")
	s.Needs.HostMemoryGB = 4
	one.HostMemoryMeasured, two.HostMemoryMeasured = true, true
	one.HostAvailableMemoryGB, two.HostAvailableMemoryGB = 16, 16
	one.HostMemoryBudgetGB, two.HostMemoryBudgetGB = 16, 16
	j := submit(t, q, s)
	first := check(t, q, one, nil).Job
	check(t, q, two, nil)
	check(t, q, machine("three", "fake"), nil)
	c.Advance(LossAfter)
	if err := q.update(func() error {
		q.s.Machines["two"].LastSeen = c.Now()
		q.s.Machines["three"].LastSeen = c.Now()
		q.maintain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	second := check(t, q, two, nil).Job
	if second == nil || second.Attempt == first.Attempt {
		t.Fatal("second attempt not assigned")
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
		t.Fatal(a)
	}
	// Completion of the current execution does not settle its lost predecessor.
	finish(t, q, second, true)
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
		t.Fatal("old copy forgotten", a)
	}
	q.refreshHostMemory()
	if q.s.Machines[one.ID].HostMemoryInUseGB != 4 {
		t.Fatal("lost copy no longer charged")
	}
	stale := result(first, false)
	stale.ReleaseOnly = true
	stale.Session = "wrong"
	if err := q.Complete(stale); err != nil {
		t.Fatal(err)
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
		t.Fatal("stale release accepted", a)
	}
	exact := result(first, false)
	exact.ReleaseOnly = true
	if err := q.Complete(exact); err != nil {
		t.Fatal(err)
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" {
		t.Fatal(a)
	}
	if len(q.s.HostReleases) != 0 {
		t.Fatal("release retained after exact confirmation")
	}
	assertNoCancellationAttempt(t, q, second)
}

func TestCancellationDurabilityAndRollback(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "write-failure"
	j := submit(t, q, s)
	if _, err := q.db.Exec(`CREATE TRIGGER reject_queue BEFORE INSERT ON queue_state BEGIN SELECT RAISE(ABORT,'test write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := q.CancelJob(j.ID); err == nil {
		t.Fatal("failed write acknowledged")
	}
	if _, err := q.CancelSubmission(s.Project, "new-key"); err == nil {
		t.Fatal("failed reservation acknowledged")
	}
	if cancelled(q.s.Jobs[j.ID]) || q.s.Submissions[submissionDigest(s.Project, "new-key")] != nil {
		t.Fatal("write failure did not roll back")
	}
	if _, err := q.db.Exec(`DROP TRIGGER reject_queue`); err != nil {
		t.Fatal(err)
	}
	cancelAck(t, q, j.ID)
	if _, err := q.CancelSubmission(s.Project, "reserved"); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, c)
	if a := cancelAck(t, q, j.ID); a.State != "cancelled" {
		t.Fatal(a)
	}
	a, err := q.CancelSubmission(s.Project, "reserved")
	if err != nil || a.State != "absent" {
		t.Fatalf("persisted reservation: %+v %v", a, err)
	}
	c.Advance(submissionRetention - time.Nanosecond)
	a, err = q.CancelSubmission(s.Project, "reserved")
	if err != nil || a.State != "absent" {
		t.Fatal("early expiry", a, err)
	}
	c.Advance(time.Nanosecond)
	a, err = q.CancelSubmission(s.Project, "reserved")
	if err != nil || a.State != "unknown" {
		t.Fatal("reservation expiry", a, err)
	}
	q = reopenCancellationQueue(t, q, c)
	a, err = q.CancelSubmission(s.Project, "reserved")
	if err != nil || a.State != "unknown" {
		t.Fatal("marker lost", a, err)
	}
}

func TestCancellationStartRace(t *testing.T) {
	t.Parallel()
	for _, cancelFirst := range []bool{true, false} {
		q, _ := fixture(t)
		j := submit(t, q, spec("experiment"))
		j = check(t, q, machine("one", "fake"), nil).Job
		if cancelFirst {
			cancelAck(t, q, j.ID)
		}
		err := q.Started(result(j, true))
		if cancelFirst && !errors.Is(err, ErrStale) || !cancelFirst && err != nil {
			t.Fatalf("start race: %v", err)
		}
		if !cancelFirst {
			cancelAck(t, q, j.ID)
		}
		if check(t, q, machine("one", "fake"), j).Stop != true {
			t.Fatal("stop not issued")
		}
		assertNoCancellationAttempt(t, q, j)
	}
}

func TestCancellationQueuedLostCopyRelease(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	m.MaxJobs = 1
	j = check(t, q, m, nil).Job
	check(t, q, machine("two", "fake"), nil)
	check(t, q, machine("three", "fake"), nil)
	c.Advance(LossAfter)
	if err := q.update(func() error {
		q.s.Machines["two"].LastSeen = c.Now()
		q.s.Machines["three"].LastSeen = c.Now()
		q.maintain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[j.ID].State != "queued" {
		t.Fatal("not requeued after loss")
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
		t.Fatal("lost attempt forgotten", a)
	}
	release := result(j, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" {
		t.Fatal("lost attempt never settled", a)
	}
	assertNoCancellationAttempt(t, q, j)
}

func TestCancellationMaintenanceKeepsExactCustody(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	m.MaxJobs = 1
	j = check(t, q, m, nil).Job
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	cancelAck(t, q, j.ID)
	if err := setMaintenance(t, q, m.ID, "disable"); err != nil {
		t.Fatal(err)
	}
	if len(q.s.HostReleases) != 1 {
		t.Fatal("maintenance discharged cancelled execution without exact evidence")
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
		t.Fatal(a)
	}
	if err := q.Complete(Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, ReleaseOnly: true}); err != nil {
		t.Fatal(err)
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" {
		t.Fatal(a)
	}
	assertNoCancellationAttempt(t, q, j)
}

func ackRelease(ack Cancellation) string {
	if ack.Release == nil {
		return ""
	}
	return *ack.Release
}
