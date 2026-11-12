package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// #601: a preempted copy releases between the start-fence refusal and the
// failure report. The original predicate, not its current value, owns charging.
func TestStartFenceRefusalSurvivesClearedFence(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		for _, boundary := range []string{"launch", "reserved", "started"} {
			t.Run(priority+"/"+boundary, func(t *testing.T) {
				q, clock := fixture(t)
				m := machine("worker", "fake")
				submit(t, q, spec(priority))
				a := check(t, q, m, nil).Job
				server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
				defer server.Close()
				client := Client{URL: server.URL, Token: "test"}
				custody := NativeCustodyReport{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Machine: a.Machine, Session: a.Session, ProcessID: "process", CustodyID: "group", Phase: "reserved"}
				if boundary == "started" {
					if err := q.NativeCustody(custody); err != nil {
						t.Fatal(err)
					}
					custody.Phase = "started"
				}
				// A retained copy is still stopping on the same host. Production's
				// worker is recovering; the experiment's host withdraws admission.
				release := HostRelease{Host: hostID(&m), Job: "victim", Machine: m.ID, Session: m.Session, Attempt: 1, Until: clock.Now().Add(10 * time.Second)}
				if err := q.update(func() error {
					q.s.HostReleases = map[string]HostRelease{}
					if priority == "experiment" {
						q.touchMachine(m.ID).Leaving = true
					} else {
						q.touchMachine(m.ID).Recovering = true
					}
					q.s.HostReleases[releaseKey(release)] = release
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				var logs testLogBuffer
				old := log.Writer()
				log.SetOutput(&logs)
				defer log.SetOutput(old)
				var err error
				if boundary == "launch" {
					err = (Worker{Machine: m, Client: client}).authorizeLaunch(HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)})
				} else {
					err = client.Call(context.Background(), "POST", "/api/workers/native-custody", custody, nil)
					err = fmt.Errorf("%w: %w", errWorkerStartAuthorization, err)
				}
				var refusal *HTTPError
				if !errors.As(err, &refusal) || refusal.Status != 409 || refusal.Code != "request_stale" {
					t.Fatalf("start fence did not refuse: %v", err)
				}
				predicate := "host_launch"
				if !strings.Contains(logs.String(), "predicate="+predicate) {
					t.Errorf("refusal omitted predicate: %s", logs.String())
				}
				clock.Advance(420 * time.Millisecond)
				if err := q.update(func() error {
					if !q.settleRelease(Result{Execution: Execution{ID: release.Job, Attempt: release.Attempt}, Machine: release.Machine, Session: release.Session}, true) {
						t.Fatal("blocking release was not retained")
					}
					q.touchMachine(m.ID).Leaving, q.touchMachine(m.ID).Recovering = false, false
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if predicate := q.startPlacementRejection(q.s.Jobs[a.ID]); predicate != "" {
					t.Fatalf("fence did not clear: %s", predicate)
				}
				r := result(a, false)
				r.Interrupted = true
				r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("command", err))
				if r.ExecutionCause.Class != "admission_changed" {
					t.Errorf("409 misclassified: %+v", r.ExecutionCause)
				}
				// Also accept an old worker's diagnostic during a rolling upgrade.
				if priority == "experiment" {
					r.ExecutionCause.Class = "io_error"
					r.ExecutionCause.Operation = "start_handoff"
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, a.ID)
				if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || (len(got.AttemptHistory) != 1 || !strings.Contains(got.AttemptHistory[0].Reason, predicate)) {
					t.Fatalf("cleared refusal charged or lost predicate: state=%s losses=%d retries=%d reason=%q", got.State, got.Losses, got.InfrastructureRetries, got.Reason)
				}
				b := check(t, q, m, nil).Job
				if b == nil || b.Attempt != a.Attempt+1 {
					t.Fatal("same-session retry not assigned")
				}
				// A real timeout on the new attempt must not inherit the refusal.
				r = result(b, false)
				r.Interrupted = true
				r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "timeout"}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got = job(t, q, a.ID)
				if got.Losses != 1 || priority == "production" && got.InfrastructureRetries != 1 {
					t.Fatalf("real failure not charged: %+v", got)
				}
			})
		}
	}
}

func TestStartFenceStillChargesRealFailure(t *testing.T) {
	for _, superseded := range []bool{false, true} {
		t.Run(fmt.Sprintf("successful_reauthorization=%t", superseded), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			submit(t, q, spec("production"))
			a := check(t, q, m, nil).Job
			launch := HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)}
			setLeaving := func(leaving bool) {
				t.Helper()
				if err := q.update(func() error { q.touchMachine(m.ID).Leaving = leaving; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			setLeaving(true)
			if superseded {
				if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
					t.Fatal("missing refusal", err)
				}
				// Verify the decision survives a queue restart, before permission
				// is successfully retried on this same attempt.
				q = reopenQueue(t, q, clock)
				if recordedStartRefusal(q.s.Jobs[a.ID]) != "host_launch" {
					t.Fatal("refusal not durable")
				}
				setLeaving(false)
				check(t, q, m, a)
				if err := q.AuthorizeLaunch(launch); err != nil {
					t.Fatal("reauthorization failed", err)
				}
				setLeaving(true)
			}
			clock.Advance(time.Second)
			r := result(a, false)
			r.Interrupted = true
			r.ExecutionCause = runtimeDiagnostic(fmt.Errorf("%w: %w", errWorkerStartAuthorization, context.DeadlineExceeded))
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, a.ID)
			if got.State != "queued" || got.Losses != 1 || got.InfrastructureRetries != 1 {
				t.Fatalf("live fence forgave real transport failure: state=%s losses=%d retries=%d", got.State, got.Losses, got.InfrastructureRetries)
			}
		})
	}
}

// #1299: the queue's host_launch fence refuses before the payload runs. That is
// an admission requeue: no start counted, no interrupted attempt, a queue
// admission finding, and the attempt number stays fenced for the retry.
func TestLaunchRefusalIsAdmissionRequeue(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	submit(t, q, spec("production"))
	a := check(t, q, m, nil).Job
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	setLeaving := func(leaving bool) {
		t.Helper()
		if err := q.update(func() error { q.touchMachine(m.ID).Leaving = leaving; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	setLeaving(true)
	launch := HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)}
	err := (Worker{Machine: m, Client: Client{URL: server.URL, Token: "test"}}).authorizeLaunch(launch)
	r := startFailureOutcome(result(a, false), err)
	if c := r.ExecutionCause; c == nil || c.Operation != "start_authorization" || c.Class != "admission_changed" || r.Success {
		t.Fatalf("worker did not report a launch refusal: %v %+v", err, r)
	}
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, a.ID)
	if got.State != "queued" || got.Starts != 0 || got.AdmissionRequeues != 1 || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("refusal counted as a start: state=%s starts=%d admission=%d losses=%d retries=%d", got.State, got.Starts, got.AdmissionRequeues, got.Losses, got.InfrastructureRetries)
	}
	if len(got.AttemptHistory) != 1 || got.AttemptHistory[0].Interrupted || !got.AttemptHistory[0].AdmissionRequeue || got.AttemptHistory[0].Attempt != a.Attempt {
		t.Fatalf("history not an admission event: %+v", got.AttemptHistory)
	}
	if public, err := q.jobSnapshot(a.ID); err != nil || public.Finding == nil || public.Finding.Code != "queue_admission" || public.Finding.Owner != findingOwnerMycelium {
		t.Fatalf("finding does not name a queue admission event: %+v %v", public.Finding, err)
	}
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatal("refused attempt's report was accepted again", err)
	}
	setLeaving(false)
	b := check(t, q, m, nil).Job
	if b == nil || b.Attempt != a.Attempt+1 || b.Starts != 1 {
		t.Fatalf("retry reused the refused attempt or miscounted: %+v", b)
	}
	launch.Attempt = strconv.Itoa(b.Attempt)
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatal("stale refused attempt touched the retry", err)
	}
	clock.Advance(time.Second)
	if err := q.Complete(result(b, true)); err != nil {
		t.Fatal(err)
	}
	got = job(t, q, a.ID)
	if got.State != "succeeded" || got.Starts != 1 || got.Losses != 0 || len(got.AttemptHistory) != 2 || got.AttemptHistory[1].AdmissionRequeue || !got.AttemptHistory[0].AdmissionRequeue {
		t.Fatalf("retry did not succeed normally: state=%s starts=%d history=%+v", got.State, got.Starts, got.AttemptHistory)
	}
}

// #1299 hotfix: a dependent fan-out stage carries the caller's explicit
// allocation-time cutoff (allocation + 150s). After readiness, exact pre-main
// launch refusals each spend about 70s of preparation. They must not consume
// the cutoff, even when the refusal report lands after it; queue waiting after
// a refusal still counts against the restored remainder.
func TestExplicitStartByNotConsumedByLaunchRefusals(t *testing.T) {
	for _, phase := range []string{"retry-succeeds", "queue-wait-still-counts"} {
		t.Run(phase, func(t *testing.T) {
			q, clock, m, s := fanOutFixture(t, 1)
			s.Outputs = map[string]string{"out": "results"}
			producer := submit(t, q, s)
			c := s
			c.Priority = "production"
			c.After = &PieceDependency{Job: producer.ID, Outputs: map[string]string{"input": "item-0/out"}}
			cutoff := clock.Now().Add(150 * time.Second)
			c.StartBy = &cutoff
			consumer := submit(t, q, c)
			clock.Advance(30 * time.Second)
			wave := check(t, q, m, nil).Job
			if wave == nil || wave.Batch == nil || wave.Batch.Parent != producer.ID {
				t.Fatal("producer wave not assigned", wave)
			}
			finish(t, q, wave, true)
			setLeaving := func(leaving bool) {
				t.Helper()
				if err := q.update(func() error { q.touchMachine(m.ID).Leaving = leaving; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			refuse := func(child *Job, lateReport time.Duration) {
				t.Helper()
				clock.Advance(70 * time.Second) // environment preparation and sidecars
				setLeaving(true)
				launch := HostLaunch{Machine: child.Machine, Session: child.Session, Kind: "batch", Key: child.ID, Attempt: strconv.Itoa(child.Attempt)}
				if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
					t.Fatal("launch not refused", err)
				}
				// The refused attempt is still running until its worker reports;
				// an expiry sweep in between must leave the decision to the report.
				for waited := time.Duration(0); waited < lateReport; waited += 10 * time.Second {
					clock.Advance(10 * time.Second)
					check(t, q, m, child) // the worker stays alive while reporting late
				}
				if err := q.update(func() error { q.maintainStartDeadlines(); return nil }); err != nil {
					t.Fatal(err)
				}
				before := job(t, q, child.ID)
				if before.State != "running" {
					t.Fatalf("refused attempt expired before its report: %s %q", before.State, before.Reason)
				}
				if lateReport > 0 && !clock.Now().After(*before.Spec.StartBy) {
					t.Fatal("late report did not land past the cutoff in force")
				}
				r := result(child, false)
				r.Interrupted = true
				r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "admission_changed", Predicate: "launch_authorization"}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, child.ID)
				want := clock.Now().Add(before.Spec.StartBy.Sub(*before.AssignedAt))
				if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Spec.StartBy == nil || !got.Spec.StartBy.Equal(want) {
					t.Fatalf("refusal consumed the caller cutoff: state=%s losses=%d cutoff=%v want=%v", got.State, got.Losses, got.Spec.StartBy, want)
				}
			}
			var child *Job
			for i := range 3 {
				setLeaving(false)
				child = check(t, q, m, nil).Job
				if child == nil || child.Batch == nil || child.Batch.Parent != consumer.ID {
					t.Fatalf("refusal %d: consumer wave not assigned: %+v", i, child)
				}
				if !absoluteStartBy(child) {
					t.Fatal("child lost the caller's explicit cutoff")
				}
				late := time.Duration(0)
				if i == 2 {
					late = 90 * time.Second // the report lands past the current cutoff
				}
				refuse(child, late)
			}
			if clock.Now().Before(cutoff.Add(2 * time.Minute)) {
				t.Fatal("scenario did not run past the original cutoff")
			}
			if phase == "queue-wait-still-counts" {
				remaining := job(t, q, child.ID).Spec.StartBy.Sub(clock.Now())
				clock.Advance(remaining - time.Second)
				if err := q.update(func() error { q.maintainStartDeadlines(); return nil }); err != nil {
					t.Fatal(err)
				}
				if got := job(t, q, child.ID); got.State != "queued" {
					t.Fatalf("restored remainder cut short: %s %q", got.State, got.Reason)
				}
				clock.Advance(2 * time.Second)
				if err := q.update(func() error { q.maintainStartDeadlines(); return nil }); err != nil {
					t.Fatal(err)
				}
				if got := job(t, q, child.ID); got.State != "failed" || !strings.Contains(got.Reason, "production start deadline exceeded") {
					t.Fatalf("queue wait no longer bounded by the cutoff: %s %q", got.State, got.Reason)
				}
				return
			}
			setLeaving(false)
			next := check(t, q, m, nil).Job
			if next == nil || next.ID != child.ID || next.Attempt <= child.Attempt {
				t.Fatalf("retry not assigned with a fresh attempt: %+v", next)
			}
			if err := q.Complete(result(child, true)); !errors.Is(err, ErrStale) {
				t.Fatal("stale refused attempt accepted", err)
			}
			if err := q.Started(result(next, false)); err != nil {
				t.Fatal(err)
			}
			finish(t, q, next, true)
			if got := job(t, q, child.ID); got.State != "succeeded" {
				t.Fatalf("retry did not succeed: %s %q", got.State, got.Reason)
			}
		})
	}
}
