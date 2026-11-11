package myce

import (
	"bytes"
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
				// A retained copy is still stopping on the same host. Production
				// waits for its release; the experiment's host withdraws admission.
				release := HostRelease{Host: hostID(&m), Job: "victim", Machine: m.ID, Session: m.Session, Attempt: 1, Until: clock.Now().Add(10 * time.Second)}
				if err := q.update(func() error {
					q.s.Hosts = map[string]*HostState{hostID(&m): {Policy: HostPolicy{ProductionHold: priority == "production"}, Active: true}}
					q.s.HostReleases = map[string]HostRelease{}
					if priority == "experiment" {
						q.touchMachine(m.ID).Leaving = true
					}
					q.s.HostReleases[releaseKey(release)] = release
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				var logs bytes.Buffer
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
				predicate := "placement_policy"
				if priority == "experiment" {
					predicate = "host_launch"
				}
				if !strings.Contains(logs.String(), "predicate="+predicate) {
					t.Errorf("refusal omitted predicate: %s", logs.String())
				}
				clock.Advance(420 * time.Millisecond)
				if err := q.update(func() error {
					if !q.settleRelease(Result{Execution: Execution{ID: release.Job, Attempt: release.Attempt}, Machine: release.Machine, Session: release.Session}, true) {
						t.Fatal("blocking release was not retained")
					}
					q.s.Hosts[hostID(&m)].Active = false
					q.touchMachine(m.ID).Leaving = false
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
