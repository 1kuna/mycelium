package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Real helpers are ready, Started is authorized, then the final main exec gate
// receives an actual queue 409. Authorization is not payload execution proof.
func TestStartAuthorizationRefusalAfterStarted(t *testing.T) {
	q, clock := fixture(t)
	s := nativeSpec(t)
	s.Priority, s.SubmissionKey = "production", "start-refusal"
	s.Run.Env["NATIVE_TEST_BLOCK"] = "0"
	deadline := clock.Now().Add(5 * time.Minute)
	s.StartBy = &deadline
	m := machine("worker", "fake", "host")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	submit(t, q, s)
	a := check(t, q, m, nil).Job
	handler := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(rw, r)
		if r.URL.Path == "/api/workers/started" {
			if err := q.update(func() error { q.s.Machines[m.ID].Leaving = true; return nil }); err != nil {
				t.Error(err)
			}
		}
	}))
	defer server.Close()
	w := recoveryWorker(t)
	w.Clock, w.Machine, w.Client = clock, m, Client{URL: server.URL, Token: "test"}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.nativeListenerProbe = func(string, int) error { return nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = m.Session
	p, err := startAfterFixtureInputs(t, w, store, *a)
	if err != nil || p == nil {
		t.Fatal("native start", err)
	}
	var r Result
	select {
	case f := <-p.done:
		if f.err != nil {
			t.Fatal(f.err)
		}
		r = f.result
	case <-time.After(testWaitTimeout):
		p.stop(clock.Now(), 0, true)
		t.Fatal("native refusal did not finish")
	}
	c := r.ExecutionCause
	if c == nil || c.Operation != "start_authorization" || c.Class != "admission_changed" || c.Predicate != "launch_authorization" || r.Success || !r.Interrupted {
		t.Fatalf("wrong refusal: %+v", r)
	}
	if e := r.ExecutionEvidence; e == nil || e.Attempt != a.Attempt || e.Main != executionNotStarted || e.Sidecars != executionMayHaveRun {
		t.Fatalf("wrong boundaries: %+v", e)
	}
	if job(t, q, a.ID).StartedAt == nil {
		t.Fatal("Started authorization race not exercised")
	}
	if _, err := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "main.json")); !os.IsNotExist(err) {
		t.Fatal("refused main executed", err)
	}
	if _, err := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "side.json")); err != nil {
		t.Fatal("helper did not execute", err)
	}
	nativeNoSurvivors(t, &nativeTestRun{p: p, dir: p.job.Spec.Run.Workdir})
	clock.Advance(time.Second)
	r.ReleasePending = true // Delay the final exact release acknowledgement.
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, a.ID)
	if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || !got.Spec.StartBy.Equal(deadline) {
		t.Fatalf("refusal charged or caller cutoff moved: %+v", got)
	}
	if err := q.update(func() error { q.s.Machines[m.ID].Leaving = false; return nil }); err != nil {
		t.Fatal(err)
	}
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("unreleased helper custody reused", next)
	}
	release := r
	release.ReleaseOnly, release.ReleasePending = true, false
	stale := release
	stale.Attempt++
	_ = q.Complete(stale) // An idempotent stale acknowledgement grants no release.
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("wrong attempt release cleared custody", next)
	}
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	next := check(t, q, m, nil).Job
	if next == nil || next.ID != a.ID || next.Attempt != a.Attempt+1 || next.Spec.SubmissionKey != s.SubmissionKey {
		t.Fatal("logical retry lost", next)
	}
}

// Positive proof must belong to this exact attempt. Unknown execution and real
// failures retain normal accounting even when a start refusal was also recorded.
func TestStartAuthorizationRefusalAccountingGuards(t *testing.T) {
	for _, name := range []string{"unknown", "may-have-run", "missing", "wrong-attempt", "timeout", "payload-failure", "no-refusal"} {
		t.Run(name, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("worker", "fake")
			submit(t, q, spec("production"))
			a := check(t, q, m, nil).Job
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			if name != "no-refusal" {
				if err := q.update(func() error { q.s.Machines[m.ID].Leaving = true; return nil }); err != nil {
					t.Fatal(err)
				}
				if err := q.AuthorizeLaunch(HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)}); !errors.Is(err, ErrStale) {
					t.Fatal("missing queue refusal", err)
				}
			}
			r := result(a, false)
			r.Interrupted = true
			r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "admission_changed", Predicate: "launch_authorization"}
			r.ExecutionEvidence = unstartedExecution(a.Attempt)
			switch name {
			case "unknown":
				r.ExecutionEvidence.Main = executionUnknown
			case "may-have-run":
				r.ExecutionEvidence.Main = executionMayHaveRun
			case "missing":
				r.ExecutionEvidence = nil
			case "wrong-attempt":
				r.ExecutionEvidence.Attempt++
			case "timeout":
				r.ExecutionCause.Class = "timeout"
			case "payload-failure":
				r.ExecutionEvidence.Main = executionMayHaveRun
				r.Interrupted = false
				r.ExecutionCause = &RuntimeCause{Operation: "command", Class: "stage_failed"}
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, a.ID)
			if name == "payload-failure" {
				if got.State != "failed" {
					t.Fatalf("payload failure forgiven: %+v", got)
				}
			} else if got.Losses != 1 || got.InfrastructureRetries != 1 {
				t.Fatalf("real/unknown execution forgiven: %+v", got)
			}
		})
	}
}

func TestStartedEndpointRecordsAdmissionRefusal(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	submit(t, q, spec("production"))
	a := check(t, q, m, nil).Job
	if err := q.update(func() error { q.s.Machines[m.ID].Leaving = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Started(result(a, false)); !errors.Is(err, ErrStale) {
		t.Fatal("missing Started refusal", err)
	}
	q = reopenQueue(t, q, clock)
	if reason := recordedStartRefusal(q.s.Jobs[a.ID]); reason != "host_launch" {
		t.Fatal("Started refusal not durable", reason)
	}
	if err := q.update(func() error { q.s.Machines[m.ID].Leaving = false; return nil }); err != nil {
		t.Fatal(err)
	}
	r := result(a, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "start_handoff", Class: "admission_changed"}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, a.ID)
	if got.Losses != 0 || got.InfrastructureRetries != 0 || got.State != "queued" {
		t.Fatalf("cleared Started refusal charged: %+v", got)
	}
}

// Relative recovery windows follow current startup policy; only an absolute
// caller cutoff must remain fixed after a never-executed main is requeued.
func TestStartAuthorizationRefusalRestoresRelativeWindow(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	s := spec("production")
	s.StartWindow = "600s"
	submit(t, q, s)
	a := check(t, q, m, nil).Job
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := q.update(func() error { q.s.Machines[m.ID].Leaving = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.AuthorizeLaunch(HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)}); !errors.Is(err, ErrStale) {
		t.Fatal("missing refusal", err)
	}
	r := result(a, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "admission_changed", Predicate: "launch_authorization"}
	r.ExecutionEvidence = unstartedExecution(a.Attempt)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, a.ID)
	if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Spec.StartBy.Sub(clock.Now()) != 600*time.Second {
		t.Fatalf("relative recovery budget lost: state=%s losses=%d retries=%d cutoff=%v", got.State, got.Losses, got.InfrastructureRetries, got.Spec.StartBy)
	}
}
