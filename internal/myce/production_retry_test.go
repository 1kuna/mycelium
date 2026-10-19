package myce

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestProductionInfrastructureRetryLimit(t *testing.T) {
	for _, mode := range []string{"interrupted", "lost", "worker-restart", "queue-restart", "host-close", "leave", "operator", "polite"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			m.HostID = "host"
			s := spec("production")
			s.SubmissionKey = "same-stage"
			s.Resumable = false
			submitted := submit(t, q, s)
			for attempt := 1; attempt <= 3; attempt++ {
				running := check(t, q, m, nil).Job
				if running == nil || running.ID != submitted.ID || running.Attempt != attempt || running.Spec.SubmissionKey != s.SubmissionKey {
					t.Fatalf("attempt %d: %+v", attempt, running)
				}
				if err := q.Started(result(running, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
				switch mode {
				case "lost":
					clock.Advance(ProductionLossAfter)
					check(t, q, observer(), nil)
				case "queue-restart":
					// Reopen the real persisted queue, then expire the unreported attempt.
					path, root := "", q.root
					if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
						t.Fatal(err)
					}
					if err := q.Close(); err != nil {
						t.Fatal(err)
					}
					var err error
					q, err = Open(path, root, clock)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { q.Close() })
					q.checkpointProbe = func() bool { return true }
					q.SetRestartGrace(time.Second)
					clock.Advance(time.Second)
					if err := q.Tick(); err != nil {
						t.Fatal(err)
					}
				case "worker-restart":
					old := m.Session
					m.Session = fmt.Sprintf("session-%d", attempt)
					if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: old, Running: &Execution{ID: running.ID, Attempt: running.Attempt}}); err != nil {
						t.Fatal(err)
					}
				case "host-close":
					request := closeExchange(m.HostID, clock)
					request.HostRevision.Revision = uint64(attempt*2 - 1)
					request.Sessions = []HostSession{{Machine: m.ID, Session: m.Session}}
					ack, err := q.ReconcileHost(request)
					if err != nil || len(ack.Pending) != 0 {
						t.Fatal(ack, err)
					}
					// Release confirmation must be idempotent and cannot consume another retry.
					if _, err := q.ReconcileHost(request); err != nil {
						t.Fatal(err)
					}
					if attempt < 3 {
						request.LastClose = &HostClose{HostRevision: request.HostRevision, Settlement: "confirmed"}
						request.HostRevision = HostRevision{Revision: uint64(attempt * 2), Operation: ID()}
						request.Desired, request.Sessions = "open", nil
						if _, err := q.ReconcileHost(request); err != nil {
							t.Fatal(err)
						}
					}
				default:
					if mode == "leave" {
						if err := q.Leave(m.ID, true); err != nil {
							t.Fatal(err)
						}
					}
					r := result(running, false)
					r.Interrupted, r.OperatorStop, r.PoliteStop = true, mode == "operator", mode == "polite"
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					// A leaving worker is replaced by a new session before its next pull.
					if mode == "leave" {
						m.ID, m.Session = fmt.Sprintf("leave-%d", attempt), fmt.Sprintf("leave-%d", attempt)
					}
				}
				got := job(t, q, submitted.ID)
				want := "queued"
				if attempt == 3 {
					want = "failed"
				}
				if got.InfrastructureRetries != min(attempt, 2) || got.State != want || attempt == 3 && got.Reason != "infrastructure_retries_exhausted" {
					t.Fatalf("after interruption %d: state=%s reason=%s, want %s", attempt, got.State, got.Reason, want)
				}
				if attempt == 3 && (got.FinishedAt == nil || !got.FinishedAt.Equal(clock.Now())) {
					t.Fatal("missing terminal time", got)
				}
				if attempt < 3 {
					if err := q.Complete(result(running, true)); !errors.Is(err, ErrStale) {
						t.Fatal("late result accepted", err)
					}
				}
				replay := submit(t, q, s)
				if replay.ID != submitted.ID || replay.Attempt != got.Attempt || replay.State != got.State {
					t.Fatal("key replay changed job", replay)
				}
			}
			if got := check(t, q, m, nil).Job; got != nil {
				t.Fatal("fourth attempt assigned", got)
			}
		})
	}
}

func TestProductionStageFailureDoesNotRetry(t *testing.T) {
	for _, mode := range []string{"exit", "oom", "container", "oom-interrupted", "closed-oom", "closed-exit"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			m.HostID = "host"
			submit(t, q, spec("production"))
			running := check(t, q, m, nil).Job
			if err := q.Started(result(running, false)); err != nil {
				t.Fatal(err)
			}
			if mode == "closed-oom" || mode == "closed-exit" {
				if _, err := q.ReconcileHost(closeExchange(m.HostID, clock)); err != nil {
					t.Fatal(err)
				}
			}
			r := result(running, false)
			r.Error = "exit status 7"
			if mode == "container" {
				r.Error = "container command failed"
			}
			r.OutOfMemory = mode == "oom" || mode == "oom-interrupted" || mode == "closed-oom"
			r.Interrupted = mode == "oom-interrupted"
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, running.ID)
			if got.State != "failed" || got.Attempt != 1 || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatal(got)
			}
			if got := check(t, q, m, nil).Job; got != nil {
				t.Fatal("stage failure retried", got)
			}
		})
	}
}

func TestProductionCancelPendingPreventsRetry(t *testing.T) {
	for _, hostClose := range []bool{false, true} {
		t.Run(stringBool(hostClose), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			m.HostID = "host"
			submit(t, q, spec("production"))
			running := check(t, q, m, nil).Job
			ack := cancelAck(t, q, running.ID)
			if !ack.WillNotRun || ackRelease(ack) != "pending" {
				t.Fatal(ack)
			}
			clock.Advance(time.Second)
			if hostClose {
				request := closeExchange(m.HostID, clock)
				request.Sessions = []HostSession{{Machine: m.ID, Session: m.Session}}
				if _, err := q.ReconcileHost(request); err != nil {
					t.Fatal(err)
				}
			} else {
				r := result(running, false)
				r.Interrupted = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			assertNoCancellationAttempt(t, q, running)
			if job(t, q, running.ID).InfrastructureRetries != 0 {
				t.Fatal("cancel consumed retry")
			}
		})
	}
}

func TestProductionInfrastructureRetryConfiguration(t *testing.T) {
	for _, limit := range []int{0, 1, 4} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.SetProductionInfrastructureRetries(-1); err == nil {
				t.Fatal("negative limit accepted")
			}
			if err := q.SetProductionInfrastructureRetries(limit); err != nil {
				t.Fatal(err)
			}
			limit = min(limit, 2)
			submit(t, q, spec("production"))
			m := machine("one", "fake")
			for i := 0; i <= limit; i++ {
				running := check(t, q, m, nil).Job
				if running == nil {
					t.Fatal("retry stopped before configured limit")
				}
				if err := q.Started(result(running, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
				r := result(running, false)
				r.Interrupted = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, running.ID)
				if got.InfrastructureRetries != min(i+1, limit) {
					t.Fatal("incorrect durable retry count", got.InfrastructureRetries)
				}
				if i == limit && (got.State != "failed" || got.Reason != "infrastructure_retries_exhausted") {
					t.Fatal(got)
				}
			}
			snap, err := q.publicSnapshot()
			if err != nil || len(snap.Jobs) != 1 || snap.Jobs[0].Reason != "infrastructure_retries_exhausted" {
				t.Fatal("status hid exhaustion reason", err)
			}
		})
	}
}

func TestProductionRetryRejectsOldProgressAndCompletesOnce(t *testing.T) {
	q, clock := fixture(t)
	m := machine("one", "fake")
	s := spec("production")
	s.SubmissionKey = "stable-stage"
	submit(t, q, s)
	old := check(t, q, m, nil).Job
	if err := q.Started(result(old, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(ProductionLossAfter)
	check(t, q, observer(), nil)
	// Unlike experiment recovery, a superseded production execution cannot win
	// back ownership by returning before reassignment or with newer progress.
	if reply := check(t, q, m, old); !reply.Kill {
		t.Fatal("old attempt recovered ownership", reply)
	}
	current := check(t, q, m, nil).Job
	if current == nil || current.ID != old.ID || current.Attempt != 2 {
		t.Fatal(current)
	}
	if err := q.Started(result(current, false)); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(result(old, true)); !errors.Is(err, ErrStale) {
		t.Fatal("late old result", err)
	}
	finish(t, q, current, true)
	finish(t, q, current, true) // delivery retry is idempotent
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("completed job ran again", next)
	}
	got := job(t, q, current.ID)
	if got.State != "succeeded" || got.Attempt != 2 || got.InfrastructureRetries != 1 || submit(t, q, s).ID != got.ID {
		t.Fatal(got)
	}
}

func TestProductionExtensionOOMDoesNotRetry(t *testing.T) {
	q, _ := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("small", "separate", 64, 24, e)
	check(t, q, m, nil)
	s := placementSpec(true)
	s.Priority = "production"
	submit(t, q, s)
	running := check(t, q, m, nil).Job
	if running == nil {
		t.Fatal("extension not assigned")
	}
	r := result(running, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, running.ID)
	if got.State != "failed" || got.InfrastructureRetries != 0 || got.OOMRetries != 0 {
		t.Fatal(got)
	}
}
