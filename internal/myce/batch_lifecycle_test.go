package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBatchLifecycleInfrastructureProgressSurvivesLosses(t *testing.T) {
	for _, count := range []int{1, 64} {
		for _, priority := range []string{"production", "experiment"} {
			t.Run(fmt.Sprintf("%s/%d", priority, count), func(t *testing.T) {
				q, clock := fixture(t)
				for i := 1; i < count; i++ {
					check(t, q, machine(fmt.Sprint(i), "fake"), nil)
				}
				s := spec(priority)
				s.SubmissionKey = "progressing"
				s.AllowedMachines = []string{"worker"}
				j := submit(t, q, s)
				m := machine("worker", "fake")
				m.CapabilityProfile.LogicalCPUs = count * 2
				m.CapabilityProfile.SystemMemoryBytes = uint64(count*8) << 30
				m.TotalMemoryGB, m.FreeMemoryGB = float64(count*8), float64(count*8)
				m.CapabilityProfile.DiskTotalBytes = uint64(count) << 40
				m.CapabilityProfile.DiskFreeBytes = uint64(count) << 39
				for attempt := 1; attempt <= 8; attempt++ {
					a := check(t, q, m, nil).Job
					if a == nil || a.ID != j.ID || a.Attempt != attempt {
						t.Fatalf("assignment %d: %+v", attempt, a)
					}
					if err := q.Started(result(a, false)); err != nil {
						t.Fatal(err)
					}
					clock.Advance(time.Second)
					dir := attemptCheckpoint(*a)
					if !filepath.IsAbs(dir) {
						dir = filepath.Join(q.root, dir)
					}
					if err := os.MkdirAll(dir, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte(fmt.Sprintf(`{"myce_checkpoint":{"generation":%d}}`, attempt)), 0600); err != nil {
						t.Fatal(err)
					}
					if _, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: a.ID, Attempt: a.Attempt, Progress: int64(attempt)}}); err != nil {
						t.Fatal(err)
					}
					if got := job(t, q, j.ID); got.Progress != int64(attempt) {
						t.Fatal("progress not accepted", got.Progress)
					}
					r := result(a, false)
					r.Interrupted = true
					r.ExecutionCause = &RuntimeCause{Operation: "reconcile", Class: "io_error"}
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					got := job(t, q, j.ID)
					if got.State != "queued" || got.Spec.SubmissionKey != s.SubmissionKey {
						t.Fatal("progressing infrastructure loss exhausted job", got.State, got.Reason)
					}
					if priority == "production" && got.InfrastructureRetries != attempt {
						t.Fatal("lost durable retry count", got.InfrastructureRetries)
					}
					if !errors.Is(q.Complete(result(a, true)), ErrStale) {
						t.Fatal("stale result accepted")
					}
					if attempt == 4 {
						q = reopenCancellationQueue(t, q, clock)
					}
				}
				a := check(t, q, m, nil).Job
				// Real command failures stop, irrespective of prior useful progress.
				r := result(a, false)
				r.Error = "exit status 7"
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, j.ID)
				if got.State != "failed" || !strings.HasPrefix(got.Reason, "command failed:") {
					t.Fatal(got.State, got.Reason)
				}
			})
		}
	}
}

func TestBatchLifecycleStartUsesSubmittedDeadline(t *testing.T) {
	for _, latency := range []time.Duration{time.Millisecond, 30 * time.Minute} {
		t.Run(latency.String(), func(t *testing.T) {
			q, clock := fixture(t)
			s := spec("production")
			deadline := clock.Now().Add(time.Hour)
			s.Deadline = &deadline
			j := submit(t, q, s)
			if j.Spec.StartBy == nil || !j.Spec.StartBy.Equal(deadline) {
				t.Fatal("did not use submitted deadline")
			}
			m := machine("worker", "fake")
			a := check(t, q, m, nil).Job
			clock.Advance(latency)
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			r := result(a, false)
			r.Interrupted = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); got.Spec.StartBy == nil || !got.Spec.StartBy.Equal(deadline) {
				t.Fatal("recovery moved submitted deadline")
			}
			q = reopenCancellationQueue(t, q, clock)
			clock.Advance(deadline.Sub(clock.Now()))
			if got := job(t, q, j.ID); got.State != "failed" {
				t.Fatal("deadline did not bind", got.State)
			}
		})
	}
}

func TestBatchLifecycleMeasuredStopAndCustody(t *testing.T) {
	for _, cadence := range []time.Duration{time.Millisecond, 10 * time.Minute} {
		t.Run(cadence.String(), func(t *testing.T) {
			q, clock := fixture(t)
			s := spec("experiment")
			j := submit(t, q, s)
			m := machine("worker", "fake")
			a := check(t, q, m, nil).Job
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			if err := q.update(func() error {
				clock.Advance(cadence)
				q.observeBatchProgress(q.touchJob(j.ID), 1)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			q = reopenCancellationQueue(t, q, clock)
			if err := q.update(func() error { q.stop(q.touchJob(j.ID), "test stop"); q.retainRelease(q.s.Jobs[j.ID]); return nil }); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.StopAt == nil || got.StopAt.Sub(clock.Now()) != cadence {
				t.Fatal("stop ignored measured cadence", got.StopAt)
			}
			clock.Advance(cadence)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); got.ReleaseConfirmed {
				t.Fatal("elapsed cleanup proved release")
			}
			if len(q.s.HostReleases) == 0 {
				t.Fatal("custody disappeared")
			}
			stale := result(a, false)
			stale.ReleaseOnly = true
			stale.Session += "-foreign"
			if err := q.Complete(stale); err != nil {
				t.Fatal(err)
			}
			if len(q.s.HostReleases) == 0 {
				t.Fatal("foreign release discharged custody")
			}
		})
	}
}

func TestBatchLifecycleUnchangedInfrastructureWaitsForEvidence(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		t.Run(priority, func(t *testing.T) {
			q, clock := fixture(t)
			j := submit(t, q, spec(priority))
			m := machine("worker", "fake")
			a := check(t, q, m, nil).Job
			r := result(a, false)
			r.Interrupted = true
			r.ExecutionCause = &RuntimeCause{Operation: "input_staging", Class: "permission_denied", Staging: &StagingCause{Operation: "publish", Error: "EACCES"}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			q = reopenCancellationQueue(t, q, clock)
			for range 4 {
				clock.Advance(time.Hour)
				m.FreeMemoryGB++ // routine pressure noise is not a repaired permission.
				if next := check(t, q, m, nil).Job; next != nil {
					t.Fatal("retried unchanged failure", next.ID)
				}
			}
			got := job(t, q, j.ID)
			if got.State != "queued" || !strings.Contains(got.Reason, "changed worker") {
				t.Fatal(got.State, got.Reason)
			}
			previous := m.Session
			m.Session += "-repaired"
			reply, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: previous})
			if err != nil {
				t.Fatal(err)
			}
			if next := reply.Job; next == nil || next.ID != j.ID {
				t.Fatal("new worker evidence did not permit recovery")
			}
		})
	}
}

func TestBatchLifecycleExpiredFanOutDoesNotReclaim(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 1)
	s.Priority = "production"
	deadline := clock.Now().Add(time.Millisecond)
	s.Deadline = &deadline
	parent := submit(t, q, s)
	// Expire before the next control transaction, then offer lower tier work.
	clock.Advance(time.Millisecond)
	q = reopenCancellationQueue(t, q, clock)
	lower := submit(t, q, cpuSpec(1, 1))
	next := check(t, q, m, nil).Job
	if next == nil || next.ID != lower.ID {
		t.Fatal("expired fan-out reclaimed capacity", next)
	}
	got := job(t, q, parent.ID)
	if got.State != "failed" || got.FanOut.Next != 0 || len(got.FanOut.Children) != 0 {
		t.Fatal("expired parent materialized children", got.State, got.FanOut)
	}
	if job(t, q, lower.ID).State != "running" {
		t.Fatal("expired parent preempted lower tier")
	}
}

func TestBatchLifecycleColdRecoveryAcceptsForce(t *testing.T) {
	dir, identity := t.TempDir(), ID()
	ready := filepath.Join(dir, "ready")
	cmd := exec.Command("sh", "-c", `trap '' TERM; touch "$1"; while :; do sleep .01; done`, "fixture", ready)
	cmd.Env = append(os.Environ(), "MYCE_PROCESS_ID="+identity)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() }()
	nativeFile(t, dir, "ready")
	clock := &manualWorkerClock{now: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}
	deadline := clock.Now().Add(unmeasuredBatchGrace)
	j := recoveryJob(t, []string{"true"})
	j.Spec.Resumable = false // test cleanup rather than checkpoint transport.
	j.Session = "session"
	birth, err := processBirth(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	st := workerState{Identity: identity, PID: cmd.Process.Pid, Session: j.Session, Job: &j, StopAt: &deadline, Groups: map[int]map[int]string{cmd.Process.Pid: {cmd.Process.Pid: birth}}}
	if err := saveWorkerJSON(dir, "attempt-"+identity+".json", st); err != nil {
		t.Fatal(err)
	}
	s := &workerStore{dir: dir, state: st}
	w := recoveryWorker(t)
	w.Clock = clock
	w.control = &processControl{clock: clock}
	recovered := make(chan error, 1)
	go func() { _, err := w.recoverAttempt(s); recovered <- err }()
	select {
	case err := <-recovered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cold recovery blocked control startup")
	}
	if s.state.Result == nil || !s.state.Result.ReleasePending {
		t.Fatal("cold recovery lost custody")
	}
	a := &attemptSupervisor{store: s, job: j, worker: w, report: AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session}, wake: make(chan struct{}, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	go a.run(ctx)
	defer func() { cancel(); <-a.done }()
	a.update(w, AttemptDirective{Stop: true, Kill: true}, true, false)
	await(t, func() bool {
		_, _, finished, err := a.snapshot()
		a.mu.Lock()
		released := a.released
		a.mu.Unlock()
		return released || finished && err == nil
	})
	members, err := workerGroupMembers(cmd.Process.Pid)
	if err != nil || len(members) != 0 {
		t.Fatal("force failed to release recovered group", members, err)
	}
}

func TestBatchLifecyclePlacementEstimateReachesReceipt(t *testing.T) {
	q, clock := fixture(t)
	s := spec("production")
	first := submit(t, q, s)
	m := machine("worker", "fake")
	a := check(t, q, m, nil).Job
	clock.Advance(20 * time.Minute)
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	finish(t, q, a, true)
	if job(t, q, first.ID).State != "succeeded" {
		t.Fatal("sample did not finish")
	}
	next := submit(t, q, s)
	got := job(t, q, next.ID)
	if got.Spec.StartBy != nil || !strings.Contains(got.Reason, "observed placement latency 1200.000s") {
		t.Fatal("receipt lost advisory evidence", got.Reason)
	}
}
