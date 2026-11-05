package myce

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
