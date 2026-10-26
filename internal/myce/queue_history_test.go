package myce

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// #224: retained answers must not serialize heartbeats behind whole-history
// snapshots. Use real SQLite commits, check-ins and concurrent keyed submits.
func TestQueueHistoryBurst(t *testing.T) {
	for _, count := range []int{1000, 5000, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			q, c := fixture(t)
			// Approximate the incident's 4.6 MB jobs + 4.7 MB keyed answers per 1k.
			q.mu.Lock()
			q.s.Submissions = map[string]*submissionRecord{}
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("history-%06d", i)
				s := spec("experiment")
				s.SubmissionKey = id
				s.Run.Command = []string{"echo", strings.Repeat("x", 3000)}
				j := &Job{ID: id, Spec: s, State: "succeeded", Queued: c.Now(), FinishedAt: &c.now, ReleaseConfirmed: true}
				q.s.Jobs[id] = j
				q.s.Submissions[submissionDigest(s.Project, id)] = &submissionRecord{Kind: submissionBinding, Project: s.Project, Key: id, JobID: id, Job: j, Created: &c.now}
			}
			q.mu.Unlock()
			// Fixtures seed history once, just like startup loading the existing DB.
			if err := q.update(func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			workers := make([]Machine, 8)
			for i := range workers {
				workers[i] = machine(fmt.Sprintf("worker-%d", i), "fake")
				check(t, q, workers[i], nil)
			}
			var waits, holds []time.Duration
			q.observeUpdate = func(wait, hold time.Duration) { waits = append(waits, wait); holds = append(holds, hold) }
			for range 12 {
				if err := q.update(func() error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			var update time.Duration
			for _, d := range holds {
				update += d
			}
			update /= time.Duration(len(holds))
			waits = nil
			holds = nil
			start := make(chan struct{})
			errs := make(chan error, 40)
			var wg sync.WaitGroup
			for i := range 40 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					if i%5 == 0 {
						s := spec("experiment")
						s.SubmissionKey = fmt.Sprintf("burst-%d", i)
						s.Needs.MemoryGB = 100
						_, err := q.Submit(s)
						errs <- err
					} else {
						_, err := q.Checkin(Checkin{Machine: workers[i%len(workers)]})
						errs <- err
					}
				}(i)
			}
			close(start)
			wg.Wait()
			close(errs)
			q.observeUpdate = nil
			for err := range errs {
				if err != nil {
					t.Error(err)
				}
			}
			slices.Sort(waits)
			slices.Sort(holds)
			percentile := func(ds []time.Duration, p int) time.Duration { return ds[(len(ds)-1)*p/100] }
			t.Logf("history=%d update_mean=%s wait_p50=%s wait_p95=%s wait_p99=%s hold_p50=%s hold_p95=%s hold_p99=%s", count, update, percentile(waits, 50), percentile(waits, 95), percentile(waits, 99), percentile(holds, 50), percentile(holds, 95), percentile(holds, 99))
			if os.Getenv("MYCE_MEASURE_BASELINE") == "" && percentile(waits, 99) > 100*time.Millisecond {
				t.Errorf("burst p99 lock wait exceeded 100ms: %s", percentile(waits, 99))
			}
			for _, m := range workers {
				if q.s.Machines[m.ID].Gone {
					t.Errorf("reporting worker %s judged lost", m.ID)
				}
			}
		})
	}
}

// Cold terminal records still accept durable exclusion and exact-attempt release;
// a failed SQLite write must restore both the job and its keyed retained answer.
func TestRetainedAnswerTransactionRollback(t *testing.T) {
	q, c := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "retained-rollback"
	j := submit(t, q, s)
	assigned := check(t, q, machine("worker", "fake"), nil).Job
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	finish(t, q, assigned, true)
	before := cloneJob(q.s.Jobs[j.ID])
	if _, err := q.db.Exec(`CREATE TRIGGER reject_retained BEFORE INSERT ON queue_state BEGIN SELECT RAISE(ABORT,'write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := q.CancelJob(j.ID); err == nil {
		t.Fatal("failed write acknowledged cancellation")
	}
	if q.s.Jobs[j.ID].CancelledAt != nil || q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)].Job.CancelledAt != nil {
		t.Fatal("failed cold cancellation escaped rollback")
	}
	if _, err := q.db.Exec(`DROP TRIGGER reject_retained`); err != nil {
		t.Fatal(err)
	}
	if _, err := q.CancelJob(j.ID); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, c)
	got := submit(t, q, s)
	if got.ID != j.ID || got.State != "succeeded" || got.CancelledAt == nil || got.Attempt != before.Attempt {
		t.Fatal("cold answer lost state/attempt/exclusion on restart")
	}
	stale := result(assigned, true)
	stale.Attempt++
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatalf("stale terminal payload accepted: %v", err)
	}
	if got := job(t, q, j.ID); got.Attempt != before.Attempt || got.State != "succeeded" {
		t.Fatal("stale result changed retained answer")
	}
}
