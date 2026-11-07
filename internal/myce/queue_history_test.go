package myce

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// #224: retained answers must not serialize heartbeats behind whole-history
// snapshots. Use real SQLite commits, check-ins and concurrent keyed submits.
func TestQueueHistoryBurst(t *testing.T) {
	var baseline queueHistoryBurstTiming
	for _, count := range []int{1000, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			// A directly selected 10k subtest must measure its own 1k baseline.
			if count == 10000 && baseline == (queueHistoryBurstTiming{}) {
				baseline = measureQueueHistoryBurst(t, 1000)
			}
			got := measureQueueHistoryBurst(t, count)
			if count == 1000 {
				baseline = got
				return
			}
			// Ten times the retained history must not cause near-linear lock
			// contention. Allow 3x for host noise and the small live workload;
			// all budgets come from this run, never a host's wall-clock speed.
			t.Logf("10k/1k ratios: burst_wait=%.2f burst_hold=%.2f launch_wait=%.2f launch_hold=%.2f",
				float64(got.burstWait)/float64(baseline.burstWait),
				float64(got.burstHold)/float64(baseline.burstHold),
				float64(got.launchWait)/float64(baseline.launchWait),
				float64(got.launchHold)/float64(baseline.launchHold))
			// Under CPU load the baseline goroutines can run serially, with
			// virtually no wait. Wait growth alone then says nothing about
			// contention; require proportional occupied work in both bursts.
			if got.burstWait > 3*baseline.burstWait && got.burstHold > 3*baseline.burstHold {
				t.Errorf("10k-history burst p99 lock wait %s and total hold %s exceeded 3x 1k baselines %s and %s",
					got.burstWait, got.burstHold, baseline.burstWait, baseline.burstHold)
			}
			// A long waiter alone can be scheduler delay after unlock. Require
			// growth in actual occupied lock time too, including pre-update
			// Checkin locks, I/O and preemption while holding q.mu.
			if got.launchWait > 3*baseline.launchWait && got.launchHold > 3*baseline.launchHold {
				t.Errorf("10k-history launch p99 lock wait %s and total hold %s exceeded 3x 1k baselines %s and %s",
					got.launchWait, got.launchHold, baseline.launchWait, baseline.launchHold)
			}
		})
	}
}

type queueHistoryBurstTiming struct {
	burstWait, burstHold, launchWait, launchHold time.Duration
}

func measureQueueHistoryBurst(t *testing.T, count int) queueHistoryBurstTiming {
	t.Helper()
	// A median of three independent fixtures suppresses a single scheduler or
	// storage outlier without retrying failed assertions or dropping samples.
	var waits, burstHolds, launchWaits, launchHolds []time.Duration
	for sample := range 3 {
		// A subtest ends each fixture's SQLite/queue lifetime before the next
		// sample. Keep all three independent histories and every observation.
		if !t.Run(fmt.Sprintf("sample-%d", sample), func(t *testing.T) {
			got := queueHistoryBurst(t, count)
			waits = append(waits, got.burstWait)
			burstHolds = append(burstHolds, got.burstHold)
			launchWaits = append(launchWaits, got.launchWait)
			launchHolds = append(launchHolds, got.launchHold)
		}) {
			t.FailNow()
		}
	}
	slices.Sort(waits)
	slices.Sort(burstHolds)
	slices.Sort(launchWaits)
	slices.Sort(launchHolds)
	return queueHistoryBurstTiming{waits[1], burstHolds[1], launchWaits[1], launchHolds[1]}
}

func queueHistoryBurst(t *testing.T, count int) queueHistoryBurstTiming {
	t.Helper()
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
	var burstHoldTotal time.Duration
	q.mu.observeHold = func(hold time.Duration) { burstHoldTotal += hold }
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
	q.mu.observeHold = nil
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	slices.Sort(waits)
	slices.Sort(holds)
	percentile := func(ds []time.Duration, p int) time.Duration { return ds[(len(ds)-1)*p/100] }
	t.Logf("history=%d update_mean=%s wait_p50=%s wait_p95=%s wait_p99=%s hold_p50=%s hold_p95=%s hold_p99=%s", count, update, percentile(waits, 50), percentile(waits, 95), percentile(waits, 99), percentile(holds, 50), percentile(holds, 95), percentile(holds, 99))
	t.Logf("history=%d burst_hold_total=%s", count, burstHoldTotal)
	burstP99 := percentile(waits, 99)
	for _, m := range workers {
		if q.s.Machines[m.ID].Gone {
			t.Errorf("reporting worker %s judged lost", m.ID)
		}
	}

	// The same backlog also delayed start authorization in #223. Exercise the
	// real launch fence concurrently with current-attempt heartbeats.
	sp := spec("production")
	sp.AllowedMachines = []string{workers[0].ID}
	submitted := submit(t, q, sp)
	active := check(t, q, workers[0], nil).Job
	if active == nil || active.ID != submitted.ID {
		t.Fatal("burst launch fixture was not assigned")
	}
	if err := q.Started(result(active, true)); err != nil {
		t.Fatal(err)
	}
	waits = nil
	holds = nil
	q.observeUpdate = func(wait, hold time.Duration) { waits = append(waits, wait); holds = append(holds, hold) }
	// Observe q.mu itself: Checkin also locks before updateRequest. Counting
	// only update holds would conceal a regression in that earlier section.
	var launchHoldTotal time.Duration
	q.mu.observeHold = func(hold time.Duration) { launchHoldTotal += hold }
	start = make(chan struct{})
	errs = make(chan error, 16)
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				errs <- q.AuthorizeLaunch(HostLaunch{Kind: "batch", Key: active.ID, Machine: active.Machine, Session: active.Session, Attempt: strconv.Itoa(active.Attempt)})
			} else {
				m := workers[0]
				m.FreeMemoryGB = 0
				_, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: active.ID, Attempt: active.Attempt}})
				errs <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	q.observeUpdate = nil
	q.mu.observeHold = nil
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	slices.Sort(waits)
	slices.Sort(holds)
	t.Logf("history=%d launch_burst_wait_p50=%s launch_burst_wait_p95=%s launch_burst_wait_p99=%s launch_burst_update_hold_p99=%s", count, percentile(waits, 50), percentile(waits, 95), percentile(waits, 99), percentile(holds, 99))
	t.Logf("history=%d launch_burst_hold_total=%s", count, launchHoldTotal)
	return queueHistoryBurstTiming{burstP99, burstHoldTotal, percentile(waits, 99), launchHoldTotal}
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
