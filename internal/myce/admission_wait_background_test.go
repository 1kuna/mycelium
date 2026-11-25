package myce

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Stop the background builder before it starts planning. A check-in must still
// return, and no second builder may enter while this one is held here.
type admissionWaitGateClock struct {
	base    Clock
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *admissionWaitGateClock) Now() time.Time {
	c.once.Do(func() { close(c.entered); <-c.release })
	return c.base.Now()
}

func TestAdmissionWaitBackgroundCoalescesAndPaces(t *testing.T) {
	q, clock := fixture(t)
	asynchronousAdmissionWait(t, q)
	m := cpuMachine("worker", "host", "cpu", 1, 16)
	if _, err := q.Checkin(Checkin{Machine: m, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(4, 1)
	s.Priority = "production"
	demand := submit(t, q, s)
	drainAdmissionWait(t, q)
	if err := q.update(func() error { q.touchJob(demand.ID).Spec.Needs.CPUCores++; return nil }); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	capture := q.freezeAdmissionWait()
	q.mu.Unlock()
	gate := &admissionWaitGateClock{base: clock, entered: make(chan struct{}), release: make(chan struct{})}
	capture.clock, capture.service = gate, time.Second
	released := false
	defer func() {
		if !released {
			close(gate.release)
		}
		drainAdmissionWait(t, q)
	}()
	q.enqueueAdmissionWait(capture)
	<-gate.entered
	b := &q.admissionWaitBuild
	b.mu.Lock()
	firstDone := b.done
	b.mu.Unlock()
	for range 5 {
		clock.Advance(time.Second)
		if _, err := q.Checkin(Checkin{Machine: m, period: time.Second}); err != nil {
			t.Fatal(err)
		}
	}
	lastCheckin := clock.Now()
	clock.Advance(time.Millisecond)
	if _, err := q.Snapshot(); err != nil {
		t.Fatal(err)
	}
	// Delayed post-commit hooks must not replace the newest pending generation.
	q.enqueueAdmissionWait(capture)
	b.mu.Lock()
	pending := b.pending
	running, done := b.running, b.done
	b.mu.Unlock()
	if !running || done != firstDone || pending == nil || pending.index != q.publishedReads.Load() {
		t.Fatal("check-ins did not coalesce into one latest pending snapshot")
	}
	if len(pending.admissions[demand.ID]) == 0 {
		t.Fatal("maintenance coalescing erased the unpublished check-in decision")
	}
	close(gate.release)
	released = true
	<-firstDone
	b.mu.Lock()
	next := b.next
	b.mu.Unlock()
	if next.Sub(clock.Now()) != time.Second {
		t.Fatal("cooldown did not follow measured service cost")
	}
	// A newer telemetry generation must not starve publication of the older
	// snapshot while its exact wait episode remains current.
	q.mu.Lock()
	evidence := q.s.Jobs[demand.ID].AdmissionWait
	q.mu.Unlock()
	if evidence == nil || evidence.Latest == nil || evidence.Latest.ClosedAt != nil || len(evidence.Latest.Targets) == 0 || evidence.Latest.Targets[0].RequiredCores != 5 {
		t.Fatal("telemetry commit prevented evidence publication")
	}
	q.enqueueAdmissionWait(pending)
	b.mu.Lock()
	running, done = b.running, b.done
	b.mu.Unlock()
	if running || done != firstDone {
		t.Fatal("build started during cooldown")
	}
	clock.Advance(time.Second)
	q.enqueueAdmissionWait(pending)
	b.mu.Lock()
	done = b.done
	b.mu.Unlock()
	if done == firstDone {
		t.Fatal("due pending capture did not start")
	}
	<-done
	q.mu.Lock()
	evidence = q.s.Jobs[demand.ID].AdmissionWait
	q.mu.Unlock()
	decision := evidence.Latest.Targets[0].AdmissionDecision
	if decision == nil || !decision.At.Equal(lastCheckin) {
		t.Fatal("coalescing lost or retimestamped the real check-in decision")
	}
}

func TestAdmissionWaitBackgroundFencesResumeAndRetry(t *testing.T) {
	q, _ := fixture(t)
	s := cpuSpec(4, 1)
	s.Priority = "production"
	demand := submit(t, q, s)
	original := admissionWaitAPI(t, q, demand.ID)
	q.mu.Lock()
	capture := q.freezeAdmissionWait()
	q.mu.Unlock()
	paused, err := q.PauseJob(demand.ID, PauseJobRequest{Reason: "test pause"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.ResumeJob(demand.ID, paused.PauseRevision); err != nil {
		t.Fatal(err)
	}
	drainAdmissionWait(t, q)
	if err = q.commitAdmissionWait(capture, map[string]admissionWaitUpdate{demand.ID: {evidence: original}}); err != nil {
		t.Fatal(err)
	}
	after := admissionWaitAPI(t, q, demand.ID)
	if after.CaptureGaps == 0 || after.DeferredAt == nil {
		t.Fatal("old wait episode accepted after resume")
	}
	q.mu.Lock()
	capture = q.freezeAdmissionWait()
	q.mu.Unlock()
	if err = q.update(func() error { j := q.touchJob(demand.ID); j.Attempt++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err = q.commitAdmissionWait(capture, map[string]admissionWaitUpdate{demand.ID: {evidence: original}}); err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, demand.ID); got.CaptureGaps <= after.CaptureGaps {
		t.Fatal("old attempt accepted")
	}
}

func TestAdmissionWaitBackgroundRotatesUnchangedEvaluation(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	ids := []string{submit(t, q, s).ID, submit(t, q, s).ID}
	slices.Sort(ids)
	oldest, next := ids[0], ids[1]
	before := admissionWaitAPI(t, q, oldest).Latest
	if before.Predicate != "no_qualified_target" || admissionWaitAPI(t, q, next).Latest.Predicate != before.Predicate {
		t.Fatal("fixture lacks two equivalent job explanations")
	}
	producer := submit(t, q, cpuSpec(1, 1))
	if err := q.update(func() error {
		q.touchJob(next).Spec.After = &PieceDependency{Job: producer.ID}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	build := func() {
		q.mu.Lock()
		capture := q.freezeAdmissionWait()
		q.mu.Unlock()
		capture.clock = coldProbeAdvancingClock{&fakeClock{now: clock.Now()}}
		capture.service = 2 * time.Nanosecond // One diagnostic evaluation fits.
		q.enqueueAdmissionWait(capture)
	}
	build()
	if got := admissionWaitAPI(t, q, next); got.DeferredAt == nil || got.Latest.Predicate != before.Predicate {
		t.Fatal("limited build should have deferred the second job")
	}
	if got := admissionWaitAPI(t, q, oldest); got.DeferredAt != nil || !got.Latest.At.Equal(before.At) {
		t.Fatal("unchanged evaluation moved the semantic endpoint or missed its turn")
	}
	build() // Queue time is identical. The previous evaluation still yields.
	if got := admissionWaitAPI(t, q, next); got.DeferredAt != nil || got.Latest.Predicate != "dependency" {
		t.Fatal("unchanged oldest reason starved an updated wait reason")
	}
}

func TestAdmissionWaitGapCommitKeepsPrunedHistory(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	j := submit(t, q, s)
	first := admissionWaitAPI(t, q, j.ID).Latest
	middle := &AdmissionWaitSnapshot{At: clock.Now().Add(time.Second), Predicate: "dependency"}
	clock.Advance(2 * time.Second)
	latest := &AdmissionWaitSnapshot{At: clock.Now(), Predicate: "admission"}
	evidence := retainAdmissionWait(&AdmissionWaitEvidence{First: first, Latest: middle}, latest, 1<<20, false)
	if len(evidence.Transitions) != 1 {
		t.Fatal("fixture did not retain a transition")
	}
	if err := q.update(func() error { q.touchJob(j.ID).AdmissionWait = evidence; return nil }); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	capture := q.freezeAdmissionWait()
	q.mu.Unlock()
	next := *evidence
	now := clock.Now()
	next.CaptureGaps++
	next.DeferredAt = &now

	q.evidenceMaintenance.capacity = func(context.Context) (RetentionCapacity, error) {
		return RetentionCapacity{MemoryAvailableBytes: 1, DiskAvailableBytes: 1}, nil
	}
	drainEvidence(t, q)
	pruned := admissionWaitAPI(t, q, j.ID)
	if len(pruned.Transitions) != 0 || pruned.Dropped != evidence.Dropped+1 {
		t.Fatal("retention did not prune the queued job's history")
	}
	if err := q.commitAdmissionWait(capture, map[string]admissionWaitUpdate{j.ID: {deferredAt: next.DeferredAt}}); err != nil {
		t.Fatal(err)
	}
	pruned.CaptureGaps, pruned.DeferredAt = next.CaptureGaps, next.DeferredAt
	if got := admissionWaitAPI(t, q, j.ID); !reflect.DeepEqual(got, pruned) {
		t.Fatalf("gap commit replaced retained evidence: transitions=%d dropped=%d history=%d", len(got.Transitions), got.Dropped, got.HistoryBytes)
	}
	// An evaluated sample can carry the same stale transitions. Let a later
	// build plan against the pruned history before publishing that sample.
	evaluated := &AdmissionWaitSnapshot{At: now, Predicate: "dependency"}
	fresh := retainAdmissionWait(evidence, evaluated, 1<<20, false)
	if err := q.commitAdmissionWait(capture, map[string]admissionWaitUpdate{j.ID: {droppedBefore: evidence.Dropped, evidence: fresh}}); err != nil {
		t.Fatal(err)
	}
	got := admissionWaitAPI(t, q, j.ID)
	if got.DeferredAt == nil || got.CaptureGaps != pruned.CaptureGaps+1 {
		t.Fatalf("skipped stale sample must record a capture gap: gaps=%d deferred=%v", got.CaptureGaps, got.DeferredAt)
	}
	got.CaptureGaps, got.DeferredAt = pruned.CaptureGaps, pruned.DeferredAt
	if !reflect.DeepEqual(got, pruned) {
		t.Fatal("stale evaluated sample replaced retained evidence")
	}
}

func TestAdmissionWaitClearingGapRefreshesTelemetry(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 1, 8)
	check(t, q, m, nil)
	s := cpuSpec(4, 1)
	s.Priority = "production"
	j := submit(t, q, s)
	check(t, q, m, nil)
	before := admissionWaitAPI(t, q, j.ID)
	if len(before.Latest.Targets) != 1 || before.Latest.Targets[0].AvailableHostGB != 8 {
		t.Fatalf("fixture did not record the original host memory: %+v", before.Latest)
	}
	deferCapture := func() {
		t.Helper()
		if err := q.update(func() error {
			j := q.touchJob(j.ID)
			j.AdmissionWait = deferAdmissionWait(j.AdmissionWait, clock.Now())
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	deferCapture()
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB = 4
	check(t, q, m, nil)
	after := admissionWaitAPI(t, q, j.ID)
	if after.DeferredAt != nil || after.CaptureGaps != before.CaptureGaps+1 || !sameAdmissionWait(before.Latest, after.Latest) {
		t.Fatalf("fixture did not clear the gap with an equivalent blocker: before=%+v after=%+v", before.Latest, after.Latest)
	}
	if got := after.Latest.Targets[0].AvailableHostGB; got != 4 || !after.Latest.At.Equal(clock.Now()) {
		t.Fatalf("cleared gap kept stale telemetry: %g GiB at %v", got, after.Latest.At)
	}
	deferCapture()
	clock.Advance(time.Second)
	check(t, q, m, nil)
	if got := admissionWaitAPI(t, q, j.ID); got.DeferredAt != nil || got.CaptureGaps != after.CaptureGaps+1 || !reflect.DeepEqual(got.Latest, after.Latest) {
		t.Fatal("clearing a gap with identical readings rewrote the endpoint or lost coverage")
	}
}

func TestAdmissionWaitWaveClosureKeepsExistingGap(t *testing.T) {
	q, clock, _, s := fanOutFixture(t, 2)
	s.Priority = "production"
	j := submit(t, q, s)
	if err := q.update(func() error {
		j := q.touchJob(j.ID)
		j.AdmissionWait = deferAdmissionWait(j.AdmissionWait, clock.Now())
		j.FanOut.Next = len(j.Spec.FanOut.Pieces)
		q.admissionWaitPending = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got := admissionWaitAPI(t, q, j.ID)
	if got.Latest.ClosedReason != "wave_admitted" || got.Latest.ClosedAt == nil || got.DeferredAt == nil || got.CaptureGaps != 1 {
		t.Fatalf("wave closure should preserve its existing gap: closed=%q deferred=%v gaps=%d", got.Latest.ClosedReason, got.DeferredAt, got.CaptureGaps)
	}
}

func TestAdmissionWaitBackgroundRotatesUnderSustainedArrivals(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	j := submit(t, q, s)
	producer := submit(t, q, cpuSpec(1, 1))
	if got := admissionWaitAPI(t, q, j.ID); got.Latest.Predicate != "no_qualified_target" {
		t.Fatal("fixture did not evaluate the original blocker")
	}
	if err := q.update(func() error {
		q.touchJob(j.ID).Spec.After = &PieceDependency{Job: producer.ID}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := q.update(func() error {
			newcomer := *q.s.Jobs[j.ID]
			newcomer.ID = fmt.Sprintf("arrival-%d", i)
			newcomer.AdmissionWait = nil
			q.putJob(newcomer.ID, &newcomer)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		capture := q.freezeAdmissionWait()
		q.mu.Unlock()
		capture.clock = coldProbeAdvancingClock{&fakeClock{now: clock.Now()}}
		capture.service = 2 * time.Nanosecond // One diagnostic evaluation fits.
		q.enqueueAdmissionWait(capture)
		got := admissionWaitAPI(t, q, j.ID)
		if got.Latest.Predicate == "dependency" && got.DeferredAt == nil {
			return
		}
	}
	t.Fatal("a new arrival took every evaluation while the older blocker went stale")
}

// Same retained fixture as BenchmarkCheckinPersistence: five workers, 115 live
// jobs (80 production-queued) and 6,500 retained jobs, real SQLite FULL commits.
// CPU and allocations include scheduled background work, including an active
// build's tail after the final check-in. Pending cooldown work is not forced.
func BenchmarkAdmissionWaitCheckin(b *testing.B) {
	q, workers := candidateCostFixture(b)
	drainAdmissionWait(b, q)
	asynchronousAdmissionWait(b, q)
	clock := q.clock.(*workerFakeClock)
	for i := 160; i < 6500; i++ {
		source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j := *source
		j.ID = fmt.Sprintf("retained-%04d", i)
		timing := *source.Timing
		timing.Execution.ID = j.ID
		j.Timing = &timing
		q.s.Jobs[j.ID] = &j
	}
	if err := q.update(func() error { return nil }); err != nil {
		b.Fatal(err)
	}
	var mu sync.Mutex
	var hold time.Duration
	q.observePersistenceHold = func(p queueHoldPhases) {
		mu.Lock()
		defer mu.Unlock()
		for _, elapsed := range p.phases {
			hold += elapsed
		}
	}
	cpu := func() time.Duration {
		var r syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
			b.Fatal(err)
		}
		return time.Duration(r.Utime.Sec+r.Stime.Sec)*time.Second + time.Duration(r.Utime.Usec+r.Stime.Usec)*time.Microsecond
	}
	var latency time.Duration
	b.ReportAllocs()
	before := cpu()
	b.ResetTimer()
	for i := range b.N {
		clock.Advance(time.Second)
		m := workers[i%len(workers)]
		started := time.Now()
		_, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i%len(workers)), Attempt: 1}, Session: m.Session, Started: true}}})
		latency += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
	}
	admissionWaitBenchmarkSettle(q)
	b.StopTimer()
	used := cpu() - before
	n := float64(b.N)
	b.ReportMetric(float64(used)/float64(time.Millisecond)/n, "CPU-ms/checkin")
	b.ReportMetric(float64(latency)/float64(time.Millisecond)/n, "request-ms/checkin")
	mu.Lock()
	b.ReportMetric(float64(hold)/float64(time.Millisecond)/n, "hold-ms/checkin")
	mu.Unlock()
}

func admissionWaitBenchmarkSettle(q *Queue) {
	b := &q.admissionWaitBuild
	b.mu.Lock()
	running, done := b.running, b.done
	b.mu.Unlock()
	if running {
		<-done
	}
}
