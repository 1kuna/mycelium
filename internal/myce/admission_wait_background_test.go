package myce

import (
	"fmt"
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
	if err = q.commitAdmissionWait(capture, map[string]*AdmissionWaitEvidence{demand.ID: original}); err != nil {
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
	if err = q.commitAdmissionWait(capture, map[string]*AdmissionWaitEvidence{demand.ID: original}); err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, demand.ID); got.CaptureGaps <= after.CaptureGaps {
		t.Fatal("old attempt accepted")
	}
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
