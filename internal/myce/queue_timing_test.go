package myce

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// Logging may outlive the control call that triggered admission capture.
// Readers and resets must share a lock with writes, not just log.Logger's
// writer lock. Keep the buffer private so snapshots cannot alias live bytes.
type testLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *testLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *testLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *testLogBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *testLogBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func testLogEvent(t testing.TB, output, event string) string {
	t.Helper()
	var found string
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(" "+line+" ", " event="+event+" ") {
			if found != "" {
				t.Fatalf("duplicate %s event: %s", event, output)
			}
			found = line
		}
	}
	if found == "" {
		t.Fatalf("missing %s event: %s", event, output)
	}
	return found
}

// The writer checks the production logging boundary, not just the log text.
type launchLogWriter struct {
	q *Queue
	t *testing.T
	testLogBuffer
}

func (w *launchLogWriter) Write(b []byte) (int, error) {
	if strings.Contains(string(b), "event=slow_launch_authorization") || strings.Contains(string(b), "event=slow_queue_mutex_hold") {
		if !w.q.mu.TryLock() {
			w.t.Error("slow launch logging held the queue lock")
		} else {
			w.q.mu.Unlock()
		}
	}
	return w.testLogBuffer.Write(b)
}

func TestQueueMutexHoldTelemetry(t *testing.T) {
	q, _ := fixture(t)
	logs := &launchLogWriter{q: q, t: t}
	old := log.Writer()
	log.SetOutput(logs)
	defer log.SetOutput(old)
	var sampler queueHoldSampler
	for i := range 33 {
		q.mu.Lock()
		started := sampler.start()
		if sampled := !started.IsZero(); sampled != true {
			t.Errorf("hold %d sampled=%t", i, sampled)
		}
		duration := queueHoldDuration(started)
		q.mu.Unlock()
		_ = duration
		logQueueHold(time.Microsecond, "fast", nil)
	}
	if logs.Len() != 0 {
		t.Fatal("fast holds emitted telemetry", logs.String())
	}
	// Exercise the real update boundary. Wall time only models a slow critical
	// section; queue decisions still use the fixture's injected clock.
	q.updateHolds.count.Store(0)
	operation := func() error { <-time.After(300 * time.Millisecond); return nil }
	seedHoldTelemetry(operation)
	if err := q.update(operation); err != nil {
		t.Fatal(err)
	}
	if line := logs.String(); strings.Count(line, "event=slow_queue_mutex_hold") != 1 || !strings.Contains(line, "TestQueueMutexHoldTelemetry") || !strings.Contains(line, "duration_ms=") || !strings.Contains(line, "sample_every=1") {
		t.Fatal("missing slow hold attribution", line)
	}
}

func TestHostLaunchSlowAuthorizationTelemetry(t *testing.T) {
	q, _ := fixture(t)
	m := machine("private-machine", "fake")
	submit(t, q, spec("experiment"))
	a := check(t, q, m, nil).Job
	logs := &launchLogWriter{q: q, t: t}
	old := log.Writer()
	log.SetOutput(logs)
	defer log.SetOutput(old)
	queueHoldTelemetry.Store("launch", &telemetryBudget{})
	holdTelemetry("launch").admit(time.Millisecond)
	entered := make(chan struct{})
	q.checkpointProbe = func() bool { close(entered); return true }
	q.mu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- q.AuthorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: a.ID, Attempt: "1"})
	}()
	<-entered
	// Telemetry measures wall time; this is lock contention, not scheduling time.
	<-time.After(750 * time.Millisecond)
	q.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	q.checkpointProbe = func() bool { return true }
	line := logs.String()
	if strings.Count(line, "event=slow_launch_authorization") != 1 || strings.Contains(line, "private") {
		t.Fatal("missing, duplicated or unsafe slow request log", line)
	}
	line = testLogEvent(t, line, "slow_launch_authorization")
	fields := map[string]float64{}
	for _, word := range strings.Fields(line) {
		key, value, ok := strings.Cut(word, "=")
		if ok {
			var number float64
			if _, err := fmt.Sscan(value, &number); err == nil {
				fields[key] = number
			}
		}
	}
	for _, key := range []string{"duration_ms", "mutex_wait_ms", "sqlite_commit_ms"} {
		if _, ok := fields[key]; !ok {
			t.Errorf("missing %s in %s", key, line)
		}
	}
	if fields["duration_ms"] < 500 || fields["mutex_wait_ms"] < 500 || fields["sqlite_commit_ms"] <= 0 || !strings.Contains(line, "ok=true") {
		t.Error("timing lost the blocked mutex or real commit", line)
	}
	logs.Reset()
	if err := q.AuthorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: a.ID, Attempt: "1"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "event=slow_launch_authorization") {
		t.Fatal("fast authorization logged as slow", logs.String())
	}
}

func TestQueueSlowUpdatePhaseBreakdown(t *testing.T) {
	q, clock := fixture(t)
	// Both queue decisions and the observational clock are deterministic here.
	q.holdClock = clock
	q.updateHolds.count.Store(0)
	logs := &launchLogWriter{q: q, t: t}
	old := log.Writer()
	log.SetOutput(logs)
	defer log.SetOutput(old)
	operation := func() error { clock.Advance(300 * time.Millisecond); return nil }
	seedHoldTelemetry(operation)
	if err := q.update(operation); err != nil {
		t.Fatal(err)
	}
	line := logs.String()
	for _, field := range []string{"event=slow_queue_mutex_hold", "duration_ms=300.000", "records_snapshot_ms=0.000", "fn_body_ms=300.000", "persist_commit_ms=0.000", "usage_accounting_ms=0.000", "notify_ms=0.000"} {
		if !strings.Contains(line, field) {
			t.Errorf("missing %s in %s", field, line)
		}
	}
	logs.Reset()
	q.updateHolds.count.Store(0)
	if err := q.update(operation); err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 0 {
		t.Fatal("subthreshold update logged", logs.String())
	}
}

func seedHoldTelemetry(operation func() error) {
	name := runtime.FuncForPC(reflect.ValueOf(operation).Pointer()).Name()
	queueHoldTelemetry.Store(name, &telemetryBudget{})
	holdTelemetry(name).admit(time.Millisecond)
}

func TestCheckinBodyPhaseTelemetry(t *testing.T) {
	q, clock := fixture(t)
	m := machine("private-machine", "fake")
	if _, err := q.Checkin(Checkin{Machine: m, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	decisionTime := clock.Now()
	observational := &fakeClock{now: clock.Now()}
	q.holdClock = observational
	// Advance only telemetry time, leaving every scheduling decision unchanged.
	// Inclusive helpers finish before these advances; their enclosing phases
	// must attribute the delay, including the deferred reply plans and tail.
	q.observeBodyPhase = func(name string, _ time.Duration) {
		switch name {
		case "maintain":
			observational.Advance(300 * time.Millisecond)
		case "environment_plan":
			observational.Advance(200 * time.Millisecond)
		case "health":
			observational.Advance(100 * time.Millisecond)
		}
	}
	var measured queueHoldPhases
	q.observePersistenceHold = func(p queueHoldPhases) {
		if p.checkinBody {
			measured = p
		}
	}
	if _, err := q.Checkin(Checkin{Machine: m, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	if q.activeHold != nil || !clock.Now().Equal(decisionTime) {
		t.Fatal("telemetry leaked transaction state or changed the decision clock")
	}
	logs := &launchLogWriter{q: q, t: t}
	old := log.Writer()
	log.SetOutput(logs)
	defer log.SetOutput(old)
	const operation = "checkin_phase_test"
	queueHoldTelemetry.Store(operation, &telemetryBudget{})
	holdTelemetry(operation).admit(time.Millisecond)
	logQueueHold(measured.last.Sub(measured.started), operation, nil, &measured)
	line := logs.String()
	for _, field := range []string{
		"fn_body_ms=600.000", "body_checkin_inventory_ms=0.000",
		"body_checkin_memory_ms=0.000", "body_checkin_reconcile_ms=0.000",
		"body_checkin_maintain_ms=300.000", "body_checkin_attempts_ms=0.000",
		"body_checkin_plans_ms=200.000", "body_transaction_tail_ms=100.000",
		"body_environment_plan_ms=0.000", "body_model_plan_ms=0.000",
		"body_copy_plan_ms=0.000", "body_health_ms=0.000",
	} {
		if !strings.Contains(line, field) {
			t.Errorf("missing %s in %s", field, line)
		}
	}
	if strings.Contains(line, "private-machine") {
		t.Fatal("check-in telemetry disclosed request data", line)
	}
	// A failed callback still ends its last phase and clears the timer.
	m.Session = "other-session"
	if _, err := q.Checkin(Checkin{Machine: m}); err == nil || q.activeHold != nil {
		t.Fatal("failed check-in lost its error or retained an active hold", err)
	}
}
