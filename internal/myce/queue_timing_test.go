package myce

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"
)

// The writer checks the production logging boundary, not just the log text.
type launchLogWriter struct {
	q *Queue
	t *testing.T
	bytes.Buffer
}

func (w *launchLogWriter) Write(b []byte) (int, error) {
	if strings.Contains(string(b), "event=slow_launch_authorization") || strings.Contains(string(b), "event=slow_queue_mutex_hold") {
		if !w.q.mu.TryLock() {
			w.t.Error("slow launch logging held the queue lock")
		} else {
			w.q.mu.Unlock()
		}
	}
	return w.Buffer.Write(b)
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
		if sampled := !started.IsZero(); sampled != (i%16 == 0) {
			t.Errorf("hold %d sampled=%t", i, sampled)
		}
		duration := queueHoldDuration(started)
		q.mu.Unlock()
		logQueueHold(duration, "fast", nil)
	}
	if logs.Len() != 0 {
		t.Fatal("fast holds emitted telemetry", logs.String())
	}
	// Exercise the real update boundary. Wall time only models a slow critical
	// section; queue decisions still use the fixture's injected clock.
	q.updateHolds.count.Store(0)
	if err := q.update(func() error {
		<-time.After(300 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if line := logs.String(); strings.Count(line, "event=slow_queue_mutex_hold") != 1 || !strings.Contains(line, "TestQueueMutexHoldTelemetry") || !strings.Contains(line, "duration_ms=") || !strings.Contains(line, "sample_every=16") {
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
