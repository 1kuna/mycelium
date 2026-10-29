package myce

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// synctest drives the existing I/O budget and simulated metadata delays with
// fake timers; callbacks and real filesystem work must finish before time moves.
func TestWorkerPreparationToleratesSlowMetadata(t *testing.T) {
	synctest.Test(t, testWorkerPreparationToleratesSlowMetadata)
}

func testWorkerPreparationToleratesSlowMetadata(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"true"})
	finished := make(chan struct{})
	w.checkpointPrepare = func(j Job) (string, error) {
		defer close(finished)
		// Healthy SMB fsync can take over a second before a job starts.
		<-time.After(1500 * time.Millisecond)
		return prepareCheckpoint(j)
	}
	defer func() { <-finished }()
	p, err := startAfterFixtureInputs(t, w, s, j)
	if err != nil {
		t.Fatalf("healthy slow preparation rejected: %v", err)
	}
	defer p.stop(w.Clock.Now(), 0, true)
	select {
	case f := <-p.done:
		if f.err != nil || f.result.Error != "" {
			t.Fatalf("command failed after preparation: %+v", f)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("command did not finish")
	}
}

func TestWorkerPreparationTimeoutRetainsSingleCall(t *testing.T) {
	synctest.Test(t, testWorkerPreparationTimeoutRetainsSingleCall)
}

func testWorkerPreparationTimeoutRetainsSingleCall(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"true"})
	release, finished := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	w.checkpointPrepare = func(Job) (string, error) {
		calls.Add(1)
		defer close(finished)
		<-release
		return "", nil
	}
	defer func() { close(release); <-finished }()
	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() { _, err := startAfterFixtureInputs(t, w, s, j); done <- err }()
		select {
		case err := <-done:
			if !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "NAS unreachable: filesystem operation timed out") {
				t.Fatalf("timeout error changed: %v", err)
			}
			if !s.checkpointUnavailable || s.state.Result != nil || s.state.PID != 0 {
				t.Fatal("timed-out preparation started or failed the job")
			}
		case <-time.After(checkpointPreparationBudget + time.Second):
			t.Fatal("hung preparation exceeded its bounded wait")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("blocked preparations accumulated: %d", calls.Load())
	}
}

func TestWorkerNASProbeToleratesSlowMetadata(t *testing.T) {
	synctest.Test(t, testWorkerNASProbeToleratesSlowMetadata)
}

func testWorkerNASProbeToleratesSlowMetadata(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for i := 0; i < 2; i++ {
		available, err := s.nasProbe.run("nas", func() (bool, error) {
			<-time.After(75 * time.Millisecond)
			return true, nil
		})
		if err != nil || !available {
			t.Fatalf("healthy slow NAS reported unavailable: %v", err)
		}
	}
}

func TestWorkerNASProbeBlockedAndRecovery(t *testing.T) {
	synctest.Test(t, testWorkerNASProbeBlockedAndRecovery)
}

func testWorkerNASProbeBlockedAndRecovery(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	release := make(chan struct{})
	defer func() {
		if release != nil {
			close(release)
		}
	}()
	var calls atomic.Int32
	probe := func() (bool, error) {
		calls.Add(1)
		<-release
		return true, nil
	}
	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() { _, err := s.nasProbe.run("nas", probe); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("blocked NAS reported available")
			}
		case <-time.After(time.Second):
			t.Fatal("blocked NAS hangs heartbeat")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("blocked probes accumulate")
	}
	close(release)
	// Keep the channel value stable until the outstanding callback exits.
	available, err := s.nasProbe.run("nas", probe)
	release = nil
	if err != nil || !available {
		t.Fatalf("completed probe did not recover: %v", err)
	}
	want := errors.New("permission denied")
	available, err = s.nasProbe.run("nas", func() (bool, error) { return false, want })
	if available || !errors.Is(err, want) {
		t.Fatalf("filesystem error lost: %v", err)
	}
}

// Exercise the real queue adapter without making a network filesystem a test prerequisite.
func TestQueueNASProbeToleratesSlowMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _ := fixture(t)
		defer func() {
			q.nasProbe.mu.Lock()
			pending := q.nasProbe.pending
			q.nasProbe.mu.Unlock()
			if pending != nil {
				<-pending.done
			}
		}()
		q.checkpointProbe = func() bool {
			available, _ := q.nasProbe.run(q.root, func() (bool, error) {
				time.Sleep(500 * time.Millisecond)
				return true, nil
			})
			return available
		}
		j := submit(t, q, spec("experiment"))
		if r := check(t, q, machine("worker", "fake"), nil); r.Job == nil || r.Job.ID != j.ID {
			t.Fatal("healthy slow NAS prevented assignment")
		}
	})
}

func TestWorkerPreparationCancellationRetainsSingleCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := recoveryWorker(t)
		s, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		release, entered, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		w.checkpointPrepare = func(Job) (string, error) {
			close(entered)
			defer close(finished)
			<-release
			return "", nil
		}
		defer func() { close(release); <-finished }()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := startAfterFixtureInputsContext(t, ctx, w, s, recoveryJob(t, []string{"true"}))
			done <- err
		}()
		<-entered
		cancel()
		if err := <-done; !errors.Is(err, errWorkerCheckpoint) || s.state.PID != 0 || s.state.Result != nil {
			t.Fatalf("cancelled preparation launched or failed job: %v", err)
		}
	})
}

func TestCheckpointAvailabilitySlowSuccessStallAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _ := fixture(t)
		w := recoveryWorker(t)
		s, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		for _, probe := range []*checkpointAvailability{&q.nasProbe, &s.nasProbe} {
			if ok, err := probe.run("share", func() (bool, error) { return true, nil }); !ok || err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			slow := func() (bool, error) { calls.Add(1); time.Sleep(6074 * time.Millisecond); return true, nil }
			// Refresh as control requests arrive, without flapping while a healthy
			// observation progresses. The caller still returns within one heartbeat.
			start := time.Now()
			for time.Since(start) < 7*time.Second {
				if ok, err := probe.run("share", slow); !ok || err != nil {
					t.Fatalf("slow working share fenced: %v", err)
				}
			}
			if calls.Load() > 2 {
				t.Fatalf("overlapping observations: %d", calls.Load())
			}
			probe.mu.Lock()
			pending := probe.pending
			probe.mu.Unlock()
			if pending != nil {
				<-pending.done
			}
			// Consume any late completion before fault injection.
			_, _ = probe.run("share", slow)
			release := make(chan struct{})
			var blockedCalls atomic.Int32
			blocked := func() (bool, error) { blockedCalls.Add(1); <-release; return true, nil }
			start = time.Now()
			for time.Since(start) < checkpointAvailabilityStallBudget {
				_, _ = probe.run("share", blocked)
			}
			if ok, err := probe.run("share", blocked); ok || err == nil {
				t.Fatal("stalled share remained available")
			}
			if blockedCalls.Load() != 1 {
				t.Fatalf("blocked observations accumulated: %d", blockedCalls.Load())
			}
			close(release)
			if ok, err := probe.run("share", blocked); !ok || err != nil {
				t.Fatalf("share did not recover: %v", err)
			}
			failure := errors.New("I/O failure")
			if ok, err := probe.run("share", func() (bool, error) { return false, failure }); ok || !errors.Is(err, failure) {
				t.Fatalf("failed storage accepted: %v", err)
			}
		}
	})
}

func TestCheckpointAvailabilityProgressOutlivesAggregateDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _ := fixture(t)
		w := recoveryWorker(t)
		s, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		for _, probe := range []*checkpointAvailability{&q.nasProbe, &s.nasProbe} {
			if ok, err := probe.run("share", func() (bool, error) { return true, nil }); !ok || err != nil {
				t.Fatalf("initial success: %v", err)
			}
			var calls atomic.Int32
			progressDone := make(chan struct{})
			defer func() { <-progressDone }()
			progressing := func(progress func()) (bool, error) {
				defer close(progressDone)
				calls.Add(1)
				// Reproduce the measured successful create and unlink. Together
				// they exceed both the old total cap and the new stall backstop.
				for _, delay := range []time.Duration{17734 * time.Millisecond, 23223 * time.Millisecond} {
					time.Sleep(delay)
					progress()
				}
				return true, nil
			}
			start := time.Now()
			for {
				ok, err := probe.runProgress("share", progressing)
				if !ok || err != nil {
					t.Fatalf("progressing storage fenced after %s: %v", time.Since(start), err)
				}
				if time.Since(start) >= 40957*time.Millisecond {
					break
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("duplicate probe: %d", calls.Load())
			}
			// A new root must establish its own success, even while the former
			// root has retained healthy evidence.
			release := make(chan struct{})
			if ok, err := probe.runProgress("other", func(func()) (bool, error) { <-release; return true, nil }); ok || !errors.Is(err, errCheckpointIOPending) {
				t.Fatalf("cold root accepted: %v", err)
			}
			close(release)
			if ok, err := probe.run("other", func() (bool, error) { return true, nil }); !ok || err != nil {
				t.Fatalf("cold root recovery: %v", err)
			}
		}
	})
}
