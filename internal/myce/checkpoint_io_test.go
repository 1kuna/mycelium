package myce

import (
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
		// Healthy CIFS operation sequences measured up to 109 ms; allow margin
		// beyond that and the previously observed 16 ms cold stat.
		<-time.After(150 * time.Millisecond)
		return prepareCheckpoint(j)
	}
	defer func() { <-finished }()
	p, err := w.start(s, j)
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
		go func() { _, err := w.start(s, j); done <- err }()
		select {
		case err := <-done:
			if !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "NAS unreachable: filesystem operation timed out") {
				t.Fatalf("timeout error changed: %v", err)
			}
			if !s.checkpointUnavailable || s.state.Result != nil || s.state.PID != 0 {
				t.Fatal("timed-out preparation started or failed the job")
			}
		case <-time.After(time.Second):
			t.Fatal("hung preparation exceeded one-second wall-clock test bound")
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
