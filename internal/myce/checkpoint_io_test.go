package myce

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerNASProbeToleratesSlowMetadata(t *testing.T) {
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
