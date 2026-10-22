package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestOnboardingAndHeartbeatAcceptSMBMetadataLatency(t *testing.T) {
	c, command := onboardingNAS(t, "darwin")
	synctest.Test(t, func(t *testing.T) {
		first := true
		fs := onboardingStorageFS{lstat: func(path string) (os.FileInfo, error) {
			if first {
				first = false
				time.Sleep(500 * time.Millisecond)
			}
			return os.Lstat(path)
		}, mkdir: os.Mkdir, probe: probeNASDirectory}
		_, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
		if err != nil {
			t.Errorf("healthy 500 ms SMB metadata refused: %v", err)
		}
		// Join the owned sequence even when the baseline times out.
		onboardingNASIO.mu.Lock()
		pending := onboardingNASIO.pending
		onboardingNASIO.mu.Unlock()
		if pending != nil {
			<-pending.done
		}
		onboardingNASIO.mu.Lock()
		onboardingNASIO.pending = nil
		onboardingNASIO.mu.Unlock()
		w := recoveryWorker(t)
		s, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		available, err := s.nasProbe.run("smb", func() (bool, error) { time.Sleep(500 * time.Millisecond); return true, nil })
		if err != nil || !available {
			t.Errorf("heartbeat rejected healthy 500 ms SMB metadata: %v", err)
		}
		s.nasProbe.mu.Lock()
		heartbeat := s.nasProbe.pending
		s.nasProbe.mu.Unlock()
		if heartbeat != nil {
			<-heartbeat.done
		}
	})
}

func TestOnboardingAcceptsSlowSMBFsync(t *testing.T) {
	c, command := onboardingNAS(t, "darwin")
	synctest.Test(t, func(t *testing.T) {
		defer joinOnboardingNASSequence()
		probes := 0
		fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, abort context.CancelCauseFunc, _ string) error {
			// Model the uninterruptible fsync inside the real bounded probe.
			return onboardingFSError(ctx, abort, func() error {
				time.Sleep(1500 * time.Millisecond)
				probes++
				return nil
			})
		}}
		_, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
		if err != nil {
			t.Errorf("healthy 1.5 s SMB fsync refused: %v", err)
		}
		if err == nil && probes != 3 {
			t.Errorf("validated %d roots, want all three", probes)
		}
	})
}

func TestOnboardingStalledFsyncIsBounded(t *testing.T) {
	c, command := onboardingNAS(t, "darwin")
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer joinOnboardingNASSequence()
		defer close(release)
		fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, abort context.CancelCauseFunc, _ string) error {
			return onboardingFSError(ctx, abort, func() error { <-release; return nil })
		}}
		start := time.Now()
		_, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
		requirePreflightCode(t, err, "nas_unresponsive")
		if elapsed := time.Since(start); elapsed > 15*time.Second {
			t.Errorf("stalled fsync exceeded hard total: %s", elapsed)
		}
	})
}

func TestOnboardingSlowOperationsShareTotalBound(t *testing.T) {
	c, command := onboardingNAS(t, "darwin")
	st, err := os.Stat(c.NAS)
	if err != nil {
		t.Fatal(err)
	}
	c.Checkpoints = filepath.Join(c.NAS, strings.Repeat("part/", 200), "checkpoints")
	synctest.Test(t, func(t *testing.T) {
		defer joinOnboardingNASSequence()
		fs := onboardingStorageFS{lstat: func(string) (os.FileInfo, error) {
			time.Sleep(1500 * time.Millisecond)
			return st, nil
		}, mkdir: func(string, os.FileMode) error { return nil }, probe: func(string) error { return nil }}
		start := time.Now()
		_, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
		requirePreflightCode(t, err, "nas_unresponsive")
		if elapsed := time.Since(start); elapsed != 15*time.Second {
			t.Errorf("whole-check deadline=%s, want 15s despite individually healthy operations", elapsed)
		}
	})
}

// A timed-out syscall remains owned until it returns. Never leave a synctest
// channel in the process-wide guard for the next test to encounter.
func joinOnboardingNASSequence() {
	onboardingNASIO.mu.Lock()
	pending := onboardingNASIO.pending
	onboardingNASIO.mu.Unlock()
	if pending != nil {
		<-pending.done
	}
	onboardingNASIO.mu.Lock()
	onboardingNASIO.pending = nil
	onboardingNASIO.mu.Unlock()
}

func TestOnboardingMountTimeoutKeepsTypedRefusal(t *testing.T) {
	c, _ := onboardingNAS(t, "darwin")
	_, err := PrepareOnboardingStorage(context.Background(), c, false, func(context.Context, string, ...string) ([]byte, error) {
		// A command may report its own deadline before the parent expires.
		return nil, context.DeadlineExceeded
	})
	requirePreflightCode(t, err, "nas_unresponsive")
}
