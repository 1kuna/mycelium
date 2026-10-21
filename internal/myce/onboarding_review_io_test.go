package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestReviewHungNASOperationsBounded(t *testing.T) {
	for _, operation := range []string{"mount-lstat", "ancestor-lstat", "mkdir", "write-sync-rename"} {
		t.Run(operation, func(t *testing.T) {
			c, command := onboardingNAS(t, "linux")
			blocked, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			block := func() {
				if calls.Add(1) == 1 {
					close(blocked)
					<-release
					close(finished)
				}
			}
			fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probe: probeNASDirectory}
			switch operation {
			case "mount-lstat":
				fs.lstat = func(path string) (os.FileInfo, error) {
					if path == c.NAS {
						block()
					}
					return os.Lstat(path)
				}
			case "ancestor-lstat":
				fs.lstat = func(path string) (os.FileInfo, error) {
					if path == filepath.Join(c.NAS, "mycelium") {
						block()
					}
					return os.Lstat(path)
				}
			case "mkdir":
				fs.mkdir = func(path string, mode os.FileMode) error { block(); return os.Mkdir(path, mode) }
			case "write-sync-rename":
				fs.probe = func(string) error { block(); return nil }
			}
			done := make(chan error, 1)
			start := time.Now()
			go func() { _, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs); done <- err }()
			<-blocked
			completed := false
			select {
			case err := <-done:
				completed = true
				elapsed := time.Since(start)
				requirePreflightCode(t, err, "nas_unresponsive")
				if elapsed > networkFilesystemBudget+100*time.Millisecond {
					t.Errorf("NAS took %s", elapsed)
				}
				// A second caller cannot duplicate an uninterruptible syscall.
				_, err = prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
				requirePreflightCode(t, err, "nas_unresponsive")
			case <-time.After(networkFilesystemBudget + 100*time.Millisecond):
				t.Error("hung NAS exceeded network-filesystem budget")
			}
			close(release)
			<-finished
			// Drain the original operation before another subtest reuses the guard.
			if calls.Load() != 1 {
				t.Errorf("duplicated NAS operation: %d", calls.Load())
			}
			if !completed {
				<-done
			}
			for {
				_, err := onboardingNASIO.run("drain", func() (bool, error) { return true, nil })
				if err == nil {
					break
				}
				if !errors.Is(err, errCheckpointIOPending) {
					t.Fatal(err)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestReviewSlowHealthyNASGetsNetworkAllowance(t *testing.T) {
	c, command := onboardingNAS(t, "linux")
	first := true
	fs := onboardingStorageFS{lstat: func(path string) (os.FileInfo, error) {
		if first {
			first = false
			time.Sleep(150 * time.Millisecond)
		}
		return os.Lstat(path)
	}, mkdir: os.Mkdir, probe: probeNASDirectory}
	_, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
	if err != nil {
		t.Fatalf("healthy network metadata rejected: %v", err)
	}
}
