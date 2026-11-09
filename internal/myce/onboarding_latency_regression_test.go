package myce

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Incident replay: 30 individually healthy operations, 68.236s overall.
func TestOnboardingProgressBeyondFormerAggregateDeadline(t *testing.T) {
	c, command := onboardingNAS(t, "darwin")
	synctest.Test(t, func(t *testing.T) {
		defer joinOnboardingNASSequence()
		operations := 0
		fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, abort context.CancelCauseFunc, _ string) error {
			for i := 0; i < 10; i++ {
				if err := onboardingFSError(ctx, abort, func() error {
					time.Sleep(2274523300 * time.Nanosecond)
					operations++
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		}}
		start := time.Now()
		got, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
		if err != nil || !got.RequireMountedNAS || operations != 30 {
			t.Fatalf("progressing validation refused: operations=%d err=%v", operations, err)
		}
		if elapsed := time.Since(start); elapsed < 68*time.Second {
			t.Fatalf("fixture did not reproduce aggregate latency: %s", elapsed)
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

// Replay the #289 delete tail without spending real time or using a NAS. The
// normal probe still proves write/fsync/rename/delete on each configured root.
func TestOnboardingSlowCompletingDeleteAndHungDelete(t *testing.T) {
	for _, hung := range []bool{false, true} {
		name := "slow-completion"
		if hung {
			name = "hung"
		}
		t.Run(name, func(t *testing.T) {
			c, command := onboardingNAS(t, "darwin")
			var logs bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(previous)
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer joinOnboardingNASSequence()
				defer close(release)
				var deletes atomic.Int32
				fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, abort context.CancelCauseFunc, dir string) error {
					if err := probeNASDirectoryContext(ctx, abort, dir); err != nil {
						return err
					}
					f, err := os.CreateTemp(dir, ".myce-delete-replay-")
					if err != nil {
						return err
					}
					if err := f.Close(); err != nil {
						return err
					}
					return onboardingFSError(ctx, abort, func() error {
						deletes.Add(1)
						if hung {
							<-release
						} else {
							time.Sleep(14200 * time.Millisecond)
						}
						return os.Remove(f.Name())
					}, "delete")
				}}
				start := time.Now()
				got, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
				if hung {
					requirePreflightCode(t, err, "nas_unresponsive")
					if elapsed := time.Since(start); elapsed != onboardingFilesystemStallBudget {
						t.Errorf("hung delete refused after %s, want %s", elapsed, onboardingFilesystemStallBudget)
					}
					// Refusal must not surrender ownership of the blocked delete to a retry.
					_, err = prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
					requirePreflightCode(t, err, "nas_unresponsive")
					if deletes.Load() != 1 {
						t.Errorf("retry duplicated blocked deletes: %d", deletes.Load())
					}
				} else {
					if err != nil || !got.RequireMountedNAS || deletes.Load() != 3 {
						t.Fatalf("slow successful delete refused: deletes=%d err=%v", deletes.Load(), err)
					}
				}
			})
			if hung {
				if !strings.Contains(logs.String(), "operation=delete stalled budget=30s") {
					t.Errorf("missing stall diagnostic: %s", &logs)
				}
			} else if !strings.Contains(logs.String(), "NAS slow: delete took 14.2 s") {
				t.Errorf("missing slow-completion diagnostic: %s", &logs)
			}
		})
	}
}
