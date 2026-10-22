package myce

import (
	"context"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

// Config-only reinstall reproduced a healthy SMB operation exceeding 3 seconds.
func TestOnboardingStorageSlowSMBReconfigure(t *testing.T) {
	c, command := onboardingNAS(t, "darwin")
	synctest.Test(t, func(t *testing.T) {
		defer joinOnboardingNASSequence()
		fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, abort context.CancelCauseFunc, _ string) error {
			return onboardingFSError(ctx, abort, func() error { time.Sleep(4 * time.Second); return nil })
		}}
		if _, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs); err != nil {
			t.Fatal("healthy four-second SMB fsync refused:", err)
		}
	})
}
