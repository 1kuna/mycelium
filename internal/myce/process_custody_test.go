package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A stalled inspector is killed and reports its deadline, rather than an
// ambiguous SIGKILL. exec avoids leaving a fixture grandchild behind.
func TestProcessPSStalledInspector(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "ps"), []byte("#!/bin/sh\nexec /bin/sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	// A real stalled child is OS timing, so this stays on wall time. The bound
	// itself is a constant; a short one proves the same cancellation path.
	if _, err := processPSWithin(100*time.Millisecond, "-ax"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing inspector cancellation evidence: %v", err)
	}
}

func TestCommandLockRefusesEmptyStateDir(t *testing.T) {
	if unlock, err := commandLock("", "fleet-update.lock", false); err == nil {
		unlock()
		t.Fatal("empty state dir must not lock in the working directory")
	}
}
