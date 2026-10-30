package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A stalled inspector is killed and reports its deadline, rather than an
// ambiguous SIGKILL. exec avoids leaving a fixture grandchild behind.
func TestProcessPSStalledInspector(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "ps"), []byte("#!/bin/sh\nexec /bin/sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if _, err := processPS("-ax"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing inspector cancellation evidence: %v", err)
	}
}
