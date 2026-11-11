package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQueueUploadSpoolFlagsRejectNonpositiveBudget(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-token")
	for _, budget := range []string{"0", "-2"} {
		t.Run(budget, func(t *testing.T) {
			dir := t.TempDir()
			err := run([]string{"queue", "--db", filepath.Join(dir, "queue.db"), "--checkpoints", dir, "--upload-spool-dir", filepath.Join(dir, "uploads"), "--upload-spool-bytes", budget})
			if err == nil || !strings.Contains(err.Error(), "upload-spool-bytes must be positive") {
				t.Fatalf("queue flags not validated: %v", err)
			}
			if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
				t.Fatalf("invalid queue flags started storage: entries=%v error=%v", entries, err)
			}
		})
	}
}
