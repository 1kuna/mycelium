package myce

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #201: B70 kept every finished attempt's job-files folder (13 GB in four days).
func TestWorkerRetentionRemovesUnclaimedAttemptFiles(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)}
	folders := []string{"accepted/1", "crashed/2", "live/2", "live/3", "live/.inputs-1", "orphan/.inputs-2"}
	for _, name := range folders {
		if err := os.MkdirAll(filepath.Join(dir, "job-files", name, ".venv"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	// Accepted completion clears custody; a crash leaves no attempt state at all.
	if err := saveWorkerJSON(dir, "state-attempt-accepted.json", workerState{Session: "s"}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(dir, "state-attempt-live.json", workerState{Session: "s", Job: &Job{ID: "live", Attempt: 3, Session: "s"}}); err != nil {
		t.Fatal(err)
	}
	finished := filepath.Join(dir, ID()+".log")
	if err := os.WriteFile(finished, []byte("synthetic"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(finished, clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}

	if err := workerRetention(dir, clock.Now(), DefaultLogRetention); err != nil {
		t.Fatal(err)
	}
	for _, gone := range []string{"accepted", "crashed", "live/2", "orphan"} {
		if _, err := os.Stat(filepath.Join(dir, "job-files", gone)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unclaimed attempt files %s retained: %v", gone, err)
		}
	}
	for _, kept := range []string{"live/3/.venv", "live/.inputs-1"} {
		if _, err := os.Stat(filepath.Join(dir, "job-files", kept)); err != nil {
			t.Fatalf("held attempt files %s removed: %v", kept, err)
		}
	}
	if _, err := os.Stat(finished); err != nil {
		t.Fatal("finished log removed before its retention window")
	}

	// Custody, not age, protects attempt files; logs keep their own window.
	clock.Advance(8 * 24 * time.Hour)
	if err := workerRetention(dir, clock.Now(), DefaultLogRetention); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "job-files", "live", "3", ".venv")); err != nil {
		t.Fatal("long-running held attempt lost its files")
	}
	if _, err := os.Stat(finished); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expired log retained")
	}
}
