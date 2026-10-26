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

	pass := func() {
		t.Helper()
		if err := workerRetention(dir, clock.Now(), DefaultLogRetention); err != nil {
			t.Fatal(err)
		}
	}
	exists := func(name string) bool {
		_, err := os.Stat(filepath.Join(dir, "job-files", name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		return err == nil
	}

	// Unclaimed folders keep failed runs' files for diagnosis for a day.
	pass()
	clock.Advance(23 * time.Hour)
	pass()
	for _, name := range []string{"accepted/1/.venv", "crashed/2/.venv", "live/2/.venv", "orphan/.inputs-2/.venv"} {
		if !exists(name) {
			t.Fatalf("unclaimed attempt files %s removed inside the grace", name)
		}
	}
	clock.Advance(time.Hour)
	pass()
	for _, gone := range []string{"accepted", "crashed", "live/2", "orphan"} {
		if exists(gone) {
			t.Fatalf("unclaimed attempt files %s retained after the grace", gone)
		}
	}
	for _, kept := range []string{"live/3/.venv", "live/.inputs-1"} {
		if !exists(kept) {
			t.Fatalf("held attempt files %s removed", kept)
		}
	}
	if _, err := os.Stat(finished); err != nil {
		t.Fatal("finished log removed before its retention window")
	}

	// Custody, not age, protects attempt files; logs keep their own window.
	clock.Advance(8 * 24 * time.Hour)
	pass()
	if !exists("live/3/.venv") {
		t.Fatal("long-running held attempt lost its files")
	}
	if _, err := os.Stat(finished); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expired log retained")
	}
}

func TestAttemptFilesRetentionRefusesLinkedRoot(t *testing.T) {
	dir, target := t.TempDir(), t.TempDir()
	victim := filepath.Join(target, "job", "1")
	if err := os.MkdirAll(victim, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "job-files")); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	for _, at := range []time.Time{now, now.Add(48 * time.Hour)} {
		if err := pruneAttemptFiles(dir, at); err == nil {
			t.Fatal("linked job-files root accepted")
		}
	}
	entries, err := os.ReadDir(victim)
	if err != nil || len(entries) != 0 {
		t.Fatalf("link target touched: %v %v", entries, err)
	}
}

// Root-owned Docker output cannot be removed; it must not stop other cleanup
// or report the same failure every minute.
func TestAttemptFilesRetentionRetriesStuckFolderDaily(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can remove read-only folders")
	}
	dir := t.TempDir()
	stuck := filepath.Join(dir, "job-files", "stuck", "1", "out")
	if err := os.MkdirAll(stuck, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stuck, "root-owned"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stuck, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stuck, 0700) })
	if err := os.MkdirAll(filepath.Join(dir, "job-files", "done", "1"), 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	if err := pruneAttemptFiles(dir, now); err != nil {
		t.Fatal(err)
	}
	now = now.Add(AttemptFilesGrace)
	if err := pruneAttemptFiles(dir, now); err == nil {
		t.Fatal("stuck removal not reported")
	}
	if _, err := os.Stat(filepath.Join(dir, "job-files", "done")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stuck folder blocked other cleanup")
	}
	now = now.Add(time.Minute)
	if err := pruneAttemptFiles(dir, now); err != nil {
		t.Fatalf("stuck folder reported again a minute later: %v", err)
	}
	now = now.Add(AttemptFilesGrace)
	if err := pruneAttemptFiles(dir, now); err == nil {
		t.Fatal("stuck folder not retried after a day")
	}
}
