package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #201: a day of successful gates filled the M4 even though their outputs
// were already accepted. Only durable publication confirmation shortens grace.
func TestWorkerRetentionRemovesPublishedAttemptFiles(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"published", "failed", "live", "pending"} {
		if err := os.MkdirAll(filepath.Join(dir, "job-files", name, "1", ".venv"), 0700); err != nil {
			t.Fatal(err)
		}
		st := workerState{PublishedAttempt: &Execution{ID: name, Attempt: 1}}
		switch name {
		case "failed":
			st.PublishedAttempt = nil
		case "live":
			st.Job = &Job{ID: name, Attempt: 1}
		case "pending":
			st.Job = &Job{ID: name, Attempt: 1}
			st.Result = &Result{ReleasePending: true}
		}
		if err := saveWorkerJSON(dir, "state-attempt-"+name+".json", st); err != nil {
			t.Fatal(err)
		}
	}
	// Reading saved records simulates a restart between acknowledgement and cleanup.
	if err := workerRetention(dir, now, testLogRetention); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "job-files", "published")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published attempt retained", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state-attempt-published.json")); err != nil {
		t.Fatal("small diagnostic record removed", err)
	}
	for _, at := range []time.Time{now.Add(23 * time.Hour), now.Add(24 * time.Hour)} {
		if err := workerRetention(dir, at, testLogRetention); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"failed", "live", "pending"} {
			_, err := os.Stat(filepath.Join(dir, "job-files", name, "1", ".venv"))
			if err != nil {
				t.Fatalf("%s removed before eligible: %v", name, err)
			}
		}
	}
}

// A successful process alone is insufficient: release and queue acceptance
// belong to the exact attempt, and a cancelled success may have no accepted output.
func TestPublishedAttemptFilesRequiresAcceptedResult(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	_, ref := retentionUpload(t, c, "result")
	s := spec("experiment")
	s.Files = FileManifest{"input": ref}
	s.Outputs = map[string]string{"output": "results"}
	j := submit(t, q, s)
	m := machine("worker", "fake", "session")
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("not assigned")
	}
	r := Result{Execution: Execution{ID: j.ID, Attempt: assigned.Attempt}, Machine: m.ID, Session: m.Session, Success: true, OutputRefs: FileManifest{"output": ref}}
	st := workerState{Job: assigned, Result: &r}
	w := Worker{Client: c}
	confirm := func(want bool) {
		t.Helper()
		published, err := w.publishedAttemptFiles(context.Background(), st)
		if err != nil || (published != nil) != want {
			t.Fatalf("publication=%v want=%t: %v", published, want, err)
		}
	}
	confirm(false) // Uploaded bytes have not been accepted as a result.
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	confirm(true)
	r.ReleasePending = true
	confirm(false)
	r.ReleasePending = false
	r.Attempt++
	confirm(false)
}

// #201: A worker kept every finished attempt's job-files folder (13 GB in four days).
func TestWorkerRetentionRemovesUnclaimedAttemptFiles(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)}
	folders := []string{"accepted/1", "crashed/2", "live/2", "live/3", "live/.inputs-1", "orphan/.inputs-2"}
	for _, name := range folders {
		if err := os.MkdirAll(filepath.Join(dir, "job-files", name, ".venv"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	// Unknown read-only Docker output and ordinary unknown bytes keep custody.
	if os.Geteuid() != 0 {
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
		if err := workerRetention(dir, clock.Now(), testLogRetention); err != nil {
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

	// Unknown/unpublished contents have no verified replacement; age alone
	// cannot authorize their removal.
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
	for _, kept := range []string{"accepted", "crashed", "live/2", "orphan"} {
		if !exists(kept) {
			t.Fatalf("unpublished attempt files %s lost on age", kept)
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

	if os.Geteuid() != 0 {
		now := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
		for _, age := range []time.Duration{2 * AttemptFilesGrace} {
			if err := workerRetention(dir, now.Add(age), testLogRetention); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"stuck", "done"} {
				if _, err := os.Stat(filepath.Join(dir, "job-files", name, "1")); err != nil {
					t.Fatal("unknown data retired on age", name, err)
				}
			}
		}

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
