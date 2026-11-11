package myce

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func openShardQueue(t *testing.T, db, root string, c Clock) *Queue {
	t.Helper()
	q, err := Open(db, root, c, WithCheckpointShards())
	if err != nil {
		t.Fatal(err)
	}
	if done := q.kickCheckpointShards(); done != nil {
		<-done
	}
	return q
}

func writeSave(t *testing.T, dir string, generation string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"myce_checkpoint":{"generation":`+generation+`},"value":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
}

// #894: new jobs shard; jobs recorded with flat folders keep them and resume.
func TestCheckpointShardsKeepLegacyFlatJobsResumable(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, c)
	if err != nil {
		t.Fatal(err)
	}
	legacy := submit(t, q, spec("experiment"))
	if legacy.Checkpoint != filepath.Join(root, legacy.ID) {
		t.Fatalf("flat layout changed: %s", legacy.Checkpoint)
	}
	writeSave(t, filepath.Join(legacy.Checkpoint, "attempt-1"), "3")
	q.Close()

	q = openShardQueue(t, db, root, c)
	defer q.Close()
	for _, shard := range []string{"00", "7f", "ff"} {
		if info, err := os.Stat(filepath.Join(root, shard)); err != nil || !info.IsDir() {
			t.Fatalf("shard %s not prepared: %v", shard, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, checkpointShardMarker)); err != nil {
		t.Fatal("marker missing after preparation")
	}
	fresh := submit(t, q, spec("experiment"))
	if fresh.Checkpoint != filepath.Join(root, fresh.ID[:2], fresh.ID) {
		t.Fatalf("new job not sharded: %s", fresh.Checkpoint)
	}
	q.mu.Lock()
	kept := q.s.Jobs[legacy.ID].Checkpoint
	q.mu.Unlock()
	if kept != filepath.Join(root, legacy.ID) {
		t.Fatalf("legacy job moved: %s", kept)
	}
	// Predecessor workers only require the folder's parent to exist already.
	if info, err := os.Stat(filepath.Dir(fresh.Checkpoint)); err != nil || !info.IsDir() {
		t.Fatal("sharded parent missing for predecessor workers")
	}

	resumed := Job{ID: legacy.ID, Attempt: 2, Checkpoint: kept}
	dest, err := prepareCheckpoint(resumed)
	if err != nil {
		t.Fatal(err)
	}
	if progress, err := checkpointProgress(dest); err != nil || progress != 3 {
		t.Fatalf("legacy flat checkpoint did not resume: %d %v", progress, err)
	}

	// A removed shard is recreated beneath an existing root; a missing root is
	// still refused rather than created.
	if err := os.Remove(filepath.Dir(fresh.Checkpoint)); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCheckpoint(Job{ID: fresh.ID, Attempt: 1, Checkpoint: fresh.Checkpoint}); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "unmounted")
	if _, err := prepareCheckpoint(Job{ID: fresh.ID, Attempt: 1, Checkpoint: filepath.Join(missing, fresh.ID[:2], fresh.ID)}); err == nil {
		t.Fatal("missing root accepted")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing root was created")
	}
}

func TestCheckpointShardPreparationRetriesWithoutCreatingRoot(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	root := filepath.Join(t.TempDir(), "unmounted")
	q := openShardQueue(t, filepath.Join(t.TempDir(), "queue.db"), root, c)
	defer q.Close()
	if q.shards.ready.Load() {
		t.Fatal("missing root reported ready")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("preparation created the root")
	}
	if j := submit(t, q, spec("experiment")); j.Checkpoint != filepath.Join(root, j.ID) {
		t.Fatal("unprepared root assigned a shard")
	}
	if q.kickCheckpointShards() != nil {
		t.Fatal("retried before the backoff")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	c.Advance(checkpointShardRetry)
	if done := q.kickCheckpointShards(); done == nil {
		t.Fatal("did not retry after the backoff")
	} else {
		<-done
	}
	if !q.shards.ready.Load() {
		t.Fatal("not ready after the root appeared")
	}
}

func expiryCheckin(t *testing.T, q *Queue, m Machine, reports ...CheckpointExpiryReport) []CheckpointExpiry {
	t.Helper()
	v, err := q.Checkin(Checkin{Machine: m, period: time.Second, CheckpointExpiryProtocol: checkpointExpiryProtocol, CheckpointExpiryReports: reports})
	if err != nil {
		t.Fatal(err)
	}
	if v.CheckpointExpiryProtocol != checkpointExpiryProtocol {
		t.Fatal("protocol not advertised")
	}
	return v.CheckpointExpiries
}

func expiryIDs(tasks []CheckpointExpiry) []string {
	var ids []string
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	slices.Sort(ids)
	return ids
}

// Only terminal jobs with delivered outputs and released attempts expire; claims
// prevent duplicate work, failures back off, and removals persist.
func TestCheckpointExpiryUsesSettledRecordsAndClaims(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q := openShardQueue(t, db, root, c)
	fakeCheckpointAvailability(t, q, true)
	jobs := map[string]*Job{}
	for _, name := range []string{"succeeded", "legacy", "queued", "delivery", "release", "unstarted", "files"} {
		jobs[name] = submit(t, q, spec("experiment"))
	}
	now := c.Now()
	settle := func(j *Job, state string) {
		j.State, j.Attempt, j.ReleaseConfirmed, j.Copies, j.FinishedAt = state, 1, true, nil, &now
	}
	if err := q.update(func() error {
		settle(q.touchJob(jobs["succeeded"].ID), "succeeded")
		legacy := q.touchJob(jobs["legacy"].ID)
		settle(legacy, "failed")
		legacy.Checkpoint = filepath.Join(root, legacy.ID)
		settle(q.touchJob(jobs["delivery"].ID), "succeeded")
		q.touchJob(jobs["delivery"].ID).Delivery = &OutputDelivery{State: "pending"}
		settle(q.touchJob(jobs["release"].ID), "cancelled")
		q.touchJob(jobs["release"].ID).ReleaseConfirmed = false
		settle(q.touchJob(jobs["unstarted"].ID), "cancelled")
		q.touchJob(jobs["unstarted"].ID).Attempt = 0
		settle(q.touchJob(jobs["files"].ID), "succeeded")
		q.touchJob(jobs["files"].ID).FileTransport = true
		q.touchJob(jobs["queued"].ID).Spec.Needs.MemoryGB = 1024 // Never placed here.
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	one, two := machine("one", "fake"), machine("two", "fake")

	if v := check(t, q, one, nil); len(v.CheckpointExpiries) != 0 {
		t.Fatal("expiry sent to a worker that did not advertise the protocol")
	}
	tasks := expiryCheckin(t, q, one)
	want := []string{jobs["legacy"].ID, jobs["succeeded"].ID}
	slices.Sort(want)
	if got := expiryIDs(tasks); !slices.Equal(got, want) {
		t.Fatalf("expiries = %v, want settled %v", got, want)
	}
	for _, task := range tasks {
		path := jobs["succeeded"].ID[:2] + "/" + jobs["succeeded"].ID
		if task.ID == jobs["legacy"].ID {
			path = task.ID
		}
		if task.Path != path {
			t.Fatalf("path %q, want %q", task.Path, path)
		}
	}
	if got := expiryCheckin(t, q, two); len(got) != 0 {
		t.Fatal("live claim reissued to another worker")
	}
	if got := expiryCheckin(t, q, one); len(got) != 0 {
		t.Fatal("busy worker received another batch")
	}
	expiryCheckin(t, q, one, CheckpointExpiryReport{ID: jobs["succeeded"].ID, Removed: true}, CheckpointExpiryReport{ID: jobs["legacy"].ID, Error: "remove_failed"})
	q.mu.Lock()
	expired := q.s.Jobs[jobs["succeeded"].ID].CheckpointExpired
	q.mu.Unlock()
	if expired == nil || !expired.Equal(now) {
		t.Fatal("removal not recorded")
	}
	if got := expiryCheckin(t, q, two); len(got) != 0 {
		t.Fatal("failed removal retried before its claim lapsed")
	}
	c.Advance(checkpointExpiryAllowance)
	if got := expiryIDs(expiryCheckin(t, q, two)); !slices.Equal(got, []string{jobs["legacy"].ID}) {
		t.Fatalf("lapsed claim not reissued: %v", got)
	}
	// A departed worker cannot finish its batch; it is reissued at once.
	if err := q.update(func() error { q.touchMachine(two.ID); q.s.Machines[two.ID].Gone = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if got := expiryIDs(expiryCheckin(t, q, machine("three", "fake"))); !slices.Equal(got, []string{jobs["legacy"].ID}) {
		t.Fatalf("departed worker's claim not reissued: %v", got)
	}
	q.Close()

	q = openShardQueue(t, db, root, c)
	defer q.Close()
	q.mu.Lock()
	_, stillPending := q.records.checkpointExpiry[jobs["succeeded"].ID]
	_, legacyPending := q.records.checkpointExpiry[jobs["legacy"].ID]
	q.mu.Unlock()
	if stillPending || !legacyPending {
		t.Fatal("recorded removal did not survive restart")
	}
}

func TestWorkerCheckpointExpiryRemovesOnlyVerifiedJobFolders(t *testing.T) {
	root := t.TempDir()
	id, flat := "ab0123456789abcdef0123456789abcd", "cd0123456789abcdef0123456789abcd"
	sharded := filepath.Join(root, "ab", id)
	writeSave(t, filepath.Join(sharded, "attempt-1"), "1")
	writeSave(t, filepath.Join(root, flat, "attempt-2"), "2")
	w := Worker{checkpointExpiry: newWorkerCheckpointExpiry()}
	run := func(tasks ...CheckpointExpiry) []CheckpointExpiryReport {
		t.Helper()
		e := w.checkpointExpiry
		e.reports = nil
		if done := w.expireCheckpoints(tasks, root); done != nil {
			<-done
		}
		return e.reports
	}

	// Without the queue's marker the root is unverified (for example, an empty
	// mountpoint) and nothing is removed or reported removed.
	if r := run(CheckpointExpiry{ID: id, Path: "ab/" + id}); r[0].Removed || r[0].Error != "root_unverified" {
		t.Fatalf("unverified root: %+v", r)
	}
	if _, err := os.Stat(sharded); err != nil {
		t.Fatal("folder removed from an unverified root")
	}
	if err := prepareCheckpointShards(root); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), id)
	writeSave(t, outside, "1")
	if err := os.Remove(filepath.Join(root, "ef")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(outside), filepath.Join(root, "ef")); err != nil {
		t.Fatal(err)
	}
	reports := run(
		CheckpointExpiry{ID: id, Path: "../" + id},
		CheckpointExpiry{ID: id, Path: "cd/" + id},
		CheckpointExpiry{ID: "ef" + id[2:], Path: "ef/ef" + id[2:]},
		CheckpointExpiry{ID: id, Path: "ab/" + id},
		CheckpointExpiry{ID: flat, Path: flat},
		CheckpointExpiry{ID: "ab1111111111111111111111111111aa", Path: "ab/ab1111111111111111111111111111aa"},
	)
	wantErrors := []string{"invalid_path", "invalid_path", "unexpected_parent", "", "", ""}
	for i, r := range reports {
		if r.Error != wantErrors[i] || r.Removed != (wantErrors[i] == "") {
			t.Fatalf("report %d = %+v, want error %q", i, r, wantErrors[i])
		}
	}
	for _, gone := range []string{sharded, filepath.Join(root, flat)} {
		if _, err := os.Stat(gone); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s not removed", gone)
		}
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatal("symlinked shard target removed")
	}

	w.checkpointExpiry.remove = func(string) error { return errors.New("busy") }
	writeSave(t, sharded, "1")
	if r := run(CheckpointExpiry{ID: id, Path: "ab/" + id}); r[0].Removed || r[0].Error != "remove_failed" {
		t.Fatalf("failed removal reported as %+v", r)
	}

	// Reports wait for the queue's advertisement and leave only once acknowledged.
	w.checkpointExpiry.supported = false
	if c, sent := w.checkpointExpiryCheckin(Checkin{}); sent != 0 || c.CheckpointExpiryProtocol != "" {
		t.Fatal("reports sent before the queue advertised the protocol")
	}
	w.acceptCheckpointExpiryReply(0, Reply{CheckpointExpiryProtocol: checkpointExpiryProtocol})
	c, sent := w.checkpointExpiryCheckin(Checkin{})
	if sent != 1 || len(c.CheckpointExpiryReports) != 1 || c.CheckpointExpiryProtocol != checkpointExpiryProtocol {
		t.Fatalf("advertised check-in carried %d reports", sent)
	}
	w.acceptCheckpointExpiryReply(sent, Reply{})
	if len(w.checkpointExpiry.reports) != 0 || w.checkpointExpiry.supported {
		t.Fatal("acknowledged reports retained or support kept after a non-advertising reply")
	}
}
