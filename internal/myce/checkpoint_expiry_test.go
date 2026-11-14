package myce

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

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

	if got := expiryCheckin(t, q, one); len(got) != 0 {
		t.Fatal("settled checkpoint expired before the grace period")
	}
	c.Advance(DefaultCheckpointExpiryGrace)
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
	if expired == nil || !expired.Equal(now.Add(DefaultCheckpointExpiryGrace)) {
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

// Legacy terminal records that never stamped finished_at derive a conservative
// end time from their own record; records with no timestamp are kept and
// counted. The reported root (#1314) held 9,322 flat entries growing by about
// 490 a day; the fixture keeps that age distribution at a tenth of the size.
func TestCheckpointExpiryDatesLegacyRecords(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	root := t.TempDir()
	q := openShardQueue(t, filepath.Join(t.TempDir(), "queue.db"), root, c)
	defer q.Close()
	now := c.Now()
	ptr := func(v time.Time) *time.Time { return &v }
	const total, perDay = 933, 49
	ids := make([]string, total)
	ago := func(i int) time.Duration { return time.Duration(float64(i) / perDay * float64(24*time.Hour)) }
	if err := q.update(func() error {
		for i := range total {
			// Entry i ended i/perDay days ago; newest first.
			ended := now.Add(-ago(i))
			j := &Job{ID: ID(), State: "succeeded", Attempt: 1, ReleaseConfirmed: true}
			j.Checkpoint = filepath.Join(root, j.ID)
			j.Queued = ended.Add(-time.Hour)
			switch {
			case i%200 == 7: // No timestamp anywhere: kept and counted.
				j.Queued = time.Time{}
			case i%100 == 3: // Still active: never eligible.
				j.State, j.ReleaseConfirmed = "running", false
			case i%3 == 0: // Stamped terminal time.
				j.FinishedAt = ptr(ended)
			case i%3 == 1: // Unstamped; the attempt history records the end.
				j.AttemptHistory = []AttemptOutcome{{EndedAt: ended.Add(-time.Minute)}}
				j.Copies = []Copy{{ReleaseConfirmed: true, ReleasedAt: ptr(ended)}}
			default: // Unstamped with only submission/start times (fallback).
				j.StartedAt = ptr(ended.Add(-30 * time.Minute))
			}
			ids[i] = j.ID
			q.putJob(j.ID, j)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	status := *q.checkpointExpiryCounts()
	var due, stamped int
	for _, entry := range q.records.checkpointExpiry {
		if !now.Before(entry.due) {
			due++
		}
	}
	for _, id := range ids {
		if e, ok := q.records.checkpointExpiry[id]; ok && !now.Before(e.due) && q.s.Jobs[id].FinishedAt != nil {
			stamped++
		}
	}
	derived := q.records.checkpointExpiry[ids[1]].due // i=1: attempt history + release.
	fallback := q.records.checkpointExpiry[ids[2]].due
	q.mu.Unlock()
	t.Logf("legacy fixture: %d entries, %d settled with a basis, %d due now (%d stamped as #1314 allowed, %d newly dated), %d undated kept", total, status.Waiting, due, stamped, due-stamped, status.Undated)
	// The conservative end is the latest recorded timestamp (the release).
	if want := now.Add(-ago(1)).Add(DefaultCheckpointExpiryGrace); !derived.Equal(want) {
		t.Fatalf("derived due %s, want %s", derived, want)
	}
	if want := now.Add(-ago(2)).Add(-30 * time.Minute).Add(DefaultCheckpointExpiryGrace); !fallback.Equal(want) {
		t.Fatalf("fallback due %s, want %s", fallback, want)
	}
	undated, active := 0, 0
	for i := range total {
		if i%200 == 7 {
			undated++
		} else if i%100 == 3 {
			active++
		}
	}
	if status.Undated != undated || status.Waiting != total-undated-active {
		t.Fatalf("status %+v, want undated %d waiting %d", status, undated, total-undated-active)
	}
	// Within the grace period nothing derived is due; well beyond it, most are.
	if due-stamped < 400 || stamped > due/2 {
		t.Fatalf("legacy dating made too few entries due: due=%d stamped=%d", due, stamped)
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.CheckpointExpiry == nil || *snap.CheckpointExpiry != status {
		t.Fatalf("status projection %+v", snap.CheckpointExpiry)
	}
	tasks := expiryCheckin(t, q, machine("one", "fake"))
	if len(tasks) != checkpointExpiryBatch {
		t.Fatalf("dispatched %d legacy folders", len(tasks))
	}
	for _, task := range tasks {
		if task.Path != task.ID {
			t.Fatalf("legacy flat path %q", task.Path)
		}
	}
}

// Refused attempts may have left completed copies or unpublished .tmp copies
// before #1607. Expiry removes the entire settled job root, independent of an
// attempt having a save, a history entry, or a successful Started hand-off.
func TestCheckpointExpiryReclaimsOrphanedAttemptCopies(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)}
	root := t.TempDir()
	q := openShardQueue(t, filepath.Join(t.TempDir(), "queue.db"), root, clock)
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	j := submit(t, q, spec("experiment"))
	ended := clock.Now()
	if err := q.update(func() error {
		live := q.touchJob(j.ID)
		live.State, live.Attempt, live.ReleaseConfirmed, live.FinishedAt = "cancelled", 4, true, &ended
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"attempt-1", "attempt-2", "attempt-99", ".myce-prepare-orphan.tmp"} {
		path := filepath.Join(j.Checkpoint, name)
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "pool.bin"), []byte("fake checkpoint payload"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	worker := machine("expiry", "fake")
	clock.Advance(DefaultCheckpointExpiryGrace - time.Nanosecond)
	if got := expiryCheckin(t, q, worker); len(got) != 0 {
		t.Fatal("expired early", got)
	}
	clock.Advance(time.Nanosecond)
	tasks := expiryCheckin(t, q, worker)
	if len(tasks) != 1 || tasks[0].ID != j.ID {
		t.Fatal("orphan copies omitted from retention", tasks)
	}
	removed := 0
	report := expireCheckpoint(root, tasks[0], func(path string) error {
		removed++
		if path != j.Checkpoint {
			t.Fatal("not the whole owned job root", path)
		}
		return os.RemoveAll(path)
	})
	if !report.Removed || removed != 1 {
		t.Fatal("orphan copies not reclaimed", report)
	}
	if _, err := os.Stat(j.Checkpoint); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("orphan copy survived retention", err)
	}
	expiryCheckin(t, q, worker, report)
	if job(t, q, j.ID).CheckpointExpired == nil {
		t.Fatal("expiry not acknowledged")
	}
}
