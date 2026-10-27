package myce

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestHistoryRetentionReplayRestartAndExpiry(t *testing.T) {
	q, c := fixture(t)
	s := keyedSpec()
	s.Priority = "experiment"
	j := submit(t, q, s)
	assigned := check(t, q, machine("one", "fake"), nil).Job
	finish(t, q, assigned, true)
	c.Advance(jobHistoryRetention - time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[j.ID] == nil {
		t.Fatal("job removed before result window")
	}
	c.Advance(time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[j.ID] != nil {
		t.Fatal("terminal job not retired")
	}
	q = reopenCancellationQueue(t, q, c)
	if got := submit(t, q, s); got.ID != j.ID || got.State != "succeeded" {
		t.Fatal("retained replay changed", got)
	}
	changed := s
	changed.Name = "changed"
	_, err := q.Submit(changed)
	submissionError(t, err, 409, "submission_key_conflict")
	if err := q.Complete(result(assigned, true)); !errors.Is(err, ErrStale) {
		t.Fatal("pruned attempt accepted", err)
	}
	c.Advance(submissionRetention - time.Nanosecond)
	if got := submit(t, q, s); got.ID != j.ID {
		t.Fatal("early key expiry")
	}
	c.Advance(time.Nanosecond)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, c)
	_, err = q.Submit(s)
	submissionError(t, err, 410, "submission_key_expired")
	if r := q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)]; r.Kind != submissionExpired || r.Job != nil {
		t.Fatal("full answer survived expiry", r)
	}
}

func TestHistoryRetentionBatchRollbackAndCustody(t *testing.T) {
	q, c := fixture(t)
	// Real row deletion must roll back with its full replay answer on commit failure.
	for i := 0; i < fileReclaimBatch+3; i++ {
		s := spec("experiment")
		s.SubmissionKey = fmt.Sprint("key-", i)
		ended := c.Now()
		j := &Job{ID: s.SubmissionKey, Spec: s, State: "succeeded", Queued: ended, FinishedAt: &ended}
		if err := q.update(func() error {
			q.putJob(j.ID, j)
			q.putSubmission(submissionDigest(s.Project, s.SubmissionKey), &submissionRecord{Kind: submissionBinding, JobID: j.ID, Job: j})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Unreleased current and historical copies remain available regardless of age.
	if err := q.update(func() error { j := q.touchJob("key-0"); j.Attempt = 1; j.ReleaseConfirmed = false; return nil }); err != nil {
		t.Fatal(err)
	}
	c.Advance(jobHistoryRetention)
	if err := q.pruneBatchLogs(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`CREATE TRIGGER reject_prune BEFORE DELETE ON queue_state BEGIN SELECT RAISE(ABORT,'prune failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := q.pruneJobHistory(); err == nil {
		t.Fatal("failed commit accepted")
	}
	if len(q.s.Jobs) != fileReclaimBatch+3 {
		t.Fatal("failed commit lost jobs")
	}
	for _, r := range q.s.Submissions {
		if r.RemovedAt != nil || r.Job != q.s.Jobs[r.JobID] {
			t.Fatal("rollback lost retained alias", r)
		}
	}
	if _, err := q.db.Exec(`DROP TRIGGER reject_prune`); err != nil {
		t.Fatal(err)
	}
	before := len(q.s.Jobs)
	if err := q.pruneJobHistory(); err != nil {
		t.Fatal(err)
	}
	if n := before - len(q.s.Jobs); n == 0 || n > fileReclaimBatch {
		t.Fatal("unbounded or stalled batch", n)
	}
	if q.s.Jobs["key-0"] == nil {
		t.Fatal("unconfirmed custody removed")
	}
	// A second wave drains the backlog (including the final log-cleanup wave).
	if err := q.pruneBatchLogs(); err != nil {
		t.Fatal(err)
	}
	if err := q.pruneJobHistory(); err != nil {
		t.Fatal(err)
	}
	if len(q.s.Jobs) != 1 {
		t.Fatal("history backlog not drained", len(q.s.Jobs))
	}
	q = reopenCancellationQueue(t, q, c)
	if q.s.Jobs["key-0"] == nil || len(q.s.Jobs) != 1 {
		t.Fatal("restart lost protected custody")
	}
}

func TestHistoryRetentionProtectedWaveDoesNotStarve(t *testing.T) {
	q, c := fixture(t)
	ended := c.Now().Add(-2 * jobHistoryRetention)
	if err := q.update(func() error {
		for i := 0; i < fileReclaimBatch; i++ {
			id := fmt.Sprint("protected-", i)
			q.putJob(id, &Job{ID: id, State: "failed", FinishedAt: &ended, Attempt: 1})
		}
		later := ended.Add(time.Hour)
		q.putJob("eligible", &Job{ID: "eligible", State: "succeeded", FinishedAt: &later})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if q.s.Jobs["eligible"] != nil {
		t.Fatal("protected expired wave starved eligible history")
	}
	if len(q.s.Jobs) != fileReclaimBatch {
		t.Fatal("unconfirmed attempts lost")
	}
}

func TestHistoryRetentionMissingTimestampStartsOnce(t *testing.T) {
	q, c := fixture(t)
	if err := q.update(func() error { q.putJob("legacy", &Job{ID: "legacy", State: "failed"}); return nil }); err != nil {
		t.Fatal(err)
	}
	observed := c.Now()
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if j := q.s.Jobs["legacy"]; j == nil || j.FinishedAt == nil || !j.FinishedAt.Equal(observed) {
		t.Fatal("first maintenance observation not durable", j)
	}
	c.Advance(jobHistoryRetention / 2)
	q = reopenCancellationQueue(t, q, c)
	if !q.s.Jobs["legacy"].FinishedAt.Equal(observed) {
		t.Fatal("restart moved retention window")
	}
	c.Advance(jobHistoryRetention / 2)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs["legacy"] != nil {
		t.Fatal("missing timestamp started a second retention window")
	}
}

func TestHistoryRetentionTransfersSharedBlobCleanup(t *testing.T) {
	q, c := fixture(t)
	client := retentionClient(t, q)
	_, ref := retentionUpload(t, client, "shared retention bytes")
	s := spec("experiment")
	s.Files = FileManifest{"input": ref}
	s.SubmissionKey = "retiring-input"
	old := submit(t, q, s)
	s.SubmissionKey = "current-input"
	live := submit(t, q, s)
	if err := q.Cancel(old.ID); err != nil {
		t.Fatal(err)
	}
	c.Advance(jobHistoryRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[old.ID] != nil || !q.s.RetiredJobFiles[ref.SHA256] {
		t.Fatal("shared cleanup retained full job or lost cleanup ownership")
	}
	storeRoot := q.files.root
	q = reopenCancellationQueue(t, q, c)
	if err := q.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	c.Advance(submissionRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if !q.s.RetiredJobFiles[ref.SHA256] {
		t.Fatal("key expiry lost deferred cleanup")
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("shared file deleted", err)
	}
	if err := q.Cancel(live.ID); err != nil {
		t.Fatal(err)
	}
	c.Advance(jobHistoryRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.RetiredJobFiles[ref.SHA256] {
		t.Fatal("released shared blob cleanup did not finish")
	}
}
