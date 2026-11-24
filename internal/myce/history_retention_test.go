package myce

import (
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
	if q.s.Jobs[j.ID] == nil {
		t.Fatal("custody ledger retired")
	}
	q = reopenCancellationQueue(t, q, c)
	if got := submit(t, q, s); got.ID != j.ID || got.State != "succeeded" {
		t.Fatal("retained replay changed", got)
	}
	changed := s
	changed.Name = "changed"
	_, err := q.Submit(changed)
	submissionError(t, err, 409, "submission_key_conflict")
	if err := q.Complete(result(assigned, true)); err != nil {
		t.Fatal("exact completion replay lost", err)
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
	replay, err := q.Submit(s)
	if err != nil || replay.ID != j.ID {
		t.Fatal("durable replay expired", err)
	}
	if r := q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)]; r.Kind != submissionBinding || r.Job == nil {
		t.Fatal("durable answer lost without release", r)
	}
}

// Finished records expire 14 days after they finish and leave a summary;
// learned memory evidence is unchanged across expiry and restart; open work,
// held work and after: parents stay.

func TestJobRecordExpiryKeyedReplay(t *testing.T) {
	q, c := fixture(t)
	s := keyedSpec()
	j := submit(t, q, s)
	assigned := check(t, q, machine("one", "fake"), nil).Job
	finish(t, q, assigned, true)
	check(t, q, machine("one", "fake"), nil) // The worker confirms release.
	c.Advance(jobRecordRetention + time.Hour)
	// A worker-confirmed checkpoint removal; until then the record stays.
	if err := q.Tick(); err != nil || q.s.Jobs[j.ID] == nil {
		t.Fatal("record left before its checkpoint folder", err)
	}
	if err := q.update(func() error { removed := c.Now(); q.touchJob(j.ID).CheckpointExpired = &removed; return nil }); err != nil {
		t.Fatal(err)
	}
	c.Advance(jobExpiryRecheck)
	for range 4 {
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if q.s.Jobs[j.ID] != nil {
		t.Fatal("keyed record kept")
	}
	q = reopenCancellationQueue(t, q, c)
	got, err := q.Submit(s)
	if err != nil || got.ID != j.ID || got.State != "succeeded" {
		t.Fatal("keyed replay lost", got, err)
	}
	r := q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)]
	if r == nil || r.Job == nil || len(r.Job.AttemptHistory) > 0 || r.Job.Timing != nil {
		t.Fatal("keyed answer kept the full record", r)
	}
}

func TestHistoryRetentionReleasedFilesKeepSharedCustody(t *testing.T) {
	q, clock := fixture(t)
	client := retentionClient(t, q)
	_, ref := retentionUpload(t, client, "shared retention bytes")
	s := spec("experiment")
	s.Files = FileManifest{"input": ref}
	old := submit(t, q, s)
	live := submit(t, q, s)
	if err := q.Cancel(old.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[old.ID] == nil {
		t.Fatal("file expiry erased unique history")
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("open work lost shared data", err)
	}
	if err := q.Cancel(live.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention)
	for len(q.records.files.scheduled) > 0 {
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
	}
}
