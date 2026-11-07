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

func TestHistoryRetentionUnkeyedIntentAndCancellationSurviveLogPolicy(t *testing.T) {
	q, clock := fixture(t)
	q.fileRetention = 0
	j := submit(t, q, spec("experiment"))
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10000 * 24 * time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, clock)
	got, err := q.jobSnapshot(j.ID)
	if err != nil || got.State != "cancelled" || got.Queued != j.Queued || got.Spec.Name != j.Spec.Name {
		t.Fatal("unique unkeyed history expired with logs", got, err)
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
