package myce

import (
	"context"
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

// #2202: an after: parent and its keyed dependent are released and collected,
// then the dependent's record expires into a replay. Returns the queue, the
// parent and the parent's reclaimed file.
func prunedReleasedDependent(t *testing.T) (*Queue, *fakeClock, *Job, *Job, FileRef) {
	t.Helper()
	q, clock, c := retentionFixture(t)
	if err := q.ConfigureLogRetention(0); err != nil {
		t.Fatal(err)
	}
	parent, _, ref := releaseTerminal(t, q, c, "parent")
	dependent, _, _ := releaseTerminal(t, q, c, "dependent")
	if err := q.update(func() error {
		d := q.touchJob(dependent.ID)
		d.Spec.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"upstream": "result"}}
		d.Attempt, d.ReleaseConfirmed = 1, true
		d.Copies = []Copy{{Attempt: 1, ReleaseConfirmed: true}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Second)
	applyFixtureRelease(t, q, releasePlan(t, q, releaseSelection(parent, dependent)))
	collectFixtureBlobs(t, q)
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("fixture did not reclaim the parent's file", err)
	}
	assertReleaseSettled(t, q, parent, ref, "before expiry")
	// A host still holding the dependent keeps the chain pending across
	// expiry: the replay keeps the job ID that HostReleases names.
	if err := q.update(func() error {
		q.s.HostReleases = map[string]HostRelease{"held": {Job: dependent.ID}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(jobRecordRetention + time.Second)
	if err := q.pruneJobHistory(); err != nil {
		t.Fatal(err)
	}
	r := q.s.Submissions[q.records.bindings[dependent.ID]]
	if q.s.Jobs[dependent.ID] != nil || q.s.Jobs[parent.ID] == nil || r == nil || !r.FileCustodyPruned {
		t.Fatalf("fixture did not prune only the dependent: dependent=%t parent=%t", q.s.Jobs[dependent.ID] != nil, q.s.Jobs[parent.ID] != nil)
	}
	if !q.jobReleasePending(q.s.Jobs[parent.ID]) || !q.cancellationObservation().jobReleasePending(q.s.Jobs[parent.ID]) {
		t.Fatal("pruning dropped a release a host still holds")
	}
	if err := q.update(func() error { q.s.HostReleases = nil; return nil }); err != nil {
		t.Fatal(err)
	}
	return q, clock, parent, dependent, ref
}

func assertReleaseSettled(t *testing.T, q *Queue, parent *Job, ref FileRef, when string) {
	t.Helper()
	p := q.s.Jobs[parent.ID]
	if q.jobReleasePending(p) || q.cancellationObservation().jobReleasePending(p) || q.observedRelease(p) == "pending" {
		t.Fatalf("%s: settled release reopened", when)
	}
	report, err := q.ProjectFileHoldings(context.Background(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range report.Files {
		if f.SHA256 == ref.SHA256 {
			t.Fatalf("%s: reclaimed file held again: %+v", when, f)
		}
	}
	// The collector must not keep the reclaimed digest for the parent either.
	q.mu.RLock()
	defer q.mu.RUnlock()
	q.visitDurableFileReferences(false, nil, func(r FileRef, flags fileReferenceFlags, owner FileKeepingReference) {
		if r.SHA256 == ref.SHA256 && flags&fileKept != 0 {
			t.Fatalf("%s: collector keeps reclaimed file for %s", when, owner.JobID)
		}
	})
}

func TestHistoryExpiryKeepsDependentReleaseSettled(t *testing.T) {
	q, clock, parent, _, ref := prunedReleasedDependent(t)
	assertReleaseSettled(t, q, parent, ref, "after expiry")
	q = reopenCancellationQueue(t, q, clock)
	assertReleaseSettled(t, q, parent, ref, "after reopen")
}

// Replays saved before #2202 have no ReleaseConfirmed; older ones also lack
// the pruned marker, which open restores from the history summary.
func TestHistoryExpiryLegacyReplayLoadsReleased(t *testing.T) {
	for _, unmarked := range []bool{false, true} {
		q, clock, parent, dependent, ref := prunedReleasedDependent(t)
		if err := q.update(func() error {
			r := q.touchSubmission(q.records.bindings[dependent.ID])
			replay := *r.Job
			replay.ReleaseConfirmed = false
			r.Job, r.FileCustodyPruned = &replay, !unmarked
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		q = reopenCancellationQueue(t, q, clock)
		assertReleaseSettled(t, q, parent, ref, fmt.Sprintf("legacy replay (unmarked=%t)", unmarked))
	}
}
