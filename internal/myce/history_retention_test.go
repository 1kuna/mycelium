package myce

import (
	"context"
	"errors"
	"maps"
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
func TestJobRecordExpiryKeepsSummaryAndLearning(t *testing.T) {
	q, _ := candidateCostFixture(t)
	clock := q.clock.(*fakeClock)
	source := storedTestJob(t, q, "retained-001")
	key := *source.Timing.Key
	configuration := memoryConfiguration(source.Spec.Needs, source.Spec.Args, source.Spec.Program)
	evidence := func(stage string, q *Queue) memoryConstraintEvidence {
		t.Helper()
		q.mu.Lock()
		defer q.mu.Unlock()
		q.memoryPlanning = newMemoryPlanningCache()
		cached := q.memoryConstraintEvidence(key, configuration, false)
		q.memoryPlanning = nil
		full := q.memoryConstraintEvidenceFrom(key, configuration, maps.Values(q.s.Jobs), false, time.Time{})
		if cached != full {
			t.Fatalf("%s: cached %+v, full %+v", stage, cached, full)
		}
		return cached
	}
	before := evidence("before", q)
	if before.lower == 0 {
		t.Fatal("fixture has no learned evidence")
	}
	// A held job naming a finished parent in after: keeps the parent.
	held := storedTestJob(t, q, "queued-001")
	held.ID, held.Pause = "held-child", &JobPause{}
	held.Spec.After = &PieceDependency{Job: "retained-002", Outputs: map[string]string{"in": "out"}}
	if err := q.update(func() error {
		q.putJob(held.ID, &held)
		for id, j := range q.s.Jobs {
			if terminal(j.State) { // The fixture shares the clock's own time value.
				ended := clock.Now()
				j = q.touchJob(id)
				j.StartedAt, j.FinishedAt = &ended, &ended
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	expire := func(q *Queue) {
		t.Helper()
		for range 64 {
			if err := q.pruneJobHistory(); err != nil {
				t.Fatal(err)
			}
			if h := q.records.history.heap; len(h) == 0 || h[0].at.After(q.clock.Now()) {
				return
			}
		}
	}
	clock.Advance(jobRecordRetention - time.Nanosecond)
	expire(q)
	if q.s.Jobs["retained-001"] == nil {
		t.Fatal("record expired before 14 days")
	}
	clock.Advance(24*time.Hour + time.Nanosecond)
	expire(q)
	for _, id := range []string{"running-0", "queued-000", "held-child", "retained-002"} {
		if q.s.Jobs[id] == nil {
			t.Fatalf("%s expired", id)
		}
	}
	if q.s.Jobs["retained-001"] != nil {
		t.Fatal("15-day-old finished record kept")
	}
	summary, err := q.expiredJobSummary(context.Background(), "retained-001")
	if err != nil || summary == nil || summary.State != "succeeded" || summary.Project != source.Spec.Project || summary.PeakMemoryGB != 3 {
		t.Fatalf("summary %+v, %v", summary, err)
	}
	_, err = q.jobSnapshot("retained-001")
	var gone *HTTPError
	if !errors.As(q.expiredJobError(context.Background(), err), &gone) || gone.Status != 410 || gone.Code != "job_expired" {
		t.Fatalf("expired lookup: %v", err)
	}
	if after := evidence("after expiry", q); after != before {
		t.Fatalf("learned evidence changed: %+v -> %+v", before, after)
	}
	reopened := reopenCancellationQueue(t, q, clock)
	reopened.mu.Lock()
	reopened.unreported = map[string]bool{} // Workers check in again after restart.
	reopened.mu.Unlock()
	if reopened.s.Jobs["retained-001"] != nil || reopened.s.Jobs["retained-002"] == nil {
		t.Fatal("restart changed expiry")
	}
	if after := evidence("after restart", reopened); after != before {
		t.Fatalf("learned evidence lost on restart: %+v -> %+v", before, after)
	}
	// Once its dependent goes, the parent expires on its next look.
	if err := reopened.update(func() error { reopened.deleteJob("held-child"); return nil }); err != nil {
		t.Fatal(err)
	}
	clock.Advance(jobExpiryRecheck)
	expire(reopened)
	if reopened.s.Jobs["retained-002"] != nil {
		t.Fatal("parent kept after its dependent left")
	}
}

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
