package myce

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func progressReport(completed, total float64, unit string) *ProgressReport {
	return &ProgressReport{Completed: &completed, Total: &total, Unit: unit}
}

func TestApplicationProgressAttemptScopeStalenessAndAdvisory(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, ApplicationProgress: progressReport(10, 10, "frames")}
	send := func(r AttemptReport) {
		t.Helper()
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{r}}); err != nil {
			t.Fatal(err)
		}
	}
	send(report)
	got := runtimeJob(t, q, j.ID)
	if got.ApplicationProgress == nil || got.ApplicationProgress.Completed != 10 || got.ApplicationProgress.Stale || got.State != "running" || got.Progress != 0 || len(got.OutputRefs) != 0 {
		t.Fatalf("advisory progress: %+v", got)
	}
	// Ordered source updates teach the reporter cadence; a cold report alone
	// cannot establish staleness.
	report.ApplicationProgress.UpdatedAt = clock.Now()
	send(report)
	clock.Advance(time.Second)
	report.ApplicationProgress.UpdatedAt = clock.Now()
	send(report)
	clock.Advance(31 * time.Second)
	got = runtimeJob(t, q, j.ID)
	if !got.ApplicationProgress.Stale || got.ApplicationProgress.AgeSeconds != 31 || !strings.Contains(JobTiming(got), "stale") {
		t.Fatalf("not stale: %+v", got.ApplicationProgress)
	}
	// Heartbeat repeats an unchanged file with worker-observed age, not a new report.
	report.ApplicationProgress.AgeSeconds = 31
	send(report)
	if !runtimeJob(t, q, j.ID).ApplicationProgress.Stale {
		t.Fatal("heartbeat refreshed old file")
	}
	old := report
	old.Session = "old-session"
	old.ApplicationProgress = progressReport(1, 10, "frames")
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{old}}); err == nil {
		t.Fatal("old session accepted")
	}
	if runtimeJob(t, q, j.ID).ApplicationProgress.Completed != 10 {
		t.Fatal("old session overwrote progress")
	}
	// Retry/assignment uses the existing path and clears all application evidence.
	if err := q.Complete(Result{Execution: report.Execution, Machine: j.Machine, Session: j.Session, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	next := check(t, q, m, nil).Job
	if next == nil || next.Attempt == j.Attempt {
		t.Fatal("not retried", next)
	}
	send(report)
	if runtimeJob(t, q, j.ID).ApplicationProgress != nil {
		t.Fatal("old attempt revived progress")
	}
}

func TestApplicationProgressTwoStagesUnknownRemaining(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	stage, stages := 1, 2
	p := progressReport(10, 10, "records")
	p.StageIndex, p.StageCount, p.TotalWorkKnown = &stage, &stages, true
	p.UpdatedAt = clock.Now()
	report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, ApplicationProgress: p}
	send := func() {
		t.Helper()
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
			t.Fatal(err)
		}
	}
	send()
	got := runtimeJob(t, q, j.ID)
	text := JobTiming(got)
	if got.State != "running" || got.Runtime.LastAcceptedSeconds != nil || got.Progress != 0 || !strings.Contains(text, "stage 1/2") || !strings.Contains(text, "whole-job progress unknown") || strings.Contains(text, "remaining ~0") {
		t.Fatalf("stage completion claimed job completion: %s / %+v", text, got.Runtime)
	}
	clock.Advance(5 * time.Second)
	stage = 2
	p = progressReport(0, 100, "iterations")
	p.StageIndex, p.StageCount = &stage, &stages
	p.UpdatedAt = clock.Now()
	report.ApplicationProgress = p
	send()
	got = runtimeJob(t, q, j.ID)
	if *got.ApplicationProgress.StageIndex != 2 || got.Runtime.LastAcceptedSeconds != nil || !strings.Contains(JobTiming(got), "stage 2/2") {
		t.Fatalf("invented stage-two timing: %+v", got)
	}
	// A delayed older heartbeat in this same execution cannot rewind stage two.
	oldStage := 1
	old := progressReport(10, 10, "records")
	old.StageIndex, old.StageCount, old.AgeSeconds = &oldStage, &stages, 5
	old.UpdatedAt = clock.Now().Add(-5 * time.Second)
	report.ApplicationProgress = old
	send()
	if *runtimeJob(t, q, j.ID).ApplicationProgress.StageIndex != 2 {
		t.Fatal("late stage-one report overwrote stage two")
	}
	// A delayed report within the stage also cannot overwrite fresher counts.
	old.StageIndex, old.Unit = &stage, "iterations"
	// Both were fresh at send time, but the older file's heartbeat arrives last.
	old.AgeSeconds = 0
	send()
	if runtimeJob(t, q, j.ID).ApplicationProgress.Completed != 0 {
		t.Fatal("older file evidence overwrote progress")
	}
	clock.Advance(31 * time.Second)
	got = runtimeJob(t, q, j.ID)
	if !got.ApplicationProgress.Stale || got.ApplicationProgress.AgeSeconds != 31 || !strings.Contains(JobTiming(got), "stale") {
		t.Fatal("stage evidence did not age", got.ApplicationProgress)
	}
}

func TestApplicationProgressSourceOrderWithTransportDelay(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	sentAt := clock.Now()
	stage, count := 1, 2
	p := progressReport(10, 10, "records")
	p.UpdatedAt, p.StageIndex, p.StageCount = sentAt, &stage, &count
	report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, ApplicationProgress: p}
	send := func() {
		t.Helper()
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
			t.Fatal(err)
		}
	}
	// First report spends two seconds in transport after being sampled fresh.
	clock.Advance(2 * time.Second)
	send()
	// Stage two was written at t=1 and arrives at t=3 with age two. Source
	// order is newer although age exceeds the queue's age-one first report.
	clock.Advance(time.Second)
	stage = 2
	p = progressReport(1, 100, "iterations")
	p.UpdatedAt, p.AgeSeconds, p.StageIndex, p.StageCount = sentAt.Add(time.Second), 2, &stage, &count
	report.ApplicationProgress = p
	send()
	got := runtimeJob(t, q, j.ID)
	if *got.ApplicationProgress.StageIndex != 2 || got.ApplicationProgress.AgeSeconds != 2 {
		t.Fatalf("new source rejected due to transport: %+v", got.ApplicationProgress)
	}
	// Same-stage old evidence can also arrive fresh-at-send, after the newer one.
	old := progressReport(0, 100, "iterations")
	old.UpdatedAt, old.StageIndex, old.StageCount = sentAt, &stage, &count
	report.ApplicationProgress = old
	clock.Advance(time.Second)
	send()
	got = runtimeJob(t, q, j.ID)
	if got.ApplicationProgress.Completed != 1 || got.ApplicationProgress.AgeSeconds != 3 {
		t.Fatalf("late same-stage report replaced source: %+v", got.ApplicationProgress)
	}
}

func TestApplicationProgressBoundedFileValidationAndRestartAge(t *testing.T) {
	dir := t.TempDir()
	j := Job{ID: "job", Attempt: 2, Session: "session"}
	path := applicationProgressFile(dir, j)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	for _, body := range []string{`{"completed":1,"total":2,"unit":"frames"}`, `{"completed":0,"total":0,"unit":"records"}`} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, now, now); err != nil {
			t.Fatal(err)
		}
		// A fresh worker reading the same attempt retains actual file age.
		p := readApplicationProgress(dir, j, now.Add(40*time.Second))
		if p == nil || p.AgeSeconds != 40 {
			t.Fatal("restart renewed report", p)
		}
	}
	for _, body := range []string{`{"total":2,"unit":"frames"}`, `{"completed":-1,"total":2,"unit":"frames"}`, `{"completed":3,"total":2,"unit":"frames"}`, `{"completed":1,"total":2,"unit":"<script>"}`, `{"completed":1e999,"total":2,"unit":"frames"}`, strings.Repeat("x", 4097), `{"completed":`} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if p := readApplicationProgress(dir, j, now); p != nil {
			t.Errorf("accepted invalid report %q", body)
		}
	}
	if progressReport(math.NaN(), 2, "frames").valid() || progressReport(1, math.Inf(1), "frames").valid() {
		t.Fatal("nonfinite counts")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/dev/zero", path); err != nil {
		t.Fatal(err)
	}
	if readApplicationProgress(dir, j, now) != nil {
		t.Fatal("read special file")
	}
}

func TestApplicationProgressQueueRestartRetainsAge(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, ApplicationProgress: progressReport(1, 2, "records")}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	report.ApplicationProgress.UpdatedAt = clock.Now()
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	report.ApplicationProgress.UpdatedAt = clock.Now()
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	var database string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(31 * time.Second)
	reopened, err := Open(database, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.jobSnapshot(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ApplicationProgress == nil || !got.ApplicationProgress.Stale || got.ApplicationProgress.AgeSeconds != 31 {
		t.Fatalf("restart refreshed progress: %+v", got.ApplicationProgress)
	}
}

func TestApplicationProgressCachedHeartbeatAge(t *testing.T) {
	_, clock := fixture(t)
	dir := t.TempDir()
	j := Job{ID: "job", Attempt: 1, Session: "session"}
	path := applicationProgressFile(dir, j)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"completed":1,"total":2,"unit":"frames"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	cached := readApplicationProgress(dir, j, clock.Now())
	if cached == nil {
		t.Fatal("missing report")
	}
	// A slow actor may not collect another file sample while machine heartbeats
	// continue. Sending its cached observation must include that intervening age.
	clock.Advance(31 * time.Second)
	sent := cached.aged(clock.Now())
	if sent.AgeSeconds != 31 || cached.AgeSeconds != 0 {
		t.Fatalf("cached report was renewed or mutated: sent=%+v cached=%+v", sent, cached)
	}
	clock.Advance(time.Second)
	if cached.aged(clock.Now()).AgeSeconds != 32 {
		t.Fatal("repeated heartbeat renewed report")
	}
}
