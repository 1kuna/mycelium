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
