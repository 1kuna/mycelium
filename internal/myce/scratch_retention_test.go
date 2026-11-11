package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// #1227: old markers must neither pin accepted/released bytes forever nor
// authorize losing the only copy. Exercise the real API from the worker sweep.
func TestScratchRetentionQueueAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, state  string
		policy       time.Duration
		expired      bool
		unreleased   bool
		oldAttempt   bool
		foreign      bool
		parked       bool
		pending      bool
		unknown      bool
		ownerRelease bool
		wantRemoved  bool
	}{
		{name: "accepted legacy success", state: "succeeded", wantRemoved: true},
		{name: "failed explicit policy", state: "failed", policy: 3 * time.Hour, wantRemoved: true},
		{name: "cancelled day policy", state: "cancelled", policy: 24 * time.Hour, wantRemoved: true},
		{name: "default policy", state: "failed", policy: DefaultFileRetention, wantRemoved: true},
		{name: "collected result", state: "succeeded", oldAttempt: true, expired: true, wantRemoved: true},
		{name: "released failed result", state: "failed", expired: true, wantRemoved: true},
		{name: "reviewed owner release", state: "failed", ownerRelease: true, wantRemoved: true},
		{name: "automatic keeps unique failure", state: "failed"},
		{name: "automatic keeps unique cancellation", state: "cancelled"},
		{name: "released attempt superseded by newer success", state: "succeeded", oldAttempt: true, wantRemoved: true},
		{name: "unreleased terminal", state: "failed", policy: time.Hour, unreleased: true},
		{name: "wrong worker", state: "succeeded", foreign: true},
		{name: "parked custody", state: "succeeded", parked: true, expired: true},
		{name: "pending upload replay", state: "succeeded", pending: true},
		{name: "legacy orphan", unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.ConfigureFileRetention(tc.policy); err != nil {
				t.Fatal(err)
			}
			client := retentionClient(t, q)
			j := submit(t, q, spec("experiment"))
			ended := clock.Now()
			if err := q.update(func() error {
				current := q.s.Jobs[j.ID]
				current.State, current.Attempt, current.Machine, current.Session = tc.state, 1, "worker", "session"
				current.FinishedAt, current.ReleaseConfirmed, current.FilesExpired = &ended, !tc.unreleased, tc.expired
				if tc.oldAttempt {
					current.Attempt = 2
					current.Copies = []Copy{{Attempt: 1, Machine: "worker", Session: "earlier", ReleaseConfirmed: true}}
				}
				if tc.parked {
					current.Spec.StorageProtocol = "retained-protected-protocol"
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			id := j.ID
			if tc.unknown {
				id = ID()
			}
			path := filepath.Join(dir, "job-files", id, "1")
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			payload := filepath.Join(path, "partial")
			if err := os.WriteFile(payload, []byte("unique unless explicitly accepted or released"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := attemptFilesExpired(path, clock.Now().Add(-40*24*time.Hour), false); err != nil {
				t.Fatal(err)
			}
			if tc.pending {
				// Even an already accepted completion must keep its exact local
				// replay owner, including legacy result-only custody.
				st := workerState{Result: &Result{Execution: Execution{ID: id, Attempt: 1}, ReleasePending: true}}
				if err := saveWorkerJSON(dir, "state-attempt-pending.json", st); err != nil {
					t.Fatal(err)
				}
			}
			w := Worker{Client: client, Machine: machine("worker", "fake", "session"), Clock: clock}
			if tc.foreign {
				w.Machine.ID = "other-worker"
			}
			pass := func(wantRemoved bool) {
				t.Helper()
				if err := w.retainWorkerFiles(context.Background(), dir, clock.Now()); err != nil {
					t.Fatal(err)
				}
				_, err := os.Stat(payload)
				if removed := errors.Is(err, os.ErrNotExist); removed != wantRemoved || err != nil && !removed {
					t.Fatalf("removed=%t want=%t: %v", removed, wantRemoved, err)
				}
				if wantRemoved {
					if _, err := os.Stat(filepath.Join(path, unclaimedMarker)); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("expired marker retained", err)
					}
				}
			}
			if tc.ownerRelease {
				pass(false)
				plan, err := q.PlanFileRelease(context.Background(), FileReleaseSelection{Project: j.Spec.Project, Jobs: []string{j.ID}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := q.ApplyFileRelease(context.Background(), FileReleaseApply{Selection: plan.Selection, PlanID: plan.ID, AcceptExpiry: true}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.policy > 0 {
				clock.Advance(tc.policy - time.Nanosecond)
				pass(false) // Old marker cannot shorten the authored window.
				clock.Advance(time.Nanosecond)
			} else {
				clock.Advance(365 * 24 * time.Hour)
			}
			pass(tc.wantRemoved)
		})
	}
}

func TestScratchRetentionReportingAndUnavailableQueue(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)}
	for _, id := range []string{"accepted", "unavailable", "live", "legacy"} {
		path := filepath.Join(dir, "job-files", id, "1")
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "data"), make([]byte, 8192), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveWorkerJSON(dir, "state-attempt-live.json", workerState{Job: &Job{ID: "live", Attempt: 1}}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("queue unavailable")
	c := &attemptFilesCollector{ctx: context.Background(), report: AttemptFilesCollection{Reasons: map[string]int64{}}}
	c.resolve = func(id string, attempt int) (AttemptFilesDisposition, error) {
		if id == "live" || attempt != 1 {
			t.Fatal("queried local custody or wrong attempt")
		}
		if id == "unavailable" {
			return AttemptFilesDisposition{}, failure
		}
		return AttemptFilesDisposition{Reclaim: id == "accepted", Reason: "unique_unreleased"}, nil
	}
	if err := pruneAttemptFiles(dir, clock.Now(), c); !errors.Is(err, failure) {
		t.Fatal("queue error lost", err)
	}
	if c.report.ReclaimedBytes <= 0 || c.report.RetainedBytes <= 0 {
		t.Fatal("missing allocated byte accounting", c.report)
	}
	for _, reason := range []string{"local_custody", "queue_unavailable", "unique_unreleased"} {
		if c.report.Reasons[reason] <= 0 {
			t.Fatal("missing retention reason", reason, c.report)
		}
	}
	for _, id := range []string{"unavailable", "live", "legacy"} {
		if _, err := os.Stat(filepath.Join(dir, "job-files", id, "1", "data")); err != nil {
			t.Fatal("protected data lost", id, err)
		}
	}
}

// Repeated yields/requeues kept one released scratch copy per attempt for the
// whole terminal file-retention window, filling worker disks. Superseded
// released attempts go on the next sweep; the current, recoverable, unreleased
// and locally held attempts stay.
func TestScratchRetentionCollectsSupersededAttempts(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileRetention(7 * 24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	client := retentionClient(t, q)
	m := machine("worker", "fake")
	j := submit(t, q, spec("experiment"))
	dir := t.TempDir()
	payload := func(attempt int) string {
		return filepath.Join(dir, "job-files", j.ID, strconv.Itoa(attempt), "partial")
	}
	var running *Job
	for attempt := 1; attempt <= 6; attempt++ {
		running = check(t, q, m, nil).Job
		if running == nil || running.ID != j.ID || running.Attempt != attempt {
			cur := job(t, q, j.ID)
			t.Fatalf("attempt %d not assigned: %+v state=%s reason=%q", attempt, running, cur.State, cur.Reason)
		}
		if err := os.MkdirAll(filepath.Dir(payload(attempt)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(payload(attempt), make([]byte, 8192), 0600); err != nil {
			t.Fatal(err)
		}
		if attempt < 6 {
			if err := q.Complete(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: attempt}, Interrupted: true, PoliteStop: true}); err != nil {
				t.Fatal(err)
			}
		}
		clock.Advance(time.Hour)
	}
	if err := q.update(func() error {
		for i := range q.s.Jobs[j.ID].Copies {
			c := &q.s.Jobs[j.ID].Copies[i]
			if c.Attempt < 6 && !c.ReleaseConfirmed {
				t.Fatalf("requeued attempt %d not released", c.Attempt)
			}
			c.Recoverable = c.Attempt == 2
			c.ReleaseConfirmed = c.ReleaseConfirmed && c.Attempt != 3
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Attempt 4 still owes the queue a pending upload/replay.
	pending := workerState{Result: &Result{Execution: Execution{ID: j.ID, Attempt: 4}, ReleasePending: true}}
	if err := saveWorkerJSON(dir, "state-attempt-pending.json", pending); err != nil {
		t.Fatal(err)
	}
	w := Worker{Client: client, Machine: m, Clock: clock}
	c := &attemptFilesCollector{ctx: context.Background(), report: AttemptFilesCollection{Reasons: map[string]int64{}}}
	c.resolve = func(id string, attempt int) (AttemptFilesDisposition, error) {
		var d AttemptFilesDisposition
		err := w.Client.Call(context.Background(), "POST", "/api/workers/attempt-files", AttemptFilesRequest{Execution: Execution{ID: id, Attempt: attempt}, Machine: m.ID}, &d)
		return d, err
	}
	if err := pruneAttemptFiles(dir, clock.Now(), c); err != nil {
		t.Fatal(err)
	}
	for attempt, wantRemoved := range map[int]bool{1: true, 2: false, 3: false, 4: false, 5: true, 6: false} {
		_, err := os.Stat(payload(attempt))
		if removed := errors.Is(err, os.ErrNotExist); removed != wantRemoved || err != nil && !removed {
			t.Errorf("attempt %d removed=%t want=%t: %v", attempt, removed, wantRemoved, err)
		}
	}
	if c.report.ReclaimedBytes <= 0 {
		t.Fatal("no reclaimed bytes reported", c.report)
	}
	for _, reason := range []string{"recoverable_attempt", "unresolved_custody", "local_custody"} {
		if c.report.Reasons[reason] <= 0 {
			t.Error("missing retention reason", reason, c.report)
		}
	}
}
