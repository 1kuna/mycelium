package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
		{name: "collected result", state: "succeeded", oldAttempt: true, expired: true, wantRemoved: true},
		{name: "released failed result", state: "failed", expired: true, wantRemoved: true},
		{name: "reviewed owner release", state: "failed", ownerRelease: true, wantRemoved: true},
		{name: "automatic keeps unique failure", state: "failed"},
		{name: "automatic keeps unique cancellation", state: "cancelled"},
		{name: "old attempt not accepted by newer success", state: "succeeded", oldAttempt: true},
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
