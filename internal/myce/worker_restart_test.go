package myce

import (
	"testing"
	"time"
)

// #2172 round-3 soak: with the installed zero loss minimums, the learned loss
// allowance is about two 1 s check-in periods. A worker restarted 3.3 s after
// its SIGTERM exit lost attempt 1 and reran its finished command. An announced
// restart holds the session's attempts for the restart grace; an unannounced
// silence of the same length is still a loss at the learned allowance.
const soakRestartDelay = 3300 * time.Millisecond

func TestAnnouncedWorkerRestartKeepsAttempts(t *testing.T) {
	q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
	allowance := q.machineLossTimeout(worker.ID)
	if allowance <= 0 || allowance >= soakRestartDelay {
		t.Fatalf("learned allowance %s does not reproduce the soak", allowance)
	}
	if err := q.AnnounceRestart(worker.ID, worker.Session); err != nil {
		t.Fatal(err)
	}
	advanceReportingPeer(t, q, clock, peer, soakRestartDelay)
	got := q.s.Jobs[job.ID]
	if q.s.Machines[worker.ID].Gone || got.State != "running" || got.Attempt != job.Attempt || got.Losses != 0 {
		t.Fatalf("announced restart lost the attempt after %s: state=%s attempt=%d losses=%d reason=%q", soakRestartDelay, got.State, got.Attempt, got.Losses, got.Reason)
	}
	// The restarted worker returns with the same session and the same attempt.
	check(t, q, worker, job)
	if _, held := q.s.WorkerRestarts[worker.ID]; held {
		t.Fatal("the restarted worker's check-in did not end the restart hold")
	}
	// Having returned, it dies without a notice: the learned allowance applies.
	advanceReportingPeer(t, q, clock, peer, allowance)
	if got := q.s.Jobs[job.ID]; !q.s.Machines[worker.ID].Gone || got.State != "queued" || got.Losses != 1 {
		t.Fatalf("a returned worker's later death was not detected within %s: %+v", allowance, got)
	}
}

func TestUnannouncedWorkerSilenceIsStillLost(t *testing.T) {
	q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
	allowance := q.machineLossTimeout(worker.ID)
	advanceReportingPeer(t, q, clock, peer, allowance)
	got := q.s.Jobs[job.ID]
	if !q.s.Machines[worker.ID].Gone || got.State != "queued" || got.Losses != 1 {
		t.Fatalf("a dead worker was not lost at the learned %s: %+v", allowance, got)
	}
}

func TestAnnouncedWorkerRestartThatNeverReturnsIsLost(t *testing.T) {
	q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
	if err := q.AnnounceRestart(worker.ID, "another-session"); err == nil {
		t.Fatal("a stale session's notice was accepted")
	}
	if err := q.AnnounceRestart(worker.ID, worker.Session); err != nil {
		t.Fatal(err)
	}
	advanceReportingPeer(t, q, clock, peer, RestartGrace-time.Second)
	if q.s.Machines[worker.ID].Gone {
		t.Fatal("lost before the restart grace ran out")
	}
	advanceReportingPeer(t, q, clock, peer, time.Second)
	got := q.s.Jobs[job.ID]
	if !q.s.Machines[worker.ID].Gone || got.State != "queued" || got.Losses != 1 || got.Reason != "machine quiet for 3m0s" {
		t.Fatalf("an announced restart that never returned was not lost at the grace: %+v", got)
	}
	if len(q.s.WorkerRestarts) != 0 {
		t.Fatal("a lapsed notice was kept")
	}
	if err := q.AnnounceRestart(worker.ID, worker.Session); err == nil {
		t.Fatal("a lost machine's notice was accepted")
	}
}
