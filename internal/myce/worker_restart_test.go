package myce

import (
	"errors"
	"net/http"
	"strconv"
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

func TestWorkerRestartKeepsPreStartAttemptWhileMountProbePending(t *testing.T) {
	q, clock := fixture(t)
	if err := q.SetLossTimeouts(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	worker := cpuMachine("worker", "host", "cpu", 4, 16)
	peer := machine("peer", "fake")
	check(t, q, worker, nil)
	check(t, q, peer, nil)
	spec := cpuSpec(1, 1)
	spec.Resumable = true
	spec.AllowedMachines = []string{worker.ID}
	j := submit(t, q, spec)
	assigned := check(t, q, worker, nil).Job
	if assigned == nil || assigned.ID != j.ID || assigned.StartedAt != nil {
		t.Fatalf("expected an unstarted assignment, got %+v", assigned)
	}
	launch := HostLaunch{Machine: worker.ID, Session: worker.Session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(assigned.Attempt)}
	if err := q.AnnounceRestart(worker.ID, worker.Session); err != nil {
		t.Fatal(err)
	}
	advanceReportingPeer(t, q, clock, peer, soakRestartDelay)

	reportMount := func(reason string, roots ...UnavailableRoot) {
		report := worker
		report.UnavailableRoots = roots
		report.HostHealth = &HostHealth{History: []HostHealthSample{{
			At: clock.Now(), Mounts: []HostMountHealth{{Root: "checkpoint", Reason: reason}},
		}}}
		applyHostMountHealth(Worker{}, &report)
		check(t, q, report, assigned)
	}
	reportMount("mount_probe_pending")
	r := result(assigned, true)
	for _, gate := range []struct {
		name string
		call func() error
	}{
		{"launch", func() error { return q.AuthorizeLaunch(launch) }},
		{"started", func() error { return q.Started(r) }},
	} {
		err := gate.call()
		var refusal *HTTPError
		if !errors.As(err, &refusal) || refusal.Status != http.StatusServiceUnavailable || !retryTransient(err) {
			t.Fatalf("%s during pending mount probe = %v, want adjacent-client retry", gate.name, err)
		}
	}
	if got := job(t, q, j.ID); got.Attempt != assigned.Attempt || got.Session != assigned.Session || got.StartedAt != nil || got.ReleaseConfirmed {
		t.Fatalf("waiting changed assignment custody: %+v", got)
	}

	clock.Advance(time.Second)
	reportMount("mount_probe_pending", UnavailableRoot{Root: "checkpoint", Reason: "configured NAS mount missing"})
	if err := q.AuthorizeLaunch(launch); err == nil || retryTransient(err) {
		t.Fatalf("missing mount under a pending probe = %v, want hard refusal", err)
	}
	clock.Advance(time.Second)
	reportMount("mount_probe_stalled")
	if err := q.AuthorizeLaunch(launch); err == nil || retryTransient(err) {
		t.Fatalf("stalled mount = %v, want hard refusal", err)
	}
	clock.Advance(time.Second)
	reportMount("")
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	if err := q.Started(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.Attempt != assigned.Attempt || got.Session != assigned.Session || got.StartedAt == nil || got.Losses != 0 {
		t.Fatalf("mount recovery did not start original attempt: %+v", got)
	}

	advanceReportingPeer(t, q, clock, peer, q.machineLossTimeout(worker.ID)+time.Second)
	if got := job(t, q, j.ID); got.State != "queued" || got.Losses != 1 {
		t.Fatalf("expected actual worker loss, got state=%s losses=%d", got.State, got.Losses)
	}
	if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
		t.Fatalf("lost execution launch = %v, want stale refusal", err)
	}
	if err := q.Started(r); !errors.Is(err, ErrStale) {
		t.Fatalf("lost execution start = %v, want stale refusal", err)
	}
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatalf("lost execution completion = %v, want stale refusal", err)
	}
}
