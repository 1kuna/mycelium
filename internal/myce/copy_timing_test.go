package myce

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCopyLifecycleTimesSurviveReleaseRetryAndRestart(t *testing.T) {
	q, clock := fixture(t)
	m := sharedDiscreteMachine("xpu")
	first := submit(t, q, sharedSpec("experiment", 3))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != first.ID {
		t.Fatal("experiment not assigned", run)
	}
	assignedAt := clock.Now()
	clock.Advance(time.Second)
	startedAt := clock.Now()
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal("start replay", err)
	}
	stoppedAt := clock.Now()
	demand := submit(t, q, sharedSpec("production", 1))
	requireState(t, q, run.ID, "stopping")
	clock.Advance(q.gpuStopGrace() + time.Second)
	machineCheck(t, q, m, run)
	stale := result(run, false)
	stale.ReleaseOnly, stale.Session = true, "stale-session"
	if err := q.Complete(stale); err != nil {
		t.Fatal(err)
	}
	if c := job(t, q, run.ID).Copies[0]; c.ReleasedAt != nil || c.ReleaseConfirmed {
		t.Fatal("grace or stale receipt manufactured release", c)
	}
	releasedAt := clock.Now()
	ack := result(run, false)
	ack.ReleaseOnly = true
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := q.Complete(ack); err != nil {
		t.Fatal("release replay", err)
	}
	production := assignedJob(machineCheck(t, q, m))
	if production == nil || production.ID != demand.ID {
		t.Fatal("production not assigned after release", production)
	}
	finish(t, q, production, true)
	retry := assignedJob(machineCheck(t, q, m))
	if retry == nil || retry.ID != run.ID || retry.Attempt == run.Attempt {
		t.Fatal("experiment not retried", retry)
	}
	q = reopenCancellationQueue(t, q, clock)
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	got, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), run.ID)
	if err != nil || len(got.Copies) != 2 {
		t.Fatal("job GET lost copies", got, err)
	}
	c := got.Copies[0]
	if c.StartedAt == nil || !c.StartedAt.Equal(startedAt) || c.StoppedAt == nil || !c.StoppedAt.Equal(stoppedAt) || c.ReleasedAt == nil || !c.ReleasedAt.Equal(releasedAt) {
		t.Fatal("copy lost or rewrote its injected-clock times", c)
	}
	if next := got.Copies[1]; next.StartedAt != nil || next.StoppedAt != nil || next.ReleasedAt != nil {
		t.Fatal("new attempt inherited old lifecycle times", next)
	}
	page, err := q.JobLedger(run.ID, 0, -1, 256)
	if err != nil || len(page.Entries) == 0 || page.Entries[0].Kind != "assigned" || !page.Entries[0].At.Equal(assignedAt) {
		t.Fatal("existing ledger lost per-attempt assignment time", page, err)
	}
	for _, kind := range []string{"stop", "cancel", "held-cancel"} {
		t.Run("keyed-idle-"+kind, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("keyed", "xpu")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
			queued := submit(t, q, sharedSpec("experiment", 3))
			run := check(t, q, m, nil).Job
			if run == nil {
				t.Fatal("no keyed attempt", job(t, q, queued.ID).Reason)
			}
			if kind != "stop" {
				if err := q.Cancel(run.ID); err != nil {
					t.Fatal(err)
				}
			} else if err := q.update(func() error {
				live := q.touchJob(run.ID)
				q.stop(live, "test stop")
				q.retainRelease(live)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if kind == "held-cancel" {
				if err := q.update(func() error {
					q.s.Hosts = map[string]*HostState{hostID(&m): {Policy: HostPolicy{ProductionHold: true, ProductionHoldScope: "device"}}}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "stop" {
				clock.Advance(StopGrace + time.Second)
			} else {
				// Exercise idle cancellation before grace maintenance makes it terminal.
				clock.Advance(time.Second)
			}
			check(t, q, m, nil)
			copy := job(t, q, run.ID).Copies[0]
			if copy.ReleaseConfirmed {
				t.Fatal("idle check-in discharged custody", copy)
			}
			if copy.ReleasedAt != nil {
				t.Fatal("idle check-in invented an exact release time", copy)
			}
			ack := result(run, false)
			ack.ReleaseOnly = true
			if err := q.Complete(ack); err != nil {
				t.Fatal(err)
			}
			if copy := job(t, q, run.ID).Copies[0]; copy.ReleasedAt == nil || !copy.ReleasedAt.Equal(clock.Now()) {
				t.Fatal("exact receipt did not record its own time", copy)
			}
		})
	}
}
