package myce

import (
	"testing"
	"time"
)

func fleetAtStartProbe(t *testing.T) (*Queue, *fakeClock, *fleetUpgradeHost) {
	t.Helper()
	q, c, _ := fleetFixture(t, "q", "a", "b")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	fleetRestart(t, q, c, i, true)
	for n := 0; n <= int(fleetStableFor/time.Second); n++ {
		fleetRefreshWithoutStart(t, q, c)
		c.Advance(time.Second)
	}
	h := q.fleetActiveHost()
	if h == nil || h.Phase != "worker_start" || h.StartProbe == "" || h.WorkerDone {
		t.Fatalf("health alone did not leave a pending start proof: %+v", h)
	}
	if q.s.FleetUpgrade.Hosts[1].Phase != "pending" || q.maintenanceBlocked("b") {
		t.Fatal("advanced before start proof")
	}
	return q, c, h
}

func TestFleetUpgradeStartProofIdleHost(t *testing.T) {
	q, _, h := fleetAtStartProbe(t)
	if len(q.s.Jobs) != 1 {
		t.Fatal("idle host did not get exactly one queue-owned probe")
	}
	// A stale service/session cannot supply acceptance for this probe.
	j := job(t, q, h.StartProbe)
	stale := result(j, true)
	stale.Session = "previous-session"
	if err := q.Started(stale); err == nil || job(t, q, j.ID).StartedAt != nil {
		t.Fatal("stale start accepted", err)
	}
	fleetCompleteStartProbe(t, q)
	proof := job(t, q, j.ID)
	if !h.WorkerDone || h.Phase != "complete" || proof.StartedAt == nil || proof.State != "succeeded" || !proof.ReleaseConfirmed || proof.Session != q.s.Machines[h.Machine].Session {
		t.Fatalf("idle worker failed assignment acceptance: %+v %+v", h, proof)
	}
	fleetTick(t, q)
	if next := q.fleetActiveHost(); next == nil || next.Host != "b" {
		t.Fatal("verified start did not advance to next host", next)
	}
}

func TestFleetUpgradeHealthyWorkerNeverStartsStops(t *testing.T) {
	q, c, h := fleetAtStartProbe(t)
	probe := h.StartProbe
	// #1709: service health and check-ins continue, but every assignment
	// remains before Started. Queue time, not a sleep, exhausts the bound.
	for q.s.FleetUpgrade.State == "running" {
		c.Advance(fleetRuntimeFreshFor / 2)
		fleetRefreshWithoutStart(t, q, c)
	}
	if h.Failure != "worker_start_not_verified" || h.WorkerDone || job(t, q, probe).StartedAt != nil || q.s.FleetUpgrade.Hosts[1].Phase != "pending" {
		t.Fatalf("healthy never-started worker accepted: %+v", q.fleetUpgradeStatus())
	}
	if p := q.s.HostMaintenance[h.Host]; p.Mode != "drained" || p.DrainOwner != q.s.FleetUpgrade.Operation {
		t.Fatal("operation-owned drain not restored", p)
	}
	if !cancelled(job(t, q, probe)) {
		t.Fatal("failed operation left its probe runnable")
	}
	// A same-release rerun retains the debt rather than skipping current bytes.
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	next := q.fleetActiveHost()
	if next == nil || next.Host != h.Host || next.WorkerDone || next.StartIdentity == "" || next.Phase != "worker_start" || next.StartProbe == probe {
		t.Fatalf("rerun skipped failed acceptance: %+v", q.fleetUpgradeStatus())
	}
}

func TestFleetUpgradeStartProofRuntimeChangeAndFailure(t *testing.T) {
	for _, mode := range []string{"process", "failed-command", "maintenance"} {
		t.Run(mode, func(t *testing.T) {
			q, _, h := fleetAtStartProbe(t)
			want := "worker_start_runtime_changed"
			switch mode {
			case "process":
				q.s.Machines[h.Machine].Fleet.PID++
			case "failed-command":
				j := job(t, q, h.StartProbe)
				if err := q.Started(result(j, true)); err != nil {
					t.Fatal(err)
				}
				finish(t, q, j, false)
				want = "worker_start_probe_failed"
			case "maintenance":
				if _, err := q.ChangeHostMaintenance(h.Host, HostMaintenanceRequest{Action: "disable"}); err != nil {
					t.Fatal(err)
				}
				want = "maintenance_changed"
			}
			fleetTick(t, q)
			if q.s.FleetUpgrade.State != "stopped" || h.Failure != want || h.WorkerDone || q.s.FleetUpgrade.Hosts[1].Phase != "pending" {
				t.Fatalf("start failure advanced: %+v", q.fleetUpgradeStatus())
			}
			if mode == "maintenance" && q.s.HostMaintenance[h.Host].Mode != "disabled" {
				t.Fatal("operator policy overwritten")
			}
		})
	}
}
