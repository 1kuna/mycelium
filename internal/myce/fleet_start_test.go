package myce

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func fleetAtStartProbe(t *testing.T, room ...float64) (*Queue, *fakeClock, *fleetUpgradeHost) {
	t.Helper()
	q, c, _ := fleetFixture(t, "q", "a", "b")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	fleetRestart(t, q, c, i, true)
	if len(room) > 0 {
		q.s.Machines[i.Machine].HostAvailableMemoryGB = room[0]
	}
	for n := 0; n <= int(fleetStableFor/time.Second); n++ {
		fleetRefreshWithoutStart(t, q, c)
		if q.s.FleetUpgrade.Hosts[0].Phase == "deferred" {
			break
		}
		c.Advance(time.Second)
	}
	h := q.s.FleetUpgrade.Hosts[0]
	phase := "worker_start"
	if len(room) > 0 {
		phase = "deferred"
	}
	if h == nil || h.Phase != phase || h.StartProbe == "" || h.WorkerDone {
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
	if probe := q.s.Jobs[h.StartProbe]; !probe.Spec.Needs.cpuOnly() || probe.Spec.Needs.MemoryGB != 0 || probe.Spec.Needs.HostMemoryGB != 0.125 {
		t.Fatal("start probe must use normalized CPU resources", probe.Spec.Needs)
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

func TestFleetUpgradeStartProofUsesUsefulAssignment(t *testing.T) {
	q, c, workers := fleetFixture(t, "q", "a", "b")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	s := spec("production")
	s.AllowedMachines = []string{workers[0].ID}
	useful := submit(t, q, s)
	fleetRestart(t, q, c, i, true)
	for n := 0; n <= int(fleetStableFor/time.Second); n++ {
		fleetRefreshWithoutStart(t, q, c)
		c.Advance(time.Second)
	}
	h := q.fleetActiveHost()
	assigned := job(t, q, useful.ID)
	if h == nil || h.Phase != "worker_start" || assigned.Attempt != 1 || assigned.StartedAt != nil {
		t.Fatal("useful work did not win normal admission", h, assigned)
	}
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if !h.WorkerDone || h.StartAccepted != useful.ID || job(t, q, useful.ID).State != "running" || !cancelled(job(t, q, h.StartProbe)) {
		t.Fatal("fresh useful start not accepted without interrupting it", h)
	}
}

func TestFleetUpgradeStartProbeNeverPlansStops(t *testing.T) {
	q, _, h := fleetAtStartProbe(t)
	probe := q.s.Jobs[h.StartProbe]
	plan := &placementPlan{stops: 1}
	// Both uncached/preparation and cached shared-vector paths are covered.
	m := q.s.Machines[h.Machine]
	set := &candidateSet{q: q, m: m, resources: map[batchResourceKey]batchResourceRoom{resourceKey(probe): {plan: plan}}}
	if q.fleetStartPlan(probe, plan) != nil || set.batchPlan(probe) != nil {
		t.Fatal("smoke borrowed a cached plan that stops useful work")
	}
	if q.fleetStartPlan(&Job{ID: "customer"}, plan) != plan {
		t.Fatal("ordinary customer preemption changed")
	}
}

func TestFleetUpgradeStartProofPersistsAcrossQueueRestart(t *testing.T) {
	q, c, h := fleetAtStartProbe(t)
	probe, operation := h.StartProbe, q.s.FleetUpgrade.Operation
	machine := cloneMachine(q.s.Machines[h.Machine])
	var path string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	fakeCheckpointAvailability(t, restarted, true)
	h = restarted.fleetActiveHost()
	if h == nil || h.StartProbe != probe || h.WorkerDone || restarted.s.FleetUpgrade.Operation != operation || len(restarted.s.Jobs) != 1 {
		t.Fatal("restart lost or duplicated unaccepted probe", restarted.fleetUpgradeStatus())
	}
	check(t, restarted, machine, nil)
	fleetCompleteStartProbe(t, restarted)
	if !h.WorkerDone || job(t, restarted, probe).StartedAt == nil {
		t.Fatal("restarted queue did not accept exact probe", restarted.fleetUpgradeStatus())
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
	for _, mode := range []string{"process", "session", "failed-command", "maintenance"} {
		t.Run(mode, func(t *testing.T) {
			q, _, h := fleetAtStartProbe(t)
			want := "worker_start_runtime_changed"
			switch mode {
			case "process":
				q.s.Machines[h.Machine].Fleet.PID++
			case "session":
				q.s.Machines[h.Machine].Session = "unproved-session"
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

// Real check-ins and scheduler accounting refuse this never-assigned probe;
// fake queue time drives all upgrade decisions, including the return pass.
func TestFleetUpgradeStartProbeDefersAndReturns(t *testing.T) {
	for _, recovers := range []bool{true, false} {
		t.Run(fmt.Sprint(recovers), func(t *testing.T) {
			q, c, h := fleetAtStartProbe(t, 0)
			probe, identity, policy, operation := h.StartProbe, h.StartIdentity, h.Policy, q.s.FleetUpgrade.Operation
			if h.Outcome != FleetHostDeferred || !strings.HasPrefix(h.DeferredReason, "no_capacity:") || h.WorkerDone || job(t, q, probe).Attempt != 0 {
				t.Fatalf("unadmitted probe not deferred: %+v", h)
			}
			// Deferral cannot lend this probe another host's stop plan.
			if q.fleetStartPlan(q.s.Jobs[probe], &placementPlan{stops: 1}) != nil {
				t.Fatal("deferred probe may interrupt useful work")
			}
			var order []string
			for n := 0; n < 10 && q.s.FleetUpgrade.State == "running"; n++ {
				fleetTick(t, q)
				active := q.fleetActiveHost()
				if active == nil {
					continue
				}
				if active.Host == "b" && recovers {
					q.s.Machines[h.Machine].HostAvailableMemoryGB = 32
				}
				order = append(order, fleetActivate(t, q, c))
			}
			if q.s.FleetUpgrade.State != "complete" || q.s.FleetUpgrade.Operation != operation || h.StartProbe != probe || h.StartIdentity != identity || !reflect.DeepEqual(h.Policy, policy) {
				t.Fatalf("return pass lost operation/fence or failed to finish: %+v", q.fleetUpgradeStatus())
			}
			want := []string{"b/worker-b", "q/@queue"}
			if recovers {
				if len(order) > 2 {
					want = append(want, "a/worker-a")
				}
				if !h.StartRevisited || !h.WorkerDone || h.Phase != "complete" || h.Outcome != "" || h.DeferredReason != "" || h.StartAccepted != probe {
					t.Fatalf("recovered probe did not prove same worker: %+v", h)
				}
			} else {
				if h.WorkerDone || h.Phase != "deferred" || h.Outcome != FleetHostDeferred || h.Failure != "" || !cancelled(job(t, q, probe)) {
					t.Fatalf("persistent pressure became rollout failure or false acceptance: %+v", h)
				}
				status := q.fleetUpgradeStatus()
				data, err := json.Marshal(status)
				if err != nil || !strings.Contains(string(data), `"outcome":"deferred"`) || !strings.Contains(string(data), `"deferred_reason":"no_capacity:`) {
					t.Fatal("fleet upgrade response hid typed deferral", string(data), err)
				}
				compact := NewCompactStatus(Snapshot{FleetUpgrade: status})
				if !strings.Contains(strings.Join(compact.FleetUpgrade, "\n"), "no_capacity:") {
					t.Fatal("completed rollout hid deferred host", compact.FleetUpgrade)
				}
				// Same-release reruns must not skip the remaining start debt.
				q.s.Machines[h.Machine].HostAvailableMemoryGB = 32
				if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
					t.Fatal(err)
				}
				fleetTick(t, q)
				next := q.fleetActiveHost()
				if next == nil || next.Host != h.Host || next.WorkerDone || next.StartIdentity == "" || next.StartProbe == probe {
					t.Fatal("rerun skipped deferred acceptance debt", next)
				}
			}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("host/queue order %v, want %v", order, want)
			}
		})
	}
}

func TestFleetUpgradeDeferredProbeStillStopsOnFailure(t *testing.T) {
	for _, mode := range []string{"started-failed", "health", "session", "maintenance"} {
		t.Run(mode, func(t *testing.T) {
			q, _, h := fleetAtStartProbe(t, 0)
			phaseBeforeFailure := q.s.FleetUpgrade.Hosts[1].Phase
			switch mode {
			case "started-failed":
				q.s.Machines[h.Machine].HostAvailableMemoryGB = 32
				check(t, q, *q.s.Machines[h.Machine], nil)
				j := job(t, q, h.StartProbe)
				if j.Attempt != 1 {
					t.Fatal("recovered deferred probe not assigned", j)
				}
				if err := q.Started(result(j, true)); err != nil {
					t.Fatal(err)
				}
				phaseBeforeFailure = q.s.FleetUpgrade.Hosts[1].Phase
				finish(t, q, j, false)
			case "health":
				q.s.Machines[h.Machine].Fleet.ServiceHealthy = false
			case "session":
				q.s.Machines[h.Machine].Session = "stale-session"
			case "maintenance":
				if _, err := q.ChangeHostMaintenance(h.Host, HostMaintenanceRequest{Action: "disable"}); err != nil {
					t.Fatal(err)
				}
			}
			fleetTick(t, q)
			if q.s.FleetUpgrade.State != "stopped" || h.Failure == "" || h.WorkerDone || q.s.FleetUpgrade.Hosts[1].Phase != phaseBeforeFailure {
				t.Fatalf("deferred failure advanced rollout: %+v", q.fleetUpgradeStatus())
			}
		})
	}
}

func TestFleetUpgradeStartProbeAdmissionReasons(t *testing.T) {
	t.Run("owner pressure requires measured evidence", func(t *testing.T) {
		q, _ := fixture(t)
		m := sharedHostFixture(t, q)
		m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
		m.SharedHostPressure.MemoryLevel = 2
		check(t, q, m, nil)
		probe := &Job{Spec: fleetStartProbeSpec(m.ID), State: "queued"}
		if why := q.fleetStartAdmissionReason(q.s.Machines[m.ID], probe); !strings.HasPrefix(why, "shared_host_owner_pressure:") {
			t.Fatal("measured owner pressure not deferrable", why)
		}
		// A policy refusal cannot borrow a simultaneous owner-pressure reason.
		probe.Spec.Class = "test"
		if why := q.fleetStartAdmissionReason(q.s.Machines[m.ID], probe); why != "" {
			t.Fatal("structural refusal became admission deferral", why)
		}
		probe.Spec.Class = "maintenance"
		m.SharedHostPressure = nil
		check(t, q, m, nil)
		if why := q.fleetStartAdmissionReason(q.s.Machines[m.ID], probe); why != "" {
			t.Fatal("unknown pressure evidence became deferral", why)
		}
	})
	t.Run("occupied CPU capacity", func(t *testing.T) {
		q, _ := fixture(t)
		m := cpuMachine("worker", "host", "cpu", 1, 8)
		check(t, q, m, nil)
		fleetRun(t, q, m, cpuSpec(1, 1))
		probe := &Job{Spec: fleetStartProbeSpec(m.ID), State: "queued"}
		if why := q.fleetStartAdmissionReason(q.s.Machines[m.ID], probe); !strings.HasPrefix(why, "no_capacity:") || !strings.Contains(why, "CPU") {
			t.Fatal("occupied CPU capacity not deferrable", why)
		}
	})
}

func TestFleetUpgradeDeferredProbePersistsAcrossQueueRestart(t *testing.T) {
	q, c, h := fleetAtStartProbe(t, 0)
	probe, identity, operation := h.StartProbe, h.StartIdentity, q.s.FleetUpgrade.Operation
	var path string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	h = restarted.s.FleetUpgrade.Hosts[0]
	if h.Phase != "deferred" || h.Outcome != FleetHostDeferred || h.DeferredReason == "" || h.StartProbe != probe || h.StartIdentity != identity || h.WorkerDone || h.StartRevisited || restarted.s.FleetUpgrade.Operation != operation || len(restarted.s.Jobs) != 1 {
		t.Fatalf("restart lost deferred fences or return debt: %+v", h)
	}
}

func TestFleetUpgradeDeferredProbeAssignedAtCompletionStillNeedsProof(t *testing.T) {
	q, _, h := fleetAtStartProbe(t, 0)
	// Replay the boundary after one return pass deferred this host, while a
	// different deferred host was still being verified. Ordinary admission
	// can assign this probe before the final completion transaction.
	h.StartRevisited = true
	q.s.Machines[h.Machine].HostAvailableMemoryGB = 32
	check(t, q, *q.s.Machines[h.Machine], nil)
	if job(t, q, h.StartProbe).Attempt != 1 {
		t.Fatal("probe not assigned at completion boundary")
	}
	for _, other := range q.s.FleetUpgrade.Hosts {
		if other != h {
			other.Phase = "complete"
		}
	}
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "running" || h.Phase != "worker_start" || cancelled(job(t, q, h.StartProbe)) {
		t.Fatal("assigned probe was cancelled or accepted as a capacity deferral", h)
	}
	fleetCompleteStartProbe(t, q)
	if q.s.FleetUpgrade.State != "complete" || !h.WorkerDone || h.Outcome != "" {
		t.Fatal("late-admitted probe did not finish its proof", h)
	}
}

func TestFleetUpgradeInitialRuntimeDefersAndReturns(t *testing.T) {
	for _, kind := range []string{"stale", "unhealthy"} {
		for _, recovers := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recovers=%t", kind, recovers), func(t *testing.T) {
				q, c, workers := fleetFixture(t, "q", "a", "b")
				m := q.s.Machines[workers[0].ID]
				if kind == "stale" {
					m.Fleet.ObservedAt = c.Now().Add(-37 * time.Second)
				} else {
					m.Fleet.ServiceHealthy = false
				}
				status, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
				if err != nil {
					t.Fatal("temporarily unavailable managed worker refused whole operation", err)
				}
				h := q.s.FleetUpgrade.Hosts[0]
				if status.State != "running" || h.Phase != "deferred" || h.Outcome != FleetHostDeferred || !strings.HasPrefix(h.DeferredReason, "worker_runtime_unavailable:") || h.StartProbe != "" || h.StartIdentity != "" || h.Before == nil {
					t.Fatalf("missing visible initial deferral: %+v", status)
				}
				if kind == "stale" && !recovers {
					// Subsequent reports carry the same stopped probe's increasing
					// age, beyond the existing filesystem-stall proof ceiling.
					q.s.Machines[h.Machine].Fleet.ObservationAgeSeconds = (fleetProofMaxAge + 37*time.Second).Seconds()
				}
				operation := status.Operation
				var order []string
				for n := 0; n < 10 && q.s.FleetUpgrade.State == "running"; n++ {
					fleetTick(t, q)
					active := q.fleetActiveHost()
					if active == nil {
						continue
					}
					if active.Host == "b" && recovers {
						live := q.s.Machines[h.Machine]
						live.Fleet.ObservedAt, live.Fleet.ObservationAgeSeconds, live.Fleet.ServiceHealthy = c.Now(), 0, true
					}
					order = append(order, fleetActivate(t, q, c))
				}
				if q.s.FleetUpgrade.State != "complete" || q.s.FleetUpgrade.Operation != operation || !h.StartRevisited {
					t.Fatalf("initial deferral never returned/finished: %+v", q.fleetUpgradeStatus())
				}
				if recovers {
					if !h.WorkerDone || h.Phase != "complete" || h.Outcome != "" || h.StartAccepted == "" {
						t.Fatal("recovered managed host skipped update/start proof", h)
					}
					if !reflect.DeepEqual(order, []string{"b/worker-b", "a/worker-a", "q/@queue"}) {
						t.Fatal("recovered initial host did not preserve queue-host-last", order)
					}
				} else {
					if h.WorkerDone || h.Phase != "deferred" || h.Outcome != FleetHostDeferred || h.Failure != "" || h.Before.Version != "v1.0.0" {
						t.Fatal("unrecovered initial host falsely accepted or failed whole operation", h)
					}
					if !reflect.DeepEqual(order, []string{"b/worker-b", "q/@queue"}) {
						t.Fatal("initial deferral blocked/reordered healthy hosts", order)
					}
				}
			})
		}
	}
}

func TestFleetUpgradeInitialDeferralStillRefusesInvalidWorkers(t *testing.T) {
	for _, kind := range []string{"unmanaged", "protocol", "incompatible", "siblings"} {
		t.Run(kind, func(t *testing.T) {
			q, c, workers := fleetFixture(t, "q", "a")
			m := q.s.Machines[workers[0].ID]
			m.Fleet.ObservedAt = c.Now().Add(-37 * time.Second)
			want := "fleet_worker_unmanaged"
			switch kind {
			case "unmanaged":
				m.Fleet = nil
			case "protocol":
				m.Fleet.StartGate = "unsupported"
			case "incompatible":
				m.CapabilityProfile.OS = "unsupported"
				want = "fleet_worker_target_incompatible"
			case "siblings":
				copy := cloneMachine(m)
				copy.ID = "sibling"
				q.s.Machines[copy.ID] = &copy
				want = "fleet_legacy_siblings"
			}
			_, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
			if err == nil || !strings.Contains(err.Error(), want) || q.s.FleetUpgrade != nil {
				t.Fatalf("invalid worker was deferred instead of refused: %v; %+v", err, q.fleetUpgradeStatus())
			}
		})
	}
}
