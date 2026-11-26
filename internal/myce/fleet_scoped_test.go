package myce

import (
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"
)

// #2140: `myce fleet upgrade --host` updates one selected worker through the
// ordinary takeover path. A non-resumable attempt keeps its custodian, attempt
// and session across the swap and later delivers one accepted result. The
// queue service and the other workers keep their processes, releases and
// admission policy.
func TestFleetUpgradeScopedWorkerTakeover(t *testing.T) {
	q, c, workers := fleetFixture(t, "q", "a", "b", "q")
	a, b, local := workers[0], workers[1], workers[2]
	s := spec("experiment")
	s.Needs.HostMemoryGB = 1
	s.Resumable = false
	run := fleetRun(t, q, a, s)
	other := fleetRun(t, q, b, spec("experiment"))

	spool, id := t.TempDir(), ID()
	store := &workerStore{dir: spool, state: workerState{Job: run, Session: run.Session, Custodian: id}}
	live := custodianSnapshot{Protocol: custodianProtocol, ID: id, PID: 4242, Birth: "birth-4242", At: c.Now(), Started: true, State: workerState{Job: run, Session: run.Session}}
	g := &attemptCustodian{dir: custodianDir(spool), id: id, observed: true, last: live}
	q.s.Machines[a.ID].Fleet.Adoption = workerAdoptionProof(map[attemptKey]*attemptSupervisor{jobKey(*run): {job: *run, custodian: g}}, c.Now(), time.Second)

	policies := maps.Clone(q.s.HostMaintenance)
	untouched := map[string]FleetRuntimeProof{b.ID: *q.s.Machines[b.ID].Fleet, local.ID: *q.s.Machines[local.ID].Fleet, "@queue": *q.fleetQueue.Proof}

	out, err := q.StartFleetUpgrade(FleetUpgradeRequest{Host: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Host != "a" || out.State != "running" {
		t.Fatalf("scoped operation not reported: %+v", out)
	}
	for _, h := range out.Hosts {
		if h.Host != "a" && (h.Phase != "skipped" || !strings.Contains(h.Wait, "not selected")) {
			t.Fatalf("unselected host is part of the operation: %+v", h)
		}
	}
	if again, err := q.StartFleetUpgrade(FleetUpgradeRequest{Host: "a"}); err != nil || again.Operation != out.Operation {
		t.Fatal("rerun replaced the running scoped operation", again, err)
	}
	for _, request := range []FleetUpgradeRequest{{}, {Host: "b"}} {
		var refusal *HTTPError
		if _, err := q.StartFleetUpgrade(request); !errors.As(err, &refusal) || refusal.Code != "fleet_upgrade_target_conflict" || !strings.Contains(refusal.Message, "host a") {
			t.Fatalf("different scope not refused with the running operation named: %v", err)
		}
	}

	fleetTick(t, q)
	h := q.fleetActiveHost()
	if h == nil || h.Host != "a" || !h.Takeover || h.Phase != "worker_update" || h.Owned || q.maintenanceBlocked("a") {
		t.Fatalf("selected worker drained or waited: %+v", h)
	}
	// The brief cutover fence covers the selected host only.
	if !q.fleetTakeoverFence("a") || q.fleetTakeoverFence("b") || q.fleetTakeoverFence("q") {
		t.Fatal("cutover fence outside the selected host")
	}
	if !q.hostLaunchAllowed(q.s.Machines[b.ID], b.Session) || q.hostLaunchAllowed(q.s.Machines[a.ID], a.Session) {
		t.Fatal("launch fence applied to the wrong host")
	}
	for _, ask := range [][3]string{{"b", b.ID, b.Session}, {"q", local.ID, local.Session}, {"q", "@queue", q.fleetQueue.Proof.Session}} {
		if i, err := q.fleetInstruction(ask[0], ask[1], ask[2]); err != nil || i.Operation != "" {
			t.Fatalf("instruction issued outside the selected host: %v %+v %v", ask, i, err)
		}
	}

	i := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	if v := q.s.WorkerHandovers[a.ID]; v.Session != a.Session || len(v.Attempts) != 1 || v.Attempts[0] != runExecution(run) {
		t.Fatalf("handover did not retain the exact attempt: %+v", v)
	}
	// The replacement process (new PID, same session) adopts the same custodian.
	updated := cloneMachine(q.s.Machines[a.ID])
	updated.Devices = m3Machine().Devices[:1]
	updated.Fleet = fleetTestProof(c, a.Session, 777, true)
	updated.Fleet.Adoption = workerAdoptionProof(map[attemptKey]*attemptSupervisor{jobKey(*run): {job: *run, custodian: g}}, c.Now(), time.Second)
	if _, err := q.Checkin(Checkin{Machine: updated, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: runExecution(run), Session: run.Session, Started: true}}}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	for n := 0; n <= int(fleetStableFor/time.Second) && q.s.FleetUpgrade.State == "running"; n++ {
		c.Advance(time.Second)
		fleetRefresh(t, q, c)
	}
	if q.s.FleetUpgrade.State != "complete" || h.Phase != "complete" || !h.WorkerDone || h.StartAccepted == "" {
		t.Fatalf("scoped update did not verify the selected worker: %+v", q.fleetUpgradeStatus())
	}
	if q.retainWorkerHandover(q.s.Machines[a.ID]) {
		t.Fatal("exact adoption did not clear the handover")
	}
	got := job(t, q, run.ID)
	if got.State != "running" || got.Attempt != run.Attempt || got.Session != run.Session || got.Losses != 0 || got.StopAt != nil {
		t.Fatalf("takeover changed the running attempt: %+v", got)
	}
	if v := g.snapshot(); v.PID != live.PID || v.Birth != live.Birth {
		t.Fatalf("custodian process changed: %+v", v)
	}

	// Everything outside the selected host is as it was.
	if !reflect.DeepEqual(policies, q.s.HostMaintenance) {
		t.Fatalf("admission policy changed: %+v -> %+v", policies, q.s.HostMaintenance)
	}
	for machine, before := range untouched {
		now := q.fleetQueue.Proof
		if machine != "@queue" {
			now = q.s.Machines[machine].Fleet
		}
		if now.PID != before.PID || now.Session != before.Session || now.Version != before.Version || now.SHA256 != before.SHA256 {
			t.Fatalf("%s was replaced: %+v -> %+v", machine, before, now)
		}
	}
	if v := job(t, q, other.ID); v.State != "running" || v.Attempt != other.Attempt || v.Losses != 0 || v.StopAt != nil {
		t.Fatalf("another worker's attempt changed: %+v", v)
	}
	for _, j := range q.s.Jobs {
		if j.Spec.Name == fleetStartProbeName && (len(j.Spec.AllowedMachines) != 1 || j.Spec.AllowedMachines[0] != a.ID) {
			t.Fatalf("start probe outside the selected worker: %+v", j.Spec.AllowedMachines)
		}
	}

	// The same custodian finishes after the swap; the new worker publishes its
	// durable result, which the queue accepts once with confirmed release.
	if err := PrepareStateRoots(g.dir); err != nil {
		t.Fatal(err)
	}
	receipt := result(run, true)
	done := live
	done.At, done.Finished, done.State.Result = c.Now(), true, &receipt
	if err := saveWorkerJSON(g.dir, custodianName(id, "snapshot"), done); err != nil {
		t.Fatal(err)
	}
	p, err := (Worker{Clock: c}).adoptCustodian(store)
	if err != nil {
		t.Fatal(err)
	}
	defer p.custodian.detach()
	var final Result
	select {
	case f := <-p.done:
		if f.err != nil {
			t.Fatal(f.err)
		}
		final = f.result
	case <-time.After(testWaitTimeout):
		t.Fatal("replacement did not recover the result")
	}
	if v := p.custodian.snapshot(); v.PID != live.PID || v.Birth != live.Birth {
		t.Fatalf("result came from a different custodian process: %+v", v)
	}
	stale := final
	stale.Attempt++
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatalf("different attempt was accepted: %v", err)
	}
	for range 2 {
		if err := q.Complete(final); err != nil {
			t.Fatal(err)
		}
	}
	got = job(t, q, run.ID)
	if got.State != "succeeded" || !got.ReleaseConfirmed || got.Attempt != run.Attempt || got.Session != run.Session || len(got.AttemptHistory) != 1 {
		t.Fatalf("result was lost or settled more than once: %+v", got)
	}
}

// A scoped request refuses what it cannot update and otherwise takes the
// ordinary natural wait when takeover is unproved. Unselected hosts never
// block it and are never drained.
func TestFleetUpgradeScopedWorkerRefusalAndNaturalWait(t *testing.T) {
	refused := func(t *testing.T, q *Queue, host, code string) {
		t.Helper()
		var refusal *HTTPError
		if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Host: host}); !errors.As(err, &refusal) || refusal.Code != code {
			t.Fatalf("--host %s: want %s, got %v", host, code, err)
		}
		if q.s.FleetUpgrade != nil {
			t.Fatal("refused request created an operation")
		}
	}
	t.Run("no worker", func(t *testing.T) {
		q, _, _ := fleetFixture(t, "q", "a")
		refused(t, q, "missing", "fleet_host_no_worker")
		refused(t, q, "q", "fleet_host_no_worker") // The queue service alone is never selected.
	})
	t.Run("selected worker unavailable", func(t *testing.T) {
		q, c, workers := fleetFixture(t, "q", "a", "b")
		c.Advance(time.Hour)
		q.fleetQueue.Proof.ObservedAt = c.Now()
		m := cloneMachine(q.s.Machines[workers[1].ID])
		m.Fleet.ObservedAt = c.Now()
		check(t, q, m, nil)
		refused(t, q, "a", "fleet_worker_runtime_unavailable")
		var refusal *HTTPError
		_, err := q.StartFleetUpgrade(FleetUpgradeRequest{Host: "a"})
		if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "worker "+workers[0].ID+" on host a") {
			t.Fatalf("refusal does not name the selected worker: %v", err)
		}
		// Another host's stale proof does not block a scoped update of b.
		if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Host: "b"}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("natural wait", func(t *testing.T) {
		q, c, workers := fleetFixture(t, "q", "a", "b")
		a := workers[0]
		s := spec("experiment")
		s.Resumable = false
		run := fleetRun(t, q, a, s)
		policy := q.s.HostMaintenance["b"]
		q.s.Machines[a.ID].Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Reason: "attempt has no detached custodian"}
		if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Host: "a"}); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < 5; n++ {
			c.Advance(StopGrace)
			fleetRefresh(t, q, c)
		}
		h := q.fleetActiveHost()
		if h == nil || h.Host != "a" || h.Takeover || h.Phase != "draining" || !strings.Contains(h.Wait, "finish naturally") || h.TakeoverWait == "" || len(h.StopRequests) != 0 {
			t.Fatalf("unproved takeover did not take the ordinary natural wait: %+v", h)
		}
		if i, err := q.fleetInstruction("a", a.ID, a.Session); err != nil || i.Operation != "" {
			t.Fatal("unproved takeover issued an instruction", i, err)
		}
		if got := job(t, q, run.ID); got.State != "running" || got.StopAt != nil || got.Attempt != run.Attempt {
			t.Fatalf("natural wait interrupted work: %+v", got)
		}
		if q.maintenanceBlocked("b") || !reflect.DeepEqual(policy, q.s.HostMaintenance["b"]) {
			t.Fatal("unselected host drained", q.s.HostMaintenance["b"])
		}
		finish(t, q, run, true)
		fleetRefresh(t, q, c)
		if h.Phase != "worker_update" {
			t.Fatalf("natural completion did not permit the update: %+v", h)
		}
	})
}

// A scoped operation records the hosts it leaves alone so the next full
// rollout still adopts their retained drains and owed start proofs.
func TestFleetUpgradeScopedKeepsOtherHostsDebt(t *testing.T) {
	q, c, workers := fleetFixture(t, "q", "a", "b")
	b := workers[1]
	current := cloneMachine(q.s.Machines[b.ID])
	current.Fleet = fleetTestProof(c, b.Session, 300, true)
	check(t, q, current, nil)
	if _, err := q.ChangeHostMaintenance("b", HostMaintenanceRequest{Action: "drain", DrainOwner: "old-op"}); err != nil {
		t.Fatal(err)
	}
	q.s.FleetUpgrade = &fleetUpgrade{Operation: "old-op", State: "stopped", Revision: 1, Release: *q.s.EnrollmentRelease, Hosts: []*fleetUpgradeHost{
		{FleetHostStatus: FleetHostStatus{Host: "b", Phase: "deferred", Outcome: FleetHostDeferred, DeferredReason: "host_capacity"}, Machine: b.ID, StartIdentity: "owed", Owned: true, Policy: q.s.HostMaintenance["b"]},
	}}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Host: "a"}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	fleetActivate(t, q, c)
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "complete" {
		t.Fatal("scoped operation incomplete", q.fleetUpgradeStatus())
	}
	if p := q.s.HostMaintenance["b"]; p.Mode != "drained" || p.DrainOwner != "old-op" {
		t.Fatal("scoped operation touched another host's drain", p)
	}
	for _, h := range q.fleetUpgradeStatus().Hosts {
		if h.Host == "b" && (h.Phase != "skipped" || h.Outcome != FleetHostDeferred || h.DeferredReason != "host_capacity") {
			t.Fatalf("scoped operation hid another host's deferral: %+v", h)
		}
	}
	scoped := q.s.FleetUpgrade.Operation
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	for _, h := range q.s.FleetUpgrade.Hosts {
		if h.Host == "b" && (h.Phase != "pending" || h.WorkerDone || !h.Owned || h.StartIdentity == "") {
			t.Fatalf("full rollout lost the debt the scoped operation carried: %+v", h)
		}
	}
	for n := 0; n < 6 && q.s.FleetUpgrade.State == "running"; n++ {
		fleetTick(t, q)
		if q.fleetActiveHost() != nil {
			fleetActivate(t, q, c)
		}
	}
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "complete" || q.s.FleetUpgrade.Operation == scoped || q.maintenanceBlocked("b") {
		t.Fatalf("full rollout did not prove and resume the carried host: %+v %+v", q.fleetUpgradeStatus(), q.s.HostMaintenance)
	}
}
