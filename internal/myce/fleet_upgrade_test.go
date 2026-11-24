package myce

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func fleetTestProof(clock Clock, session string, pid int, target bool) *FleetRuntimeProof {
	version, hash := "v1.0.0", strings.Repeat("b", 64)
	if target {
		version, hash = "v1.0.1", strings.Repeat("a", 64)
	}
	return &FleetRuntimeProof{Version: version, SHA256: hash, Session: session, PID: pid, ServiceHealthy: true, RunnerSmoke: true, StartGate: startGateProtocol, AttemptProtocol: keyedAttemptProtocol, ObservedAt: clock.Now()}
}

// fleetFixture approves a signed v1.0.1 and starts a queue on queueHost and
// one v1.0.0 worker per host, all with fresh runtime proofs.
func fleetFixture(t *testing.T, queueHost string, hosts ...string) (*Queue, *fakeClock, []Machine) {
	t.Helper()
	q, c := fixture(t)
	signed, key, _ := signedTestRelease(t)
	if err := q.ConfigureEnrollmentRelease(signed, key, "https://publisher.invalid/release"); err != nil {
		t.Fatal(err)
	}
	q.fleetQueue = &fleetQueueRuntime{Host: queueHost, OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-old", 100, false), ObservedAt: c.Now()}
	var workers []Machine
	for n, host := range hosts {
		m := machine("worker-"+host, "fake", host)
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 32
		m.Fleet = fleetTestProof(c, m.Session, 200+n, false)
		check(t, q, m, nil)
		workers = append(workers, m)
	}
	return q, c, workers
}
func fleetTick(t *testing.T, q *Queue) {
	t.Helper()
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
}
func fleetInstructionFor(t *testing.T, q *Queue) FleetUpdateInstruction {
	t.Helper()
	h := q.fleetActiveHost()
	if h == nil {
		t.Fatal("no active host", q.fleetUpgradeStatus())
	}
	machine, session := h.Machine, ""
	if h.Before != nil {
		session = h.Before.Session
	}
	if h.Phase == "queue_update" {
		machine, session = "@queue", h.QueueBefore.Session
	}
	i, err := q.fleetInstruction(h.Host, machine, session)
	if err != nil || i.Operation == "" {
		t.Fatalf("instruction at %s: %+v %v", h.Phase, i, err)
	}
	return i
}

// fleetRestart plays the helper's restart: the service reports a new process
// running proof's release.
func fleetRestart(t *testing.T, q *Queue, c *fakeClock, i FleetUpdateInstruction, target bool) {
	t.Helper()
	if i.Machine == "@queue" {
		q.fleetQueue.Proof = fleetTestProof(c, "queue-new", q.fleetQueue.Proof.PID+1, target)
		return
	}
	m := cloneMachine(q.s.Machines[i.Machine])
	m.Session = "new-" + m.Session
	m.Fleet = fleetTestProof(c, m.Session, m.Fleet.PID+1000, target)
	if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, Fresh: true, PreviousSession: i.Session}); err != nil {
		t.Fatal(err)
	}
}

// fleetActivate updates the active service and waits out its stability window.
func fleetActivate(t *testing.T, q *Queue, c *fakeClock) string {
	t.Helper()
	if h := q.fleetActiveHost(); h != nil && h.Phase == "worker_start" {
		host := h.Host + "/" + h.Machine
		fleetCompleteStartProbe(t, q)
		return host
	}
	i := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	fleetRestart(t, q, c, i, true)
	fleetTick(t, q)
	// Deliver proofs often enough to preserve other hosts' running attempts,
	// rather than committing every second or jumping past their loss deadlines.
	remaining := fleetStableFor + time.Second
	for remaining > 0 {
		step := min(remaining, fleetRuntimeFreshFor/2)
		for id := range q.s.Machines {
			step = min(step, q.machineLossTimeout(id)/2)
		}
		c.Advance(step)
		fleetRefresh(t, q, c)
		remaining -= step
	}
	return i.Host + "/" + i.Machine
}

func fleetHeartbeatAdvance(t *testing.T, q *Queue, c *fakeClock) {
	t.Helper()
	c.Advance(time.Second)
	fleetRefresh(t, q, c)
}

// fleetRefresh delivers one current check-in from every worker, with its
// running attempts, and fresh proofs, then ticks.
func fleetRefresh(t *testing.T, q *Queue, c *fakeClock) {
	t.Helper()
	fleetRefreshWithoutStart(t, q, c)
	fleetCompleteStartProbe(t, q)
}

func fleetCompleteStartProbe(t *testing.T, q *Queue) {
	t.Helper()
	h := q.fleetActiveHost()
	if h == nil || h.Phase != "worker_start" || q.s.FleetUpgrade.State != "running" {
		return
	}
	j := q.s.Jobs[h.StartProbe]
	if j == nil {
		t.Fatal("missing start probe", h)
	}
	if j.Attempt == 0 {
		q.clock.(*fakeClock).Advance(time.Nanosecond)
		reply := check(t, q, *q.s.Machines[h.Machine], nil)
		// The ordinary scheduler can prefer useful resumed work. Its fresh
		// Started is equally valid proof; do not finish that customer's job.
		if offered := reply.Job; offered != nil && offered.ID != j.ID {
			if err := q.Started(result(offered, true)); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			return
		}
	}
	j = q.s.Jobs[h.StartProbe]
	if j.Attempt != 1 || j.Machine != h.Machine || j.State != "running" {
		t.Fatalf("start probe not assigned: state=%s attempt=%d reason=%s", j.State, j.Attempt, job(t, q, j.ID).Reason)
	}
	r := cloneJob(j)
	if err := q.Started(result(&r, true)); err != nil {
		t.Fatal(err)
	}
	finish(t, q, &r, true)
	fleetTick(t, q)
}

func fleetRefreshWithoutStart(t *testing.T, q *Queue, c *fakeClock) {
	t.Helper()
	drainAdmissionWait(t, q)
	for _, m := range q.s.Machines {
		if m.Fleet != nil {
			m.Fleet.ObservedAt = c.Now()
		}
		checkin := Checkin{Machine: *m, period: time.Second}
		for _, j := range q.liveJobs() {
			if j.Machine == m.ID && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") {
				checkin.AttemptProtocol = keyedAttemptProtocol
				checkin.Attempts = append(checkin.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt, Progress: j.Progress}, Session: j.Session, Started: j.StartedAt != nil})
			}
		}
		if _, err := q.Checkin(checkin); err != nil {
			t.Fatal(err)
		}
		drainAdmissionWait(t, q)
	}
	q.s.LastWorkerContact = c.Now()
	if q.fleetQueue != nil && q.fleetQueue.Proof != nil {
		q.fleetQueue.Proof.ObservedAt = c.Now()
	}
	fleetTick(t, q)
}

func fleetRun(t *testing.T, q *Queue, m Machine, s Spec) *Job {
	t.Helper()
	s.AllowedMachines = []string{m.ID}
	j := submit(t, q, s)
	r := check(t, q, m, nil).Job
	if r == nil || r.ID != j.ID {
		t.Fatal("job not assigned", m.ID)
	}
	if err := q.Started(result(r, true)); err != nil {
		t.Fatal(err)
	}
	return r
}

// An unhealthy result stops advancing: later hosts stay untouched and the
// failed host stays drained. A rerun is a new operation that adopts that drain.
func TestFleetUpgradeStopsOnUnhealthyHost(t *testing.T) {
	cases := map[string]struct {
		fail func(*testing.T, *Queue, *fakeClock, FleetUpdateInstruction)
		want string
	}{
		"helper failure": {func(t *testing.T, q *Queue, c *fakeClock, i FleetUpdateInstruction) {
			if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "fleet_health_failed"}); err != nil {
				t.Fatal(err)
			}
		}, "service_health_failed"},
		"wrong bytes after restart": {func(t *testing.T, q *Queue, c *fakeClock, i FleetUpdateInstruction) {
			fleetRestart(t, q, c, i, false)
		}, "worker_running_hash_mismatch"},
		"never healthy": {func(t *testing.T, q *Queue, c *fakeClock, i FleetUpdateInstruction) {
			c.Advance(fleetStepTimeout)
		}, "worker_update_not_verified"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			q, c, w := fleetFixture(t, "q", "a", "b")
			out, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
			if err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			i := fleetInstructionFor(t, q)
			if i.Host != "a" {
				t.Fatal("first host", i.Host)
			}
			tc.fail(t, q, c, i)
			fleetRefresh(t, q, c)
			for n := 0; n < 3; n++ {
				fleetHeartbeatAdvance(t, q, c)
			}
			status := q.fleetUpgradeStatus()
			if status.State != "stopped" || status.Hosts[0].Failure != tc.want {
				t.Fatalf("not stopped with %s: %+v", tc.want, status)
			}
			if p := q.s.HostMaintenance["a"]; p.Mode != "drained" || p.DrainOwner != out.Operation {
				t.Fatal("failed host lost its drain", p)
			}
			if q.maintenanceBlocked("b") || status.Hosts[1].Phase != "pending" {
				t.Fatal("advanced past an unhealthy host", status)
			}
			if i, _ := q.fleetInstruction("b", w[1].ID, w[1].Session); i.Operation != "" {
				t.Fatal("instruction for a later host")
			}
			// Fix forward: rerun. The new operation owns the left drain.
			again, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
			if err != nil || again.Operation == out.Operation {
				t.Fatal("rerun did not start a new operation", again, err)
			}
			fleetTick(t, q)
			for n := 0; n < 4 && q.s.FleetUpgrade.State == "running"; n++ {
				if h := q.fleetActiveHost(); h != nil && h.Phase != "draining" {
					fleetActivate(t, q, c)
				}
				fleetHeartbeatAdvance(t, q, c)
			}
			if q.s.FleetUpgrade.State != "complete" || q.maintenanceBlocked("a") || q.maintenanceBlocked("b") {
				t.Fatalf("rerun did not finish and resume: %+v %+v", q.fleetUpgradeStatus(), q.s.HostMaintenance)
			}
		})
	}
}

// A rerun while an operation runs returns it; a different target is refused.
// Hosts already on the release are not drained again.
func TestFleetUpgradeRerunAndAlreadyCurrent(t *testing.T) {
	q, c, w := fleetFixture(t, "q", "a", "b")
	m := cloneMachine(q.s.Machines[w[1].ID])
	m.Fleet = fleetTestProof(c, m.Session, 300, true)
	if _, err := q.Checkin(Checkin{Machine: m, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	out, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if again, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil || again.Operation != out.Operation {
		t.Fatal("rerun replaced a running operation", again, err)
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Urgency: "critical"}); err == nil || !strings.Contains(err.Error(), out.Operation) {
		t.Fatal("conflicting request not refused with the running operation named", err)
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Urgency: "urgent"}); err == nil {
		t.Fatal("unknown urgency accepted")
	}
	for _, h := range q.s.FleetUpgrade.Hosts {
		if h.Host == "b" && (h.Phase != "complete" || h.Wait != "already on the release") {
			t.Fatalf("current host not skipped: %+v", h)
		}
	}
	fleetTick(t, q)
	fleetActivate(t, q, c)
	fleetTick(t, q)
	fleetActivate(t, q, c)
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "complete" {
		t.Fatal("incomplete", q.fleetUpgradeStatus())
	}
}

// Operator maintenance is never overridden to manufacture start acceptance.
// A host without usable runtime proof is still skipped visibly.
func TestFleetUpgradeOperatorMaintenanceStaysOperators(t *testing.T) {
	q, c, w := fleetFixture(t, "q", "a", "b")
	if _, err := q.ChangeHostMaintenance("a", HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance("b", HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	q.s.Machines[w[1].ID].Fleet = nil
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if got := fleetActivate(t, q, c); got != "a/worker-a" {
		t.Fatal(got)
	}
	status := q.fleetUpgradeStatus()
	if status.State != "stopped" || status.Hosts[0].Failure != "worker_start_maintenance_blocked" || status.Hosts[1].Phase != "skipped" || status.Hosts[1].Failure != "worker_unmanaged" {
		t.Fatalf("out-of-service host not skipped visibly: %+v", status)
	}
	if p := q.s.HostMaintenance["a"]; p.Mode != "drained" || p.DrainOwner != "operator" {
		t.Fatal("operator drain resumed or replaced", p)
	}
	if q.s.Machines[w[0].ID].Fleet.Version != "v1.0.1" {
		t.Fatal("drained host not updated")
	}
}

// Operations retired by earlier releases and a stopped one left drains. The
// next upgrade adopts those, verifies the hosts and resumes them, and the old
// records are dropped from the state.
func TestFleetUpgradeCleansUpAbandonedOperations(t *testing.T) {
	q, c, _ := fleetFixture(t, "q", "a", "b", "c")
	var legacy state
	if err := json.Unmarshal([]byte(`{"fleet_superseded":[{"urgency":"normal","operation":"old-1","release":"v0.9.0","state":"stopped","queue_only":true,"reason":"hosts since upgraded by per-host installers","hosts":[{"host":"a","phase":"draining"}],"retained_drains":["a"],"next":"run myce fleet upgrade"}]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	q.s.FleetSuperseded = legacy.FleetSuperseded
	for host, owner := range map[string]string{"a": "old-1", "b": "old-2", "c": "operator"} {
		if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "drain", DrainOwner: owner}); err != nil {
			t.Fatal(err)
		}
	}
	q.s.FleetUpgrade = &fleetUpgrade{Operation: "old-2", State: "stopped", Revision: 1, Release: *q.s.EnrollmentRelease}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	if q.s.FleetSuperseded != nil {
		t.Fatal("abandoned records kept")
	}
	for q.s.FleetUpgrade.State == "running" {
		fleetTick(t, q)
		if q.fleetActiveHost() == nil {
			break
		}
		fleetActivate(t, q, c)
	}
	fleetTick(t, q)
	// The abandoned operation drains were accepted and resumed. The unrelated
	// operator drain now correctly stops assignment acceptance, without being
	// adopted or resumed just to finish the rollout.
	if q.s.FleetUpgrade.State != "stopped" || q.s.FleetUpgrade.Hosts[2].Failure != "worker_start_maintenance_blocked" || q.maintenanceBlocked("a") || q.maintenanceBlocked("b") || q.s.HostMaintenance["c"].DrainOwner != "operator" {
		t.Fatalf("abandoned drains not adopted and resumed: %+v %+v", q.fleetUpgradeStatus(), q.s.HostMaintenance)
	}
	data, err := json.Marshal(q.s)
	if err != nil || strings.Contains(string(data), "fleet_superseded") {
		t.Fatal("abandoned records persisted", err)
	}
}
