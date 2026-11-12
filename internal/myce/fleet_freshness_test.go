package myce

import (
	"errors"
	"testing"
	"time"
)

// slowProofWorker models a loaded worker (#1450): it checks in every 2 s, but
// each check-in carries the proof its runtime last collected, which is renewed
// only every 6–8 s, so the queue sees healthy proofs 6–10 s old.
type slowProofWorker struct {
	m                    Machine
	pid                  int
	target, alive        bool
	collected, next      time.Time
	renewals, checkinsAt int
}

var slowProofIntervals = []time.Duration{6 * time.Second, 8 * time.Second, 7 * time.Second}

func (w *slowProofWorker) step(t *testing.T, q *Queue, now time.Time, extra Checkin) {
	t.Helper()
	if !w.alive {
		return
	}
	if !now.Before(w.next) {
		w.collected = now
		w.next = now.Add(slowProofIntervals[w.renewals%len(slowProofIntervals)])
		w.renewals++
	}
	if w.checkinsAt++; w.checkinsAt%2 != 1 && extra.PreviousSession == "" {
		return
	}
	m := w.m
	m.Fleet = fleetTestProof(q.clock, m.Session, w.pid, w.target)
	m.Fleet.ObservationAgeSeconds = now.Sub(w.collected).Seconds()
	extra.Machine, extra.period = m, 2*time.Second
	if _, err := q.Checkin(extra); err != nil {
		t.Fatal(err)
	}
}

type slowProofFleet struct {
	q       *Queue
	c       *fakeClock
	workers map[string]*slowProofWorker
}

func newSlowProofFleet(t *testing.T) *slowProofFleet {
	q, c, machines := fleetFixture(t, "q", "a", "b")
	f := &slowProofFleet{q: q, c: c, workers: map[string]*slowProofWorker{}}
	for n, m := range machines {
		f.workers[m.ID] = &slowProofWorker{m: m, pid: 200 + n, alive: true, collected: c.Now(), next: c.Now().Add(time.Duration(n+1) * 3 * time.Second)}
	}
	return f
}

// second advances one second: workers deliver what is due, the queue's own
// local proof stays current, and the queue ticks.
func (f *slowProofFleet) second(t *testing.T) {
	t.Helper()
	f.c.Advance(time.Second)
	for _, id := range []string{"worker-a", "worker-b"} {
		f.workers[id].step(t, f.q, f.c.Now(), Checkin{})
	}
	if f.q.fleetQueue.Proof != nil {
		f.q.fleetQueue.Proof.ObservedAt = f.c.Now()
	}
	fleetTick(t, f.q)
}

func (f *slowProofFleet) oldestProofAge() time.Duration {
	var oldest time.Duration
	for _, m := range f.q.s.Machines {
		if m.Fleet != nil {
			oldest = max(oldest, f.c.Now().Sub(m.Fleet.ObservedAt))
		}
	}
	return oldest
}

// restartDue plays each service's updater once its instruction is issued: the
// worker comes back as a new process (target or wrong bytes), the queue on
// the release.
func (f *slowProofFleet) restartDue(t *testing.T, target bool, dies bool) {
	t.Helper()
	h := f.q.fleetActiveHost()
	if h == nil {
		return
	}
	machine, session := h.Machine, ""
	if h.Phase == "queue_update" && h.QueueBefore != nil {
		machine, session = "@queue", h.QueueBefore.Session
	} else if h.Phase == "worker_update" && h.Before != nil {
		session = h.Before.Session
	} else {
		return
	}
	i, err := f.q.fleetInstruction(h.Host, machine, session)
	if err != nil || i.Operation == "" {
		return
	}
	if err := f.q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	if machine == "@queue" {
		f.q.fleetQueue.Proof = fleetTestProof(f.c, "queue-new", f.q.fleetQueue.Proof.PID+1, true)
		return
	}
	w := f.workers[machine]
	old := w.m.Session
	w.m.Session, w.pid, w.target = "new-"+old, w.pid+1000, target
	w.collected, w.next = f.c.Now(), f.c.Now().Add(slowProofIntervals[w.renewals%len(slowProofIntervals)])
	w.step(t, f.q, f.c.Now(), Checkin{Fresh: true, PreviousSession: old})
	w.alive = !dies
}

func TestFleetProofFreshnessFollowsMeasuredCadence(t *testing.T) {
	start := func(t *testing.T, f *slowProofFleet) *FleetUpgradeStatus {
		t.Helper()
		for n := 0; n < 60; n++ {
			f.second(t)
		}
		// Start at a moment the fixed 5 s bound would have refused.
		for f.oldestProofAge() < 6*time.Second {
			f.second(t)
		}
		out, err := f.q.StartFleetUpgrade(FleetUpgradeRequest{})
		if err != nil {
			t.Fatalf("healthy slow-proof fleet refused with proofs %s old: %v", f.oldestProofAge(), err)
		}
		return out
	}
	run := func(t *testing.T, f *slowProofFleet, target, dies bool) *FleetUpgradeStatus {
		t.Helper()
		for n := 0; n < int((fleetStepTimeout+time.Minute)/time.Second) && f.q.s.FleetUpgrade.State == "running"; n++ {
			f.restartDue(t, target, dies)
			f.second(t)
		}
		return f.q.fleetUpgradeStatus()
	}

	t.Run("loaded fleet completes", func(t *testing.T) {
		f := newSlowProofFleet(t)
		start(t, f)
		status := run(t, f, true, false)
		if status.State != "complete" {
			t.Fatalf("rollout did not complete under slow proofs: %+v", status)
		}
		for id, w := range f.workers {
			m := f.q.s.Machines[id]
			if m.Session != w.m.Session || !f.q.fleetCurrent(m.Fleet, "linux", "amd64") {
				t.Fatalf("%s not on the release: %+v", id, m.Fleet)
			}
		}
		for _, host := range []string{"a", "b", "q"} {
			if f.q.maintenanceBlocked(host) {
				t.Fatal("host left drained", host)
			}
		}
	})
	t.Run("wrong bytes stop", func(t *testing.T) {
		f := newSlowProofFleet(t)
		start(t, f)
		status := run(t, f, false, false)
		if status.State != "stopped" || status.Hosts[0].Failure != "worker_running_hash_mismatch" {
			t.Fatalf("wrong bytes not stopped: %+v", status)
		}
	})
	t.Run("dead new process stops", func(t *testing.T) {
		f := newSlowProofFleet(t)
		start(t, f)
		status := run(t, f, true, true)
		if status.State != "stopped" || status.Hosts[0].Failure != "worker_update_not_verified" {
			t.Fatalf("dead worker passed health: %+v", status)
		}
		if p := f.q.s.HostMaintenance["a"]; p.Mode != "drained" {
			t.Fatal("failed host lost its drain", p)
		}
	})
	t.Run("silent worker refuses start", func(t *testing.T) {
		f := newSlowProofFleet(t)
		for n := 0; n < 60; n++ {
			f.second(t)
		}
		f.workers["worker-b"].alive = false
		for n := 0; n <= int(fleetProofMaxAge/time.Second); n++ {
			f.second(t)
		}
		_, err := f.q.StartFleetUpgrade(FleetUpgradeRequest{})
		var refusal *HTTPError
		if !errors.As(err, &refusal) || refusal.Code != "fleet_worker_unmanaged" {
			t.Fatal("a silent worker's last proof still authorized the rollout", err)
		}
	})
}

// One late proof keeps a running health window; a proof that stays missing past
// one further bound restarts it, and a new process starts at the strict bound.
func TestFleetProofLateKeepsHealthWindow(t *testing.T) {
	q, c, _ := fleetFixture(t, "q", "a")
	before := fleetTestProof(c, "old", 1, false)
	p := fleetTestProof(c, "new", 2, true)
	if q.fleetProofBound(p) != fleetRuntimeFreshFor {
		t.Fatal("unmeasured process not judged strictly", q.fleetProofBound(p))
	}
	h := &fleetUpgradeHost{StepAt: c.Now()}
	if q.fleetVerify(h, p, before, "linux", "amd64", "worker") || h.HealthSince.IsZero() {
		t.Fatal("fresh new process did not start its window", h)
	}
	since := h.HealthSince
	c.Advance(fleetRuntimeFreshFor + time.Second)
	if q.fleetVerify(h, p, before, "linux", "amd64", "worker") || h.HealthSince != since {
		t.Fatal("one late proof restarted the window", h)
	}
	c.Advance(fleetRuntimeFreshFor)
	if q.fleetVerify(h, p, before, "linux", "amd64", "worker") || !h.HealthSince.IsZero() {
		t.Fatal("a missing proof kept the window", h)
	}
	unhealthy := *p
	unhealthy.ServiceHealthy = false
	h.HealthSince, h.HealthIdentity = since, fleetHealthIdentity(p)
	if q.fleetVerify(h, &unhealthy, before, "linux", "amd64", "worker") || !h.HealthSince.IsZero() {
		t.Fatal("an unhealthy proof kept the window", h)
	}
}
