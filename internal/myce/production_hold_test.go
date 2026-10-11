package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func holdFleet(t *testing.T, enabled bool) (*Queue, *fakeClock, Machine, Machine, Machine) {
	t.Helper()
	q, c := fixture(t)
	if enabled {
		if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true, ProductionIdle: "180s"}}); err != nil {
			t.Fatal(err)
		}
	}
	a, b, s := machine("b70-0", "fake"), machine("b70-1", "fake"), machine("spark", "fake")
	a.HostID = "b70"
	b.HostID = "b70"
	check(t, q, a, nil)
	check(t, q, b, nil)
	check(t, q, s, nil)
	return q, c, a, b, s
}
func TestProductionHoldDisplacesBothCardsAndCooldown(t *testing.T) {
	q, c, a, b, s := holdFleet(t, true)
	e := spec("experiment")
	x := submit(t, q, e)
	x = check(t, q, a, nil).Job
	y := submit(t, q, e)
	y = check(t, q, b, nil).Job
	if x == nil || y == nil {
		t.Fatal("experiments not started")
	}
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	p.Needs.MemoryGB = 1
	prod := submit(t, q, p)
	if !check(t, q, a, x).Stop || !check(t, q, b, y).Stop {
		t.Fatal("both cards must stop")
	}
	c.Advance(3 * time.Second)
	if reason := job(t, q, prod.ID).Reason; !strings.Contains(reason, "stopping 2") || !strings.Contains(reason, "7s") {
		t.Fatal(reason)
	}
	finish(t, q, x, false)
	if check(t, q, a, nil).Job != nil {
		t.Fatal("production started before sibling release")
	}
	resumed := check(t, q, s, nil).Job
	if resumed == nil || resumed.ID != x.ID || resumed.Attempt != 2 || resumed.Checkpoint != x.ID || resumed.Losses != 0 {
		t.Fatalf("resume: %+v", resumed)
	}
	// The new worker consumes the NAS-relative assignment under its local root.
	saved := filepath.Join(q.root, x.ID, "attempt-1", "saved")
	if e := os.MkdirAll(filepath.Dir(saved), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(saved, []byte("unit-7"), 0600); e != nil {
		t.Fatal(e)
	}
	w := recoveryWorker(t)
	w.Machine = s
	store, e2 := w.openStore()
	if e2 != nil {
		t.Fatal(e2)
	}
	defer store.close()
	payload := cloneJob(resumed)
	payload.Checkpoint = filepath.Join(q.root, payload.Checkpoint)
	payload.Spec.Run.Command = []string{"sh", "-c", `test "$MYCE_ATTEMPT" = 2 && test "$(cat "$MYCE_CHECKPOINT_DIR/saved")" = unit-7 && test "$MYCE_MACHINE" = spark`}
	process, e2 := w.start(store, payload)
	if e2 != nil {
		t.Fatal(e2)
	}
	receipt := <-process.done
	if receipt.err != nil || !receipt.result.Success {
		t.Fatalf("NAS resume %+v", receipt)
	}
	if payload.Checkpoint != filepath.Join(q.root, x.ID) {
		t.Fatal("checkpoint custody changed")
	}
	finish(t, q, y, false)
	run := check(t, q, a, nil).Job
	if run == nil || run.ID != prod.ID {
		t.Fatalf("production: %+v", run)
	}
	finish(t, q, run, true)
	if check(t, q, b, nil).Job != nil {
		t.Fatal("admitted during cooldown")
	}
	c.Advance(179 * time.Second)
	check(t, q, a, nil)
	check(t, q, b, nil)
	check(t, q, s, resumed)
	if check(t, q, b, nil).Job != nil {
		t.Fatal("admitted at 179s")
	}
	c.Advance(time.Second)
	if got := check(t, q, b, nil).Job; got == nil || got.ID != y.ID || got.Attempt != 2 {
		t.Fatalf("after cooldown: %+v", got)
	}
}

func TestProductionHoldDefaultOffAndOtherHost(t *testing.T) {
	for _, mode := range []string{"off", "spark", "impossible"} {
		t.Run(mode, func(t *testing.T) {
			q, _, a, b, s := holdFleet(t, mode != "off")
			x := submit(t, q, spec("experiment"))
			x = check(t, q, a, nil).Job
			y := submit(t, q, spec("experiment"))
			y = check(t, q, b, nil).Job
			p := spec("production")
			p.AllowedMachines = []string{a.ID}
			if mode == "spark" {
				p.AllowedMachines = []string{s.ID}
			}
			if mode == "impossible" {
				p.Needs.MemoryGB = 100
			}
			submit(t, q, p)
			if check(t, q, b, y).Stop {
				t.Fatal("unrelated card stopped")
			}
			if check(t, q, a, x).Stop != (mode == "off") {
				t.Fatal("wrong preemption")
			}
		})
	}
}
func TestProductionHoldSecondArrivalAndZeroIdle(t *testing.T) {
	for _, idle := range []string{"180s", "0s"} {
		t.Run(idle, func(t *testing.T) {
			q, c := fixture(t)
			if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true, ProductionIdle: idle}}); err != nil {
				t.Fatal(err)
			}
			a, b := machine("a", "fake"), machine("b", "fake")
			a.HostID = "b70"
			b.HostID = "b70"
			check(t, q, a, nil)
			check(t, q, b, nil)
			p := spec("production")
			p.AllowedMachines = []string{a.ID}
			x := submit(t, q, p)
			x = check(t, q, a, nil).Job
			e := submit(t, q, spec("experiment"))
			second := submit(t, q, p)
			finish(t, q, x, true)
			if check(t, q, b, nil).Job != nil {
				t.Fatal("second queued production must hold")
			}
			second = check(t, q, a, nil).Job
			finish(t, q, second, true)
			if idle == "180s" {
				c.Advance(180 * time.Second)
				// Arrival is committed before expiry maintenance; it wins at the boundary.
				third := submit(t, q, p)
				if check(t, q, b, nil).Job != nil {
					t.Fatal("arrival at expiry must win")
				}
				third = check(t, q, a, nil).Job
				finish(t, q, third, true)
				c.Advance(-time.Second)
				if check(t, q, b, nil).Job != nil {
					t.Fatal("backward time reopened hold")
				}
				c.Advance(181 * time.Second)
			}
			if got := check(t, q, b, nil).Job; got == nil || got.ID != e.ID {
				t.Fatalf("idle expiry: %+v", got)
			}
		})
	}
}
func TestProductionHoldExpiryAndLostWorkerRetainRelease(t *testing.T) {
	q, c, a, b, s := holdFleet(t, true)
	x := submit(t, q, spec("experiment"))
	x = check(t, q, a, nil).Job
	yspec := spec("experiment")
	yspec.Resumable = false
	y := submit(t, q, yspec)
	y = check(t, q, b, nil).Job
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	prod := submit(t, q, p)
	if got := check(t, q, b, y); !got.Stop || !got.Kill {
		t.Fatal("non-resumable must stop immediately")
	}
	c.Advance(11 * time.Second)
	check(t, q, s, nil)
	if got := check(t, q, a, x).Job; got != nil {
		t.Fatal("sibling not acknowledged")
	}
	if !strings.Contains(job(t, q, prod.ID).Reason, "release unknown") {
		t.Fatal("missing unknown release")
	}
	// A stale attempt's acknowledgement settles custody without touching its replacement.
	finish(t, q, y, false)
	resumed := check(t, q, s, nil).Job
	if resumed == nil || resumed.Attempt != 2 || resumed.Losses != 0 {
		t.Fatalf("retry %+v", resumed)
	}
	finish(t, q, x, false)
	if got := check(t, q, a, nil).Job; got == nil || got.ID != prod.ID {
		t.Fatalf("released: %+v", got)
	}
}
func TestProductionHoldWorkerLoss(t *testing.T) {
	q, c, a, b, s := holdFleet(t, true)
	x := submit(t, q, spec("experiment"))
	x = check(t, q, a, nil).Job
	y := submit(t, q, spec("experiment"))
	y = check(t, q, b, nil).Job
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	prod := submit(t, q, p)
	c.Advance(61 * time.Second)
	check(t, q, a, x)
	check(t, q, s, nil)
	if got := check(t, q, a, x).Job; got != nil {
		t.Fatal("lost sibling freed hold")
	}
	if !strings.Contains(job(t, q, prod.ID).Reason, "release unknown") {
		t.Fatal("unknown loss absent")
	}
	fresh := b
	fresh.Session = "restarted"
	if _, err := q.Checkin(Checkin{Machine: fresh, Fresh: true, PreviousSession: b.Session}); err != nil {
		t.Fatal(err)
	}
	finish(t, q, x, false)
	if got := check(t, q, a, nil).Job; got == nil || got.ID != prod.ID {
		t.Fatalf("after cleanup %+v", got)
	}
	_ = y
}
func TestProductionHoldRestartDuringReclaimAndCooldown(t *testing.T) {
	q, c, a, b, _ := holdFleet(t, true)
	x := submit(t, q, spec("experiment"))
	x = check(t, q, a, nil).Job
	y := submit(t, q, spec("experiment"))
	y = check(t, q, b, nil).Job
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	prod := submit(t, q, p)
	path := q.dbPathForTest(t)
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q2, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	if got := check(t, q2, a, x); !got.Stop {
		t.Fatal("stop lost after restart")
	}
	if got := check(t, q2, a, x).Job; got != nil {
		t.Fatal("unknown sibling after restart")
	}
	finish(t, q2, y, false)
	finish(t, q2, x, false)
	// A free sibling check-in is also reconciliation.
	check(t, q2, b, nil)
	run := check(t, q2, a, nil).Job
	if run == nil || run.ID != prod.ID {
		t.Fatalf("production %+v", run)
	}
	finish(t, q2, run, true)
	c.Advance(179 * time.Second)
	if err := q2.Close(); err != nil {
		t.Fatal(err)
	}
	q3, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q3.Close()
	if got := check(t, q3, b, nil).Job; got != nil {
		t.Fatal("179s idle forgotten")
	}
	check(t, q3, a, nil)
	c.Advance(time.Second)
	if got := check(t, q3, b, nil).Job; got == nil {
		t.Fatal("idle interval not preserved")
	}
}
func (q *Queue) dbPathForTest(t *testing.T) string {
	t.Helper()
	var seq int
	var name, path string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestProductionHoldModelPathsAndResidency(t *testing.T) {
	q, c, a := modelFixture(t)
	a.HostID = "b70"
	check(t, q, a, nil)
	b := a
	b.ID = "two"
	b.Session = "session-two"
	check(t, q, b, nil)
	if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true, ProductionIdle: "180s"}}); err != nil {
		t.Fatal(err)
	}
	call := addModelCall(t, q, "experiment")
	reportModel(t, q, a, "ready")
	if call.Machine != a.ID {
		t.Fatal("experiment not dispatched")
	}
	// A loading sibling allocation must drain too.
	q.s.ModelSlots[slotKey(b.ID, "test")] = &ModelSlot{Model: "test", Machine: b.ID, Session: b.Session, State: "loading", Priority: "experiment"}
	prod := addModelCall(t, q, "production")
	if prod.Machine != "" {
		t.Fatal("production bypassed release")
	}
	for _, slot := range q.s.ModelSlots {
		if slot.State != "draining" {
			t.Fatal("lower model not drained")
		}
	}
	c.Advance(10 * time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if call.Error != "preempted by production host hold" {
		t.Fatalf("bounded preemption: %+v", call)
	}
	if err := q.finishCall(call.ID); err != nil {
		t.Fatal(err)
	}
	reportModel(t, q, a, "unloaded")
	reportModel(t, q, b, "unloaded")
	reportModel(t, q, a, "ready")
	if prod.Machine != a.ID {
		t.Fatal("production not dispatched")
	}
	if err := q.finishCall(prod.ID); err != nil {
		t.Fatal(err)
	}
	r := reportModel(t, q, a, "ready")
	if len(r.ModelPlan) != 1 || r.ModelPlan[0].State != "ready" {
		t.Fatalf("production residency should survive cooldown: %+v", r.ModelPlan)
	}
	next := addModelCall(t, q, "experiment")
	if next.Machine != "" {
		t.Fatal("resident model bypassed hold")
	}
	c.Advance(180 * time.Second)
	r = reportModel(t, q, a, "ready")
	if len(r.ModelPlan) == 0 || r.ModelPlan[0].State != "draining" {
		t.Fatal("idle production residency should drain at expiry")
	}
}
func TestProductionHoldPinProtected(t *testing.T) {
	q, _, a := modelFixture(t)
	a.HostID = "b70"
	check(t, q, a, nil)
	b := machine("two", "fake")
	b.HostID = "b70"
	check(t, q, b, nil)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: a.ID}); err != nil {
		t.Fatal(err)
	}
	reportModel(t, q, a, "ready")
	if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	p := spec("production")
	p.AllowedMachines = []string{b.ID}
	submit(t, q, p)
	for _, slot := range reportModel(t, q, a, "ready").ModelPlan {
		if slot.State != "ready" {
			t.Fatal("pin drained")
		}
	}
}
func TestProjectMachineAllowListForCalls(t *testing.T) {
	q, _, a := modelFixture(t)
	b := a
	b.ID = "spark"
	b.Session = "spark-session"
	check(t, q, b, nil)
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{b.ID}}}); e != nil {
		t.Fatal(e)
	}
	c := addModelCall(t, q, "production")
	if plan := reportModel(t, q, a, "ready").ModelPlan; len(plan) != 0 {
		t.Fatalf("loaded forbidden model %+v", plan)
	}
	reportModel(t, q, b, "ready")
	if c.Machine != b.ID {
		t.Fatalf("wrong call placement %+v", c)
	}
}

func TestProductionHoldCancelsPreparationAndWaitsForAcknowledgement(t *testing.T) {
	q, _, a, b, _ := holdFleet(t, true)
	b.EnvironmentBuilder = true
	b.EnvironmentReports = []EnvironmentReport{{Key: "background", State: "building"}}
	b.CopyReports = []CopyReport{{Model: "background", State: "copying"}}
	check(t, q, b, nil)
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	prod := submit(t, q, p)
	r := check(t, q, b, nil)
	if len(r.EnvironmentPlan) > 0 || len(r.CopyPlan) > 0 {
		t.Fatal("background preparation still admitted")
	}
	if check(t, q, a, nil).Job != nil {
		t.Fatal("production admitted before build/copy cleanup")
	}
	b.EnvironmentReports[0].State = "paused"
	b.CopyReports[0].State = "paused"
	check(t, q, b, nil)
	if got := check(t, q, a, nil).Job; got == nil || got.ID != prod.ID {
		t.Fatal("cleanup did not release production")
	}
}

func TestProductionHoldFailedCleanupIsNotRelease(t *testing.T) {
	q, _, a, b, _ := holdFleet(t, true)
	x := submit(t, q, spec("experiment"))
	x = check(t, q, b, nil).Job
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	submit(t, q, p)
	r := result(x, false)
	r.ReleasePending = true
	r.Forced = true
	if e := q.Complete(r); e != nil {
		t.Fatal(e)
	}
	if got := check(t, q, a, nil).Job; got != nil {
		t.Fatal("failed process cleanup granted production")
	}
	if j := job(t, q, x.ID); j.LastStopOutcome != "forced; release pending" {
		t.Fatalf("lost forced classification %+v", j)
	}
}

func TestProductionHoldReportsPinnedConflict(t *testing.T) {
	q, _, a := modelFixture(t)
	a.HostID = "b70"
	check(t, q, a, nil)
	if e := q.Pin(ModelPin{Project: "test", Model: "test", Machine: a.ID}); e != nil {
		t.Fatal(e)
	}
	reportModel(t, q, a, "ready")
	if e := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); e != nil {
		t.Fatal(e)
	}
	p := submit(t, q, spec("production"))
	if reason := job(t, q, p.ID).Reason; !strings.Contains(reason, "pinned model test") {
		t.Fatal(reason)
	}
}

func TestProductionHoldRestartReconcilesModelInference(t *testing.T) {
	q, c, a := modelFixture(t)
	a.HostID = "b70"
	check(t, q, a, nil)
	if e := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); e != nil {
		t.Fatal(e)
	}
	addModelCall(t, q, "production")
	reportModel(t, q, a, "ready")
	path, root := q.dbPathForTest(t), q.root
	if e := q.Close(); e != nil {
		t.Fatal(e)
	}
	restarted, e := Open(path, root, c)
	if e != nil {
		t.Fatal(e)
	}
	defer restarted.Close()
	if e = restarted.ConfigureModels(q.models); e != nil {
		t.Fatal(e)
	}
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	prod := submit(t, restarted, p)
	a.FreeMemoryGB = 0
	r, e := restarted.Checkin(Checkin{Machine: a, ModelReports: []ModelReport{{Name: "test", State: "ready", InFlight: 1, CallsMeasured: true}}})
	if e != nil {
		t.Fatal(e)
	}
	if r.Job != nil || !strings.Contains(job(t, restarted, prod.ID).Reason, "release unknown") {
		t.Fatal("restart inferred idle model")
	}
	r, e = restarted.Checkin(Checkin{Machine: a, ModelReports: []ModelReport{{Name: "test", State: "unloaded", CallsMeasured: true}}})
	if e != nil {
		t.Fatal(e)
	}
	a.FreeMemoryGB = 8
	if got := check(t, restarted, a, nil).Job; got == nil || got.ID != prod.ID {
		t.Fatalf("reconciled %+v", got)
	}
}

func TestProductionHoldWaitsForExperimentCallOnPinnedModel(t *testing.T) {
	q, c, a := modelFixture(t)
	a.HostID = "b70"
	check(t, q, a, nil)
	b := machine("two", "fake")
	b.HostID = "b70"
	check(t, q, b, nil)
	if e := q.Pin(ModelPin{Project: "test", Model: "test", Machine: a.ID}); e != nil {
		t.Fatal(e)
	}
	reportModel(t, q, a, "ready")
	call := addModelCall(t, q, "experiment")
	if call.Machine != a.ID {
		t.Fatal("experiment did not start")
	}
	if e := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); e != nil {
		t.Fatal(e)
	}
	p := spec("production")
	p.AllowedMachines = []string{b.ID}
	prod := submit(t, q, p)
	if check(t, q, b, nil).Job != nil {
		t.Fatal("production bypassed live experiment on pinned model")
	}
	c.Advance(10 * time.Second)
	if e := q.Tick(); e != nil {
		t.Fatal(e)
	}
	if call.Error != "preempted by production host hold" {
		t.Fatal("pinned experiment call not canceled")
	}
	if e := q.finishCall(call.ID); e != nil {
		t.Fatal(e)
	}
	if check(t, q, b, nil).Job != nil {
		t.Fatal("proxy completion is not a worker inference-release receipt")
	}
	if _, err := q.Checkin(Checkin{Machine: a, ModelReports: []ModelReport{{Name: "test", State: "ready", CallsMeasured: true}}}); err != nil {
		t.Fatal(err)
	}
	if got := check(t, q, b, nil).Job; got == nil || got.ID != prod.ID {
		t.Fatal("settled call did not release")
	}
	for _, slot := range reportModel(t, q, a, "ready").ModelPlan {
		if slot.State != "ready" {
			t.Fatal("pin was unloaded")
		}
	}
}

func TestProductionHoldRetainsExecutionLostBeforeDemand(t *testing.T) {
	q, c, a, b, s := holdFleet(t, true)
	x := submit(t, q, spec("experiment"))
	x = check(t, q, b, nil).Job
	a.FreeMemoryGB = 0
	s.FreeMemoryGB = 0
	for i := 0; i < 61; i++ {
		c.Advance(time.Second)
		check(t, q, a, nil)
		check(t, q, s, nil)
	}
	requireState(t, q, x.ID, "queued")
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	prod := submit(t, q, p)
	a.FreeMemoryGB = 8
	if check(t, q, a, nil).Job != nil {
		t.Fatal("prior loss silently released physical host")
	}
	if !strings.Contains(job(t, q, prod.ID).Reason, "release unknown") {
		t.Fatal("missing prior loss status")
	}
	r := check(t, q, b, x)
	if !r.Kill {
		t.Fatal("late experiment resurrected on held host")
	}
	fresh := b
	fresh.Session = "reconciled"
	if _, e := q.Checkin(Checkin{Machine: fresh, Fresh: true, PreviousSession: b.Session}); e != nil {
		t.Fatal(e)
	}
	if got := check(t, q, a, nil).Job; got == nil || got.ID != prod.ID {
		t.Fatal("settled old copy did not release")
	}
}

func TestProductionHoldExpiredDemandDoesNotStopExperiments(t *testing.T) {
	q, c, a, _, _ := holdFleet(t, true)
	x := submit(t, q, spec("experiment"))
	x = check(t, q, a, nil).Job
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	now := c.Now()
	p.GiveUpAt = &now
	prod := submit(t, q, p)
	requireState(t, q, prod.ID, "failed")
	if check(t, q, a, x).Stop {
		t.Fatal("expired demand stopped an experiment")
	}
}
