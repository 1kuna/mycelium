package myce

import (
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func holdFleet(t *testing.T, enabled bool) (*Queue, *fakeClock, Machine, Machine, Machine) {
	t.Helper()
	q, c := fixture(t)
	if enabled {
		if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); err != nil {
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
func TestProductionHoldDisplacesBothCardsAndReturnsImmediately(t *testing.T) {
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
	w = workerWithPreparedCheckpoint(t, w, store, payload)
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
	if got := check(t, q, b, nil).Job; got == nil || got.ID != y.ID || got.Attempt != 2 {
		t.Fatalf("after final release: %+v", got)
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
func TestProductionHoldSecondArrival(t *testing.T) {
	q, _, a, b, _ := holdFleet(t, true)
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	submit(t, q, p)
	first := check(t, q, a, nil).Job
	e := spec("experiment")
	e.AllowedMachines = []string{b.ID}
	waiting := submit(t, q, e)
	second := submit(t, q, p)
	finish(t, q, first, true)
	if check(t, q, b, nil).Job != nil {
		t.Fatal("queued second production must hold")
	}
	running := check(t, q, a, nil).Job
	if running == nil || running.ID != second.ID {
		t.Fatal("second production not assigned")
	}
	finish(t, q, running, true)
	if got := check(t, q, b, nil).Job; got == nil || got.ID != waiting.ID {
		t.Fatal("final settled demand did not return experiment")
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

// Reopening restores real NAS probes. Fake their I/O budget too, so disk/CPU
// latency cannot masquerade as a lost release while scheduling uses c.
func TestProductionHoldRestartDuringReclaimAndReturn(t *testing.T) {
	synctest.Test(t, testProductionHoldRestartDuringReclaimAndReturn)
}

func testProductionHoldRestartDuringReclaimAndReturn(t *testing.T) {
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
	if err := q2.Close(); err != nil {
		t.Fatal(err)
	}
	q3, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q3.Close()
	check(t, q3, a, nil)
	if got := check(t, q3, b, nil).Job; got == nil {
		t.Fatal("settled demand was held after restart")
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
	if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); err != nil {
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
		t.Fatalf("production residency bypassed engine idle interval: %+v", r.ModelPlan)
	}
	next := addModelCall(t, q, "experiment")
	if next.Machine != a.ID {
		t.Fatal("resident model withheld after settled production demand")
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

func TestProductionHoldKeepsProductionEnvironmentBuild(t *testing.T) {
	q, _, a, _, _ := holdFleet(t, true)
	r := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"production-tools": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	a.EnvironmentBuilder = true
	check(t, q, a, nil)
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	p.Environment.Name = "production-tools"
	prod := submit(t, q, p)
	reply := check(t, q, a, nil)
	if reply.Job != nil || len(reply.EnvironmentPlan) != 1 {
		t.Fatalf("missing production build: %+v", reply)
	}
	a.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "building"}}
	reply = check(t, q, a, nil)
	if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatalf("hold canceled its own production build: %+v", reply)
	}
	a.EnvironmentReports[0].State = "ready"
	a.Environments = []string{r.Key}
	if got := check(t, q, a, nil).Job; got == nil || got.ID != prod.ID {
		t.Fatalf("production did not start: %+v", got)
	}
}

func TestProductionHoldLostModelNeedsPriorSessionCleanup(t *testing.T) {
	q, c, a := modelFixture(t)
	a.HostID = "b70"
	check(t, q, a, nil)
	b := machine("two", "fake")
	b.HostID = "b70"
	check(t, q, b, nil)
	if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	call := addModelCall(t, q, "experiment")
	reportModel(t, q, a, "ready")
	p := spec("production")
	p.AllowedMachines = []string{b.ID}
	prod := submit(t, q, p)
	for range 61 {
		c.Advance(time.Second)
		check(t, q, b, nil)
	}
	if !q.s.Machines[a.ID].Gone {
		t.Fatal("model worker not lost")
	}
	replacement := a
	replacement.Session = "new-without-cleanup"
	for _, previous := range []string{"", "wrong-session"} {
		_, err := q.Checkin(Checkin{Machine: replacement, Fresh: previous != "", PreviousSession: previous})
		if err == nil {
			t.Fatal("new session erased lost model execution without cleanup")
		}
	}
	if got := check(t, q, b, nil).Job; got != nil {
		t.Fatal("unknown model release granted production")
	}
	if _, err := q.Checkin(Checkin{Machine: replacement, Fresh: true, PreviousSession: a.Session}); err != nil {
		t.Fatal(err)
	}
	if err := q.finishCall(call.ID); err != nil {
		t.Fatal(err)
	}
	if got := check(t, q, b, nil).Job; got == nil || got.ID != prod.ID {
		t.Fatalf("cleaned model did not release production: %+v", got)
	}
}

func TestProductionHoldResidentCallUsesReservedHostMemory(t *testing.T) {
	q, _, a := modelFixture(t)
	config := q.models
	config.Models[0].HostMemoryGB = 1
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	a.HostID = "b70"
	a.HostMemoryMeasured = true
	a.HostAvailableMemoryGB = 1
	a.ModelConfigHash = modelConfigHash(config)
	check(t, q, a, nil)
	b := machine("two", "fake")
	b.HostID = "b70"
	b.HostMemoryMeasured = true
	b.HostAvailableMemoryGB = 1
	check(t, q, b, nil)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: a.ID}); err != nil {
		t.Fatal(err)
	}
	reportModel(t, q, a, "ready")
	x := submit(t, q, spec("experiment"))
	x = check(t, q, b, nil).Job
	if x == nil {
		t.Fatal("experiment not started")
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	prod := addModelCall(t, q, "production")
	if prod.ProductionHost != "b70" || prod.Machine != "" {
		t.Fatalf("resident call bypassed host hold: %+v", prod)
	}
	if !check(t, q, b, x).Stop {
		t.Fatal("sibling experiment not stopped")
	}
	finish(t, q, x, false)
	reportModel(t, q, a, "ready")
	if prod.Machine != a.ID {
		t.Fatalf("resident production call not dispatched: %+v", prod)
	}
}

func TestProductionHoldReportsModelUnloadFailure(t *testing.T) {
	q, _, a := modelFixture(t)
	a.HostID = "b70"
	check(t, q, a, nil)
	b := machine("two", "fake")
	b.HostID = "b70"
	check(t, q, b, nil)
	call := addModelCall(t, q, "experiment")
	reportModel(t, q, a, "ready")
	if err := q.finishCall(call.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	p := spec("production")
	p.AllowedMachines = []string{b.ID}
	prod := submit(t, q, p)
	reportModel(t, q, a, "unload_failed")
	if slot := q.s.ModelSlots[slotKey(a.ID, "test")]; slot.State != "unload_failed" {
		t.Fatalf("hold erased unload failure: %+v", slot)
	}
	if reason := job(t, q, prod.ID).Reason; !strings.Contains(reason, "release unknown") {
		t.Fatalf("failed cleanup described as cooperative stopping: %s", reason)
	}
	if got := check(t, q, b, nil).Job; got != nil {
		t.Fatal("failed cleanup granted production")
	}
}

func TestProductionHoldForceStopsBlockedSaveAndResumesDurableGeneration(t *testing.T) {
	q, clock, a, b, spark := holdFleet(t, true)
	x := submit(t, q, spec("experiment"))
	x = check(t, q, a, nil).Job
	blocked := filepath.Join(t.TempDir(), "nas-save")
	if err := syscall.Mkfifo(blocked, 0600); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Run.Env = map[string]string{"SAVE_BLOCK": blocked}
	s.Run.Command = []string{"sh", "-c", `trap 'echo saving; cat "$SAVE_BLOCK"; exit 0' TERM; echo ready; while :; do sleep 0.01; done`}
	y := submit(t, q, s)
	y = check(t, q, b, nil).Job
	if x == nil || y == nil {
		t.Fatal("both host experiments must start")
	}
	w := recoveryWorker(t)
	w.Machine = b
	workerClock := &workerFakeClock{now: clock.Now()}
	w.Clock = workerClock
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = b.Session
	payload := cloneJob(y)
	payload.Checkpoint = filepath.Join(q.root, y.Checkpoint)
	w = workerWithPreparedCheckpoint(t, w, store, payload)
	process, err := w.start(store, payload)
	if err != nil {
		t.Fatal(err)
	}
	if store.checkpointUnavailable {
		t.Fatal("completed checkpoint preparation did not recover storage")
	}
	defer process.stop(workerClock.Now(), 0, true)
	logged := func(text string) bool {
		data, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
		return strings.Contains(string(data), text)
	}
	await(t, func() bool { return logged("ready") })
	durable := []byte(`{"myce_checkpoint":{"generation":7},"value":"unit-7"}`)
	if err := os.WriteFile(filepath.Join(attemptCheckpoint(payload), "checkpoint.json"), durable, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(attemptCheckpoint(payload), "checkpoint.json.tmp"), []byte(`{"myce_checkpoint":{"generation":99}}`), 0600); err != nil {
		t.Fatal(err)
	}
	// Queue and worker NAS admission fail while the payload's save blocks on a FIFO.
	q.checkpointProbe = func() bool { return false }
	a.CheckpointUnavailable, b.CheckpointUnavailable = true, true
	prodSpec := spec("production")
	prodSpec.Resumable = false
	prodSpec.AllowedMachines = []string{a.ID}
	prod := submit(t, q, prodSpec)
	rx, ry := check(t, q, a, x), check(t, q, b, y)
	if !rx.Stop || !ry.Stop || ry.Kill || ry.GraceSeconds != 10 {
		t.Fatalf("whole-host stop: %+v %+v", rx, ry)
	}
	process.stop(workerClock.Now(), time.Duration(ry.GraceSeconds)*time.Second, ry.Kill)
	await(t, func() bool { return logged("saving") })
	finish(t, q, x, false)
	if check(t, q, a, nil).Job != nil {
		t.Fatal("production started before blocked sibling released")
	}
	clock.Advance(9 * time.Second)
	workerClock.Advance(9 * time.Second)
	process.enforceGrace(workerClock.Now())
	select {
	case <-process.done:
		t.Fatal("blocked save stopped before grace")
	default:
	}
	if got := check(t, q, a, nil).Job; got != nil {
		t.Fatal("production started before grace")
	}
	clock.Advance(time.Second)
	workerClock.Advance(time.Second)
	process.enforceGrace(workerClock.Now())
	select {
	case receipt := <-process.done:
		if receipt.err != nil || !receipt.result.Forced || !receipt.result.Interrupted || receipt.result.ReleasePending {
			t.Fatalf("force-stop did not release process group: %+v", receipt)
		}
		if err := q.Complete(receipt.result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("force-stop waits for NAS save")
	}
	run := check(t, q, a, nil).Job
	if run == nil || run.ID != prod.ID {
		t.Fatalf("production blocked after grace and release: %+v", run)
	}
	productionWorker := recoveryWorker(t)
	productionWorker.Machine = a
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	productionWorker.Client = Client{URL: server.URL, Token: "test"}
	productionWorker.checkpointPrepare = func(Job) (string, error) {
		t.Error("production touched NAS preparation")
		return "", errors.New("NAS unavailable")
	}
	productionStore, err := productionWorker.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer productionStore.close()
	productionStore.state.Session = run.Session
	productionProcess, err := productionWorker.start(productionStore, *run)
	if err != nil {
		t.Fatal(err)
	}
	if receipt := <-productionProcess.done; receipt.err != nil || !receipt.result.Success {
		t.Fatalf("NAS-independent production failed: %+v", receipt)
	}
	if check(t, q, spark, nil).Job != nil {
		t.Fatal("experiment resumed before NAS recovered")
	}
	q.checkpointProbe = func() bool { return true }
	resumed := check(t, q, spark, nil).Job
	if resumed == nil || resumed.ID != y.ID || resumed.Attempt != 2 || resumed.Losses != 0 || resumed.ProductionHost != "" {
		t.Fatalf("displaced experiment did not migrate: %+v", resumed)
	}
	resumePayload := cloneJob(resumed)
	resumePayload.Checkpoint = filepath.Join(q.root, resumed.Checkpoint)
	dest, err := prepareCheckpoint(resumePayload)
	if err != nil {
		t.Fatal(err)
	}
	if saved, err := os.ReadFile(filepath.Join(dest, "checkpoint.json")); err != nil || string(saved) != string(durable) {
		t.Fatalf("last durable generation lost: %s %v", saved, err)
	}
	if generation, err := checkpointProgress(dest); err != nil || generation != 7 {
		t.Fatalf("incomplete save superseded durable generation: %d %v", generation, err)
	}
	resumeWorker := recoveryWorker(t)
	resumeWorker.Machine = spark
	resumeStore, err := resumeWorker.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer resumeStore.close()
	resumePayload.Spec.Run.Env = map[string]string{"EXPECTED_CHECKPOINT": string(durable)}
	resumePayload.Spec.Run.Command = []string{"sh", "-c", `test "$MYCE_MACHINE" = spark && test "$MYCE_ATTEMPT" = 2 && test "$(cat "$MYCE_CHECKPOINT_DIR/checkpoint.json")" = "$EXPECTED_CHECKPOINT"`}
	resumeWorker = workerWithPreparedCheckpoint(t, resumeWorker, resumeStore, resumePayload)
	resumeProcess, err := resumeWorker.start(resumeStore, resumePayload)
	if err != nil {
		t.Fatal(err)
	}
	if resumeStore.checkpointUnavailable {
		t.Fatal("completed checkpoint preparation did not recover storage")
	}
	if receipt := <-resumeProcess.done; receipt.err != nil || !receipt.result.Success {
		t.Fatalf("migrated command did not consume its durable checkpoint: %+v", receipt)
	}
	if check(t, q, b, nil).Job != nil || !q.held("b70") {
		t.Fatal("NAS recovery reopened the production-held host")
	}
}

func TestProductionHoldKillsLateCopyDuringNASLoss(t *testing.T) {
	q, clock, a, b, spark := holdFleet(t, true)
	x := submit(t, q, spec("experiment"))
	x = check(t, q, b, nil).Job
	a.FreeMemoryGB = 0
	for i := 0; i < 61; i++ {
		clock.Advance(time.Second)
		check(t, q, a, nil)
		check(t, q, spark, nil)
	}
	current := check(t, q, spark, nil).Job
	if current == nil || current.ID != x.ID || current.Attempt != 2 {
		t.Fatalf("missing replacement attempt: %+v", current)
	}
	a.FreeMemoryGB = 8
	p := spec("production")
	p.Resumable = false
	p.AllowedMachines = []string{a.ID}
	submit(t, q, p)
	q.checkpointProbe = func() bool { return false }
	b.CheckpointUnavailable = true
	reply := check(t, q, b, x)
	if !reply.Kill || reply.Hold {
		t.Fatalf("NAS reconciliation retained a late copy on the held host: %+v", reply)
	}
	if got := job(t, q, x.ID); got.Attempt != 2 || got.Machine != spark.ID {
		t.Fatalf("late copy replaced the current attempt: %+v", got)
	}
}

func TestProductionHoldSequentialStageGap(t *testing.T) {
	for _, gap := range []time.Duration{5 * time.Second, 200 * time.Second} {
		t.Run(gap.String(), func(t *testing.T) {
			q, c, a, b, _ := holdFleet(t, true)
			stage := spec("production")
			stage.AllowedMachines = []string{a.ID}
			submit(t, q, stage)
			first := check(t, q, a, nil).Job
			e := spec("experiment")
			e.AllowedMachines = []string{b.ID}
			waiting := submit(t, q, e)
			finish(t, q, first, true)
			between := check(t, q, b, nil).Job
			if between == nil || between.ID != waiting.ID {
				t.Fatal("experiment did not return immediately in stage gap")
			}
			c.Advance(gap)
			check(t, q, a, nil)
			check(t, q, b, between)
			second := submit(t, q, stage)
			if !check(t, q, b, between).Stop {
				t.Fatal("new stage did not immediately reclaim")
			}
			if check(t, q, a, nil).Job != nil {
				t.Fatal("new stage bypassed experiment release")
			}
			finish(t, q, between, false)
			run := check(t, q, a, nil).Job
			if run == nil || run.ID != second.ID {
				t.Fatal("second stage not admitted")
			}
			finish(t, q, run, true)
			if got := check(t, q, b, nil).Job; got == nil || got.ID != waiting.ID {
				t.Fatal("final stage retained idle hold")
			}
		})
	}
}

func TestProductionDemandWithoutIdleCooldown(t *testing.T) {
	q, clock, a, b, _ := holdFleet(t, true)
	e := spec("experiment")
	e.AllowedMachines = []string{b.ID}
	first := submit(t, q, e)
	running := check(t, q, b, nil).Job
	if running == nil || running.ID != first.ID {
		t.Fatal("experiment did not start")
	}
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	prod := submit(t, q, p)
	if !check(t, q, b, running).Stop {
		t.Fatal("production did not immediately preempt")
	}
	// Neither demand cancellation nor years of elapsed grace prove release.
	if err := q.Cancel(prod.ID); err != nil {
		t.Fatal(err)
	}
	receipt := result(running, false)
	receipt.Interrupted = true
	receipt.ReleasePending = true
	if err := q.Complete(receipt); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		clock.Advance(365 * 24 * time.Hour)
		check(t, q, a, nil)
		if !q.held(hostID(&a)) {
			t.Fatal("unconfirmed release expired")
		}
	}
	receipt.ReleasePending = false
	receipt.ReleaseOnly = true
	if err := q.Complete(receipt); err != nil {
		t.Fatal(err)
	}
	// Keep worker sessions current after the deliberately long fake loss interval.
	check(t, q, a, nil)
	got := check(t, q, b, nil).Job
	if got == nil || got.ID != first.ID || got.Attempt != 2 || got.Losses != 0 {
		t.Fatalf("settled demand did not immediately return experiment: %+v", got)
	}
	next := submit(t, q, p)
	if !check(t, q, b, got).Stop {
		t.Fatal("new production did not reclaim")
	}
	finish(t, q, got, false)
	run := check(t, q, a, nil).Job
	if run == nil || run.ID != next.ID {
		t.Fatal("new production not admitted after release")
	}
	finish(t, q, run, true)
	if got := check(t, q, b, nil).Job; got == nil || got.ID != first.ID {
		t.Fatal("final demand retained idle cooldown")
	}
}

func TestProductionDemandPreservesModelIdleUnload(t *testing.T) {
	q, clock, a := modelFixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{hostID(&a): {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	prod := addModelCall(t, q, "production")
	reportModel(t, q, a, "ready")
	if prod.Machine != a.ID {
		t.Fatal("production not routed")
	}
	if err := q.finishCall(prod.ID); err != nil {
		t.Fatal(err)
	}
	slot := q.s.ModelSlots[slotKey(a.ID, "test")]
	if slot == nil || slot.State != "ready" || q.held(hostID(&a)) {
		t.Fatal("settled production replaced engine idle interval", slot)
	}
	lower := addModelCall(t, q, "experiment")
	if lower.Machine != a.ID {
		t.Fatal("resident model withheld from lower priority after settled demand")
	}
	if err := q.finishCall(lower.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if slot.State != "ready" {
		t.Fatal("engine idle deadline shortened", slot.State)
	}
	clock.Advance(time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if slot.State != "draining" {
		t.Fatal("engine idle unload deadline lost", slot.State)
	}
}
