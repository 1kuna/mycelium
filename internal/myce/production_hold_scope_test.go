package myce

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func stopped(r Reply, id string) bool {
	for _, a := range r.Attempts {
		if a.ID == id && a.Stop {
			return true
		}
	}
	return false
}

func holdMachine(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, c := fixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{"machine": {ProductionHold: true, ProductionHoldScope: "device"}}); err != nil {
		t.Fatal(err)
	}
	return q, c, m3Machine()
}

// MY-50: production waiting for a host's only GPU claims that GPU and nothing
// else. CPU-only work is admitted on the same host while production waits, and
// GPU experiments are preempted or held.
func TestProductionPriorityClaimsOnlyTheGPU(t *testing.T) {
	t.Parallel()
	q, c, m := holdMachine(t)
	m.Devices = m.Devices[:1]
	x := assignedJob(machineCheck(t, q, m))
	if x != nil {
		t.Fatal("assigned work before submission", x.ID)
	}
	submit(t, q, spec("experiment"))
	x = assignedJob(machineCheck(t, q, m))
	if x == nil || !slices.Equal(x.AllocatedDevices, []string{"0"}) {
		t.Fatal("GPU experiment did not start on the GPU", x)
	}
	prod := submit(t, q, spec("production"))
	if r := machineCheck(t, q, m, x); !stopped(r, x.ID) || assignedJob(r) != nil {
		t.Fatalf("production must preempt the GPU experiment and wait for its release: %+v", r.Attempts)
	}
	// Production is still waiting for the GPU's exact release.
	c.Advance(time.Minute)
	cpu := submit(t, q, cpuSpec(1, 1))
	gpu := submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m, x))
	if a == nil || a.ID != cpu.ID || len(a.AllocatedDevices) != 0 {
		t.Fatalf("CPU-only work not admitted beside waiting production: %+v (production: %s)", a, job(t, q, prod.ID).Reason)
	}
	if got := assignedJob(machineCheck(t, q, m, x, a)); got != nil {
		t.Fatal("work admitted onto the GPU production is waiting for", got.ID)
	}
	requireState(t, q, prod.ID, "queued")
	requireState(t, q, gpu.ID, "queued")

	finishInterrupted(t, q, x)
	p := assignedJob(machineCheck(t, q, m, a))
	if p == nil || p.ID != prod.ID || !slices.Equal(p.AllocatedDevices, []string{"0"}) {
		t.Fatal("production did not take the released GPU", p)
	}
	if got := assignedJob(machineCheck(t, q, m, a, p)); got != nil {
		t.Fatal("GPU experiment admitted beside production on the only GPU", got.ID)
	}
	requireState(t, q, cpu.ID, "running")
	requireState(t, q, gpu.ID, "queued")
}

// One physical host with CPU and two fake cards: production packs beside the
// sibling card and the CPU, and a second production reclaims exactly one card.
func TestProductionHoldScopeOnMachineWorker(t *testing.T) {
	t.Parallel()
	q, c, m := holdMachine(t)
	first := submit(t, q, spec("production"))
	p1 := assignedJob(machineCheck(t, q, m))
	if p1 == nil || p1.ID != first.ID || !slices.Equal(p1.AllocatedDevices, []string{"0"}) {
		t.Fatal("production did not start on card 0", p1)
	}
	submit(t, q, spec("experiment"))
	submit(t, q, cpuSpec(1, 1))
	a := assignedJob(machineCheck(t, q, m, p1))
	if a == nil {
		t.Fatal("no lower tier fit beside production")
	}
	b := assignedJob(machineCheck(t, q, m, p1, a))
	if b == nil {
		t.Fatal("second lower tier did not fit beside production")
	}
	x, cpu := a, b
	if x.Spec.Needs.cpuOnly() {
		x, cpu = b, a
	}
	if !slices.Equal(x.AllocatedDevices, []string{"1"}) || !cpu.Spec.Needs.cpuOnly() || len(cpu.AllocatedDevices) != 0 {
		t.Fatal("experiments not on the sibling card and the CPU", x.AllocatedDevices, cpu.AllocatedDevices)
	}

	second := submit(t, q, spec("production"))
	r := machineCheck(t, q, m, p1, x, cpu)
	if !stopped(r, x.ID) || stopped(r, cpu.ID) || stopped(r, p1.ID) || assignedJob(r) != nil {
		t.Fatalf("production must reclaim exactly card 1: %+v", r.Attempts)
	}
	// Elapsed grace is not release: card 1 stays charged until the exact
	// attempt acknowledges its stop.
	c.Advance(time.Minute)
	if got := assignedJob(machineCheck(t, q, m, p1, x, cpu)); got != nil {
		t.Fatal("production started before exact release", got.ID)
	}
	requireState(t, q, cpu.ID, "running")
	finishInterrupted(t, q, x)
	p2 := assignedJob(machineCheck(t, q, m, p1, cpu))
	if p2 == nil || p2.ID != second.ID || !slices.Equal(p2.AllocatedDevices, []string{"1"}) {
		t.Fatal("production did not take the released card", p2)
	}
	requireState(t, q, x.ID, "queued")
	requireState(t, q, cpu.ID, "running")
	requireState(t, q, p1.ID, "running")
}

// An empty startup file holds nothing, so a later file can still add a hold.
func TestProductionHoldEmptyStartupFileStillSeeds(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"machine": {ProductionHold: true, ProductionHoldScope: "device"}}); err != nil {
		t.Fatal("adding a hold after an empty startup file refused:", err)
	}
	if !q.PriorityPolicy().Policy.Hosts["machine"].ProductionHold {
		t.Fatal("hold not seeded")
	}
}

// MY-50 removed the whole-machine scope. The policy API refuses it; a startup
// file or saved state from the previous release converts to device scope, and
// both survive restart.
func TestWholeMachineHoldRefusedOrConverted(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	startup := map[string]HostPolicy{"machine": {ProductionHold: true}}
	if err := q.ConfigureHosts(startup); err != nil {
		t.Fatal("whole-machine startup file did not convert:", err)
	}
	device := HostPolicy{ProductionHold: true, ProductionHoldScope: "device"}
	record := q.PriorityPolicy()
	if got := record.Policy.Hosts["machine"]; got != device {
		t.Fatalf("startup file kept whole-machine scope: %+v", got)
	}
	for _, bad := range []HostPolicy{{ProductionHold: true}, {ProductionHold: true, ProductionHoldScope: "host"}, {ProductionHold: true, ProductionHoldScope: "card"}, {ProductionHoldScope: "device"}} {
		record.Policy.Hosts = map[string]HostPolicy{"machine": bad}
		_, err := q.SetPriorityPolicy(record.Revision, record.Policy)
		if err == nil {
			t.Fatalf("invalid host policy accepted: %+v", bad)
		}
		if bad.ProductionHold && bad.ProductionHoldScope != "card" && !strings.Contains(err.Error(), "whole-machine production hold was removed") {
			t.Fatalf("refusal does not explain the removal: %v", err)
		}
	}
	if got := q.PriorityPolicy(); got.Revision != record.Revision || got.Policy.Hosts["machine"] != device {
		t.Fatalf("rejected change altered policy: %+v", got)
	}
	// Omitting hosts (an older client) preserves them.
	record.Policy.Hosts = nil
	record, err := q.SetPriorityPolicy(record.Revision, record.Policy)
	if err != nil || record.Policy.Hosts["machine"] != device {
		t.Fatalf("omitted hosts replaced policy: %+v %v", record, err)
	}
	// Saved state written by the previous release still says whole-machine.
	if err := q.update(func() error {
		q.s.Hosts["machine"].Policy = HostPolicy{ProductionHold: true}
		q.s.HostConfig = startup
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	path, root := q.dbPathForTest(t), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if got := q.PriorityPolicy().Policy.Hosts["machine"]; got != device {
		t.Fatalf("saved whole-machine policy not converted on restart: %+v", got)
	}
	if err := q.ConfigureHosts(startup); err != nil {
		t.Fatal("unchanged startup file refused after conversion:", err)
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"machine": {ProductionHold: false}}); err == nil || !strings.Contains(err.Error(), "saved policy") {
		t.Fatalf("edited startup file accepted: %v", err)
	}

	m := m3Machine()
	submit(t, q, spec("production"))
	p := assignedJob(machineCheck(t, q, m))
	if p == nil {
		t.Fatal("production did not start after restart")
	}
	submit(t, q, spec("experiment"))
	x := assignedJob(machineCheck(t, q, m, p))
	if x == nil || slices.Equal(p.AllocatedDevices, x.AllocatedDevices) {
		t.Fatal("device scope not applied after restart", x)
	}
}

func TestProjectMachineAllowListForCalls(t *testing.T) {
	t.Parallel()
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

func TestProductionDemandPreservesModelIdleUnload(t *testing.T) {
	t.Parallel()
	q, clock, a := modelFixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{hostID(&a): {ProductionHold: true, ProductionHoldScope: "device"}}); err != nil {
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
	if slot == nil || slot.State != "ready" {
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
