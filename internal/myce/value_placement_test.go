package myce

import (
	"bytes"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func valueMachine(id, backend string, ram float64) Machine {
	m := cpuMachine(id, id, backend, 4, ram)
	m.CapabilityProfile.SystemMemoryBytes = uint64(ram * (1 << 30))
	m.TotalMemoryGB = 8
	if backend == "cpu" {
		m.CapabilityProfile.Accelerators = nil
	} else {
		m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: backend, MemoryBytes: 8 << 30}}
	}
	return m
}
func valueFleet(t *testing.T) (*Queue, *fakeClock, Machine, Machine) {
	t.Helper()
	q, c := fixture(t)
	small, big := valueMachine("z-small", "cpu", 16), valueMachine("a-accelerator", "fake", 64)
	check(t, q, small, nil)
	check(t, q, big, nil)
	return q, c, small, big
}

func lastResortPolicy(t *testing.T, q *Queue, host string, enabled bool) {
	t.Helper()
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "placement", LastResort: &enabled}); err != nil {
		t.Fatal(err)
	}
}

func TestValueLastResortCPU(t *testing.T) {
	q, _ := fixture(t)
	small, ordinary := valueMachine("small", "cpu", 8), valueMachine("ordinary", "metal", 24)
	check(t, q, small, nil)
	check(t, q, ordinary, nil)
	lastResortPolicy(t, q, small.HostID, true)
	wanted := submit(t, q, cpuSpec(1, 1))
	if got := check(t, q, small, nil).Job; got != nil {
		t.Fatal("last-resort host won by pulling first", got)
	}
	if got := check(t, q, ordinary, nil).Job; got == nil || got.ID != wanted.ID {
		t.Fatal("ordinary accelerator host did not get CPU work", got)
	}
	// A fresh policy pass must immediately restore the original MY-18 ordering.
	lastResortPolicy(t, q, small.HostID, false)
	wanted = submit(t, q, cpuSpec(1, 1))
	if got := check(t, q, small, nil).Job; got == nil || got.ID != wanted.ID {
		t.Fatal("clearing last-resort did not restore admission", got)
	}
}

func TestValueLastResortImmediateFallbackAndStatus(t *testing.T) {
	for _, mode := range []string{"busy", "lost"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, small, ordinary := valueFleet(t)
			lastResortPolicy(t, q, small.HostID, true)
			var running *Job
			if mode == "busy" {
				submit(t, q, cpuSpec(4, 1))
				running = check(t, q, ordinary, nil).Job
				if running == nil {
					t.Fatal("ordinary host did not take initial work")
				}
			} else {
				clock.Advance(LossAfter + time.Second)
			}
			wanted := submit(t, q, cpuSpec(1, 1))
			if mode == "busy" {
				snap, err := q.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				for _, j := range snap.Jobs {
					if j.ID == wanted.ID && !strings.Contains(j.Reason, "last-resort fallback") {
						t.Fatal("queued fallback reason hidden", j.Reason)
					}
				}
			}
			before := clock.Now()
			got := check(t, q, small, nil).Job
			if got == nil || got.ID != wanted.ID || !clock.Now().Equal(before) {
				t.Fatal("fallback did not start immediately", got)
			}
			if running != nil && job(t, q, running.ID).State != "running" {
				t.Fatal("fallback preempted equal-tier work")
			}
			if !strings.Contains(JobTiming(got), "last-resort fallback: no ordinary host can admit") {
				t.Fatal("missing fallback explanation", JobTiming(got))
			}
			snap, err := q.publicSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range snap.Machines {
				if m.ID == small.ID && !m.LastResort {
					t.Fatal("host policy hidden in status")
				}
			}
			var html bytes.Buffer
			if err := statusPage.Execute(&html, snap); err != nil || !strings.Contains(html.String(), "Last-resort placement") || !strings.Contains(html.String(), "last-resort fallback") {
				t.Fatal("policy or reason hidden in status page", err)
			}
		})
	}
}

func TestValueLastResortFairnessAndHigherTier(t *testing.T) {
	t.Run("fairness", func(t *testing.T) {
		q, clock, small, ordinary := valueFleet(t)
		lastResortPolicy(t, q, small.HostID, true)
		a := cpuSpec(1, 1)
		a.Project, a.AllowedMachines = "first", []string{ordinary.ID}
		first := submit(t, q, a)
		clock.Advance(time.Millisecond)
		b := cpuSpec(1, 1)
		b.Project = "second"
		second := submit(t, q, b)
		if got := check(t, q, small, nil).Job; got == nil || got.ID != second.ID {
			t.Fatal("last-resort policy waited behind another project's fair turn", got)
		}
		if got := check(t, q, ordinary, nil).Job; got == nil || got.ID != first.ID {
			t.Fatal("policy changed ordinary host's fair winner", got)
		}
	})
	t.Run("higher tier and custody", func(t *testing.T) {
		q, _, small, ordinary := valueFleet(t)
		lastResortPolicy(t, q, small.HostID, true)
		preemptionPolicy(t, q)
		low := submit(t, q, cpuSpec(4, 1))
		run := check(t, q, ordinary, nil).Job
		if run == nil {
			t.Fatal("ordinary host did not take low tier")
		}
		highSpec := cpuSpec(4, 1)
		highSpec.Project = "urgent"
		high := submit(t, q, highSpec)
		if job(t, q, low.ID).State != "running" {
			t.Fatal("preempted ordinary host despite free fallback")
		}
		if got := check(t, q, small, nil).Job; got == nil || got.ID != high.ID {
			t.Fatal("high tier did not use immediate fallback", got)
		}
		high2 := submit(t, q, highSpec)
		if job(t, q, low.ID).State != "stopping" || priorityStop(job(t, q, low.ID)) == nil {
			t.Fatal("last-resort policy blocked higher-tier preemption")
		}
		if got := check(t, q, ordinary, run).Job; got != nil {
			t.Fatal("preemption credited capacity before exact release")
		}
		finishInterrupted(t, q, run)
		if got := check(t, q, ordinary, nil).Job; got == nil || got.ID != high2.ID {
			t.Fatal("higher tier lost after release", got)
		}
	})
}

func TestValueLastResortSimulationPair(t *testing.T) {
	for _, file := range []string{"baseline.yaml", "fleet.yaml"} {
		t.Run(file, func(t *testing.T) {
			var fleet SimFleet
			var work SimWorkload
			root := "../../examples/sim/last-resort-hosts/"
			if err := ReadSimYAML(root+file, &fleet); err != nil {
				t.Fatal(err)
			}
			if err := ReadSimYAML(root+"workload.yaml", &work); err != nil {
				t.Fatal(err)
			}
			r := runSimTest(t, fleet, work, 259.2)
			if r.Completed != 2 || r.Failed != 0 || r.Unfinished != 0 || r.Production.P95 != 0 || r.Production.Unstarted != 0 || len(r.NeverPlaceable) != 0 {
				t.Fatal("paired fixture changed liveness or production wait", r)
			}
			firstHost, overflowHost := "small", "ordinary"
			if file == "fleet.yaml" {
				firstHost, overflowHost = overflowHost, firstHost
			}
			nearSim(t, r.Machines[firstHost].BusyHours*3600, 200)
			nearSim(t, r.Machines[overflowHost].BusyHours*3600, 40)
			latest := 0.0
			for _, batch := range r.Batches {
				if batch.FinishedSeconds == nil {
					t.Fatal("unfinished batch")
				}
				latest = max(latest, *batch.FinishedSeconds)
			}
			nearSim(t, latest, 200)
		})
	}
}
func TestValueCPUAndAccelerator(t *testing.T) {
	q, _, small, big := valueFleet(t)
	cpu := submit(t, q, cpuSpec(1, 1))
	if got := check(t, q, big, nil).Job; got != nil {
		t.Fatal("accelerator won by pulling first", got.ID)
	}
	run := check(t, q, small, nil).Job
	if run == nil || run.ID != cpu.ID {
		t.Fatal("small CPU host did not win", run)
	}
	gpu := submit(t, q, spec("experiment"))
	if got := check(t, q, big, nil).Job; got == nil || got.ID != gpu.ID {
		t.Fatal("accelerator job lost its accelerator", got)
	}
	snap, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var html bytes.Buffer
	if err := statusPage.Execute(&html, snap); err != nil || !strings.Contains(html.String(), "preserve scarce machines") {
		t.Fatal("ranking hidden in status", err)
	}
	if !strings.Contains(JobTiming(job(t, q, cpu.ID)), "usable RAM 16 GiB") {
		t.Fatal("missing chosen RAM/ranking")
	}
}
func TestValueNoWaitingForUnavailableCheapHost(t *testing.T) {
	for _, mode := range []string{"busy", "lost", "owner", "maintenance", "recovering"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, small, big := valueFleet(t)
			var running *Job
			switch mode {
			case "busy":
				submit(t, q, cpuSpec(4, 1))
				running = check(t, q, small, nil).Job
			case "lost":
				clock.Advance(LossAfter + time.Second)
			case "owner":
				small.CapabilityProfile.Availability = &Availability{Mode: "daily", Available: false, Reason: "owner returned", CheckedAt: clock.Now(), Signals: idleSignals()}
				check(t, q, small, nil)
			case "maintenance":
				if _, err := q.ChangeHostMaintenance(small.HostID, HostMaintenanceRequest{Action: "disable"}); err != nil {
					t.Fatal(err)
				}
			case "recovering":
				if _, err := q.Checkin(Checkin{Machine: small, Recovering: true}); err != nil {
					t.Fatal(err)
				}
			}
			wanted := submit(t, q, cpuSpec(1, 1))
			if mode == "busy" {
				snap, err := q.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				for _, j := range snap.Jobs {
					if j.ID == wanted.ID && !strings.Contains(j.Reason, "host "+big.HostID+" scarcity") {
						t.Fatal("status ranked an inadmissible cheap host", j.Reason)
					}
				}
			}
			got := check(t, q, big, nil).Job
			if got == nil || got.ID != wanted.ID {
				t.Fatal("waited for cheap host", got)
			}
			if running != nil && job(t, q, running.ID).State != "running" {
				t.Fatal("equal tier was preempted")
			}
		})
	}
}
func TestValueOwnerReturnRetainsCustody(t *testing.T) {
	q, clock, small, big := valueFleet(t)
	s := cpuSpec(1, 1)
	s.Resumable = true
	submit(t, q, s)
	run := check(t, q, small, nil).Job
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	small.CapabilityProfile.Availability = &Availability{Mode: "daily", Available: false, Reason: "owner returned", CheckedAt: clock.Now(), Signals: idleSignals()}
	reply := check(t, q, small, run)
	if !reply.Stop || job(t, q, run.ID).State != "stopping" {
		t.Fatal("owner did not stop command", reply)
	}
	if got := check(t, q, big, nil).Job; got != nil {
		t.Fatal("retried before cleanup", got)
	}
	finishInterrupted(t, q, run)
	if got := check(t, q, big, nil).Job; got == nil || got.ID != run.ID || got.Attempt <= run.Attempt {
		t.Fatal("no immediate retry on admissible host", got)
	} else if strings.Contains(JobTiming(got), "host "+small.HostID+" scarcity") {
		t.Fatal("retry kept previous attempt ranking", JobTiming(got))
	}
}
func TestValueFairnessAndHigherTier(t *testing.T) {
	q, _, small, big := valueFleet(t)
	preemptionPolicy(t, q)
	low := submit(t, q, cpuSpec(4, 1))
	run := check(t, q, small, nil).Job
	highSpec := cpuSpec(4, 1)
	highSpec.Project = "urgent"
	high := submit(t, q, highSpec)
	if job(t, q, low.ID).State != "running" {
		t.Fatal("value stopped cheap work despite free scarce host")
	}
	urgent := check(t, q, big, nil).Job
	if urgent == nil || urgent.ID != high.ID {
		t.Fatal("higher tier did not use free host", urgent)
	}
	high2 := submit(t, q, highSpec)
	if job(t, q, low.ID).State != "stopping" || priorityStop(job(t, q, low.ID)) == nil {
		t.Fatal("higher tier failed to preempt")
	}
	if got := check(t, q, small, run).Job; got != nil {
		t.Fatal("preemption credited before release")
	}
	finishInterrupted(t, q, run)
	if got := check(t, q, small, nil).Job; got == nil || got.ID != high2.ID {
		t.Fatal("higher tier lost after release", got)
	}
}
func TestValueDoesNotSteerBehindAnotherProject(t *testing.T) {
	q, clock, small, big := valueFleet(t)
	// The older job is restricted to the cheap host. Value cannot route the
	// other job there and then wait behind that project's fair turn.
	a := cpuSpec(1, 1)
	a.Project = "first"
	a.AllowedMachines = []string{small.ID}
	first := submit(t, q, a)
	clock.Advance(time.Millisecond)
	b := cpuSpec(1, 1)
	b.Project = "second"
	second := submit(t, q, b)
	if got := check(t, q, big, nil).Job; got == nil || got.ID != second.ID {
		t.Fatal("value changed the competing project", got)
	}
	if got := check(t, q, small, nil).Job; got == nil || got.ID != first.ID {
		t.Fatal("cheap host fair winner changed", got)
	}
}
func TestValueRestartCustody(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, q, true)
	small, big := valueMachine("z-small", "cpu", 16), valueMachine("a-accelerator", "fake", 64)
	check(t, q, small, nil)
	check(t, q, big, nil)
	wanted := submit(t, q, cpuSpec(1, 1))
	run := check(t, q, small, nil).Job
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fakeCheckpointAvailability(t, reopened, true)
	if got := check(t, reopened, big, nil).Job; got != nil {
		t.Fatal("restart replayed charged attempt", got)
	}
	reply := check(t, reopened, small, run)
	current := job(t, reopened, wanted.ID)
	if reply.Stop || current.State != "running" || current.Machine != small.ID || current.Attempt != run.Attempt {
		t.Fatal("restart failed adoption", reply, current.State)
	}
}

func TestValueComponentCPUViews(t *testing.T) {
	q, _ := fixture(t)
	small := valueMachine("z-small", "cpu", 16)
	check(t, q, small, nil)
	big := m3Machine()
	big.ID, big.HostID = "a-component-host", "a-component-host"
	machineCheck(t, q, big)
	wanted := submit(t, q, cpuSpec(1, 1))
	if got := assignedJob(machineCheck(t, q, big)); got != nil {
		t.Fatal("component CPU view stole cheap host work", got)
	}
	if got := check(t, q, small, nil).Job; got == nil || got.ID != wanted.ID {
		t.Fatal("small CPU host did not win", got)
	}
	gpu := submit(t, q, spec("experiment"))
	if got := assignedJob(machineCheck(t, q, big)); got == nil || got.ID != gpu.ID {
		t.Fatal("GPU did not get component", got)
	}
}
func TestValueBandsCountPhysicalHostsAndAllowances(t *testing.T) {
	q, clock := fixture(t)
	a := valueMachine("a", "cuda", 64)
	a.CapabilityProfile.Accelerators[0].MemoryBytes = 32 << 30
	a.TotalMemoryGB = 32
	b := valueMachine("b", "cuda", 64)
	b.HostID = a.HostID
	b.CapabilityProfile.Accelerators[0].MemoryBytes = 32 << 30
	b.TotalMemoryGB = 32
	c := valueMachine("c", "cuda", 64)
	c.CapabilityProfile.Accelerators[0].MemoryBytes = 16 << 30
	c.TotalMemoryGB = 16
	d := valueMachine("d", "cpu", 16)
	for _, m := range []Machine{a, b, c, d} {
		check(t, q, m, nil)
	}
	values := q.hostValues()
	// Host a supplies CUDA>=16 (two hosts), CUDA>=32 (one), RAM>=64 (two).
	if got := values[a.HostID].vector; len(got) != 3 || got[0] != 1 || got[1] != 2 {
		t.Fatal("counted cards/worker rows as supply", got)
	}
	if rank, _ := compareHostValue(values, &c, &a); rank >= 0 {
		t.Fatal("rare band did not dominate")
	}
	// Physical facts stay large, but the owner permits only 16 GiB.
	allowance := 16.0
	if _, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "drain", MemoryBudgetGB: &allowance}); err != nil {
		t.Fatal(err)
	}
	a.HostMemoryBudgetGB, b.HostMemoryBudgetGB = 16, 16
	check(t, q, a, nil)
	check(t, q, b, nil)
	if _, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	if q.hostValues()[a.HostID].ram != 16 {
		t.Fatal("ignored owner allowance")
	}
	// Occupancy never changes value.
	before := q.hostValues()[c.HostID].vector
	c.FreeMemoryGB = 0
	check(t, q, c, nil)
	if got := q.hostValues()[c.HostID].vector; !slices.Equal(got, before) {
		t.Fatal("occupancy changed value", before, got)
	}
	clock.Advance(LossAfter + time.Second)
	check(t, q, d, nil)
	if got := q.hostValues()[c.HostID]; got == nil || !slices.Equal(got.vector, before) {
		t.Fatal("stale inventory changed scarcity supply", got)
	}
}
func TestValueUnifiedAndUnknownInventory(t *testing.T) {
	q, _, small, _ := valueFleet(t)
	unified := valueMachine("unified", "metal", 32)
	unified.UnifiedMemory = true
	unified.CapabilityProfile.MemoryLayout = "unified"
	unified.TotalMemoryGB = 32
	unified.CapabilityProfile.Accelerators[0].MemoryBytes = 32 << 30
	unified.HostMemoryBudgetGB = 24
	check(t, q, unified, nil)
	h := q.hostValues()[unified.HostID]
	if h.ram != 24 || h.accelerators["metal"] != 24 {
		t.Fatal("unified RAM was added or escaped allowance", h)
	}
	unknown := m3Machine()
	unknown.ID, unknown.HostID = "unknown", "unknown"
	unknown.Devices = unknown.Devices[:1]
	unknown.Devices[0].TotalMemoryGB = 0
	unknown.Devices[0].BudgetGB = 0
	unknown.Devices[0].FreeMemoryGB = 0
	machineCheck(t, q, unknown)
	if c, known := q.valueCompare(&small, &unknown); !known || c >= 0 {
		t.Fatal("unknown ranked as cheap", c, known)
	}
	s := cpuSpec(1, 1)
	s.AllowedMachines = []string{unknown.ID}
	wanted := submit(t, q, s)
	got := assignedJob(machineCheck(t, q, unknown))
	if got == nil || got.ID != wanted.ID || !strings.Contains(JobTiming(got), "host value unknown") {
		t.Fatal("unknown-only fitting target was refused or unexplained", got)
	}
}

func TestValueDistinctDeviceTiers(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.Devices[1].TotalMemoryGB, m.Devices[1].BudgetGB, m.Devices[1].FreeMemoryGB = 16, 16, 16
	machineCheck(t, q, m)
	h := q.hostValues()[m.HostID]
	// Both measured tiers are supplied by one physical host, not two devices.
	if len(h.vector) != 1 || h.vector[0] != 2 {
		t.Fatal("lost a measured tier or duplicated host supply", h.vector)
	}
}

func TestValueSimulation(t *testing.T) {
	f := SimFleet{Machines: []SimMachine{
		{Name: "a-scarce", Host: "scarce", Platform: "fake", MemoryGB: 64, HostGB: 64, Speed: 1},
		{Name: "z-small", Host: "small", Platform: "fake", MemoryGB: 8, HostGB: 16, Speed: 1},
	}}
	general := simTestStream("ordinary", "experiment", 0, 100, 1)
	general.Batch = 2
	accelerator := simTestStream("accelerator", "experiment", 1, 50, 1)
	accelerator.MemoryGB = 32
	accelerator.Batch = 2
	urgent := simTestStream("urgent", "production", 150, 5, 1)
	urgent.Batch = 2
	r := runSimTest(t, f, simTestWork(general, accelerator, urgent), 400)
	if r.Failed != 0 || r.Unfinished != 0 || r.Completed != 3 || r.Production.P95 != 0 || r.IdleWaitingHours != 0 {
		t.Fatal("shared simulator planner regression", r)
	}
	latest := 0.0
	for _, batch := range r.Batches {
		if batch.FinishedSeconds == nil {
			t.Fatal("unfinished batch")
		}
		latest = max(latest, *batch.FinishedSeconds)
	}
	nearSim(t, latest, 200)
}

func TestValueCPUProductionHold(t *testing.T) {
	q, _, small, big := valueFleet(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{small.HostID: {ProductionHold: true}, big.HostID: {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Priority = "production"
	wanted := submit(t, q, s)
	if host := job(t, q, wanted.ID).ProductionHost; host != small.HostID {
		t.Fatal("production hold bound CPU work before ranking free hosts", host)
	}
	if got := check(t, q, big, nil).Job; got != nil {
		t.Fatal("scarce host took held work", got)
	}
	if got := check(t, q, small, nil).Job; got == nil || got.ID != wanted.ID {
		t.Fatal("small held host did not receive work", got)
	}
}

func TestValueProductionHoldFreeBeforeBusyPreparation(t *testing.T) {
	q, _, small, big := valueFleet(t)
	small.EnvironmentBuilder = true
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"cpu": binaryRecipe(t, "cpu")}}}); err != nil {
		t.Fatal(err)
	}
	low := cpuSpec(1, 1)
	low.Environment.Name = "build"
	submit(t, q, low)
	reply := check(t, q, small, nil)
	if len(reply.EnvironmentPlan) != 1 {
		t.Fatal("missing lower-tier build")
	}
	launch := *reply.EnvironmentPlan[0].Launch
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	small.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
	check(t, q, small, nil)
	if err := q.ConfigureHosts(map[string]HostPolicy{small.HostID: {ProductionHold: true}, big.HostID: {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	high := cpuSpec(1, 1)
	high.Priority = "production"
	wanted := submit(t, q, high)
	if host := job(t, q, wanted.ID).ProductionHost; host != big.HostID {
		t.Fatal("value preempted a busy cheap builder despite free capacity", host)
	}
	if got := check(t, q, big, nil).Job; got == nil || got.ID != wanted.ID {
		t.Fatal("free scarce host did not run higher-tier work", got)
	}
	if q.s.PreparationPlans[launch.Attempt].PriorityStop != nil {
		t.Fatal("unnecessary build preemption")
	}
}

func TestValueUnifiedOccupancyDoesNotChangeBands(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.Devices = m.Devices[:1]
	m.UnifiedMemory = true
	m.CapabilityProfile.MemoryLayout = "unified"
	d := &m.Devices[0]
	d.UnifiedMemory = true
	d.MemoryMode = "unified"
	d.TotalMemoryGB = 0
	d.BudgetGB = 32
	d.FreeMemoryGB = 32
	d.MemoryMeasured = true
	machineCheck(t, q, m)
	before := q.hostValues()[m.HostID]
	d.BudgetGB, d.FreeMemoryGB = 8, 8
	machineCheck(t, q, m)
	after := q.hostValues()[m.HostID]
	if !before.known || !after.known || before.accelerators["fake"] != 64 || after.accelerators["fake"] != 64 || !slices.Equal(before.vector, after.vector) {
		t.Fatal("live unified free RAM redefined host value", before, after)
	}
}

// Both cases used to recurse through candidate -> betterPlacement -> candidate.
func TestValueMultipleAcceleratorTargetsCPU(t *testing.T) {
	t.Run("two hosts", func(t *testing.T) {
		q, _ := fixture(t)
		large, small := valueMachine("a-large", "fake", 64), valueMachine("z-small", "fake", 32)
		check(t, q, large, nil)
		check(t, q, small, nil)
		wanted := submit(t, q, cpuSpec(1, 1))
		if got := check(t, q, large, nil).Job; got != nil {
			t.Fatal("expensive accelerator won", got)
		}
		if got := check(t, q, small, nil).Job; got == nil || got.ID != wanted.ID {
			t.Fatal("no CPU progress on accelerator", got)
		}
	})
	t.Run("two devices", func(t *testing.T) {
		q, _ := fixture(t)
		m := m3Machine()
		machineCheck(t, q, m)
		wanted := submit(t, q, cpuSpec(1, 1))
		got := assignedJob(machineCheck(t, q, m))
		if got == nil || got.ID != wanted.ID || len(got.AllocatedDevices) != 0 {
			t.Fatal("no CPU progress with two GPU targets", got)
		}
	})
}

func TestValueTieRequiresImmediateAdmission(t *testing.T) {
	for _, mode := range []string{"environment", "preparation"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			a, z := valueMachine("a-peer", "fake", 32), valueMachine("z-puller", "fake", 32)
			check(t, q, a, nil)
			check(t, q, z, nil)
			s := spec("experiment")
			if mode == "environment" {
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"fake": binaryRecipe(t, "fake")}}}); err != nil {
					t.Fatal(err)
				}
				s.Environment.Name = "build"
				z.Environments = []string{q.environments.Environments["build"]["fake"].Key}
				check(t, q, z, nil)
			}
			wanted := submit(t, q, s)
			if mode == "preparation" {
				q.s.PreparationPlans = map[string]HostLaunch{"busy": {Machine: a.ID, Session: a.Session, Kind: "environment", Key: "other", Attempt: "busy", Started: true}}
			}
			if got := check(t, q, z, nil).Job; got == nil || got.ID != wanted.ID {
				t.Fatal("tie waited for unready lower-ID peer", got)
			}
		})
	}
}

func TestValueTemporaryAvailabilityKeepsScarcity(t *testing.T) {
	for _, mode := range []string{"owner", "stale", "maintenance"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			a, b := valueMachine("a", "fake", 32), valueMachine("b", "fake", 32)
			c := valueMachine("c", "cpu", 16)
			for _, m := range []Machine{a, b, c} {
				check(t, q, m, nil)
			}
			before := q.hostValues()
			switch mode {
			case "owner":
				a.CapabilityProfile.Availability = &Availability{Mode: "daily", Available: false, CheckedAt: clock.Now(), Signals: idleSignals()}
				check(t, q, a, nil)
			case "stale":
				clock.Advance(LossAfter + time.Second)
			case "maintenance":
				if _, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "disable"}); err != nil {
					t.Fatal(err)
				}
			}
			after := q.hostValues()
			for id, h := range before {
				if after[id] == nil || !slices.Equal(h.vector, after[id].vector) {
					t.Fatalf("%s changed scarcity: before %v after %v", id, h, after[id])
				}
			}
			wanted := submit(t, q, spec("experiment"))
			if got := check(t, q, b, nil).Job; got == nil || got.ID != wanted.ID {
				t.Fatal("unavailable peer blocked accelerator", got)
			}
		})
	}
}

func TestValueSimulationCPUStreams(t *testing.T) {
	var fleet SimFleet
	var work SimWorkload
	if err := ReadSimYAML("../../examples/sim/value-placement-cpu/fleet.yaml", &fleet); err != nil {
		t.Fatal(err)
	}
	if err := ReadSimYAML("../../examples/sim/value-placement-cpu/workload.yaml", &work); err != nil {
		t.Fatal(err)
	}
	r := runSimTest(t, fleet, work, 864)
	if r.Completed != 4 || r.Failed != 0 || r.Unfinished != 0 || r.Production.P95 != 0 || r.Production.Unstarted != 0 || len(r.NeverPlaceable) != 0 {
		t.Fatal("mixed CPU/accelerator regression", r)
	}
	latest := 0.0
	for _, batch := range r.Batches {
		if batch.FinishedSeconds == nil {
			t.Fatal("unfinished batch")
		}
		latest = max(latest, *batch.FinishedSeconds)
	}
	nearSim(t, latest, 201)
}

// Round 2 steered only J1: every CPU peer independently claimed the same head,
// so the accelerator took J2 while four CPU hosts remained idle.
func TestValueGlobalPlanClaimsDistinctCPUJobs(t *testing.T) {
	q, _ := fixture(t)
	accelerator := valueMachine("a-accelerator", "fake", 64)
	check(t, q, accelerator, nil)
	cpus := make([]Machine, 4)
	for i := range cpus {
		cpus[i] = valueMachine(fmt.Sprintf("cpu-%d", i), "cpu", 16)
		check(t, q, cpus[i], nil)
	}
	wanted := map[string]bool{}
	for range 3 {
		wanted[submit(t, q, cpuSpec(1, 1)).ID] = true
	}
	if got := check(t, q, accelerator, nil).Job; got != nil {
		t.Fatal("accelerator took CPU work despite idle CPU capacity", got.ID)
	}
	for _, m := range cpus {
		if got := check(t, q, m, nil).Job; got != nil {
			if !wanted[got.ID] {
				t.Fatal("duplicate or unexpected claim", got.ID)
			}
			delete(wanted, got.ID)
		}
	}
	if len(wanted) != 0 {
		t.Fatal("CPU jobs did not progress", wanted)
	}
}

func TestValuePlanningWorkCounts(t *testing.T) {
	for _, count := range []int{200, 800, 1600} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			q, _ := fixture(t)
			accelerator := valueMachine("a-accelerator", "fake", 64)
			check(t, q, accelerator, nil)
			lastResortPolicy(t, q, accelerator.HostID, true)
			for i := range 4 {
				id := fmt.Sprintf("cpu-%d", i)
				check(t, q, valueMachine(id, "cpu", 16), nil)
			}
			template := submit(t, q, cpuSpec(1, 1))
			// In-memory canonical queued work; no timing or storage work in the proof.
			for i := 1; i < count; i++ {
				copy := cloneJob(template)
				copy.ID = fmt.Sprintf("count-%06d", i)
				q.s.Jobs[copy.ID] = &copy
			}
			scan := q.placementScan()
			for range 2 {
				for _, m := range scan.machines {
					q.plannedPick(m, scan)
					q.candidate(m, nil, scan)
					q.valueCompare(m, m, scan)
				}
			}
			if scan.planBuilds != 1 || scan.rankingBuilds != 1 || scan.livenessBuilds != 1 || scan.candidateBuilds != len(scan.machines) {
				t.Fatalf("jobs=%d plans=%d rankings=%d liveness maps=%d candidate sets=%d targets=%d", count, scan.planBuilds, scan.rankingBuilds, scan.livenessBuilds, scan.candidateBuilds, len(scan.machines))
			}
		})
	}
}

func TestValueSimulationIdleCPUHosts(t *testing.T) {
	var fleet SimFleet
	var work SimWorkload
	if err := ReadSimYAML("../../examples/sim/value-placement-idle-cpus/fleet.yaml", &fleet); err != nil {
		t.Fatal(err)
	}
	if err := ReadSimYAML("../../examples/sim/value-placement-idle-cpus/workload.yaml", &work); err != nil {
		t.Fatal(err)
	}
	r := runSimTest(t, fleet, work, 864)
	if r.Completed != 3 || r.Failed != 0 || r.Unfinished != 0 || r.Production.P95 != 0 || r.Production.Unstarted != 0 || len(r.NeverPlaceable) != 0 {
		t.Fatal("idle CPU fleet regression", r)
	}
	var acceleratorFinish float64
	latest := 0.0
	for name, batch := range r.Batches {
		if batch.FinishedSeconds == nil {
			t.Fatal("unfinished batch", name)
		}
		latest = max(latest, *batch.FinishedSeconds)
		if strings.HasPrefix(name, "accelerator") {
			acceleratorFinish = *batch.FinishedSeconds
		}
	}
	nearSim(t, latest, 200)
	nearSim(t, acceleratorFinish, 102)
}

func TestValueBackgroundCopyDoesNotBlockDispatch(t *testing.T) {
	q, _ := fixture(t)
	m := valueMachine("worker", "fake", 32)
	check(t, q, m, nil)
	wanted := submit(t, q, cpuSpec(1, 1))
	q.s.PreparationPlans = map[string]HostLaunch{"copy": {Machine: m.ID, Session: m.Session, Kind: "copy", Key: "other-model", Attempt: "copy", Started: true}}
	if got := q.plannedPick(q.s.Machines[m.ID], q.placementScan()); got == nil || got.ID != wanted.ID {
		t.Fatal("background copy blocked foreground admission", got)
	}
}
