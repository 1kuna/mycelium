package myce

import (
	"fmt"
	"path/filepath"
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

func TestValuePlanningWorkCounts(t *testing.T) {
	for _, count := range []int{200, 800} {
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
