package myce

import (
	"errors"
	"testing"
	"time"
)

func TestCPUHigherTierUsesCommonPlacement(t *testing.T) {
	for _, mode := range []string{"reclaim", "free host", "maintenance"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			preemptionPolicy(t, q)
			m := cpuMachine("cpu", "physical", "cpu", 2, 8)
			low := submit(t, q, cpuSpec(2, 1))
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != low.ID {
				t.Fatal("CPU-only host did not assign CPU work", run)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			free := cpuMachine("free", "other", "cpu", 2, 8)
			if mode == "free host" {
				check(t, q, free, nil)
			}
			if mode == "maintenance" {
				if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
					t.Fatal(err)
				}
			}
			s := cpuSpec(2, 1)
			s.Project = "urgent"
			high := submit(t, q, s)
			current := job(t, q, low.ID)
			switch mode {
			case "free host":
				if current.State != "running" || priorityStop(current) != nil {
					t.Fatal("stopped CPU work despite free placement", current.State)
				}
				if next := check(t, q, free, nil).Job; next == nil || next.ID != high.ID {
					t.Fatal("free CPU host did not take higher tier", next)
				}
			case "maintenance":
				if !plannedMaintenanceStop(current) || priorityStop(current) != nil {
					t.Fatal("priority replaced maintenance stop")
				}
				finishInterrupted(t, q, run)
				if next := check(t, q, m, nil).Job; next != nil {
					t.Fatal("higher tier bypassed drain", next)
				}
			case "reclaim":
				stop := priorityStop(current)
				if current.State != "stopping" || stop == nil || stop.Cause != priorityPreemptionCause || stop.Demand != high.ID {
					t.Fatal("CPU release did not use P3 typed stop", current.State, stop)
				}
				if next := check(t, q, m, run).Job; next != nil || q.s.Machines[m.ID].FreeCPUCores != 0 {
					t.Fatal("credited CPU before release acknowledgement")
				}
				finishInterrupted(t, q, run)
				if current := job(t, q, low.ID); current.State != "queued" || current.InfrastructureRetries != 0 || current.Losses != 0 {
					t.Fatal("planned CPU stop consumed retries", current.State)
				}
				if next := check(t, q, m, nil).Job; next == nil || next.ID != high.ID {
					t.Fatal("higher CPU tier not assigned after release", next)
				}
			}
		})
	}
}

func TestCPUBusyHostQueuesAndRejectsLostCompletion(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("cpu", "physical", "cpu", 2, 8)
	check(t, q, m, nil)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"cpu-project": {Weight: 1, AllowedMachines: []string{m.ID}}}); err != nil {
		t.Fatal(err)
	}
	first := submit(t, q, cpuSpec(2, 1))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != first.ID {
		t.Fatal("CPU assignment missing", run)
	}
	pending, err := q.Submit(cpuSpec(2, 1))
	if err != nil || pending.State != "queued" {
		t.Fatal("busy CPU host rejected job", pending, err)
	}
	clock.Advance(LossAfter + time.Second)
	check(t, q, observer(), nil)
	if err := q.Complete(result(run, true)); !errors.Is(err, ErrStale) {
		t.Fatal("lost CPU attempt completion accepted", err)
	}
	if current := job(t, q, first.ID); current.State != "queued" {
		t.Fatal("stale CPU result published", current.State)
	}
}

func TestCPUModelLoadReclaimsCPUWork(t *testing.T) {
	q, _, m := modelFixture(t)
	preemptionPolicy(t, q)
	m.CPUReserveCores = m.CapabilityProfile.LogicalCPUs - 1
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
	check(t, q, m, nil)
	submit(t, q, cpuSpec(1, 1))
	run := check(t, q, m, nil).Job
	if run == nil {
		t.Fatal("no CPU victim")
	}
	high := &ModelCall{Project: "urgent", Model: "test", Priority: "experiment"}
	if err := q.addCall(high); err != nil {
		t.Fatal(err)
	}
	if stop := priorityStop(job(t, q, run.ID)); stop == nil || stop.Demand != high.ID {
		t.Fatal("model load did not reclaim CPU through P3", stop)
	}
	if slot := q.s.ModelSlots[slotKey(m.ID, "test")]; slot != nil {
		t.Fatal("model loaded before CPU release", slot)
	}
	finishInterrupted(t, q, run)
	if slot := q.s.ModelSlots[slotKey(m.ID, "test")]; slot == nil || slot.State != "loading" || q.s.Machines[m.ID].CPUInUseCores != 1 {
		t.Fatal("released core not reserved once by model", slot)
	}
}

func TestCPUPreparationCoreReclaimedByCommonPlanner(t *testing.T) {
	q, _, m := modelFixture(t)
	preemptionPolicy(t, q)
	m.CPUReserveCores = m.CapabilityProfile.LogicalCPUs - 1
	m.EnvironmentBuilder = true
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"fake": binaryRecipe(t, "fake")}}}); err != nil {
		t.Fatal(err)
	}
	low := spec("experiment")
	low.Environment.Name = "build"
	submit(t, q, low)
	reply := check(t, q, m, nil)
	if len(reply.EnvironmentPlan) != 1 {
		t.Fatal("no preparation", reply)
	}
	launch := *reply.EnvironmentPlan[0].Launch
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
	check(t, q, m, nil)
	high := spec("experiment")
	high.Project = "urgent"
	demand := submit(t, q, high)
	if saved := q.s.PreparationPlans[launch.Attempt]; saved.PriorityStop == nil || saved.PriorityStop.Demand != demand.ID {
		t.Fatal("preparation core not reclaimable", saved.PriorityStop)
	}
	if next := check(t, q, m, nil).Job; next != nil || q.s.Machines[m.ID].FreeCPUCores != 0 {
		t.Fatal("preparation core credited before release", next)
	}
	m.EnvironmentReports[0].State = "failed"
	if next := check(t, q, m, nil).Job; next == nil || next.ID != demand.ID {
		t.Fatal("replacement blocked after preparation release", next)
	}
}
