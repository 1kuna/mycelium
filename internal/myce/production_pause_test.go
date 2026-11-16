package myce

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// One physical host, independent device slots and a shared RAM/CPU ledger.
// The allowance is below physical RAM; no host-name exception enables overlap.
func productionPauseMachine() Machine {
	m := m3Machine()
	m.CapabilityProfile.SystemMemoryBytes = 31 << 30
	m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = 26, 24
	for i := range m.Devices {
		m.Devices[i].TotalMemoryGB, m.Devices[i].BudgetGB, m.Devices[i].FreeMemoryGB = 30, 30, 30
	}
	return m
}

func TestProductionPauseHostResourcesAndResume(t *testing.T) {
	for _, dimension := range []string{"memory", "over-budget", "cpu", "device"} {
		t.Run(dimension, func(t *testing.T) {
			q, clock := fixture(t)
			m := productionPauseMachine()
			low := cpuSpec(1, 16)
			low.Resumable = true
			high := spec("production")
			high.Needs.HostMemoryGB = 10.6
			if dimension == "cpu" {
				low.Needs.CPUCores, low.Needs.MemoryGB = 8, 1
			} else if dimension == "device" {
				low = spec("experiment")
				low.Needs.HostMemoryGB, low.Needs.MemoryGB = 1, 30
			}
			victim := submit(t, q, low)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != victim.ID {
				t.Fatal("missing lower-tier execution", run)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			if dimension == "device" {
				high.Needs.GPU = &GPUSelection{Devices: slices.Clone(run.AllocatedDevices)}
			}
			// This models durable payload progress in the real checkpoint directory.
			saved := filepath.Join(q.root, run.Checkpoint, "saved")
			if err := os.MkdirAll(filepath.Dir(saved), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(saved, []byte("progress=41"), 0600); err != nil {
				t.Fatal(err)
			}
			if dimension == "over-budget" {
				m.HostMemoryBudgetGB = 12
				machineCheck(t, q, m, run)
				if q.s.Machines[m.ID].HostMemoryBudgetStatus() == "" {
					t.Fatal("fixture did not retain the over-budget holder")
				}
			}
			demand := submit(t, q, high)
			current := job(t, q, run.ID)
			if current.State != "stopping" || priorityStop(current) == nil || priorityStop(current).Demand != demand.ID {
				t.Fatal("ready production did not promptly request a cooperative release", current.State)
			}
			if got := assignedJob(machineCheck(t, q, m, run)); got != nil {
				t.Fatal("production spent capacity before exact release", got.ID)
			}
			clock.Advance(time.Second)
			if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: run.ID, Attempt: run.Attempt, Progress: 41}, Session: run.Session, Started: true}}}); err != nil {
				t.Fatal(err)
			}
			r := result(run, false)
			r.Interrupted = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			current = job(t, q, run.ID)
			if current.State != "queued" || current.Progress != 41 || current.InfrastructureRetries != 0 || current.Losses != 0 {
				t.Fatal("pause discarded progress or charged loss", current.State, current.Progress, current.Losses)
			}
			prod := assignedJob(machineCheck(t, q, m))
			if prod == nil || prod.ID != demand.ID {
				t.Fatal("production did not acquire released capacity", prod)
			}
			finish(t, q, prod, true)
			if dimension == "over-budget" {
				// The paused payload itself needs its original allowance to return.
				m.HostMemoryBudgetGB = 24
			}
			resumed := assignedJob(machineCheck(t, q, m))
			if resumed == nil || resumed.ID != run.ID || resumed.Attempt != run.Attempt+1 || resumed.Checkpoint != run.Checkpoint {
				t.Fatal("paused work did not resume the same progress", resumed)
			}
			if data, err := os.ReadFile(saved); err != nil || string(data) != "progress=41" {
				t.Fatal("checkpoint bytes lost", string(data), err)
			}
		})
	}
}

func TestProductionPauseFencesBackfill(t *testing.T) {
	for _, dimension := range []string{"memory", "cpu", "device"} {
		t.Run(dimension, func(t *testing.T) {
			q, clock := fixture(t)
			m := productionPauseMachine()
			low := cpuSpec(1, 16)
			high := spec("production")
			high.Needs.HostMemoryGB = 10.6
			if dimension == "cpu" {
				low.Needs.CPUCores, low.Needs.MemoryGB = 7, 1
				high.Needs.CPUCores = 2
			} else if dimension == "device" {
				low = spec("experiment")
				low.Needs.HostMemoryGB = 16
				high.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
			}
			low.Resumable = true
			victim := submit(t, q, low)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != victim.ID {
				t.Fatal("missing lower-tier holder", run)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			// Existing repeated-preemption protection leaves a brief real deficit.
			if err := q.update(func() error { q.touchJob(run.ID).PreemptionCount = 2; return nil }); err != nil {
				t.Fatal(err)
			}
			demand := submit(t, q, high)
			current := job(t, q, run.ID)
			if current.State != "running" || priorityStop(current) != nil || current.StopAt != nil {
				t.Fatal("priority ignored minimum-runtime protection", current.State)
			}
			holder := q.waitHolder(HostObligation{Result: result(run, false)}, demand, clock.Now())
			if holder.Preemptible || holder.PreemptionReason != "minimum_runtime" || holder.Job != run.ID {
				t.Fatal("protected holder not explained", holder)
			}
			later := cpuSpec(1, 1)
			if dimension == "device" {
				later = spec("experiment")
				later.Needs.HostMemoryGB = 1
			}
			backfill := submit(t, q, later)
			if got := assignedJob(machineCheck(t, q, m, run)); got != nil {
				t.Fatal("more backfill deepened production's deficit", got.ID, backfill.ID)
			}
			other := cpuMachine("other", "other", "cpu", 8, 24)
			if dimension != "device" {
				if got := check(t, q, other, nil).Job; got == nil || got.ID != backfill.ID {
					t.Fatal("portable queued backfill did not use another host", got)
				}
			}
			clock.Advance(q.preemptionGrace())
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if job(t, q, run.ID).State != "stopping" {
				t.Fatal("production did not reclaim the holder after its grace")
			}
			finishInterrupted(t, q, run)
			if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != demand.ID {
				t.Fatal("production did not follow exact holder release", got)
			}
		})
	}
}

func TestProductionPauseTwoSeparationsFit(t *testing.T) {
	q, _ := fixture(t)
	m := productionPauseMachine()
	low := cpuSpec(1, 8)
	low.Resumable = true
	submit(t, q, low)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil {
		t.Fatal("missing co-tenant")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.Needs.HostMemoryGB, s.Needs.MemoryGB = 10.6, 30
	first := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m, run))
	if a == nil || a.ID != first.ID {
		t.Fatal("first separation did not fit beside co-tenant", a)
	}
	second := submit(t, q, s)
	if job(t, q, run.ID).State != "stopping" {
		t.Fatal("second separation did not reclaim lower-tier host RAM")
	}
	finishInterrupted(t, q, run)
	b := assignedJob(machineCheck(t, q, m, a))
	if b == nil || b.ID != second.ID || slices.Equal(a.AllocatedDevices, b.AllocatedDevices) {
		t.Fatal("fitting separations did not co-run on distinct cards", a, b)
	}
	if got := q.s.Machines[m.ID]; got.HostMemoryInUseGB != 21.2 || got.HostMemoryInUseGB > got.EffectiveHostMemoryBudgetGB {
		t.Fatal("two separations bypassed shared host accounting", got.HostMemoryInUseGB)
	}
}
