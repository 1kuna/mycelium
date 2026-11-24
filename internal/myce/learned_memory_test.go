package myce

import (
	"testing"
	"time"
)

const physicalMemoryProbe = "exploratory first run"

// Memory-learning fixtures model a native worker with an actual attempt guard.
// cpuMachine itself also serves admission-only and unenforced host tests.
func guardedCPUMachine(id, host, backend string, cores int, ram float64) Machine {
	m := cpuMachine(id, host, backend, cores, ram)
	m.MemoryEnforcement = cgroupMemoryEnforcement
	return m
}

// One guarded 32 GiB CPU host; cold work starts at its measured startup floor.
func learnedMemoryFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker-host", "cpu", 8, 32)
	m.MemoryEnforcement = cgroupMemoryEnforcement
	m.HostMemoryBudgetGB, m.ExtensionFingerprint = 32, "cpu-v1" // Workers report their backend probe.
	check(t, q, m, nil)
	return q, clock, m
}

func runMemory(t *testing.T, q *Queue, clock *fakeClock, m Machine, s Spec, peak float64, mutate func(*Result)) *Job {
	t.Helper()
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != queued.ID {
		t.Fatalf("not assigned: %s", job(t, q, queued.ID).Reason)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak}
	if mutate != nil {
		mutate(&r)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	return j
}

func reopenQueue(t *testing.T, q *Queue, clock *fakeClock) *Queue {
	t.Helper()
	var database string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(database, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	reopened.fileRetention, reopened.logRetention = q.fileRetention, q.logRetention
	reopened.rebuildRecords()
	t.Cleanup(func() { reopened.Close() })
	fakeCheckpointAvailability(t, reopened, true)
	return reopened
}

// One unified CUDA machine worker (96 GiB allowance; 81 GiB physical room
// above the 25 GiB floor) owning its card and CPU targets.
func unifiedGPUFixture(t *testing.T, unified bool) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.ExtensionFingerprint = "cuda-v1"
	d := MachineDevice{ID: "0", Platform: "cuda", Identity: "synthetic-gb10", Selector: "0", MemoryMode: "unified", BudgetGB: 96, FreeMemoryGB: 96, ReserveMemoryGB: 25, UnifiedMemory: true, MemoryMeasured: true, ExtensionFingerprint: "cuda-v1"}
	if !unified {
		m.UnifiedMemory, m.CapabilityProfile.MemoryLayout = false, "separate"
		d.MemoryMode, d.UnifiedMemory = "separate", false
	}
	m.Devices = []MachineDevice{d}
	machineCheck(t, q, m)
	return q, clock, m
}

// `myce run --gpu cuda -- CMD` with no --mem or --host-mem.
func plainGPUSpec() Spec {
	return Spec{Name: "one-off", Project: "gpu-project", Priority: "experiment", Run: RunSpec{Command: []string{"echo", "done"}}, Needs: ResourceNeeds{CPUCores: 1, Platform: Platform{Required: []string{"cuda"}}}}
}

func assignGPU(t *testing.T, q *Queue, m Machine, want *Job, running ...*Job) *Job {
	t.Helper()
	j := assignedJob(machineCheck(t, q, m, running...))
	if j == nil || j.ID != want.ID {
		t.Fatalf("not assigned: %s", job(t, q, want.ID).Reason)
	}
	return j
}

// #631: accepted samples of the same plain discrete-GPU command must turn
// its next run into measured work, even when throughput occupancy is unknown.

// A per-item entry with a stated hint never repeats its exact work. After two
// accepted family runs the family bound supersedes the hint; before, it stands.
