package myce

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func cpuBackfillMachine(id string, ram float64) Machine {
	m := cpuMachine(id, id, "cpu", 8, ram)
	m.CapabilityProfile.SystemMemoryBytes = uint64(2 * ram * (1 << 30))
	m.HostMemoryBudgetGB = ram
	m.Devices = []MachineDevice{} // Modern CPU worker: concurrent keyed attempts.
	return m
}

func TestHostBackfillCPUAccumulates(t *testing.T) {
	for _, tc := range []struct {
		hosts int
		ram   float64
		cores bool
	}{{1, 64, false}, {200, 1024, false}, {1, 64, true}} {
		t.Run(fmt.Sprintf("hosts=%d/ram=%g/cores=%v", tc.hosts, tc.ram, tc.cores), func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuBackfillMachine("large", tc.ram)
			if tc.hosts > 1 {
				// The only large allowance may belong to an accelerator host;
				// its CPU target must share the same physical host fence.
				m = backfillMachine("large", tc.ram)
			}
			machineCheck(t, q, m)
			for i := 1; i < tc.hosts; i++ {
				machineCheck(t, q, cpuBackfillMachine(fmt.Sprintf("peer-%03d", i), tc.ram/8))
			}
			small := cpuSpec(1, 3*tc.ram/8)
			if tc.cores {
				small = cpuSpec(3, 1)
			}
			small.AllowedMachines = []string{m.ID}
			first := submit(t, q, small)
			a := assignedJob(machineCheck(t, q, m))
			if a == nil || a.ID != first.ID {
				t.Fatal("first CPU assignment", a)
			}
			second := submit(t, q, small)
			b := assignedJob(machineCheck(t, q, m, a))
			if b == nil || b.ID != second.ID {
				t.Fatal("second CPU assignment", b)
			}
			clock.Advance(time.Second)
			large := cpuSpec(1, 3*tc.ram/4)
			if tc.cores {
				large = cpuSpec(6, 1)
			}
			demand := submit(t, q, large)
			small.Needs.MemoryGB, small.Needs.CPUCores = tc.ram/16, 1
			for _, remaining := range [][]*Job{{a, b}, {b}} {
				for i := 0; i < 3; i++ {
					clock.Advance(time.Second)
					later := submit(t, q, small)
					if got := assignedJob(machineCheck(t, q, m, remaining...)); got != nil {
						t.Fatalf("backfill %s consumed capacity ahead of %s", got.ID, demand.ID)
					}
					status, err := q.jobSnapshot(later.ID, context.Background())
					if err != nil || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demand.ID) {
						t.Fatalf("missing CPU accumulation diagnostic: %v, %v", status, err)
					}
					for _, r := range remaining {
						if q.s.Jobs[r.ID].StopAt != nil {
							t.Fatal("running CPU work was stopped")
						}
					}
				}
				finish(t, q, remaining[0], true)
			}
			if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != demand.ID {
				t.Fatal("large CPU job did not start after natural completions", got)
			}
			if tc.hosts > 1 {
				portable := submit(t, q, cpuSpec(1, tc.ram/32))
				peer := cpuBackfillMachine("peer-001", tc.ram/8)
				if got := assignedJob(machineCheck(t, q, peer)); got == nil || got.ID != portable.ID {
					t.Fatal("other hosts stopped admitting", got)
				}
			}
		})
	}
}

func TestHostBackfillCPUReadinessAndOrder(t *testing.T) {
	for _, reason := range []string{"cold", "impossible-ram", "impossible-cores", "cancelled", "paused", "higher-tier", "fair-turn"} {
		t.Run(reason, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuBackfillMachine("cpu", 64)
			m.EnvironmentBuilder = true
			machineCheck(t, q, m)
			initial := submit(t, q, cpuSpec(1, 32))
			running := assignedJob(machineCheck(t, q, m))
			if running == nil || running.ID != initial.ID {
				t.Fatal("initial CPU assignment", running)
			}
			clock.Advance(time.Second)
			large := cpuSpec(1, 48)
			if reason == "cold" {
				r := binaryRecipe(t, "cpu")
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"cold": {"cpu": r}}}); err != nil {
					t.Fatal(err)
				}
				large.Environment.Name = "cold"
			}
			if reason == "impossible-ram" {
				large.Needs.MemoryGB = 128
			}
			if reason == "impossible-cores" {
				large.Needs.CPUCores = 9
			}
			demand := submit(t, q, large)
			if reason == "cancelled" {
				if err := q.Cancel(demand.ID); err != nil {
					t.Fatal(err)
				}
			}
			if reason == "paused" {
				if _, err := q.PauseJob(demand.ID, PauseJobRequest{Reason: "not ready"}); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			small := cpuSpec(1, 4)
			if reason == "higher-tier" {
				small.Priority = "production"
			}
			if reason == "fair-turn" {
				// The demand's project already owns running core-seconds. An
				// unused project's turn precedes its older large job.
				small.Project = "unused-project"
			}
			later := submit(t, q, small)
			if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != later.ID {
				t.Fatal("CPU capacity withheld from ready work ahead of demand", got)
			}
		})
	}
}

func TestHostBackfillCPUUsesImmediateAlternative(t *testing.T) {
	q, clock := fixture(t)
	a, b := cpuBackfillMachine("a", 64), cpuBackfillMachine("b", 128)
	machineCheck(t, q, a)
	machineCheck(t, q, b)
	small := cpuSpec(1, 32)
	small.AllowedMachines = []string{a.ID}
	initial := submit(t, q, small)
	running := assignedJob(machineCheck(t, q, a))
	if running == nil || running.ID != initial.ID {
		t.Fatal("initial CPU assignment", running)
	}
	clock.Advance(time.Second)
	demand := submit(t, q, cpuSpec(1, 48))
	clock.Advance(time.Second)
	small.Needs.MemoryGB = 4
	later := submit(t, q, small)
	if got := assignedJob(machineCheck(t, q, a, running)); got == nil || got.ID != later.ID {
		t.Fatal("busy host withheld although a more valuable host fits immediately", got)
	}
	if got := assignedJob(machineCheck(t, q, b)); got == nil || got.ID != demand.ID {
		t.Fatal("CPU demand did not use immediately fitting alternative", got)
	}
}
