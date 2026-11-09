package myce

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func backfillMachine(id string, ram float64) Machine {
	m := cpuMachine(id, id, "cuda", 8, ram)
	m.CapabilityProfile.MemoryLayout = "unified"
	m.CapabilityProfile.SystemMemoryBytes = uint64(2 * ram * (1 << 30))
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 2 * ram}
	m.HostMemoryBudgetGB = ram
	m.Devices = []MachineDevice{{ID: "0", Platform: "cuda", Identity: "cuda-0", Fingerprint: "cuda", Selector: "0", UnifiedMemory: true, MemoryMode: "fixed", TotalMemoryGB: ram, BudgetGB: ram, FreeMemoryGB: ram}}
	return m
}

func TestHostBackfillAccumulatesForAccelerator(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, tc := range []struct {
			hosts int
			ram   float64
			cores bool
		}{{2, 16, false}, {1, 64, true}} {
			t.Run(fmt.Sprintf("hosts=%d/ram=%g/cores=%v", tc.hosts, tc.ram, tc.cores), func(t *testing.T) {
				q, clock := fixture(t)
				m := backfillMachine("accelerator", tc.ram)
				machineCheck(t, q, m)
				// Other hosts cannot run these Linux commands, as in the incident.
				for i := 1; i < tc.hosts; i++ {
					peer := valueMachine(fmt.Sprintf("peer-%03d", i), "cpu", tc.ram/8)
					peer.CapabilityProfile.OS = "darwin"
					check(t, q, peer, nil)
				}
				cpu := cpuSpec(1, tc.ram/4)
				if tc.cores {
					cpu = cpuSpec(3, 1)
				}
				cpu.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
				a := submit(t, q, cpu)
				first := assignedJob(machineCheck(t, q, m))
				if first == nil || first.ID != a.ID {
					t.Fatal("first CPU assignment", first, q.waitReason(q.s.Jobs[a.ID]), q.s.Machines[m.ID])
				}
				b := submit(t, q, cpu)
				second := assignedJob(machineCheck(t, q, m, first))
				if second == nil || second.ID != b.ID {
					t.Fatal("second CPU assignment", second)
				}
				gpu := spec("experiment")
				gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
				gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = tc.ram/4, tc.ram/2
				if tc.cores {
					gpu.Needs.CPUCores, gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 4, 1, 1
				}
				demand := submit(t, q, gpu)
				clock.Advance(time.Second)
				cpu.Needs.MemoryGB = tc.ram / 8
				cpu.Needs.CPUCores = 1
				later := submit(t, q, cpu)
				if got := assignedJob(machineCheck(t, q, m, first, second)); got != nil {
					t.Fatalf("later CPU backfill %s consumed RAM needed by waiting accelerator %s (later=%s)", got.ID, demand.ID, later.ID)
				}
				status, err := q.jobSnapshot(later.ID, context.Background())
				if err != nil || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demand.ID) {
					t.Fatalf("missing capacity accumulation diagnostic: %v, %v", status, err)
				}
				for _, running := range []*Job{first, second} {
					if q.s.Jobs[running.ID].StopAt != nil {
						t.Fatal("same-tier running work was stopped")
					}
				}
				finish(t, q, first, true)
				if got := assignedJob(machineCheck(t, q, m, second)); got == nil || got.ID != demand.ID {
					t.Fatal("accelerator did not start after natural CPU completion", got)
				}
			})
		}

	})

	t.Run("HostBackfillFullyOccupiedThenPartialRelease", func(t *testing.T) {
		q, clock := fixture(t)
		m := backfillMachine("accelerator", 64)
		machineCheck(t, q, m)
		firstJob := submit(t, q, cpuSpec(3, 1))
		first := assignedJob(machineCheck(t, q, m))
		secondJob := submit(t, q, cpuSpec(5, 1))
		second := assignedJob(machineCheck(t, q, m, first))
		if first == nil || second == nil || first.ID != firstJob.ID || second.ID != secondJob.ID {
			t.Fatal("missing occupied-host fixture", first, second)
		}
		gpu := spec("experiment")
		gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
		gpu.Needs.CPUCores, gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 4, 1, 1
		demand := submit(t, q, gpu)
		clock.Advance(time.Second)
		later := submit(t, q, cpuSpec(1, 1))
		if got := assignedJob(machineCheck(t, q, m, first, second)); got != nil {
			t.Fatal("assigned work on a fully occupied host", got)
		}
		finish(t, q, first, true)
		if got := assignedJob(machineCheck(t, q, m, second)); got != nil {
			t.Fatal("partial release admitted backfill ahead of the waiting demand", got)
		}
		status, err := q.jobSnapshot(later.ID, context.Background())
		if err != nil || status.DiagnosticCode != "host_capacity_accumulation" || !strings.Contains(status.Reason, demand.ID) {
			t.Fatal("partial release lost the capacity blocker", status, err)
		}
		finish(t, q, second, true)
		if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != demand.ID {
			t.Fatal("demand did not start after full release", got)
		}

	})
}

func TestHostBackfillNeedsReadyDemand(t *testing.T) {
	for _, reason := range []string{"cold", "impossible", "cancelled", "higher-tier"} {
		t.Run(reason, func(t *testing.T) {
			q, clock := fixture(t)
			m := backfillMachine("accelerator", 64)
			m.EnvironmentBuilder = true
			machineCheck(t, q, m)
			first := submit(t, q, cpuSpec(1, 32))
			running := assignedJob(machineCheck(t, q, m))
			if running == nil || running.ID != first.ID {
				t.Fatal("missing initial CPU work", running)
			}
			gpu := spec("experiment")
			gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
			gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 16, 32
			if reason == "cold" {
				r := binaryRecipe(t, "cuda")
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"cold": {"cuda": r}}}); err != nil {
					t.Fatal(err)
				}
				gpu.Environment.Name = "cold"
			}
			if reason == "impossible" {
				gpu.Needs.HostMemoryGB = 64
			}
			demand := submit(t, q, gpu)
			if reason == "cancelled" {
				if err := q.Cancel(demand.ID); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			cpu := cpuSpec(1, 4)
			if reason == "higher-tier" {
				cpu.Priority = "production"
			}
			later := submit(t, q, cpu)
			if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != later.ID {
				t.Fatal("backfill withheld without ready same/higher-tier demand", got)
			}
		})
	}
}

func TestHostBackfillProtectsOnlyOneTarget(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		a, b := backfillMachine("a", 64), backfillMachine("b", 64)
		machineCheck(t, q, a)
		machineCheck(t, q, b)
		cpu := cpuSpec(1, 32)
		cpu.AllowedMachines = []string{a.ID}
		initial := submit(t, q, cpu)
		running := assignedJob(machineCheck(t, q, a))
		if running == nil || running.ID != initial.ID {
			t.Fatal("missing initial CPU work", running)
		}
		gpu := spec("experiment")
		gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
		gpu.Needs.MemoryGB, gpu.Needs.HostMemoryGB = 16, 32
		demand := submit(t, q, gpu)
		cpu.Needs.MemoryGB = 4
		later := submit(t, q, cpu)
		if got := assignedJob(machineCheck(t, q, a, running)); got == nil || got.ID != later.ID {
			t.Fatal("busy host withheld although demand fits another host", got)
		}
		if got := assignedJob(machineCheck(t, q, b)); got == nil || got.ID != demand.ID {
			t.Fatal("demand did not start on its immediately fitting target", got)
		}

	})

	t.Run("HostBackfillCPUUsesImmediateAlternative", func(t *testing.T) {
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

	})
}

// A completely occupied host needs no backfill projection yet. After a partial
// natural release, the new pass must still protect a demand that cannot fit.
