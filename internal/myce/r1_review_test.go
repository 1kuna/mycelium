package myce

import (
	"fmt"
	"testing"
	"time"
)

func TestR1MixedHostSteersCPUAndPreservesGPUBackfill(t *testing.T) {
	// Adverse arrival: CPU work arrives first and the GPU checks in first.
	t.Run("CPU-arrives-before-GPU", func(t *testing.T) {
		q, clock := fixture(t)
		gpu, cpu := cpuMachine("gpu", "shared", "cuda", 16, 64), cpuMachine("cpu", "shared", "cpu", 16, 64)
		check(t, q, gpu, nil)
		check(t, q, cpu, nil)
		cs, gs := cpuSpec(2, 1), spec("experiment")
		gs.Needs.Platform = Platform{Required: []string{"cuda"}}
		cs.Project = gs.Project
		cj := submit(t, q, cs)
		clock.Advance(time.Millisecond)
		gj := submit(t, q, gs)
		gr, cr := check(t, q, gpu, nil).Job, check(t, q, cpu, nil).Job
		if cr == nil || gr == nil || cr.ID != cj.ID || gr.ID != gj.ID {
			t.Fatal("CPU work did not leave GPU available", cr, gr)
		}
	})
	for _, tc := range []struct {
		placement  string
		gpuWaiting bool
	}{
		{"absent", false}, {"free", false}, {"busy", false}, {"ineligible", false}, {"busy", true},
	} {
		placement, gpuWaiting := tc.placement, tc.gpuWaiting
		t.Run(fmt.Sprintf("backfill/%s/gpuWaiting=%v", placement, gpuWaiting), func(t *testing.T) {
			q, clock := fixture(t)
			gpu := cpuMachine("gpu", "shared", "cuda", 16, 64)
			cpu := cpuMachine("cpu", "shared", "cpu", 16, 64)
			check(t, q, gpu, nil)
			if placement != "absent" {
				check(t, q, cpu, nil)
			}
			if placement == "busy" {
				s := cpuSpec(1, 1)
				s.AllowedMachines = []string{cpu.ID}
				busy := submit(t, q, s)
				if run := check(t, q, cpu, nil).Job; run == nil || run.ID != busy.ID {
					t.Fatal("missing busy CPU work")
				}
			}
			s := cpuSpec(2, 1)
			if placement == "ineligible" {
				s.AllowedMachines = []string{gpu.ID}
			}
			j := submit(t, q, s)
			var gj *Job
			if gpuWaiting {
				clock.Advance(time.Millisecond)
				gs := spec("experiment")
				gs.Needs.Platform = Platform{Required: []string{"cuda"}}
				gj = submit(t, q, gs)
			}
			run := check(t, q, gpu, nil).Job
			switch {
			case gpuWaiting:
				if run == nil || run.ID != gj.ID {
					t.Fatal("CPU backfill blocked waiting same-tier GPU work")
				}
			case placement == "free":
				if run != nil {
					t.Fatal("GPU consumed CPU work with idle CPU placement")
				}
			default:
				if run == nil || run.ID != j.ID {
					t.Fatal("idle GPU did not backfill CPU work")
				}
			}
		})
	}
}

func TestR1HostReleaseSkipsUnneededDimensions(t *testing.T) {
	for _, cores := range []int{16, 2} {
		for _, zeroSlot := range []bool{false, true} {
			t.Run(fmt.Sprintf("cores=%d/zeroRAMSlot=%v", cores, zeroSlot), func(t *testing.T) {
				q, clock := fixture(t)
				a, b, c := cpuMachine("w1", "shared", "fake", cores, 12), cpuMachine("w2", "shared", "fake", cores, 12), cpuMachine("w3", "shared", "fake", cores, 12)
				check(t, q, a, nil)
				check(t, q, b, nil)
				check(t, q, c, nil)
				var low *Job
				if zeroSlot {
					model := Model{Name: "zero", Engine: "fake", Path: "weights", Platforms: []string{"fake"}, MemoryGB: 1}
					if err := q.ConfigureModels(ModelConfig{Models: []Model{model}, Idle: time.Hour, Wait: time.Minute}); err != nil {
						t.Fatal(err)
					}
					q.s.Machines[a.ID].ModelURL = "http://worker"
					q.s.Machines[a.ID].ModelConfigHash = modelConfigHash(q.models)
					q.s.ModelSlots[slotKey(a.ID, model.Name)] = &ModelSlot{LastUsed: clock.Now(), Machine: a.ID, Session: a.Session, Model: model.Name, State: "ready", Priority: "experiment", Project: "test"}
					q.refreshHostMemory()
				} else {
					s := spec("experiment")
					s.AllowedMachines = []string{a.ID}
					low = submit(t, q, s)
					if run := check(t, q, a, nil).Job; run == nil || run.ID != low.ID {
						t.Fatal("missing zero-RAM victim")
					}
				}
				s := spec("deadline")
				s.Needs.HostMemoryGB = 6
				s.AllowedMachines = []string{b.ID}
				ram := submit(t, q, s)
				if run := check(t, q, b, nil).Job; run == nil || run.ID != ram.ID {
					t.Fatal("missing RAM victim")
				}
				s = spec("production")
				s.Needs.HostMemoryGB = 8
				s.AllowedMachines = []string{c.ID}
				submit(t, q, s)
				if job(t, q, ram.ID).State != "stopping" {
					t.Fatal("RAM victim was not stopped")
				}
				if zeroSlot {
					if slot := q.s.ModelSlots[slotKey(a.ID, "zero")]; slot.State != "ready" {
						t.Fatal("stopped zero-RAM slot after RAM release covers cores", slot.State)
					}
				} else if job(t, q, low.ID).State != "running" {
					t.Fatal("stopped zero-RAM job after RAM release covers cores")
				}
			})
		}
	}
}

func TestR1VariantOOMReservationFloors(t *testing.T) {
	for _, backend := range []string{"fake", "cuda"} {
		for _, path := range []string{"kind", "program", "explicit"} {
			t.Run(path+"/retry="+backend, func(t *testing.T) {
				q, clock := fixture(t)
				e := compositionExtension(t, false)
				e.ProgramTypes = []string{"sample-script"}
				e.Variants[0].Baselines["program:sample-script"] = fakeExtension(t).Baselines["program:sample-script"]
				e.Variants[0].Needs.CPUCores = 2
				if backend != "fake" {
					other := e.Variants[0]
					other.Target.Platform = backend
					other.Needs.CPUCores = 1
					e.Variants = append(e.Variants, other)
				}
				e = freezeComposition(t, e)
				if err := q.AddExtension(e); err != nil {
					t.Fatal(err)
				}
				small := placementMachine("small", "separate", 64, 24, e)
				check(t, q, small, nil)
				s := placementSpec(path == "program")
				if path == "explicit" {
					s.Kind = ""
					s.Extension = extensionKey(e)
				}
				if path == "explicit" {
					report := newExtensionReport(e, &small, s.workType())
					report.Setup, report.Qualified, report.CheckID = true, true, "explicit-check"
					small.ExtensionReports = append(small.ExtensionReports, report)
					check(t, q, small, nil)
				}
				j := submit(t, q, s)
				run := check(t, q, small, nil).Job
				if run == nil || run.ID != j.ID {
					t.Fatal("missing first assignment")
				}
				r := result(run, false)
				r.OutOfMemory = true
				clock.Advance(time.Second)
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				floor := job(t, q, j.ID).Spec.Needs
				if floor.MemoryGB != 2 || floor.HostMemoryGB != .2 || floor.CPUCores != 2 {
					t.Fatalf("wrong stored OOM floor: %+v", floor)
				}
				big := placementMachine("big", "separate", 128, 48, e)
				big.GPU = backend
				big.CapabilityProfile.Accelerators[0].Type = backend
				report := newExtensionReport(e, &big, s.workType())
				report.Setup, report.Qualified, report.CheckID = true, true, "retry-check"
				big.ExtensionReports = []ExtensionReport{report}
				retry := check(t, q, big, nil).Job
				if retry == nil || retry.ID != j.ID || retry.Attempt != 2 {
					t.Fatal("missing OOM retry")
				}
				if retry.Spec.Needs.MemoryGB < floor.MemoryGB || retry.Spec.Needs.HostMemoryGB < floor.HostMemoryGB || retry.Spec.Needs.CPUCores < floor.CPUCores {
					t.Fatalf("retry lowered stored floor: got %+v want >= %+v", retry.Spec.Needs, floor)
				}
				saved := job(t, q, j.ID).Spec.Needs
				if saved.MemoryGB < floor.MemoryGB || saved.HostMemoryGB < floor.HostMemoryGB || saved.CPUCores < floor.CPUCores {
					t.Fatal("dispatch lowered persisted reservation")
				}
			})
		}
	}
}

func TestR1PreparingCPUWorkerAllowsGPUBackfill(t *testing.T) {
	for _, extension := range []bool{false, true} {
		t.Run(fmt.Sprintf("extension=%v", extension), func(t *testing.T) {
			q, clock := fixture(t)
			gpu := cpuMachine("gpu", "shared", "cuda", 16, 64)
			cpu := cpuMachine("cpu", "shared", "cpu", 16, 64)
			cpu.EnvironmentBuilder = true
			check(t, q, gpu, nil)
			check(t, q, cpu, nil)
			s := cpuSpec(1, 1)
			s.AllowedMachines = []string{cpu.ID}
			if extension {
				e := compositionExtension(t, false)
				e.Variants[0].Target = machineTarget(&cpu)
				e.Variants[0].Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1, HostMemoryGB: 1}
				e = freezeComposition(t, e)
				if err := q.AddExtension(e); err != nil {
					t.Fatal(err)
				}
				s = Spec{Name: "prepare", Project: "cpu-project", Priority: "experiment", Extension: extensionKey(e), AllowedMachines: []string{cpu.ID}}
			} else {
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"cpu": binaryRecipe(t, "cpu")}}}); err != nil {
					t.Fatal(err)
				}
				s.Environment.Name = "build"
			}
			submit(t, q, s)
			plan := check(t, q, cpu, nil).EnvironmentPlan
			if len(plan) != 1 || plan[0].Launch == nil {
				t.Fatal("missing preparation")
			}
			launch := *plan[0].Launch
			if err := q.AuthorizeLaunch(launch); err != nil {
				t.Fatal(err)
			}
			cpu.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
			clock.Advance(time.Second)
			if continued := check(t, q, cpu, nil).EnvironmentPlan; len(continued) != 1 || continued[0].Launch.Attempt != launch.Attempt {
				t.Fatal("active preparation was not re-offered")
			}
			backfill := submit(t, q, cpuSpec(2, 1))
			if run := check(t, q, gpu, nil).Job; run == nil || run.ID != backfill.ID {
				t.Fatal("GPU declined CPU backfill while CPU worker was preparing")
			}
		})
	}
}
