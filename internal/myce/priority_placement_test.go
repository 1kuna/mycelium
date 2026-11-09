package myce

import (
	"testing"
	"time"
)

func TestPriorityPreemptionCallCapacity(t *testing.T) {
	for _, pin := range []bool{false, true} {
		t.Run(map[bool]string{false: "unpinned", true: "pinned"}[pin], func(t *testing.T) {
			q, _, m := modelFixture(t)
			preemptionPolicy(t, q)
			q.models.Models[0].MaxInFlight = 1
			m.ModelConfigHash = modelConfigHash(q.models)
			check(t, q, m, nil)
			if pin {
				if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			}
			low := addModelCall(t, q, "experiment")
			reportModel(t, q, m, "ready")
			if low.Machine == "" {
				t.Fatal("low did not start")
			}
			high := &ModelCall{Project: "urgent", Model: "test", Priority: "experiment"}
			if err := q.addCall(high); err != nil {
				t.Fatal(err)
			}
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if low.PriorityStop == nil || low.PriorityStop.Demand != high.ID || low.Error != priorityPreemptionCause {
				t.Fatalf("higher-tier call blocked by lower-tier call: high_machine=%q low_error=%q low_stop=%+v slot=%+v", high.Machine, low.Error, low.PriorityStop, q.s.ModelSlots[slotKey(m.ID, "test")])
			}
			slot := q.s.ModelSlots[slotKey(m.ID, "test")]
			if high.Machine != "" || slot.State != "ready" || slot.InFlight != 1 {
				t.Fatal("admitted before exact call release or unloaded engine", high, slot)
			}
			if err := q.finishCall(low.ID); err != nil {
				t.Fatal(err)
			}
			if high.Machine != m.ID || high.Attempt != slot.Attempt || slot.InFlight != 1 {
				t.Fatal("replacement did not reuse released slot", high, slot)
			}
			if pin && len(q.s.Pins) != 1 {
				t.Fatal("lost pin intent")
			}

		})
	}
}

// Each row is retained work that the common placement planner could interrupt.
// Exercise every demand adapter too: free capacity must win before affinity.
func TestPriorityPreemptionFreePlacementEveryWorkKind(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, kind := range []string{"resumable", "restart", "native-group", "call", "load", "idle", "pin", "build"} {
			for _, demandKind := range []string{"batch", "native-group", "call", "pin", "build"} {
				t.Run(kind+"/"+demandKind, func(t *testing.T) {
					q, _, m := modelFixture(t)
					preemptionPolicy(t, q)
					q.models.Models[0].MaxInFlight = 1
					m.ModelConfigHash = modelConfigHash(q.models)
					m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
					m.EnvironmentBuilder = true
					if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"fake": binaryRecipe(t, "fake")}}}); err != nil {
						t.Fatal(err)
					}
					check(t, q, m, nil)
					var victim *Job
					var call *ModelCall
					var engine *ModelSlot
					var launch *HostLaunch
					switch kind {
					case "resumable", "restart", "native-group":
						s := spec("experiment")
						s.Resumable = kind != "restart"
						if kind == "native-group" {
							s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"helper"}, MemoryGB: 1}}
							s.Needs.HostMemoryGB = 2
						}
						submit(t, q, s)
						victim = check(t, q, m, nil).Job
						if victim == nil {
							t.Fatal("no batch victim")
						}
						if err := q.Started(result(victim, false)); err != nil {
							t.Fatal(err)
						}
					case "call", "load", "idle", "pin":
						if kind == "pin" {
							if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
								t.Fatal(err)
							}
						} else {
							call = addModelCall(t, q, "experiment")
						}
						if kind != "load" {
							reportModel(t, q, m, "ready")
						}
						if kind == "idle" {
							if err := q.finishCall(call.ID); err != nil {
								t.Fatal(err)
							}
						}
						engine = q.s.ModelSlots[slotKey(m.ID, "test")]
					case "build":
						s := spec("experiment")
						s.Environment.Name = "build"
						submit(t, q, s)
						reply := check(t, q, m, nil)
						if len(reply.EnvironmentPlan) != 1 {
							t.Fatal("no build victim")
						}
						l := *reply.EnvironmentPlan[0].Launch
						launch = &l
						if err := q.AuthorizeLaunch(l); err != nil {
							t.Fatal(err)
						}
						m.EnvironmentReports = []EnvironmentReport{{Key: l.Key, Attempt: l.Attempt, State: "building"}}
						check(t, q, m, nil)
					}
					free := machine("two", "fake")
					free.ModelURL, free.ModelConfigHash = "http://127.0.0.1:1235", modelConfigHash(q.models)
					free.HostMemoryMeasured, free.HostAvailableMemoryGB = true, 32
					free.EnvironmentBuilder = true
					check(t, q, free, nil)
					switch demandKind {
					case "batch", "native-group", "build":
						s := spec("experiment")
						s.Project = "urgent"
						if demandKind == "native-group" {
							s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"helper"}, MemoryGB: 1}}
							s.Needs.HostMemoryGB = 2
						}
						if demandKind == "build" {
							s.Environment.Name = "build"
						}
						demand := submit(t, q, s)
						reply := check(t, q, free, nil)
						if demandKind == "build" {
							if len(reply.EnvironmentPlan) != 1 {
								t.Fatal("free worker not offered build", reply)
							}
						} else if reply.Job == nil || reply.Job.ID != demand.ID {
							t.Fatal("free worker not chosen", reply)
						}
					case "call":
						high := &ModelCall{Project: "urgent", Model: "test", Priority: "experiment"}
						if err := q.addCall(high); err != nil {
							t.Fatal(err)
						}
						if high.Machine == "" && q.s.ModelSlots[slotKey(free.ID, "test")] == nil && (engine == nil || engine.State != "loading") {
							t.Fatal("no free call placement")
						}
					case "pin":
						if err := q.Pin(ModelPin{Project: "urgent", Model: "test", Platform: "fake"}); err != nil {
							t.Fatal(err)
						}
						if q.s.Pins[len(q.s.Pins)-1].Assigned == "" {
							t.Fatal("no free pin placement")
						}
					}
					if victim != nil && (job(t, q, victim.ID).State != "running" || priorityStop(job(t, q, victim.ID)) != nil) {
						t.Fatal("interrupted batch despite free placement")
					}
					if call != nil && (call.Error != "" || call.PriorityStop != nil) {
						t.Fatal("interrupted call despite free placement", call)
					}
					if engine != nil && (engine.PriorityStop != nil || engine.State == "draining" || engine.State == "unloading") {
						t.Fatal("interrupted engine despite free placement", engine)
					}
					if launch != nil && q.s.PreparationPlans[launch.Attempt].PriorityStop != nil {
						t.Fatal("interrupted build despite free placement")
					}
				})
			}
		}

	})

	t.Run("CPUPreemptionPrefersIdleHost", func(t *testing.T) {
		q, clock := fixture(t)
		a := m3Machine()
		a.ID, a.HostID = "host-a", "host-a"
		a.GPU, a.CapabilityProfile.Accelerators[0].Type = "xpu", "xpu"
		a.Devices[0].Platform, a.Devices[1].Platform = "xpu", "xpu"
		low := spec("experiment")
		low.Needs.CPUCores, low.Needs.HostMemoryGB = 8, 1
		victim := submit(t, q, low)
		run := assignedJob(machineCheck(t, q, a))
		if run == nil || run.ID != victim.ID {
			t.Fatal("missing XPU attempt", run, job(t, q, victim.ID).Reason)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		b := cpuMachine("host-b", "host-b", "cpu", 8, 32)
		b.Devices = []MachineDevice{}
		machineCheck(t, q, b)
		clock.Advance(time.Second)
		s := cpuSpec(8, 1)
		s.Priority = "production"
		demand := submit(t, q, s)
		if got := job(t, q, victim.ID); got.State != "running" || priorityStop(got) != nil {
			t.Fatal("preempted XPU work despite fitting idle host", got.State, priorityStop(got))
		}
		if next := assignedJob(machineCheck(t, q, b)); next == nil || next.ID != demand.ID {
			t.Fatal("idle host did not receive production CPU demand", next)
		}

	})
}

func TestPriorityPreemptionCallSlotsProtectHigherAndSameTier(t *testing.T) {
	q, _, m := modelFixture(t)
	preemptionPolicy(t, q)
	q.models.Models[0].MaxInFlight = 2
	m.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, m, nil)
	low := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	high := &ModelCall{Project: "urgent", Model: "test", Priority: "experiment"}
	if err := q.addCall(high); err != nil {
		t.Fatal(err)
	}
	same := addModelCall(t, q, "experiment")
	if same.Machine != "" || low.PriorityStop != nil {
		t.Fatal("same tier displaced a call")
	}
	winner := &ModelCall{Project: "urgent", Model: "test", Priority: "experiment"}
	if err := q.addCall(winner); err != nil {
		t.Fatal(err)
	}
	slot := q.s.ModelSlots[slotKey(m.ID, "test")]
	if low.PriorityStop == nil || low.PriorityStop.Demand != winner.ID || high.PriorityStop != nil || high.Error != "" || slot.State != "ready" || slot.InFlight != 2 {
		t.Fatal("did not select only the useful lower-tier slot", low, high, slot)
	}
	if winner.Machine != "" {
		t.Fatal("replacement admitted before release")
	}
	if err := q.finishCall(low.ID); err != nil {
		t.Fatal(err)
	}
	if winner.Machine != m.ID || high.Machine != m.ID || slot.InFlight != 2 || same.Machine != "" {
		t.Fatal("wrong post-release admission", winner, same, slot)
	}
}
