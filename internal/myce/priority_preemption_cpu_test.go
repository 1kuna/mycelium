package myce

import "testing"

// Leave GPU memory and RAM plentiful so only the shared host-core ledger can
// trigger reclamation. Reservations stay charged until exact cleanup.
func TestPriorityPreemptionCoreSaturation(t *testing.T) {
	for _, kind := range []string{"batch", "cpu-batch", "engine", "build"} {
		for _, demandKind := range []string{"batch", "call", "pin"} {
			t.Run(kind+"/"+demandKind, func(t *testing.T) {
				q, _, m := modelFixture(t)
				preemptionPolicy(t, q)
				m.CapabilityProfile.LogicalCPUs = 1
				m.TotalMemoryGB, m.FreeMemoryGB = 64, 64
				m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
				m.EnvironmentBuilder = true
				check(t, q, m, nil)
				var batch *Job
				var slot *ModelSlot
				var launch HostLaunch
				switch kind {
				case "batch", "cpu-batch":
					s := spec("experiment")
					if kind == "cpu-batch" {
						s = cpuSpec(1, 1)
					}
					submit(t, q, s)
					batch = check(t, q, m, nil).Job
					if batch == nil {
						t.Fatal("missing batch")
					}
				case "engine":
					addModelCall(t, q, "experiment")
					slot = q.s.ModelSlots[slotKey(m.ID, "test")]
					if slot == nil {
						t.Fatal("missing engine")
					}
				case "build":
					if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"fake": binaryRecipe(t, "fake")}}}); err != nil {
						t.Fatal(err)
					}
					s := spec("experiment")
					s.Environment.Name = "build"
					submit(t, q, s)
					reply := check(t, q, m, nil)
					if len(reply.EnvironmentPlan) != 1 {
						t.Fatal("missing build", reply)
					}
					launch = *reply.EnvironmentPlan[0].Launch
					if err := q.AuthorizeLaunch(launch); err != nil {
						t.Fatal(err)
					}
					m.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
					check(t, q, m, nil)
				}
				var demand *Job
				var call *ModelCall
				switch demandKind {
				case "batch":
					s := spec("experiment")
					s.Project = "urgent"
					demand = submit(t, q, s)
				case "call":
					// A different model requires another residency/core.
					model := q.models.Models[0]
					model.Name = "urgent-model"
					q.models.Models = append(q.models.Models, model)
					m.ModelConfigHash = modelConfigHash(q.models)
					check(t, q, m, batch)
					call = &ModelCall{ID: ID(), Model: model.Name, Project: "urgent", Priority: "experiment"}
					if err := q.addCall(call); err != nil {
						t.Fatal(err)
					}
				case "pin":
					model := q.models.Models[0]
					model.Name = "urgent-model"
					q.models.Models = append(q.models.Models, model)
					m.ModelConfigHash = modelConfigHash(q.models)
					check(t, q, m, batch)
					if err := q.Pin(ModelPin{Project: "urgent", Model: model.Name, Machine: m.ID}); err != nil {
						t.Fatal(err)
					}
				}
				reply := check(t, q, m, batch)
				if reply.Job != nil || call != nil && call.Machine != "" {
					t.Fatal("admitted before cleanup", reply)
				}
				if s := q.s.ModelSlots[slotKey(m.ID, "urgent-model")]; s != nil {
					t.Fatal("engine admitted before core release", s)
				}
				if q.s.Machines[m.ID].FreeCPUCores != 0 {
					t.Fatal("projected release leaked into live ledger")
				}
				switch kind {
				case "batch", "cpu-batch":
					if !reply.Stop || priorityStop(job(t, q, batch.ID)) == nil {
						t.Fatal("core saturation did not stop batch", reply)
					}
					r := result(batch, false)
					r.Interrupted = true
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
				case "engine":
					if slot.PriorityStop == nil {
						t.Fatal("core saturation did not stop engine", slot)
					}
					recoveryReport(t, q, m, slot, "unloaded")
				case "build":
					if reply.PreparationStopAt == nil {
						t.Fatal("core saturation did not stop build", reply)
					}
					m.EnvironmentReports[0].State = "paused"
				}
				reply = check(t, q, m, nil)
				if demand != nil {
					// Engine report may have already delivered the assignment.
					if got := job(t, q, demand.ID); got.State != "running" {
						t.Fatal("batch did not use released core", got)
					}
				} else if s := q.s.ModelSlots[slotKey(m.ID, "urgent-model")]; s == nil || s.State != "loading" {
					t.Fatal("engine did not use released core", reply)
				}
			})
		}
	}
}
