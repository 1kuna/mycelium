package myce

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestSharedDeviceCUDATierIsolation(t *testing.T) {
	for _, topology := range []string{"discrete", "unified"} {
		t.Run(topology, func(t *testing.T) {
			q, clock := fixture(t)
			m := sharedDiscreteMachine("cuda")
			if topology == "unified" {
				m = sharedUnifiedMachine()
			}
			var tenants []*Job
			for _, gb := range []float64{3, 2} {
				queued := submit(t, q, sharedSpec("experiment", gb))
				run := assignedJob(machineCheck(t, q, m, tenants...))
				if run == nil || run.ID != queued.ID {
					t.Fatal("experiment did not pack", run, job(t, q, queued.ID).Reason)
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				tenants = append(tenants, run)
			}
			// Reservations fit, but unobserved experiment growth could starve production.
			demand := submit(t, q, sharedSpec("production", 1))
			reply := machineCheck(t, q, m, tenants...)
			if got := assignedJob(reply); got != nil {
				t.Fatal("production joined lower-tier CUDA tenants", got.ID)
			}
			for _, run := range tenants {
				requireState(t, q, run.ID, "stopping")
				if !slices.ContainsFunc(reply.Attempts, func(a AttemptDirective) bool {
					return a.ID == run.ID && a.Attempt == run.Attempt && a.Stop && a.PriorityStop != nil
				}) {
					t.Fatal("missing exact tenant stop directive", run.ID, reply.Attempts)
				}
			}
			clock.Advance(q.gpuStopGrace() + time.Second)
			if got := assignedJob(machineCheck(t, q, m, tenants...)); got != nil {
				t.Fatal("elapsed stop grace admitted production without release", got.ID)
			}
			stale := result(tenants[0], false)
			stale.Attempt++
			stale.Interrupted = true
			if err := q.Complete(stale); !errors.Is(err, ErrStale) {
				t.Fatal("inexact tenant release was accepted", err)
			}
			finishInterrupted(t, q, tenants[0])
			if got := assignedJob(machineCheck(t, q, m, tenants[1])); got != nil {
				t.Fatal("production started before every lower-tier tenant released", got.ID)
			}
			finishInterrupted(t, q, tenants[1])
			got := assignedJob(machineCheck(t, q, m))
			if got == nil || got.ID != demand.ID || !slices.Equal(got.AllocatedDevices, []string{"0"}) {
				t.Fatal("production did not start after exact CUDA releases", got, job(t, q, demand.ID).Reason)
			}
			if err := q.Started(result(got, false)); err != nil {
				t.Fatal("production start refused after release", err)
			}
		})
	}
}

func TestSharedDeviceCUDAEqualTierPacks(t *testing.T) {
	for _, topology := range []string{"discrete", "unified"} {
		for _, priority := range []string{"experiment", "production"} {
			t.Run(topology+"/"+priority, func(t *testing.T) {
				q, _ := fixture(t)
				m := sharedDiscreteMachine("cuda")
				if topology == "unified" {
					m = sharedUnifiedMachine()
				}
				first := submit(t, q, sharedSpec(priority, 3))
				run := assignedJob(machineCheck(t, q, m))
				if run == nil || run.ID != first.ID {
					t.Fatal("first attempt not assigned", run)
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				second := submit(t, q, sharedSpec(priority, 2))
				got := assignedJob(machineCheck(t, q, m, run))
				if got == nil || got.ID != second.ID || !slices.Equal(got.AllocatedDevices, run.AllocatedDevices) {
					t.Fatal("equal-tier CUDA newcomer did not pack", got, job(t, q, second.ID).Reason)
				}
				requireState(t, q, run.ID, "running")
				if priorityStop(job(t, q, run.ID)) != nil {
					t.Fatal("equal-tier packing stopped the existing tenant")
				}
			})
		}
	}
}

func TestSharedDeviceCUDAModelLoadWaitsForTierRelease(t *testing.T) {
	for _, topology := range []string{"discrete", "unified"} {
		for _, kind := range []string{"call", "pin"} {
			t.Run(topology+"/"+kind, func(t *testing.T) {
				q, clock, m := modelFixture(t)
				inventory := sharedDiscreteMachine("cuda")
				if topology == "unified" {
					inventory = sharedUnifiedMachine()
				}
				m.GPU, m.Devices, m.CapabilityProfile = "cuda", inventory.Devices, inventory.CapabilityProfile
				m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 101
				m.MemoryObservation = inventory.MemoryObservation
				q.models.Models[0].MemoryGB, q.models.Models[0].Platforms = 1, []string{"cuda"}
				m.ModelConfigHash = modelConfigHash(q.models)
				machineCheck(t, q, m)
				var tenants []*Job
				for _, gb := range []float64{3, 2} {
					queued := submit(t, q, sharedSpec("experiment", gb))
					run := assignedJob(machineCheck(t, q, m, tenants...))
					if run == nil || run.ID != queued.ID {
						t.Fatal("experiment did not pack", run, job(t, q, queued.ID).Reason)
					}
					if err := q.Started(result(run, false)); err != nil {
						t.Fatal(err)
					}
					tenants = append(tenants, run)
				}
				if kind == "call" {
					addModelCall(t, q, "production")
				} else if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
				for _, run := range tenants {
					requireState(t, q, run.ID, "stopping")
				}
				clock.Advance(q.gpuStopGrace() + time.Second)
				if reply := machineCheck(t, q, m, tenants...); len(reply.ModelPlan) != 0 {
					t.Fatal("model loaded before lower-tier batch release", reply.ModelPlan)
				}
				finishInterrupted(t, q, tenants[0])
				if reply := machineCheck(t, q, m, tenants[1]); len(reply.ModelPlan) != 0 {
					t.Fatal("model loaded before every lower-tier batch released", reply.ModelPlan)
				}
				finishInterrupted(t, q, tenants[1])
				reply := machineCheck(t, q, m)
				if len(reply.ModelPlan) != 1 || reply.ModelPlan[0].State != "loading" || reply.ModelPlan[0].Device != "0" {
					t.Fatal("model did not load after exact batch releases", reply.ModelPlan)
				}
			})
		}
	}
}
