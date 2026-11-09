package myce

import (
	"strings"
	"testing"
	"time"
)

func growthCheck(t *testing.T, q *Queue, m Machine, runs []*Job, used []float64, age float64) Reply {
	t.Helper()
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
	for i, j := range runs {
		r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true}
		if i < len(used) {
			r.CurrentHostMemory = &CurrentHostMemory{DeviceGB: &used[i], DeviceAgeSeconds: age}
		}
		c.Attempts = append(c.Attempts, r)
	}
	r, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSharedGPUPressureGrowth(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, layout := range []string{"unified", "separate"} {
			t.Run(layout, func(t *testing.T) {
				q, clock := fixture(t)
				m := sharedUnifiedMachine()
				m.CapabilityProfile.MemoryLayout = layout
				m.Devices[0].UnifiedMemory = layout == "unified"
				a, b := sharedPair(t, q, m)
				clock.Advance(time.Second)
				growthCheck(t, q, m, []*Job{a, b}, []float64{30, 13}, 0)
				got := job(t, q, a.ID)
				if got.State != "stopping" || priorityStop(got) == nil || priorityStop(got).Cause != "shared_gpu_pressure" || got.StopAt == nil || !got.StopAt.Equal(clock.Now().Add(q.gpuStopGrace())) {
					t.Fatalf("grower did not receive a planned stop with checkpoint grace: %+v", got)
				}
				requireState(t, q, b.ID, "running")
				// More reports while cleanup is pending must not stop the survivor.
				growthCheck(t, q, m, []*Job{a, b}, []float64{30, 13}, 0)
				requireState(t, q, b.ID, "running")
				finishInterrupted(t, q, a)
				got = job(t, q, a.ID)
				if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || got.OOMRetries != 0 {
					t.Fatal("pressure stop spent a failure budget", got)
				}
				// Retry custody and its raised floor survive a queue restart.
				q = memoryFactsRestart(t, q, clock)
				fakeCheckpointAvailability(t, q, true)
				finish(t, q, b, true)
				next := assignedJob(machineCheck(t, q, m))
				if next == nil || next.ID != a.ID || next.Attempt <= a.Attempt || next.Spec.Needs.MemoryGB != 30 || next.MemoryEnvelope == nil || !strings.Contains(next.MemoryEnvelope.Source, "shared GPU measured") {
					t.Fatal("retry did not reserve measured peak times the learning factor", next)
				}
			})
		}

	})

	t.Run("SharedGPUPressureVictimOrder", func(t *testing.T) {
		q, _ := fixture(t)
		// Mixed-tier CUDA/XPU packing is intentionally forbidden; Metal still
		// permits it, so keep the pressure guard's tier/over-reservation ordering proof here.
		m := sharedUnifiedMachine()
		m.GPU, m.Devices[0].Platform = "metal", "metal"
		m.CapabilityProfile.Accelerators[0].Type = "metal"
		var runs []*Job
		for _, priority := range []string{"experiment", "production"} {
			j := submit(t, q, sharedSpec(priority, 10))
			run := assignedJob(machineCheck(t, q, m, runs...))
			if run == nil || run.ID != j.ID {
				t.Fatal("co-tenant not assigned", j.ID)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			runs = append(runs, run)
		}
		// Preserve total observed pressure47 > device budget40 with one experiment
		// and one production tenant; no private most-overreservation ranking replay.
		growthCheck(t, q, m, runs, []float64{12, 35}, 0)
		requireState(t, q, runs[0].ID, "stopping")
		requireState(t, q, runs[1].ID, "running")
	})

	t.Run("SharedGPUPressureStaleAndUnavailable", func(t *testing.T) {
		q, clock := fixture(t)
		m := sharedUnifiedMachine()
		// Dedicated cards retain the original diagnostic-only missing-counter
		// behavior. Unified RAM now sheds to exclusive on missing evidence.
		m.Devices[0].UnifiedMemory = false
		m.Devices[0].MemoryMode = "fixed"
		m.CapabilityProfile.MemoryLayout = "separate"
		a, b := sharedPair(t, q, m)
		clock.Advance(time.Second)
		growthCheck(t, q, m, []*Job{a, b}, []float64{30, 13}, 10)
		for _, j := range []*Job{a, b} {
			got := job(t, q, j.ID)
			if got.State != "running" || !strings.Contains(got.Reason, "per-process device sample unavailable or stale") {
				t.Fatal("stale sample triggered or missing evidence went unreported", got)
			}
		}
		growthCheck(t, q, m, []*Job{a, b}, nil, 0)
		requireState(t, q, a.ID, "running")
		requireState(t, q, b.ID, "running")
		// A fresh grower cannot authorize a stop without per-process evidence
		// covering its co-tenant too (host-drop fallback cannot attribute sharing).
		growthCheck(t, q, m, []*Job{a, b}, []float64{30}, 0)
		requireState(t, q, a.ID, "running")
		requireState(t, q, b.ID, "running")

	})
}
