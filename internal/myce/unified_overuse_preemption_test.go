package myce

import (
	"testing"
	"time"
)

// The #604 review probe: a lower-tier GPU job reserving 2.125 GiB on a unified
// CUDA host holds about 45 GiB of device memory its cgroup cannot see. A
// 27.9+25.5 GiB production job then read 33.9 GiB available and waited with
// no stop (#596). Only a fresh measured sample may credit the stop.
func TestUnifiedProductionPreemptsUnderDeclaredHolder(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, mode := range []string{"fresh", "stale", "discrete"} {
			t.Run(mode, func(t *testing.T) {
				q, clock := fixture(t)
				m := unifiedCUDAAdmissionMachine()
				if mode == "discrete" {
					m.UnifiedMemory, m.CapabilityProfile.MemoryLayout = false, "separate"
				}
				check(t, q, m, nil)
				holder := spec("deadline")
				holder.Project, holder.Resumable = "editor", false
				holder.Needs.MemoryGB, holder.Needs.HostMemoryGB = 1, 1
				holder.AllowedMachines = []string{m.ID}
				submit(t, q, holder)
				run := check(t, q, m, nil).Job
				if run == nil {
					t.Fatal("missing holder")
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				// About 45 GiB has left live headroom; the worker's per-process
				// device sample is the only per-attempt evidence of who holds it.
				m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 45, 60
				if mode == "discrete" {
					// Separate device memory is visible; host RAM is short for another reason.
					m.HostAvailableMemoryGB = 20
				}
				deviceAge := 1.0
				if mode == "stale" {
					deviceAge = deviceMemorySampleFreshFor.Seconds()
				}
				device := 45.0
				sample := &CurrentHostMemory{NonreclaimableGB: .5, DeviceGB: &device, DeviceAgeSeconds: deviceAge}
				keyed := func(reports ...AttemptReport) Reply {
					t.Helper()
					c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports}
					c.Machine.FreeMemoryGB = max(0, m.FreeMemoryGB-run.Spec.Needs.MemoryGB)
					reply, err := q.Checkin(c)
					if err != nil {
						t.Fatal(err)
					}
					return reply
				}
				report := AttemptReport{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: m.Session, Started: true, CurrentHostMemory: sample}
				keyed(report)
				prod := spec("production")
				prod.Project, prod.Resumable = "naming", false
				prod.Needs.MemoryGB, prod.Needs.HostMemoryGB, prod.Needs.CPUCores = 27.9, 25.5, 4
				prod.AllowedMachines = []string{m.ID}
				demand := submit(t, q, prod)
				clock.Advance(time.Second)
				stopped, admitted := false, false
				for _, d := range keyed(report).Attempts {
					stopped = stopped || d.ID == run.ID && d.Stop
					admitted = admitted || d.ID == demand.ID && d.Job != nil
				}
				if admitted {
					t.Fatal("production admitted before the holder released")
				}
				if mode != "fresh" {
					if stopped {
						t.Fatalf("%s sample stopped the holder", mode)
					}
					return
				}
				if !stopped {
					t.Fatalf("production did not stop the under-declared holder: reason=%q", job(t, q, demand.ID).Reason)
				}
				if err := q.Complete(result(run, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
				m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 90, 15
				if got := check(t, q, m, nil).Job; got == nil || got.ID != demand.ID {
					t.Fatalf("production not admitted after release: %v reason=%q", got, job(t, q, demand.ID).Reason)
				}
			})
		}

	})

	t.Run("UnifiedProductionPreemptsDeadlineHostMemory", func(t *testing.T) {
		q, clock := fixture(t)
		m := unifiedCUDAAdmissionMachine()
		m.MemoryObservation.UsedGB = 6
		check(t, q, m, nil)
		deadline := spec("deadline")
		deadline.Project, deadline.Resumable = "editor", false
		deadline.Needs.MemoryGB, deadline.Needs.HostMemoryGB, deadline.Needs.CPUCores = 64, 8, 2
		deadline.AllowedMachines = []string{m.ID}
		victim := submit(t, q, deadline)
		run := check(t, q, m, nil).Job
		if run == nil || run.ID != victim.ID {
			t.Fatal("missing deadline holder", run)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		// The holder's live use (~41 GiB) already left the measured headroom while
		// its unknown unified-device growth still reserves the full envelope.
		m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 49.3, 46.6
		check(t, q, m, run)
		prod := spec("production")
		prod.Project, prod.Resumable = "naming", false
		prod.Needs.MemoryGB, prod.Needs.HostMemoryGB, prod.Needs.CPUCores = 27.9, 25.5, 4
		prod.AllowedMachines = []string{m.ID}
		demand := submit(t, q, prod)
		clock.Advance(time.Second)
		reply := check(t, q, m, run)
		if !reply.Stop {
			t.Fatalf("production did not preempt the deadline holder: reason=%q", job(t, q, demand.ID).Reason)
		}
		if reply.Job != nil {
			t.Fatal("production admitted before the holder released", reply.Job)
		}
		// The holder exits; its unified allocation returns to live headroom.
		if err := q.Complete(result(run, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 90, 6
		if got := check(t, q, m, nil).Job; got == nil || got.ID != demand.ID {
			t.Fatalf("production not admitted after release: %v reason=%q", got, job(t, q, demand.ID).Reason)
		}

	})
}
