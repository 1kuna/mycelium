package myce

import (
	"context"
	"strings"
	"testing"
	"time"
)

func unifiedCUDAAdmissionMachine() Machine {
	m := machine("unified", "cuda", "unified-host")
	m.UnifiedMemory, m.HostMemoryMeasured = true, true
	m.CapabilityProfile.MemoryLayout = "unified"
	m.CapabilityProfile.SystemMemoryBytes = 121 << 30
	m.FreeMemoryGB, m.TotalMemoryGB = 96, 96
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 96, 90
	m.MemoryCeilingSlackGB = .125
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 15}
	return m
}

func TestUnifiedCUDAAdmissionPhysicalFloor(t *testing.T) {
	for _, mode := range []string{"unified", "discrete-cuda", "other-unified"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := unifiedCUDAAdmissionMachine()
			if mode == "discrete-cuda" {
				m.UnifiedMemory, m.CapabilityProfile.MemoryLayout = false, "separate"
			}
			if mode == "other-unified" {
				m.GPU = "fake"
			}
			m.HostAvailableMemoryGB = 20
			m.MemoryObservation.UsedGB = 97 // 24 GiB raw free, below the 25 GiB floor.
			s := spec("experiment")
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = 2, 1
			j := submit(t, q, s)
			reply := check(t, q, m, nil)
			if mode != "unified" {
				if reply.Job == nil || reply.Job.ID != j.ID {
					t.Fatal("other host admission changed", reply)
				}
				return
			}
			if reply.Job != nil {
				t.Fatal("CUDA admitted below physical floor")
			}
			if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "physical RAM floor 25.000 GiB") {
				t.Fatal("physical refusal unexplained", reason)
			}
			observation := m.MemoryObservation
			m.MemoryObservation = nil
			if run := check(t, q, m, nil).Job; run != nil {
				t.Fatal("missing physical telemetry admitted CUDA", run)
			}
			m.MemoryObservation = observation
			clock.Advance(time.Second)
			m.MemoryObservation.UsedGB = 89 // 32 - 25 = 7 GiB; charge 3.125 fits.
			if run := check(t, q, m, nil).Job; run == nil || run.ID != j.ID {
				t.Fatal("fresh physical headroom did not restore admission", run)
			}
		})
	}
}

func TestUnifiedCUDAAdmissionModelLaunchRechecksFloor(t *testing.T) {
	_, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	available := 8.0 // +16 reserve = 24 GiB raw, below the 25 GiB floor.
	w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16,
		HostMemorySource: func(context.Context) (float64, error) { return available, nil },
		MemorySource:     func(context.Context) (float64, error) { return 96, nil },
	}
	rt, err := startModels(w, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rt.close(); err != nil {
			t.Error(err)
		}
	})
	loading := &workerModel{model: Model{Name: "synthetic", MemoryGB: 24, HostMemoryGB: 8}}
	if err := rt.checkLoadMemory(t.Context(), loading); err == nil || !strings.Contains(err.Error(), "insufficient host RAM") {
		t.Fatal("model bypassed live physical floor", err)
	}
	available = 59 // 75 raw - 25 reserve = 50 GiB, enough for 32.125 GiB.
	if err := rt.checkLoadMemory(t.Context(), loading); err != nil {
		t.Fatal("physical recovery did not permit model", err)
	}
	rt.mu.Lock()
	rt.memoryMachine.HostMemoryBudgetGB = 64 // New floor = 121 - 64 = 57 GiB.
	rt.mu.Unlock()
	if err := rt.checkLoadMemory(t.Context(), loading); err == nil || !strings.Contains(err.Error(), "insufficient host RAM") {
		t.Fatal("model retained a stale allowance floor", err)
	}
	// Legacy admission-only models may omit host RAM; CUDA still uses physical RAM.
	rt.mu.Lock()
	rt.memoryMachine.HostMemoryBudgetGB = 0
	rt.mu.Unlock()
	available, loading.model.HostMemoryGB = 8, 0
	if err := rt.checkLoadMemory(t.Context(), loading); err == nil || !strings.Contains(err.Error(), "insufficient host RAM") {
		t.Fatal("zero host vector bypassed CUDA physical floor", err)
	}
}

func TestUnifiedCUDAAdmissionUnknownDeviceRemainsFull(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	check(t, q, m, nil)
	// Current cgroup RAM exceeds the host vector. It cannot cancel the
	// device vector, whose telemetry is absent (the incident's default).
	j := &Job{ID: "holder", State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, Spec: spec("experiment")}
	j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB = 32, 21
	q.s.Jobs[j.ID] = j
	q.hostMemorySamples[m.ID] = map[attemptKey]hostMemorySample{jobKey(*j): {gb: 45, at: clock.Now()}}
	m.HostAvailableMemoryGB = 50
	m.MemoryObservation.UsedGB = 46 // 75 raw - 25 reserve = 50 usable.
	q.s.Machines[m.ID].HostAvailableMemoryGB = m.HostAvailableMemoryGB
	q.s.Machines[m.ID].MemoryObservation = m.MemoryObservation
	q.refreshHostMemory()
	if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 18 {
		t.Fatalf("device growth discounted: got %g GiB, want 50 - 32 = 18", got)
	}
	clock.Advance(hostMemorySampleFreshFor)
	q.refreshHostMemory()
	if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 0 {
		t.Fatal("stale physical sample admitted CUDA", got)
	}
	// Worker launch uses the same device floor and a new raw measurement.
	w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16, HostMemorySource: func(context.Context) (float64, error) { return 59, nil }}
	w.hostMemorySamples = map[attemptKey]hostMemorySample{jobKey(*j): {gb: 45, at: clock.Now()}}
	next := Job{Spec: cpuSpec(1, 20)}
	if err := localAttemptAdmission(w, []Job{*j}, next, nil); err == nil {
		t.Fatal("worker discounted unknown CUDA device usage")
	}
	w.HostMemorySource = func(context.Context) (float64, error) { return 16, nil } // raw 32; usable 7.
	if free, err := w.sharedHostLiveMemory(context.Background()); err != nil || free != 7 {
		t.Fatalf("worker floor not rechecked: free=%g err=%v", free, err)
	}
}

func TestUnifiedCUDAAdmissionProtectsExpectedProduction(t *testing.T) {
	q, clock, source, _, producer, _ := productionAdmissionFixture(t)
	parent := submit(t, q, producer)
	wave := check(t, q, source, nil).Job
	if wave == nil {
		t.Fatal("missing production producer")
	}
	if err := q.Started(result(wave, false)); err != nil {
		t.Fatal(err)
	}
	m := unifiedCUDAAdmissionMachine()
	m.HostAvailableMemoryGB = 90
	m.MemoryObservation.UsedGB = 16 // 105 raw - 25 reserve = 80 usable.
	check(t, q, m, nil)
	prod := spec("production")
	prod.Project = producer.Project
	prod.Needs.MemoryGB, prod.Needs.HostMemoryGB = 24, 8
	prod.AllowedMachines = []string{m.ID}
	prod.After = &PieceDependency{Job: parent.ID, Piece: "item-0", Outputs: map[string]string{"input": "out"}}
	demand := submit(t, q, prod)
	experiment := spec("experiment")
	experiment.Needs.MemoryGB, experiment.Needs.HostMemoryGB = 32, 21
	experiment.AllowedMachines = []string{m.ID}
	j := submit(t, q, experiment)
	clock.Advance(time.Second)
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("large CUDA experiment consumed expected production headroom", r.Job)
	}
	if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "unified CUDA production headroom") {
		t.Fatal("missing production headroom reason", reason)
	}
	if q.held(m.HostID) {
		t.Fatal("forecast changed production takeover")
	}
	// A smaller co-tenant fits with the production envelope and physical floor.
	small := *job(t, q, j.ID)
	small.Spec.Needs.MemoryGB, small.Spec.Needs.HostMemoryGB = 8, 4
	target := q.s.Machines[m.ID]
	if reason := q.productionAdmissionReason(&small, target); reason != "" {
		t.Fatal("small co-tenant withheld", reason)
	}
	// Rechecking an assigned, unstarted offer must not charge it twice.
	small.State, small.Machine, small.Session = "running", m.ID, m.Session
	copy := *target
	copy.FreeHostMemoryGB = hostNeed(target, 24, 8) // Exactly the production envelope remains.
	if reason := q.productionAdmissionReason(&small, &copy); reason != "" {
		t.Fatal("unstarted offer charged twice", reason)
	}
	// Qualification elsewhere does not authorize bypassing a bound host's
	// work-scoped production parity policy (#401).
	other := unifiedCUDAAdmissionMachine()
	other.ID, other.HostID, other.Session = "other", "other-host", "other-session"
	check(t, q, other, nil)
	q.s.Jobs[demand.ID].Spec.AllowedMachines = []string{m.ID, other.ID}
	q.s.Jobs[demand.ID].ProductionHost = m.HostID
	if reason := q.productionAdmissionReason(job(t, q, j.ID), q.s.Machines[m.ID]); reason == "" {
		t.Fatal("unapproved alternative bypassed production binding")
	}
	q.s.Jobs[demand.ID].ProductionHost = ""
	if reason := q.productionAdmissionReason(job(t, q, j.ID), q.s.Machines[m.ID]); reason != "" {
		t.Fatal("qualified unbound alternative did not remove sole-host protection", reason)
	}
	if _, err := q.ChangeHostMaintenance(other.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	if reason := q.productionAdmissionReason(job(t, q, j.ID), q.s.Machines[m.ID]); reason == "" {
		t.Fatal("unavailable alternative bypassed production headroom")
	}
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	if r := check(t, q, m, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("withdrawn production demand still withholds CUDA", r.Job)
	}
}

func TestUnifiedCUDAAdmissionMeasuredProductionEnvelope(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	first := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != first.ID {
		t.Fatal("missing envelope seed")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(run, true)
	host, device := 8.0, 24.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	machineCheck(t, q, m)
	s.Priority = "production"
	demand := submit(t, q, s)
	experiment := &Job{State: "queued", Spec: spec("experiment")}
	experiment.Spec.Needs.MemoryGB, experiment.Spec.Needs.HostMemoryGB = 16, 10
	target := allocationMachine(q.s.Machines[m.ID], []string{"0"})
	reason := q.productionAdmissionReason(experiment, target)
	// The tiny recipe defaults would fit. The retained peaks require
	// (8 + 24) * 1.25 + .125 = 40.125 GiB, with no overlap deduction.
	if !strings.Contains(reason, "40.125 GiB production envelope") {
		t.Fatal("measured production envelope or margin lost", reason)
	}
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	if reason := q.productionAdmissionReason(experiment, target); reason != "" {
		t.Fatal("cancelled production still protects an envelope", reason)
	}
}
