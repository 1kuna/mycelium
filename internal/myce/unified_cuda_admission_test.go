package myce

import (
	"context"
	"math"
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
	m.ReservedMemoryGB = 25 // The physical floor is the reserve; the allowance caps charges.
	m.MemoryCeilingSlackGB = .125
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 15}
	return m
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
	// The runtime reads a changed allowance at once. An authored allowance
	// caps the charge; it is never a floor on free RAM.
	rt.mu.Lock()
	rt.memoryMachine.HostMemoryBudgetGB = 64
	rt.mu.Unlock()
	if err := rt.checkLoadMemory(t.Context(), loading); err != nil {
		t.Fatal("authored allowance became a physical floor", err)
	}
	rt.mu.Lock()
	rt.memoryMachine.HostMemoryBudgetGB = 30 // Below the 32.125 GiB charge.
	rt.mu.Unlock()
	if err := rt.checkLoadMemory(t.Context(), loading); admissionDetail(err, Job{}) == nil || admissionDetail(err, Job{}).Subcheck != "configured_host_allowance" {
		t.Fatal("model retained a stale allowance", err)
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

func TestUnifiedCUDAAdmissionImpossibleProductionDoesNotStarve(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	check(t, q, m, nil)
	production := spec("production")
	production.Needs.MemoryGB, production.Needs.HostMemoryGB = 24, 80
	production.AllowedMachines = []string{m.ID}
	demand := submit(t, q, production) // 104.125 GiB can never fit the 96 GiB allowance.
	experiment := spec("experiment")
	experiment.Needs.MemoryGB, experiment.Needs.HostMemoryGB = 2, 1
	experiment.AllowedMachines = []string{m.ID}
	waiting := submit(t, q, experiment)
	clock.Advance(time.Second)
	if run := check(t, q, m, nil).Job; run == nil || run.ID != waiting.ID {
		t.Fatal("impossible production permanently withheld a fitting co-tenant", run, job(t, q, waiting.ID).Reason)
	}
	if job(t, q, demand.ID).State != "queued" {
		t.Fatal("impossible production was admitted")
	}
}

// Slow healthy check-ins used to book the entire outstanding growth as debt
// after five seconds, independently of the accepted observation's validity.
func TestUnifiedCUDAAdmissionAdaptiveObservation(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.MemoryObservation.UsedGB = 3.9
	m.HostAvailableMemoryGB = 92.1 // 121 total - 3.9 used - 25 reserve.
	observation := &ProbeObservation{PeriodSeconds: 12, CostSeconds: .1, ValidSeconds: 24.1}
	sampleAt := clock.Now()
	report := func(age float64) {
		m.HostHealth = &HostHealth{Probe: observation, AgeSeconds: age,
			History: []HostHealthSample{{At: sampleAt}}}
		if _, err := q.Checkin(Checkin{Machine: m, period: 12 * time.Second}); err != nil {
			t.Fatal(err)
		}
	}
	live := func(m *Machine) float64 {
		capacity, _, _, _ := q.hostLiveCapacity(m)
		return capacity
	}
	report(0)
	clock.Advance(10700 * time.Millisecond)
	accepted := q.s.Machines[m.ID]
	if !q.workerFresh(accepted, clock.Now()) || !q.hostHealthEvidenceValid(accepted, clock.Now()) {
		t.Fatal("incident fixture lacks healthy adaptive evidence")
	}
	// 102 GiB cannot fit a 96 GiB allowance. Refuse only the real shortfall,
	// rather than manufacturing 102 GiB debt from a fixed freshness cutoff.
	if got := live(accepted) - 102; math.Abs(got-(-9.9)) > 1e-9 {
		t.Fatalf("healthy slow observation charged fictitious debt: room=%g, want -9.9", got)
	}
	// Once #1117 removes the unused cold-probe promotion, the smaller real
	// outstanding demand fits under the same physical observation and reserve.
	if got := live(accepted) - 72; math.Abs(got-20.1) > 1e-9 {
		t.Fatalf("healthy slow observation refused fitting growth: room=%g", got)
	}
	// A continuing heartbeat carrying the same expired sensor observation
	// cannot renew its validity, even with a new accepted delivery receipt.
	clock.Advance(14 * time.Second)
	report(clock.Now().Sub(sampleAt).Seconds())
	accepted = q.s.Machines[m.ID]
	if !q.workerFresh(accepted, clock.Now()) {
		t.Fatal("stopped sensor fixture also lost worker contact")
	}
	if got := live(accepted) - 72; got != -72 {
		t.Fatalf("expired observation granted physical credit: %g", got)
	}
	// New successful evidence restores room; invalid physical data does not.
	sampleAt = clock.Now()
	report(0)
	accepted = q.s.Machines[m.ID]
	accepted.HostHealth.History[0].Error = "sensor failed"
	if got := live(accepted) - 72; got != -72 {
		t.Fatalf("failed sensor granted physical credit: %g", got)
	}
	accepted.HostHealth.History[0].Error = ""
	accepted.MemoryObservation = nil
	if got := live(accepted) - 72; got != -72 {
		t.Fatalf("missing physical observation granted credit: %g", got)
	}
	accepted.MemoryObservation = m.MemoryObservation
	clock.Advance(q.workerArrivalAllowance(accepted))
	if got := live(accepted) - 72; got != -72 {
		t.Fatalf("silent worker granted physical credit: %g", got)
	}
}

// Live numbers from the measured unified host (2026-11-11): 121.06 GiB, idle
// maximum 5.00, explicit reserve 16, allowance 100.06. Live used RAM already
// holds the idle footprint, so with a measured or authored allowance the floor
// is the reserve alone (not 21). Only the shared-interactive host keeps 21.
func TestUnifiedCUDAFloorMeasuredAllowanceCountsBaselineOnce(t *testing.T) {
	m := unifiedCUDAAdmissionMachine()
	total := 121.06
	m.CapabilityProfile.SystemMemoryBytes = uint64(total * (1 << 30))
	m.ReservedMemoryGB, m.HostMemoryBudgetGB = 16, 100.06
	for _, tc := range []struct {
		name     string
		measured bool
		shared   bool
		floor    float64
	}{
		{"measured", true, false, 16},
		{"authored", false, false, 16},
		{"measured-shared", true, true, 21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m.HostMemoryBudgetMeasured, m.SharedHost = tc.measured, nil
			if tc.shared {
				m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 1}
			}
			floor, ok := unifiedCUDAFloor(&m)
			if !ok || math.Abs(floor-tc.floor) > 1e-6 {
				t.Fatal(floor, ok)
			}
		})
	}
}
