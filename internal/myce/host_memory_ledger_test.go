package myce

import (
	"math"
	"testing"
	"time"
)

// Idle RAM offered to jobs on fleet-shaped hosts (synthetic, rounded to the
// shapes in the 2026-11-11 capacity audit): total, idle p50/p95/max, explicit
// reserve and authored allowance. Every protection is one margin on the live
// reading, so no mode subtracts the idle footprint twice.
func TestHostMemoryLedgerFleetShapes(t *testing.T) {
	type shape struct {
		name                 string
		total, p50, p95, max float64
		reserve, allowance   float64
		unified, darwin      bool
		role                 string
		measured             bool
		want                 float64
	}
	for _, s := range []shape{
		{name: "unified-authored", total: 121.06, p50: 4, p95: 4.5, max: 5, reserve: 16, allowance: 96, unified: true, want: 96},
		{name: "unified-measured", total: 121.06, p50: 4, p95: 4.5, max: 5, reserve: 16, allowance: 96, unified: true, measured: true, want: 101.06},
		{name: "services-authored", total: 31.21, p50: 8.2, p95: 12, max: 14.6, reserve: 4, allowance: 24, role: hostRoleServices, want: 19.01},
		{name: "services-measured", total: 31.21, p50: 8.2, p95: 12, max: 14.6, reserve: 4, allowance: 24, role: hostRoleServices, measured: true, want: 16.61},
		{name: "node16-authored", total: 16, p50: 6.6, p95: 8, max: 8.6, reserve: 4, allowance: 5.25, darwin: true, want: 5.25},
		{name: "node16-measured", total: 16, p50: 6.6, p95: 8, max: 8.6, reserve: 4, allowance: 5.25, darwin: true, measured: true, want: 5.4},
		{name: "node24-authored", total: 24, p50: 12.7, p95: 14, max: 14.7, reserve: 3, allowance: 6.5, darwin: true, want: 6.5},
		{name: "node24-measured", total: 24, p50: 12.7, p95: 14, max: 14.7, reserve: 3, allowance: 6.5, darwin: true, measured: true, want: 8.3},
	} {
		t.Run(s.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine(s.name, "cpu", s.name+"-host")
			m.CapabilityProfile.SystemMemoryBytes = uint64(s.total * (1 << 30))
			if s.darwin {
				m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
			}
			if s.unified {
				m.GPU, m.UnifiedMemory, m.CapabilityProfile.MemoryLayout = "cuda", true, "unified"
				m.ReservedMemoryGB = s.reserve
			}
			m.HostMemoryMeasured, m.HostMemoryBudgetGB, m.HostMemoryBudgetMeasured = true, s.allowance, s.measured
			m.CapabilityProfile.Availability = &Availability{Mode: "dedicated", Available: true}
			m.MemoryObservation = &HostMemoryObservation{TotalGB: s.total, MyceliumGB: .1, ReserveGB: s.reserve, BaselineVersion: 1, Quiescent: true}
			idle := func(used float64) {
				m.MemoryObservation.UsedGB = used
				m.HostAvailableMemoryGB = max(0, s.total-used-s.reserve)
			}
			for i := range 100 {
				idle(s.p50)
				if i >= 88 {
					idle(s.p95)
				}
				if i == 99 {
					idle(s.max)
				}
				q.recordExternalMemory(&m)
				clock.Advance(time.Minute)
			}
			q.s.HostMaintenance = map[string]HostMaintenance{m.HostID: {Role: s.role, MemoryBudget: &HostMemoryBudget{GiB: s.allowance, Measured: s.measured}}}
			idle(s.p50)
			sample := clock.Now()
			m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 12, CostSeconds: .1, ValidSeconds: 24.1}, History: []HostHealthSample{{At: sample}}}
			if _, err := q.Checkin(Checkin{Machine: m, period: 12 * time.Second}); err != nil {
				t.Fatal(err)
			}
			if s.measured {
				q.s.Machines[m.ID].HostMemoryBudgetGB = q.s.Machines[m.ID].EffectiveHostMemoryBudgetGB
			}
			q.refreshHostMemory()
			got := q.s.Machines[m.ID].FreeHostMemoryGB
			if math.Abs(got-s.want) > .01 {
				t.Fatalf("idle room %.2f GiB, want %.2f", got, s.want)
			}
		})
	}
}
