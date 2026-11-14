package myce

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"
)

// Numbers only from installed status --history, captured 2026-11-14 UTC.
// Recipe, host, hardware, environment and allowance identities are replaced
// consistently. Accepted diagnostic peaks were matched to their exact timing
// attempt, machine and session, then to the recipe/target/host/hardware/
// fingerprint/environment of these rows. No consumer names, paths, payloads or
// credentials are retained. Missing non-reclaimable counters remain missing.
type memoryCalibrationHistory struct {
	Name                  string              `json:"name"`
	Points                []*WorkObservation  `json:"points"`
	Accepted              []AttemptMemoryPeak `json:"accepted"`
	MissingNonreclaimable int                 `json:"missing_nonreclaimable"`
}

func calibrationHistory(t *testing.T) []memoryCalibrationHistory {
	t.Helper()
	b, err := os.ReadFile("testdata/envelope-calibration.json")
	if err != nil {
		t.Fatal(err)
	}
	var history []memoryCalibrationHistory
	if err := json.Unmarshal(b, &history); err != nil {
		t.Fatal(err)
	}
	return history
}

// Reconstruct the pre-#1044 host formula from retained inputs: all rows needed
// complete ranges, otherwise all used total cgroup peaks. It then added the
// spread of key maxima (or merged complete range) to the largest peak.
func legacyCalibrationHostBound(points []*WorkObservation) float64 {
	complete := true
	for _, p := range points {
		r := p.MemoryRange
		if p.Key.Component != "cpu" {
			r = p.GPUHostRange
			_, ok := p.MemoryPeak.gpuHostPeakGB()
			complete = complete && ok
		}
		complete = complete && r.valid() && r.Samples >= 2 && p.MemoryPeak.NonreclaimableGB != nil
	}
	peak, low, growth := 0.0, math.Inf(1), 0.0
	for _, p := range points {
		value := hostMemoryPeakGB(p.MemoryPeak)
		if complete {
			r := p.MemoryRange
			if p.Key.Component != "cpu" {
				r = p.GPUHostRange
			}
			value = max(r.MaxGB, *p.MemoryPeak.HostGB, *p.MemoryPeak.NonreclaimableGB)
			if combined, ok := p.MemoryPeak.gpuHostPeakGB(); ok {
				value = max(value, combined)
			}
			low = min(low, r.MinGB)
		}
		peak, low, growth = max(peak, value), min(low, value), max(growth, memoryGrowthGB(p.MemoryPeak))
	}
	return peak + max(peak-low, growth, minimumHostMemoryGB)
}

func TestMemoryCalibrationRetainedChargesAndCoverage(t *testing.T) {
	for _, h := range calibrationHistory(t) {
		t.Run(h.Name, func(t *testing.T) {
			q, clock := fixture(t)
			clock.Advance(40 * 24 * time.Hour)
			key := h.Points[0].Key
			m := &Machine{ID: "worker", HostID: key.Host, Device: key.Component, GPU: key.Target.Platform,
				UnifiedMemory: h.Points[0].MemoryLayout == "unified", HostMemoryMeasured: true}
			for i, p := range h.Points {
				if !q.acceptedMemoryPoint(p) {
					t.Fatal("fixture is not accepted evidence", i)
				}
				q.putWorkObservation(identityHash(p.Key), *p)
				m.Devices = append(m.Devices, MachineDevice{ID: p.Key.Component, Identity: p.Key.Hardware, ExtensionFingerprint: p.Key.Fingerprint})
			}
			host, _ := observedMemoryBoundForHost(h.Points, key, m)
			device, _ := observedMemoryBound(h.Points, true)
			missing := 0
			for i := range h.Accepted {
				p := &h.Accepted[i]
				if p.HostGB == nil || p.constrained() || p.PressureUnavailable {
					t.Fatal("fixture contains ineligible diagnostic evidence", i)
				}
				// Cover every measured resident/non-reclaimable peak, including
				// complete driver/outside demand. A total-only historical attempt
				// cannot establish its non-reclaimable use; report that gap.
				need := *p.HostGB
				if p.NonreclaimableGB != nil {
					need = max(need, *p.NonreclaimableGB)
				} else {
					missing++
				}
				if combined, ok := p.gpuHostPeakGB(); ok {
					need = max(combined, *p.HostGB, *p.NonreclaimableGB)
				}
				if host < need || p.DeviceGB != nil && device < *p.DeviceGB {
					t.Fatalf("accepted attempt %d undercharged: host %v < %v or device %v < %v", i, host, need, device, p.DeviceGB)
				}
				if m.UnifiedMemory && host < hostMemoryPeakGB(p) {
					t.Fatalf("incomplete unified attempt %d lost its total-cgroup fallback", i)
				}
			}
			if missing != h.MissingNonreclaimable {
				t.Fatal("historical coverage gap count changed", missing, h.MissingNonreclaimable)
			}
			t.Logf("%d accepted resident/device peaks covered (%d lack non-reclaimable peaks): host %.9f device %.9f GiB", len(h.Accepted), h.MissingNonreclaimable, host, device)
			if h.Name == "separation-xpu" {
				// Old max(merged peaks) + merged range spread charged 13.8 GiB.
				old := legacyCalibrationHostBound(h.Points)
				if old != 13.816635131835938 || math.Abs(host-11.489543914794922) > 1e-9 {
					t.Fatalf("retained calibration: old %v corrected %v", old, host)
				}
				m.HostMemoryBudgetGB, m.FreeHostMemoryGB = 24, 24
				if hostFits(m, 0, 2*old) || !hostFits(m, 0, 2*host) || hostFits(m, 0, 3*host) {
					t.Fatal("lost fitting pair or physical oversubscription refusal")
				}
			}
			if h.Name == "naming-cuda" {
				// Device startup growth remains conservative: retained peaks do
				// not establish a complete physical union or a smaller tail bound.
				if device < 22.318359375+22.201064852328635 || host < 20.391857147216797 {
					t.Fatal("unmeasured device discount")
				}
				if math.Abs(host+device+.125-65.55387202512249) > 1e-9 {
					t.Fatal("wrong current conservative unified charge", host+device+.125)
				}
				// Freeze the historical inputs behind the reported 83.376 charge;
				// the current snapshot contains a later, larger device-growth term.
				var historical []*WorkObservation
				for _, p := range h.Points {
					if p.MemoryRecordedAt.Before(time.Date(2026, 11, 11, 1, 0, 0, 0, time.UTC)) {
						historical = append(historical, p)
					}
				}
				historicalHost, _ := observedMemoryBound(historical, false)
				historicalDevice, _ := observedMemoryBound(historical, true)
				if math.Abs(legacyCalibrationHostBound(historical)+historicalDevice+.125-83.37641122684036) > 1e-9 {
					t.Fatal("lost retained 83.4 GiB reconstruction")
				}
				if math.Abs(historicalHost+historicalDevice+.125-65.37355641978967) > 1e-9 {
					t.Fatal("wrong historical corrected charge", historicalHost+historicalDevice+.125)
				}
			}
			// The actual queue reader must apply the same bound; a second global
			// range merge used to silently restore the inflated envelope.
			j := &Job{Spec: Spec{Needs: ResourceNeeds{Platform: Platform{Required: []string{key.Target.Platform}}}}}
			e := q.memoryEstimateForState(j, m, &key, "configuration", false)
			if math.Abs(e.measuredHostGB-host) > 1e-9 {
				t.Fatalf("queue restored cross-key inflation: %v != %v", e.measuredHostGB, host)
			}
		})
	}
}

func TestMemoryCalibrationIndependentFloors(t *testing.T) {
	point := func(resident, total, growth, low, high float64) *WorkObservation {
		p := &WorkObservation{Key: ObservationKey{Component: "cpu"}, MemoryPeak: &AttemptMemoryPeak{HostGB: &resident, CgroupGB: &total}}
		if growth >= 0 {
			p.MemoryPeak.GrowthGB = &growth
		}
		if high > 0 {
			p.MemoryPeak.NonreclaimableGB = &resident
			p.MemoryRange = &MemoryPeakRange{Samples: 2, MinGB: low, MaxGB: high}
		}
		return p
	}
	for _, tc := range []struct {
		name   string
		points []*WorkObservation
		want   float64
	}{
		{"older peak survives restarted range", []*WorkObservation{point(10, 20, .5, 2, 3)}, 10.125},
		{"each range forecasts its own upper tail", []*WorkObservation{point(10, 20, .1, 9.8, 10), point(8, 20, 1, 4, 8)}, 12},
		{"sampling growth protects next observation", []*WorkObservation{point(3, 20, 5, 2, 3)}, 8},
		{"incomplete neighbour retains total and growth", []*WorkObservation{point(3, 30, .5, 2, 3), point(5, 12, 1, 0, 0)}, 13},
		{"legacy unknown variation remains conservative", []*WorkObservation{point(2, 12, -1, 0, 0), point(3, 20, -1, 0, 0)}, 28},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peak, margin, _ := observedMemoryPeakAndMarginWithDrift(tc.points, false, .125)
			if math.Abs(peak+margin-tc.want) > 1e-9 {
				t.Fatalf("physical demand/variation bound %v, want %v", peak+margin, tc.want)
			}
		})
	}
}
