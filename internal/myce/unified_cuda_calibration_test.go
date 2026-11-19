package myce

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// Numeric-only installed status, 2026-11-16 07:43Z: 142 comparable accepted
// samples in 15 aggregates. A later incomplete attempt cleared the physical
// range while retaining an earlier complete 51.3077 GiB whole-host-derived
// bound. This is not a recovered synchronized per-owner physical trace.
func TestUnifiedCUDACalibrationCoverageGapRetainedShape(t *testing.T) {
	b, err := os.ReadFile("testdata/unified-cuda-coverage-gap.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []*WorkObservation
	if err := json.Unmarshal(b, &rows); err != nil {
		t.Fatal(err)
	}
	q, clock, machine, spec := unifiedAutoFixture(t, 0)
	spec.Priority = "production"
	queued := submit(t, q, spec)
	q.mu.Lock() // Admission evidence publication shares these borrowed records.
	defer q.mu.Unlock()
	m := allocationMachine(q.s.Machines[machine.ID], []string{"0"})
	production := q.s.Jobs[queued.ID]
	candidates := q.memoryCandidates(production, m)
	if len(candidates) != 1 {
		t.Fatal("missing qualified candidate", len(candidates))
	}
	provenance := *candidates[0]
	provenance.Machine, provenance.Session = m.ID, m.Session
	provenance.AllocatedDevices = []string{"0"}
	key, why := q.workObservationKey(&provenance)
	if key == nil {
		t.Fatal("missing observation key", why)
	}
	configuration := memoryConfiguration(provenance.Spec.Needs, provenance.Spec.Args, provenance.Spec.Program)
	samples := 0
	for i, row := range rows {
		row.Key = *key
		row.Key.Allowance = string(rune('a' + i))
		row.MemoryLayout, row.MemoryConfiguration = "unified", configuration
		row.MemoryRecordedAt = clock.Now()
		samples += max(1, row.MemorySamples)
		q.putWorkObservation(identityHash(row.Key), *row)
	}
	if len(rows) != 15 || samples != 142 {
		t.Fatal("retained population changed", len(rows), samples)
	}
	before, _ := json.Marshal(rows)
	host, device, physical, ok := unifiedCUDACalibration(rows, .125)
	const want = 52.00727383471761 // 51.307701110839844 + .5745727238777686 + .125.
	if !ok || math.Abs(host+device+.125-want) > 1e-9 || device != 22.443359375 || physical < 51.307701110839844 {
		t.Fatalf("charge %.12f, physical %.12f, device %.12f, calibrated %t", host+device+.125, physical, device, ok)
	}
	after, _ := json.Marshal(rows)
	if string(before) != string(after) {
		t.Fatal("fallback projection mutated retained evidence")
	}
	m.HostMemoryBudgetGB, m.ReservedMemoryGB = 112.83781051635742, 4
	m.CapabilityProfile.SystemMemoryBytes = 129992077312
	m.FreeHostMemoryGB = 54.25691604614258
	estimate := q.memoryEstimateForState(production, m, key, configuration, false)
	if estimate.device == nil || math.Abs(hostNeed(m, *estimate.device.DeviceGB*estimate.deviceFactor, estimate.measuredHostGB)-want) > 1e-9 || estimate.samples != 142 {
		t.Fatalf("queue restored the duplicated physical bound: %+v", estimate)
	}
	candidate := q.memoryCandidate(production, m)
	if math.Abs(candidate.MemoryEnvelope.HostGB-want) > 1e-9 || !hostFits(m, device, host) || q.unifiedCUDAProductionReason(production, m) != "" {
		t.Fatal("incoming production charged a further production envelope", candidate.MemoryEnvelope)
	}
	experiment := Job{ID: "experiment", State: "queued", Spec: plainGPUSpec()}
	experiment.Spec.Priority = "experiment"
	experiment.Spec.Needs.MemoryGB, experiment.Spec.Needs.HostMemoryGB = device, host
	if reason := q.unifiedCUDAProductionReason(&experiment, m); !strings.Contains(reason, "unified CUDA production headroom") || !strings.Contains(reason, "52.007 GiB production envelope") {
		t.Fatal("experiment spent protected production headroom or duplicated its device vector", reason)
	}
	// The production guard must consume the calibrated host/device split too:
	// restoring the raw combined peak as host would inflate protection again.
	experiment.Spec.Needs.MemoryGB, experiment.Spec.Needs.HostMemoryGB = .5, .5
	if reason := q.unifiedCUDAProductionReason(&experiment, m); reason != "" {
		t.Fatal("small experiment plus measured production envelope fits", reason)
	}
	t.Logf("incoming production %.12f, remaining room %.12f, residual %.12f; experiment plus protected production %.12f exceeds room", want, m.FreeHostMemoryGB, m.FreeHostMemoryGB-want, 2*want)

	// Later accepted runs restored a range. The historical physical maximum
	// still binds; correcting detached collection cannot retroactively prove
	// that an older upper bound belonged to another owner.
	b, err = os.ReadFile("testdata/unified-cuda-retained-bound.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &rows); err != nil {
		t.Fatal(err)
	}
	samples = 0
	for _, row := range rows {
		samples += max(1, row.MemorySamples)
	}
	host, device, physical, ok = unifiedCUDACalibration(rows, .125)
	const retained = 51.557701110839844 // 51.307701110839844 + .125 drift + .125 launch slack.
	if len(rows) != 17 || samples != 180 || !ok || math.Abs(host+device+.125-retained) > 1e-9 || device != 22.443359375 || physical < 51.307701110839844 {
		t.Fatalf("retained physical protection changed: rows=%d samples=%d charge=%.12f physical=%.12f device=%.12f calibrated=%t", len(rows), samples, host+device+.125, physical, device, ok)
	}
}

// Installed per-job history, 2026-11-14 22:04-23:03Z. Only numeric memory
// evidence, eligibility and frozen charges survive sanitization. The older
// comparable points/216 accepted attempts are in envelope-calibration.json.
func unifiedNamingHistory(t *testing.T) []struct {
	Peak   *AttemptMemoryPeak `json:"peak"`
	Reason string             `json:"reason"`
	Charge float64            `json:"charge"`
} {
	t.Helper()
	var h []struct {
		Peak   *AttemptMemoryPeak `json:"peak"`
		Reason string             `json:"reason"`
		Charge float64            `json:"charge"`
	}
	b, err := os.ReadFile("testdata/unified-naming-history.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &h); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestUnifiedCUDACalibrationRetainedCharges(t *testing.T) {
	var old memoryCalibrationHistory
	for _, h := range calibrationHistory(t) {
		if h.Name == "naming-cuda" {
			old = h
		}
	}
	if len(old.Points) == 0 {
		t.Fatal("missing retained CUDA history")
	}
	points := append([]*WorkObservation(nil), old.Points...)
	history := unifiedNamingHistory(t)
	row := &WorkObservation{Key: points[0].Key, MemoryLayout: "unified"}
	row.Key.Allowance = "new-allowance"
	points = append(points, row)
	q, clock := fixture(t)
	clock.Advance(40 * 24 * time.Hour)
	for i, h := range history {
		// Reconstruct each *assigned* envelope before recording its completion.
		host, _ := observedMemoryBound(points, false)
		device, _ := observedMemoryBound(points, true)
		// The last job acquired its reservation before the previous result
		// committed. Active reservations are frozen, never recomputed at start.
		if i == len(history)-1 {
			if h.Charge != history[i-1].Charge {
				t.Fatal("frozen assignment changed")
			}
		} else if math.Abs(host+device+.125-h.Charge) > 1e-8 {
			t.Fatalf("assignment %d: legacy charge %.12f, retained %.12f", i, host+device+.125, h.Charge)
		}
		if h.Reason != "eligible" {
			continue
		}
		j := &Job{Spec: Spec{Needs: ResourceNeeds{}}}
		sample := eligibleMemorySample{gateBuilt: true, work: true, job: j, result: Result{MemoryPeak: h.Peak}}
		prior := *row
		q.recordWorkMemory(sample, prior, row)
		row.MemoryConfiguration, row.MemoryFamily, row.MemoryLayout = points[0].MemoryConfiguration, points[0].MemoryFamily, "unified"
	}
	host, device, physical, ok := unifiedCUDACalibration(points, .125)
	if !ok {
		t.Fatal("complete accepted CUDA evidence was not calibrated")
	}
	charge := host + device + .125
	// Complete physical evidence covers the legacy total and resident+device
	// floors, so the unknown overlap is no longer added to the physical bound.
	if math.Abs(charge-29.783607482910156) > 1e-8 {
		t.Fatalf("corrected charge %.12f (host %.12f device %.12f physical %.12f)", charge, host, device, physical)
	}
	for _, p := range append(old.Accepted, func() []AttemptMemoryPeak {
		var out []AttemptMemoryPeak
		for _, h := range history {
			if h.Reason == "eligible" {
				out = append(out, *h.Peak)
			}
		}
		return out
	}()...) {
		need := max(*p.HostGB, valueOrZero(p.NonreclaimableGB)) + valueOrZero(p.DeviceGB)
		if p.GPUHost == nil {
			need = max(need, hostMemoryPeakGB(&p))
		}
		if combined, ok := p.gpuHostPeakGB(); ok {
			need = max(need, combined)
		}
		if charge < need {
			t.Fatalf("accepted physical peak %.12f exceeds charge %.12f", need, charge)
		}
	}
	m := &Machine{HostMemoryBudgetGB: 96, FreeHostMemoryGB: 96, HostMemoryMeasured: true, MemoryCeilingSlackGB: .125}
	if !hostFits(m, 0, 2*charge) || hostFits(m, 0, 4*charge) {
		t.Fatal("pair fit/oversubscription")
	}
	key := points[0].Key
	m.ID, m.HostID, m.Device, m.GPU, m.UnifiedMemory = "worker", key.Host, key.Component, "cuda", true
	for _, p := range points {
		q.putWorkObservation(identityHash(p.Key), *p)
	}
	j := &Job{Spec: Spec{Needs: ResourceNeeds{Platform: Platform{Required: []string{"cuda"}}}}}
	estimate := q.memoryEstimateForState(j, m, &key, points[0].MemoryConfiguration, false)
	if math.Abs(estimate.measuredHostGB+*estimate.device.DeviceGB*estimate.deviceFactor+.125-charge) > 1e-8 {
		t.Fatalf("queue restored the duplicated bound: %+v", estimate)
	}
	// A foreign or incomplete collector cannot activate calibrated sizing.
	for _, p := range points {
		if p.MemoryPeak != nil {
			p.MemoryPeak.GPUHost = nil
		}
	}
	if _, _, _, ok := unifiedCUDACalibration(points, .125); ok {
		t.Fatal("unknown attribution calibrated")
	}
}

func valueOrZero(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

// Numeric-only accepted attempt facts from the ten retained 2026-11-15 runs.
// Rejected/interrupted attempts are retained separately, never sizing inputs.
func unifiedPhysicalHistory(t *testing.T) []struct {
	Peak   *AttemptMemoryPeak `json:"peak"`
	Reason string             `json:"reason"`
} {
	t.Helper()
	var h struct {
		Accepted []struct {
			Peak   *AttemptMemoryPeak `json:"peak"`
			Reason string             `json:"reason"`
		} `json:"accepted"`
	}
	b, err := os.ReadFile("testdata/unified-physical-history.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &h); err != nil {
		t.Fatal(err)
	}
	return h.Accepted
}

func TestUnifiedCUDAPhysicalBoundRetainedCoverage(t *testing.T) {
	retainedUnifiedPhysicalCharge(t)
}

func retainedUnifiedPhysicalCharge(t *testing.T) float64 {
	t.Helper()
	q, _ := fixture(t)
	var points []*WorkObservation
	for _, h := range calibrationHistory(t) {
		if h.Name == "naming-cuda" {
			points = append(points, h.Points...)
		}
	}
	// Old total-only history still uses today's conservative estimator.
	if _, _, _, ok := unifiedCUDACalibration(points, .125); ok {
		t.Fatal("incomplete-only history calibrated")
	}
	host, _ := observedMemoryBound(points, false)
	device, _ := observedMemoryBound(points, true)
	if math.Abs(host+device+.125-65.55387202512249) > 1e-8 {
		t.Fatal("incomplete fallback changed", host, device)
	}
	row := &WorkObservation{Key: points[0].Key}
	points = append(points, row)
	var peaks []*AttemptMemoryPeak
	// Carry the prior accepted complete physical peaks as well as the new ten.
	for _, h := range unifiedNamingHistory(t) {
		if h.Reason != "eligible" {
			continue
		}
		q.recordWorkMemory(eligibleMemorySample{gateBuilt: true, work: true, job: &Job{}, result: Result{MemoryPeak: h.Peak}}, *row, row)
		peaks = append(peaks, h.Peak)
	}
	missing := 0
	for _, h := range unifiedPhysicalHistory(t) {
		if h.Reason != "eligible" || h.Peak.constrained() || h.Peak.PressureUnavailable {
			t.Fatal("ineligible retained row")
		}
		if h.Peak.NonreclaimableGB == nil {
			missing++
		}
		q.recordWorkMemory(eligibleMemorySample{gateBuilt: true, work: true, job: &Job{}, result: Result{MemoryPeak: h.Peak}}, *row, row)
		peaks = append(peaks, h.Peak)
	}
	if missing != 3 {
		t.Fatal("retained coverage changed", missing)
	}
	h, d, physical, ok := unifiedCUDACalibration(points, .125)
	if !ok || math.Abs(h+d+.125-29.783607482910156) > 1e-8 {
		t.Fatalf("charge %.12f physical %.12f ok %t range %+v", h+d+.125, physical, ok, row.GPUHostRange)
	}
	for _, p := range peaks {
		need := max(valueOrZero(p.CgroupGB), max(*p.HostGB, valueOrZero(p.NonreclaimableGB))+*p.DeviceGB)
		if combined, valid := p.gpuHostPeakGB(); valid {
			need = max(need, combined)
		}
		if physical < need {
			t.Fatal("accepted peak not covered", need, physical)
		}
	}
	t.Logf("new charge %.12f = physical %.12f + slack .125; %d accepted peaks covered", h+d+.125, physical, len(peaks))
	return h + d + .125
}

func TestUnifiedCUDACalibrationPeakVariation(t *testing.T) {
	host, device, outside := 2.0, 22.0, 25.0
	p := &WorkObservation{Key: ObservationKey{Component: "0"}, MemorySamples: 2,
		MemoryPeak: &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, DeviceGB: &device,
			GPUHost: &GPUHostMemory{Source: cudaHostMemorySource, SiblingAdjusted: true, DriverGB: &outside, OutsideGB: &outside, HostDropGB: &outside}},
		MemoryRange:  &MemoryPeakRange{Samples: 2, MinGB: 1.5, MaxGB: 2},
		GPUHostRange: &MemoryPeakRange{Samples: 2, MinGB: 26, MaxGB: 27},
		DeviceRange:  &MemoryPeakRange{Samples: 2, MinGB: 20, MaxGB: 22}}
	h, d, _, ok := unifiedCUDACalibration([]*WorkObservation{p}, .125)
	if !ok || d != 24 || h+d != 29 {
		t.Fatalf("lost device variation or combined floor: %v %v %v", h, d, ok)
	}
	// An incomplete completion clears the coverage range, but its aggregate
	// can still retain older GPU-host counters. Those counters cannot discount
	// this row's larger, undecomposed total-cgroup peak.
	gap, gapPeak := *p, *p.MemoryPeak
	total := 80.0
	gapPeak.CgroupGB = &total
	gap.MemoryPeak, gap.MemoryRange, gap.GPUHostRange = &gapPeak, nil, nil
	h, d, _, ok = unifiedCUDACalibration([]*WorkObservation{p, &gap}, .125)
	if !ok || h+d < total+device {
		t.Fatalf("cached counters discounted incomplete coverage: %v %v %v", h, d, ok)
	}
	// An accepted larger device peak raises the reservation without waiting
	// for another complete host range or silently discounting a legacy row.
	spike := *p
	peak := *p.MemoryPeak
	large := 50.0
	peak.DeviceGB, peak.GPUHost, peak.NonreclaimableGB = &large, nil, nil
	spike.MemoryPeak, spike.GPUHostRange, spike.MemoryRange, spike.DeviceRange = &peak, nil, nil, nil
	h, d, _, ok = unifiedCUDACalibration([]*WorkObservation{p, &spike}, .125)
	if !ok || d < large || h+d < large+host {
		t.Fatal("lost larger accepted peak")
	}
	p.MemoryPeak.DeviceSource = deviceUnifiedResidentSource
	if _, _, _, ok := unifiedCUDACalibration([]*WorkObservation{p}, .125); ok {
		t.Fatal("fallback attribution reduced the device bound")
	}
	p.MemoryPeak.DeviceSource = ""
	p.GPUHostRange.Samples = 1
	if _, _, _, ok := unifiedCUDACalibration([]*WorkObservation{p}, .125); ok {
		t.Fatal("one attempt established a range")
	}
}

// Replay the native facts through the real eligibility writer, SQLite restart,
// scheduler and check-in guard, with synthetic capacity and no GPU required.
func TestUnifiedCUDACalibrationAdmission(t *testing.T) {
	q, clock, m, _ := unifiedAutoFixture(t, 0)
	spec := plainGPUSpec() // Fixed CPU needs keep CPU sizing out of this memory proof.
	spec.Needs.Memory = "auto"
	m.HostMemoryBudgetGB = 96
	m.Devices[0].BudgetGB = 96
	budget := 96.0
	for _, request := range []HostMaintenanceRequest{{Action: "drain"}, {Action: "budget", MemoryBudgetGB: &budget}, {Action: "resume"}} {
		if _, err := q.ChangeHostMaintenance(m.HostID, request); err != nil {
			t.Fatal(err)
		}
	}
	for i, h := range unifiedNamingHistory(t) {
		queued := submit(t, q, spec)
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.ID != queued.ID {
			t.Fatal("history replay not assigned", job(t, q, queued.ID).Reason)
		}
		if i == 0 {
			key, _ := q.workObservationKey(run)
			for _, history := range calibrationHistory(t) {
				if history.Name != "naming-cuda" {
					continue
				}
				for _, old := range history.Points {
					point := *old
					point.Key = *key
					point.Key.Allowance = old.Key.Allowance
					point.MemoryConfiguration = memoryConfiguration(run.Spec.Needs, run.Spec.Args, run.Spec.Program)
					point.MemoryRecordedAt = clock.Now()
					q.putWorkObservation(identityHash(point.Key), point)
				}
			}
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r := result(run, true)
		r.MemoryPeak = h.Peak
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		facts := job(t, q, run.ID).MemoryAttempts
		if len(facts) != 1 || facts[0].LearningReason != h.Reason {
			t.Fatal("native eligibility changed", facts)
		}
	}
	q = memoryFactsRestart(t, q, clock)
	machineCheck(t, q, m) // Re-establish current inventory before submitting work.
	retainedRange := false
	for _, p := range q.s.WorkObservations {
		if r := p.DeviceRange; r.valid() && r.Samples == 5 && r.MinGB == 22.318359375 && r.MaxGB == 22.318359375 {
			retainedRange = true
		}
	}
	if !retainedRange {
		t.Fatal("restart lost accepted peak range or included constrained attempt")
	}
	first := submit(t, q, spec)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID || math.Abs(a.MemoryEnvelope.HostGB-29.783607482910156) > 1e-8 {
		if a == nil {
			t.Fatal("restart did not assign", job(t, q, first.ID).Reason)
		}
		t.Fatalf("restart envelope: %+v", a.MemoryEnvelope)
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	second := submit(t, q, spec)
	if assignedJob(growthCheck(t, q, m, []*Job{a}, nil, 0)) != nil {
		t.Fatal("missing device evidence permitted sharing")
	}
	clock.Advance(time.Second)
	b := assignedJob(growthCheck(t, q, m, []*Job{a}, []float64{22.318359375}, 0))
	if b == nil || b.ID != second.ID {
		t.Fatal("fitting measured pair refused", job(t, q, second.ID).Reason)
	}
	if err := q.Started(result(b, false)); err != nil {
		t.Fatal(err)
	}
	growthCheck(t, q, m, []*Job{a, b}, []float64{22.318359375, 22.318359375}, 0)
	requireState(t, q, a.ID, "running")
	requireState(t, q, b.ID, "running")
	clock.Advance(deviceMemorySampleFreshFor)
	growthCheck(t, q, m, []*Job{a, b}, []float64{22.318359375}, 0)
	requireState(t, q, b.ID, "stopping")
}
