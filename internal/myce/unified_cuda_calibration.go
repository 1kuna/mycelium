package myce

import "math"

// Calibrate only exact-local CUDA work with repeated complete host evidence.
// CUDA's absolute outside-root residual ALREADY includes uncharged device
// pages. It is a bound on the combined physical footprint, not another host
// allocation to add to NVML. No independent maxima are subtracted here.
//
// A measured range needs two complete accepted attempts, not a tuned sample
// quota. It supersedes incomplete total+device forecasts only when its physical
// forecast covers every retained total-cgroup floor AND resident+device forecast.
// Otherwise the incomplete rows retain their conservative additive fallback.
func unifiedCUDACalibration(points []*WorkObservation, drift float64) (host, device, physical float64, ok bool) {
	complete := uint64(0)
	devicePeak, deviceLow := 0.0, math.Inf(1)
	for _, point := range points {
		p := point.MemoryPeak
		if p == nil || p.constrained() || p.PressureUnavailable || p.DeviceGB == nil || !validRate(*p.DeviceGB) {
			continue
		}
		if p.DeviceSource != "" {
			return 0, 0, 0, false // Resident/host-delta estimates cannot discount driver uncertainty.
		}
		devicePeak, deviceLow = max(devicePeak, *p.DeviceGB), min(deviceLow, *p.DeviceGB)
		if r := point.DeviceRange; r.valid() {
			devicePeak, deviceLow = max(devicePeak, r.MaxGB), min(deviceLow, r.MinGB)
		}
		if _, valid := p.gpuHostPeakGB(); valid && p.GPUHost.Source == cudaHostMemorySource && p.GPUHost.SiblingAdjusted && point.GPUHostRange.valid() {
			complete += min(point.GPUHostRange.Samples, 2)
		}
	}
	if complete < 2 || devicePeak <= 0 {
		return 0, 0, 0, false
	}
	// Whole-attempt peak variation describes the next whole attempt. A model
	// loading from zero to its already reserved peak is not another model's
	// worth of unobserved demand. DeviceGrowthGB remains diagnostic; current
	// increments/rates still protect live admission and the growth-stop guard.
	deviceMargin := max(devicePeak-deviceLow, drift)
	device = devicePeak + deviceMargin
	var incomplete []*WorkObservation
	incompleteDevice, incompleteFloor, legacyPhysical := 0.0, 0.0, 0.0
	for _, point := range points {
		p := point.MemoryPeak
		if p == nil || p.HostGB == nil || p.DeviceGB == nil || p.constrained() || p.PressureUnavailable {
			continue
		}
		resident := *p.HostGB
		if p.NonreclaimableGB != nil {
			resident = max(resident, *p.NonreclaimableGB)
		}
		hostMargin := max(memoryGrowthGB(p), drift)
		combined, valid := p.gpuHostPeakGB()
		if valid && p.GPUHost.Source == cudaHostMemorySource && p.GPUHost.SiblingAdjusted && point.GPUHostRange.valid() {
			if r := point.MemoryRange; r.valid() {
				hostMargin = max(hostMargin, r.MaxGB-r.MinGB)
			}
			// The cumulative physical peak may predate a restarted range.
			// Keep it independently; do not add a newer range's spread again.
			bound := max(combined+drift, resident+device+hostMargin)
			if r := point.GPUHostRange; r.valid() {
				bound = max(bound, r.MaxGB+max(r.MaxGB-r.MinGB, memoryGrowthGB(p), deviceMargin, drift))
			}
			physical = max(physical, bound)
		} else {
			// Missing counters retain the complete old total/device floor and
			// measured host growth. This can dominate otherwise small new runs.
			// Cleared coverage also wins over cached aggregate GPU-host fields.
			legacyPhysical = max(legacyPhysical, hostMemoryPeakGB(p)+hostMargin+*p.DeviceGB+deviceMargin)
			incompleteFloor = max(incompleteFloor, hostMemoryPeakGB(p)+hostMargin, resident+device+hostMargin)
			incomplete = append(incomplete, point)
			incompleteDevice = max(incompleteDevice, *p.DeviceGB)
		}
	}
	// Total cgroup includes reclaimable cache and may include managed/pinned
	// GPU pages. It is not an extra host-only allocation once a complete physical
	// forecast covers it. Never claim its unknown cache/device overlap as measured.
	// Uncovered evidence (including a new larger peak) keeps the old forecast.
	peak, margin, _ := observedMemoryPeakAndMarginWithDrift(incomplete, false, drift)
	if len(incomplete) > 0 {
		incompleteFloor = max(incompleteFloor, peak+margin)
		legacyPhysical = max(legacyPhysical, peak+margin+incompleteDevice+deviceMargin)
		if physical < incompleteFloor {
			physical = max(physical, legacyPhysical)
		}
	}
	// Encode the combined bound in the existing host+device API. This is an
	// algebraic split of a reservation, not a claimed unique-page measurement.
	return max(drift, physical-device), device, physical, true
}
