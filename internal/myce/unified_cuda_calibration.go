package myce

import "math"

// Calibrate only exact-local CUDA work with repeated complete host evidence.
// CUDA's absolute outside-root residual ALREADY includes uncharged device
// pages. It is a bound on the combined physical footprint, not another host
// allocation to add to NVML. No independent maxima are subtracted here.
//
// Keep every incomplete row's total-cgroup PLUS device peak as a physical
// floor. Complete rows forecast non-reclaimable host plus device, and retain
// their entire proc/physical upper bound independently. Historical cache is
// never retroactively discounted by a newer complete observation.
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
	incompleteDevice := 0.0
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
			bound := max(combined, resident+*p.DeviceGB) + hostMargin + deviceMargin
			if r := point.GPUHostRange; r.valid() {
				bound = max(bound, r.MaxGB+max(r.MaxGB-r.MinGB, memoryGrowthGB(p), drift)+deviceMargin)
			}
			physical = max(physical, bound)
		} else {
			// Missing counters retain the complete old total/device floor and
			// measured host growth. This can dominate otherwise small new runs.
			// Cleared coverage also wins over cached aggregate GPU-host fields.
			physical = max(physical, hostMemoryPeakGB(p)+hostMargin+*p.DeviceGB+deviceMargin)
			incomplete = append(incomplete, point)
			incompleteDevice = max(incompleteDevice, *p.DeviceGB)
		}
	}
	// Unknown legacy host variation keeps the existing fallback, too. New
	// complete counters cannot erase uncertainty in an unrelated old row.
	peak, margin, _ := observedMemoryPeakAndMarginWithDrift(incomplete, false, drift)
	if len(incomplete) > 0 {
		physical = max(physical, peak+margin+incompleteDevice+deviceMargin)
	}
	// Encode the combined bound in the existing host+device API. This is an
	// algebraic split of a reservation, not a claimed unique-page measurement.
	return max(drift, physical-device), device, physical, true
}
