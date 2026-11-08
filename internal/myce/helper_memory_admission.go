package myce

import "time"

// Direct CUDA process counters describe the whole attempt subtree, including
// pre-main sidecars. The learned device vector is sampled from that same tree,
// not just main. A fresh single-process reading therefore occupies part of the
// attempt's existing device envelope on a unified physical pool.
func helperDeviceResident(m *Machine, s hostMemorySample, now time.Time) float64 {
	if !unifiedCUDA(m) || !m.HostMemoryMeasured || !sharedDeviceSampleReady(s, now) || s.source != "" || !validMemoryValue(s.residentGB) || s.residentGB > s.deviceGB {
		return 0
	}
	return s.residentGB
}

// Preserve the complete reservation, including measured helper overshoot.
// Summed device counters remain an upper bound for charging, never a credit.
func helperHostCharge(j *Job, m *Machine, s hostMemorySample, now time.Time, charge float64) float64 {
	if !unifiedCUDA(m) || !m.HostMemoryMeasured || !sharedDeviceSampleReady(s, now) || s.source != "" {
		return charge
	}
	device := max(j.Spec.Needs.MemoryGB, s.deviceGB+s.deviceGrowthGB+s.deviceRateGB*now.Sub(s.deviceAt).Seconds())
	return charge + max(0, device-j.Spec.Needs.MemoryGB)
}
