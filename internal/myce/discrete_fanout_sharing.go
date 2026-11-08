package myce

import "time"

func fanOutDeviceSharing(j *Job) bool {
	return j.Batch != nil && j.Batch.Parent != "" && j.Batch.ShareXPU
}

func cardTelemetry(m *Machine) *DeviceTelemetry {
	for _, d := range m.Devices {
		if d.ID == m.Device {
			return d.Telemetry
		}
	}
	return nil
}

func fanOutDeviceSampleReady(s hostMemorySample, now time.Time) bool {
	// Both queue and worker maps accept samples only from Started reports for
	// this exact execution. The worker's immutable assignment predates Started
	// and cannot supply the queue's subsequently updated StartedAt field.
	return sharedDeviceSampleReady(s, now) && s.growthObserved && !s.at.IsZero() &&
		!now.Before(s.at) && now.Sub(s.at) < hostMemorySampleFreshFor
}

func sampledDeviceCharge(j *Job, m *Machine, s hostMemorySample, now time.Time) float64 {
	charge := deviceShareGB(j)
	if sharedDeviceSampleReady(s, now) {
		if unifiedCUDA(m) {
			charge = max(charge, s.deviceGB+s.deviceGrowthGB)
		} else if fanOutDeviceSharing(j) && m.GPU == "xpu" && !m.UnifiedMemory {
			charge = max(charge, s.deviceGB+s.deviceGrowthGB+s.deviceRateGB*now.Sub(s.deviceAt).Seconds())
		}
	}
	return charge
}

// Fan-out sharing is an extension of existing single-card admission, not an
// extra slot or host allowance. Only discrete XPU is qualified by this path.
// Saturation's physical boundary is 100%; a partial activity window is an
// opportunity to measure packing, not a prediction of its throughput benefit.
func (q *Queue) fanOutDeviceHeadroom(m *Machine, demand *Job, holders []*Job) bool {
	now, telemetry := q.clock.Now(), cardTelemetry(m)
	if m.GPU != "xpu" || m.UnifiedMemory || !deviceTelemetryReady(telemetry, now) || *telemetry.ActivePercent >= 100 ||
		!m.HostMemoryMeasured || m.HostMemoryError != "" || demand.Spec.Needs.HostMemoryGB <= 0 || !hostFits(m, demand.Spec.Needs.MemoryGB, demand.Spec.Needs.HostMemoryGB) {
		return false
	}
	growth, used := demand.Spec.Needs.MemoryGB, demand.Spec.Needs.MemoryGB
	for _, j := range holders {
		s := q.hostMemorySamples[j.Machine][jobKey(*j)]
		if j.StartedAt == nil || !fanOutDeviceSampleReady(s, now) {
			return false
		}
		used += sampledDeviceCharge(j, m, s, now)
		growth += max(0, deviceShareGB(j)-s.deviceGB) + s.deviceGrowthGB + s.deviceRateGB*now.Sub(s.deviceAt).Seconds()
	}
	return used <= m.TotalMemoryGB && *telemetry.FreeGB >= growth
}
