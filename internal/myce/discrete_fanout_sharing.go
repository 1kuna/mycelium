package myce

import "time"

func fanOutDeviceAttempt(j *Job) bool {
	return j.Batch != nil && j.Batch.Parent != ""
}

func cardTelemetry(m *Machine) *DeviceTelemetry {
	for _, d := range m.Devices {
		if d.ID == m.Device {
			return d.Telemetry
		}
	}
	return nil
}

func fanOutDeviceSampleReady(j *Job, s hostMemorySample, now time.Time) bool {
	return j.StartedAt != nil && sharedDeviceSampleReady(s, now) && s.growthObserved && !s.at.IsZero() &&
		!now.Before(s.at) && now.Sub(s.at) < hostMemorySampleFreshFor
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
	growth := demand.Spec.Needs.MemoryGB
	for _, j := range holders {
		s := q.hostMemorySamples[j.Machine][jobKey(*j)]
		if !fanOutDeviceSampleReady(j, s, now) {
			return false
		}
		growth += max(0, deviceShareGB(j)-s.deviceGB) + s.deviceGrowthGB + s.deviceRateGB*now.Sub(s.deviceAt).Seconds()
	}
	return *telemetry.FreeGB >= growth
}
