package myce

import "time"

// ProgramNeeds retains the conservative historical total for old readers and
// unknown evidence. ProgramMemory separates irreversible measured demand from
// the replaceable forecast; a forecast is never an observed demand floor.
type ProgramMemory struct {
	Demand    SizeHint                   `json:"demand"`
	Forecasts map[string]ProgramForecast `json:"forecasts"`
}

type ProgramForecast struct {
	Margin     SizeHint  `json:"margin"`
	RecordedAt time.Time `json:"recorded_at"`
	Revision   int       `json:"revision"`
	Source     string    `json:"source"`
	Complete   bool      `json:"complete"`
}

type ProgramMemoryApplied struct {
	Base    SizeHint `json:"base"`
	Applied SizeHint `json:"applied"`
}

const programForecastRevision = 1

// Remove only our previous contribution. Authored/variant changes and retry
// increases are independent floors, never disposable uncertainty.
func resetProgramMemory(j *Job) {
	if old := j.ProgramMemoryApplied; old != nil && j.OOMRetries == 0 && j.MemoryEnvelope == nil && j.State != "running" && j.State != "stopping" {
		if j.Spec.Needs.MemoryGB == old.Applied.MemoryGB {
			j.Spec.Needs.MemoryGB = old.Base.MemoryGB
		}
		if j.Spec.Needs.HostMemoryGB == old.Applied.HostMemoryGB {
			j.Spec.Needs.HostMemoryGB = old.Base.HostMemoryGB
		}
		j.ProgramMemoryApplied = nil
	}
}

func programMemoryHardware(m *Machine) string {
	if m == nil || m.CapabilityProfile == nil {
		return ""
	}
	p := m.CapabilityProfile
	if p.OS == "" || p.Arch == "" || p.MemoryLayout == "" || p.SystemMemoryBytes == 0 {
		return ""
	}
	return identityHash(struct {
		OS, Arch, Layout, Fingerprint string
		RAM                           uint64
		Accelerators                  []AcceleratorProfile
	}{p.OS, p.Arch, p.MemoryLayout, m.ExtensionFingerprint, p.SystemMemoryBytes, p.Accelerators})
}

func maxSize(a, b SizeHint) SizeHint {
	return SizeHint{MemoryGB: max(a.MemoryGB, b.MemoryGB), HostMemoryGB: max(a.HostMemoryGB, b.HostMemoryGB)}
}

func (q *Queue) recordProgramMemory(sample eligibleMemorySample) {
	key, p, usage := sample.programKey, sample.result.MemoryPeak, *sample.result.Usage
	old := q.s.ProgramNeeds[key]
	if q.s.ProgramMemory == nil {
		q.s.ProgramMemory = map[string]ProgramMemory{}
	}
	evidence, known := q.s.ProgramMemory[key]
	if !known {
		// An opaque pre-upgrade total cannot safely be called uncertainty. Keep
		// it as a floor: missing provenance never licenses undercharging.
		evidence.Demand = old
	}
	evidence.Demand = maxSize(evidence.Demand, usage)
	margin := SizeHint{MemoryGB: deviceMemoryGrowthGB(p), HostMemoryGB: memoryGrowthGB(p)}
	if evidence.Forecasts == nil {
		evidence.Forecasts = map[string]ProgramForecast{}
	}
	m := allocationMachine(q.s.Machines[sample.result.Machine], sample.job.AllocatedDevices)
	hardware := programMemoryHardware(m)
	source := ""
	if p.Source == processGroupMemorySource {
		source = p.Source
	} else if p.Source == "" && p.Events != nil {
		source = "cgroup peak"
	}
	complete := hardware != "" && source != "" && p.GrowthGB != nil && validMemoryValue(*p.GrowthGB) &&
		(usage.MemoryGB == 0 || p.DeviceGrowthGB != nil && validMemoryValue(*p.DeviceGrowthGB))
	evidence.Forecasts[hardware] = ProgramForecast{Margin: margin, RecordedAt: q.clock.Now(), Revision: programForecastRevision, Source: source, Complete: complete}
	q.s.ProgramMemory[key] = evidence
	total := SizeHint{MemoryGB: evidence.Demand.MemoryGB + margin.MemoryGB, HostMemoryGB: evidence.Demand.HostMemoryGB + margin.HostMemoryGB}
	q.s.ProgramNeeds[key] = maxSize(old, maxSize(sample.programFloor, total))
}

func (q *Queue) learnedProgramMemory(j *Job, e Extension, platform string) (SizeHint, bool) {
	key := programNeedsKey(j, e, platform)
	fallback, exists := q.s.ProgramNeeds[key]
	evidence, known := q.s.ProgramMemory[key]
	if !known {
		return fallback, exists
	}
	need := maxSize(evidence.Demand, SizeHint{MemoryGB: e.MemoryGB, HostMemoryGB: e.HostMemoryGB})
	matched := false
	for _, owner := range q.s.Machines {
		targets := []*Machine{owner}
		if owner.Devices != nil {
			targets = []*Machine{cpuMachineView(owner)}
			for _, d := range owner.Devices {
				target := d.machine(*owner)
				targets = append(targets, &target)
			}
		}
		for _, m := range targets {
			if m.GPU != platform || !e.matchesTarget(m) {
				continue
			}
			matched = true
			f, ok := evidence.Forecasts[programMemoryHardware(m)]
			age := q.clock.Now().Sub(f.RecordedAt)
			// Matching accepted evidence survives idle time, like retained work
			// memory. Hardware drift, sensor downgrade, estimator invalidation
			// or a future timestamp spends no credit. These rows remain retained
			// independently of the work-observation collector and its reader pins.
			if !ok || !f.Complete || f.Revision != programForecastRevision || f.Source == "" || f.RecordedAt.IsZero() || age < 0 {
				return fallback, exists
			}
			need = maxSize(need, SizeHint{MemoryGB: evidence.Demand.MemoryGB + f.Margin.MemoryGB, HostMemoryGB: evidence.Demand.HostMemoryGB + f.Margin.HostMemoryGB})
		}
	}
	if !matched {
		return fallback, exists
	}
	return need, true
}
