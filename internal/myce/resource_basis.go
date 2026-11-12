package myce

import (
	"cmp"
	"slices"
	"strings"
)

// ResourceBasis is one admission dimension on job GET (#585 step 2): the
// submitted figure, what learned sizing would reserve with its evidence, and
// the figure admission charged. Nil means none, never zero.
type ResourceBasis struct {
	Dimension string   `json:"dimension"`
	Declared  *float64 `json:"declared,omitempty"`
	Learned   *float64 `json:"learned,omitempty"`
	Evidence  int      `json:"evidence,omitempty"`
	Used      *float64 `json:"used,omitempty"`
	// declared, learned, hint, exploratory, default, manifest or unknown.
	Basis string `json:"basis"`
	Note  string `json:"note,omitempty"`
}

// HostAdmissions counts memory admissions per host by basis: Current over
// running attempts, Recent over every retained assigned job.
type HostAdmissions struct {
	Host    string         `json:"host"`
	Current map[string]int `json:"current,omitempty"`
	Recent  map[string]int `json:"recent"`
	// Recent explicit figures with a learned comparison beside them.
	DeclaredWithEvidence int `json:"declared_with_evidence,omitempty"`
}

// Record the learned comparison once per assignment, from in-memory
// observations only, so explicit figures show what evidence would reserve.
func (q *Queue) recordLearnedMemory(j *Job, m *Machine) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	if j.MemoryEnvelope == nil {
		return
	}
	estimate := q.memoryEstimate(j, m)
	if estimate.host == nil || estimate.samples == 0 {
		return
	}
	host := hostMemoryPeakGB(estimate.host) * estimate.hostFactor
	if estimate.measuredHostGB > 0 {
		host = estimate.measuredHostGB
	}
	learned := &LearnedMemory{HostGB: max(minimumHostMemoryGB, host), Evidence: estimate.samples, Source: estimate.source}
	if p := estimate.device; !j.Spec.Needs.cpuOnly() && p != nil && p.DeviceGB != nil && validRate(*p.DeviceGB*estimate.deviceFactor) {
		gb := *p.DeviceGB * estimate.deviceFactor
		learned.DeviceGB = &gb
		if estimate.exact && !estimate.uncalibrated && !estimate.deviceUncalibrated && estimate.source != spentMemoryExploration && validRate(*p.DeviceGB) {
			learned.PeakHostGB, learned.PeakDeviceGB = hostMemoryPeakGB(estimate.host), *p.DeviceGB
		}
	}
	envelope := *j.MemoryEnvelope
	envelope.Learned = learned
	j.MemoryEnvelope = &envelope
}

func jobResources(j *Job) []ResourceBasis {
	n, e := j.Spec.Needs, j.MemoryEnvelope
	value := func(v float64) *float64 { return &v }
	host := ResourceBasis{Dimension: "host_memory_gb", Basis: "unknown"}
	device := ResourceBasis{Dimension: "device_memory_gb", Basis: "unknown"}
	if n.Memory != "auto" {
		if n.HostMemoryGB > 0 {
			host.Declared = value(n.HostMemoryGB)
		} else if n.cpuOnly() && n.MemoryGB > 0 {
			host.Declared = value(n.MemoryGB)
		}
		if !n.cpuOnly() && n.MemoryGB > 0 {
			device.Declared = value(n.MemoryGB)
		}
	}
	if e != nil {
		basis := cmp.Or(e.Basis, "unknown")
		host.Used, host.Basis = value(e.HostGB), basis
		host.Note = "used is the charged figure, including ceiling slack and on unified hosts the device share"
		device.Used, device.Basis = value(e.DeviceGB), basis
		if l := e.Learned; l != nil {
			host.Learned, host.Evidence = value(l.HostGB), l.Evidence
			if l.DeviceGB != nil {
				device.Learned, device.Evidence = value(*l.DeviceGB), l.Evidence
			}
		}
	}
	cpu := ResourceBasis{Dimension: "cpu_cores", Used: value(float64(n.cores())), Basis: "default", Note: "CPU demand is not learned yet"}
	if n.CPUCores > 0 {
		cpu.Declared, cpu.Basis = value(float64(n.CPUCores)), "declared"
	}
	if o := j.CPUObservation; o != nil && o.PeakCores > 0 {
		cpu.Learned = value(o.PeakCores)
		cpu.Note = "observed logical-job peak CPU need; shared-host admission rounds up and never charges below declaration"
		cpu.Evidence = 1
	}
	storage := ResourceBasis{Dimension: "storage_bytes", Used: value(float64(projectedDiskNeed(j))), Basis: "manifest", Note: "scratch admission uses this work's accepted peaks, else its family's peaks plus spread, else the declared hint"}
	if d := j.Spec.Disk; d != nil {
		storage.Declared = value(float64(addDiskBytes(addDiskBytes(d.InputBytes, d.OutputBytes), d.CheckpointBytes)))
		storage.Basis = "declared"
	}
	if s := j.StorageEnvelope; s != nil && s.PredictedNeed != nil {
		storage.Learned, storage.Evidence = value(float64(*s.PredictedNeed)), int(min(s.Samples, 1<<31-1))
	}
	if s := j.StorageEnvelope; s != nil && s.Basis != "" {
		storage.Basis = s.Basis // exact, family, declared or manifest
	}
	out := []ResourceBasis{host}
	if !n.cpuOnly() {
		out = append(out, device)
	}
	return append(out, cpu, storage)
}

func (q *Queue) hostAdmissions() []HostAdmissions {
	byHost := map[string]*HostAdmissions{}
	for _, j := range q.s.Jobs {
		e := j.MemoryEnvelope
		if e == nil || j.AssignedAt == nil {
			continue
		}
		host := e.Machine
		if m := q.s.Machines[e.Machine]; m != nil {
			host = hostID(m)
		}
		h := byHost[host]
		if h == nil {
			h = &HostAdmissions{Host: host, Recent: map[string]int{}}
			byHost[host] = h
		}
		basis := cmp.Or(e.Basis, "unknown")
		h.Recent[basis]++
		if j.State == "running" || j.State == "stopping" {
			if h.Current == nil {
				h.Current = map[string]int{}
			}
			h.Current[basis]++
		}
		if basis == "declared" && e.Learned != nil {
			h.DeclaredWithEvidence++
		}
	}
	out := make([]HostAdmissions, 0, len(byHost))
	for _, h := range byHost {
		out = append(out, *h)
	}
	slices.SortFunc(out, func(a, b HostAdmissions) int { return strings.Compare(a.Host, b.Host) })
	return out
}
