package myce

import (
	"fmt"
	"math"
	"time"
)

// Accepted lifetime CPU/elapsed is sustained demand, not instantaneous runnable
// parallelism. Maxima never decay into a smaller reservation. Incomplete or
// contended counters can raise only the separately labelled lower bound.
// Whole-core rounding is the existing ledger's physical accounting unit, not
// fleet headroom. No sampler, capacity multiplier or CPU enforcement is added.
type CPUDemandEvidence struct {
	Samples           int     `json:"samples"`
	LowerBoundSamples int     `json:"lower_bound_samples"`
	PeakCores         float64 `json:"peak_cores"`
	LowerBoundCores   float64 `json:"lower_bound_cores"`
	CoverageSeconds   float64 `json:"coverage_seconds"`
	UsageSeconds      float64 `json:"usage_seconds"`
	Source            string  `json:"source"`
	Reason            string  `json:"reason,omitempty"`
}

// Frozen per admission; future observations never shrink a live allocation or
// its exact cleanup receipt. The submitted CPU declaration remains untouched.
type JobCPUDemand struct {
	SharedHost  bool               `json:"shared_host,omitempty"`
	Key         *ObservationKey    `json:"key,omitempty"`
	Learned     *CPUDemandEvidence `json:"learned,omitempty"`
	ChargeCores int                `json:"charge_cores"`
	Basis       string             `json:"basis"`
	Reason      string             `json:"reason,omitempty"`
	RecordedAt  time.Time          `json:"recorded_at,omitempty"`
}

func (q *Queue) cpuDemandKey(j *Job, m *Machine) (*ObservationKey, string) {
	if !j.Spec.Needs.cpuOnly() || !plainCommandWork(j.Spec) || j.Batch != nil {
		return nil, "comparable ordinary CPU command unavailable"
	}
	copy := *j
	copy.Machine, copy.Session = m.ID, m.Session
	copy.AllocatedDevices = nil
	key, reason := q.repeatedObservationKey(&copy)
	if key != nil {
		key.CPU = true
	}
	return key, reason
}

func (q *Queue) cpuDemandFor(j *Job, m *Machine) *JobCPUDemand {
	if !j.Spec.Needs.cpuOnly() || !plainCommandWork(j.Spec) {
		return nil
	}
	e := &JobCPUDemand{ChargeCores: j.Spec.Needs.cores(), Basis: "default", SharedHost: m.SharedHost != nil}
	if j.Spec.Needs.CPUCores > 0 {
		e.Basis = "declared"
	}
	e.Key, e.Reason = q.cpuDemandKey(j, m)
	if e.Key == nil || e.Reason != "" {
		return e
	}
	old, ok := q.s.WorkObservations[identityHash(e.Key)]
	if !ok || old.CPU == nil {
		e.Reason = "no accepted comparable CPU evidence"
		return e
	}
	if old.Key != *e.Key || old.Forbidden || old.StaleReason != "" || q.clock.Now().Before(old.RecordedAt) || q.clock.Now().Sub(old.RecordedAt) > workCurveMaxAge {
		e.Reason = "comparable CPU evidence stale or invalidated"
		return e
	}
	evidence := *old.CPU
	e.Learned, e.RecordedAt = &evidence, old.RecordedAt
	need := max(evidence.PeakCores, evidence.LowerBoundCores)
	if validMemoryValue(need) && need > float64(e.ChargeCores) {
		// A learned rate cannot exceed the physical count of its exact keyed host.
		// In particular, never clamp to the smaller operator budget: it must wait.
		if m.CapabilityProfile == nil || need > float64(m.CapabilityProfile.LogicalCPUs) {
			e.Learned = nil
			e.Reason = "CPU evidence exceeds physical capacity"
			return e
		}
		e.ChargeCores = int(math.Ceil(need))
		e.Basis = "learned"
	}
	e.Reason = "accepted sustained CPU/elapsed; bursts and future demand uncertain; CPU time not enforced by this estimate"
	if evidence.Samples == 0 {
		e.Reason = "accepted CPU lower bound only; complete uncontended demand unknown"
	}
	return e
}

func (q *Queue) recordCPUDemand(j *Job, r Result) {
	// Acceptance and exact cleanup precede reusable evidence. Stale/duplicate
	// results never reach this dispatch; interrupted/resumed work cannot train.
	e := j.CPUDemand
	if e == nil || e.Key == nil || !r.Success || r.ReleasePending || r.ReleaseOnly || r.Interrupted || r.OutOfMemory || r.Forced || j.State != "running" || j.StartedAt == nil ||
		r.ID != j.ID || r.Attempt != j.Attempt || r.Machine != j.Machine || r.Session != j.Session {
		return
	}
	owner := q.s.Machines[j.Machine]
	if owner == nil {
		return
	}
	m := cpuMachineView(owner)
	key, reason := q.cpuDemandKey(j, m)
	if key == nil || reason != "" || *key != *e.Key {
		return
	}
	a := safeAttemptResources(r.Resources)
	if a == nil || a.SampledAt.IsZero() || a.CPU.UsageSeconds == nil || !validRate(a.ElapsedSeconds) || a.CPU.Unavailable != "" {
		return
	}
	// A final result should carry the sampler's final read. More than its own
	// existing sampling period of age means at least one missing final interval;
	// rejecting that sample costs evidence, never CPU or release authority.
	period := attemptMemorySamplePeriod
	if a.CPU.Source == "sampled_process_tree" {
		period = attemptStorageSamplePeriod
	}
	if a.AgeSeconds > period.Seconds() {
		return
	}
	need := *a.CPU.UsageSeconds / a.ElapsedSeconds
	if !validMemoryValue(need) || m.CapabilityProfile == nil || need > float64(m.CapabilityProfile.LogicalCPUs) {
		return
	}
	completeReason := repeatedContentionReason(r)
	if a.CPU.Source != "cgroup_v2" || a.CPU.LowerBound {
		completeReason = "sampled CPU misses unobserved descendants; lower bound"
	}
	if o := j.CPUObservation; o != nil && o.Attempt == j.Attempt && o.Machine == j.Machine && o.Session == j.Session {
		need = max(need, o.PeakCores) // #870's same-job facts remain authoritative.
	}
	id := identityHash(key)
	old, exists := q.s.WorkObservations[id]
	p := CPUDemandEvidence{}
	if old.CPU != nil {
		p = *old.CPU
	}
	if completeReason == "" {
		p.Samples++
		p.PeakCores = max(p.PeakCores, need)
	} else {
		p.LowerBoundSamples++
		p.LowerBoundCores = max(p.LowerBoundCores, need)
	}
	p.CoverageSeconds += a.ElapsedSeconds
	p.UsageSeconds += *a.CPU.UsageSeconds
	p.Source, p.Reason = a.CPU.Source, completeReason
	point := WorkObservation{Key: *key, CPU: &p, Samples: old.Samples + 1,
		RecordedAt: q.clock.Now(), Source: "accepted CPU demand", Job: j.ID,
		Machine: j.Machine, Session: j.Session, Attempt: j.Attempt}
	q.putWorkObservation(id, point)
	q.recordCurveLearning(j, old, exists, point)
}

func cpuDemandBasis(j *Job, cpu *ResourceBasis) {
	if e := j.CPUDemand; e != nil {
		cpu.Used = cpuValue(float64(max(j.Spec.Needs.cores(), e.ChargeCores)))
		cpu.Basis, cpu.Note = e.Basis, e.Reason
		if p := e.Learned; p != nil {
			cpu.Learned = cpuValue(max(p.PeakCores, p.LowerBoundCores))
			cpu.Evidence = p.Samples + p.LowerBoundSamples
			cpu.Note += fmt.Sprintf("; %d complete, %d lower-bound accepted samples; %.3fs observed CPU over %.3fs elapsed", p.Samples, p.LowerBoundSamples, p.UsageSeconds, p.CoverageSeconds)
			if p.Samples > 0 && p.LowerBoundSamples == 0 && p.PeakCores < float64(j.Spec.Needs.CPUCores) {
				cpu.Note += "; observed demand below explicit reservation: owner may lower cpu_cores; admission preserves caller intent"
			}
		}
	}
	// The shared-host observer may have raised the live charge since admission.
	if p := j.CPUObservation; p != nil && validMemoryValue(p.PeakCores) && p.PeakCores > 0 {
		if j.CPUDemand != nil && j.CPUDemand.SharedHost {
			cpu.Used = cpuValue(max(float64(j.Spec.Needs.cores()), *cpu.Used, math.Ceil(p.PeakCores)))
		}
		if cpu.Learned == nil || p.PeakCores > *cpu.Learned {
			cpu.Learned = cpuValue(p.PeakCores)
		}
		cpu.Note += "; logical-job observed peak is an authoritative lower bound"
	}
}

func cpuValue(n float64) *float64 { return &n }

// Internal release planning accepts a Spec. Carry the already selected charge
// through that interface without changing the caller's stored declaration.
func cpuAdmissionSpec(j *Job, m *Machine) Spec {
	s := j.Spec
	s.Needs.CPUCores = jobCPUCharge(j, m)
	return s
}

func cpuDemandText(j *Job) string {
	if j.CPUDemand == nil {
		return ""
	}
	cpu := ResourceBasis{Used: cpuValue(float64(j.Spec.Needs.cores()))}
	cpuDemandBasis(j, &cpu)
	declared := "undeclared"
	if j.Spec.Needs.CPUCores > 0 {
		declared = fmt.Sprint(j.Spec.Needs.CPUCores)
	}
	learned := "unknown"
	if cpu.Learned != nil {
		learned = fmt.Sprintf("%.3f (%d accepted samples)", *cpu.Learned, cpu.Evidence)
	}
	return fmt.Sprintf("; CPU declared %s, learned %s, used %.0f (%s); %s", declared, learned, *cpu.Used, cpu.Basis, cpu.Note)
}
