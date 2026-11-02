package myce

import (
	"errors"
	"fmt"
	"strings"
)

const declareMemory = "declare positive memory_gb (host RAM for CPU work, GPU memory otherwise)"

// A plain job that states no usable memory figure, judged on normalized needs
// as Validate does. Partial figures stay hints, as with an explicit auto.
func (s Spec) memoryOmitted() bool {
	if normalizeResources(&s) != nil {
		return false // Submission reports the invalid field itself.
	}
	n := s.Needs
	return !s.extensionJob() && n.Memory == "" && (!n.cpuOnly() && n.MemoryGB <= 0 || n.cpuOnly() && n.HostMemoryGB <= 0)
}

// learnedMemory is the local switch for MY-31 memory learning (#585). Off
// restores explicit figures and host-wide first runs.
func (q *Queue) learnedMemory() bool {
	p := q.effectivePriorityPolicy().LearnedMemory
	return p == nil || *p
}

// Omitted memory means auto: agents state needs, Mycelium learns footprints.
func (q *Queue) defaultMemory(s *Spec) error {
	if !s.memoryOmitted() {
		return nil
	}
	if !q.learnedMemory() {
		return reject("job_memory_invalid", errors.New(declareMemory+"; learned memory is off in this queue's policy"))
	}
	s.Needs.Memory = "auto"
	return nil
}

// Plain jobs share memory evidence by project and name, extension jobs by
// extension and work type, across configurations. It only bounds a first run.
func memoryFamily(j *Job) string {
	if j.Spec.extensionJob() {
		digest := ""
		if j.Extension != nil {
			digest = j.Extension.Digest
		}
		return identityHash(struct{ Extension, Work string }{digest, j.Spec.workType()})
	}
	return identityHash(struct{ Project, Name string }{j.Spec.Project, j.Spec.Name})
}

// Bounded first runs (#559). Host-wide exploration once reserved about 89 of a
// unified host's 96 GiB for a test suite that peaks near 4.5 GiB, blocking
// unrelated deadline work. A CPU job with no comparable accepted peak now
// starts from similar work's accepted peak × 2.5 (the 1.25 headroom × 2 used
// for same-OS cross-machine estimates), else a quarter of the host allowance,
// leaving room for three more such jobs. A constrained peak is a lower bound
// and is doubled, never shrunk. Cost: an under-estimate costs a ceiling hit or
// OOM per doubling (retryOOM). At half the allowance or more, the host-wide
// exploration and its custody fences apply as before. GPU work, whose card is
// already exclusive, keeps its exploration except on unified hosts (below).
const (
	coldMemoryFraction  = .25
	similarMemoryFactor = 1.25 * 2
	boundedFirstRun     = "(bounded: "
)

func (q *Queue) coldMemoryBound(j *Job, m *Machine, estimate memoryEstimate, floor float64) (float64, string, int, bool) {
	if !j.Spec.Needs.cpuOnly() || !q.learnedMemory() {
		return 0, "", 0, false
	}
	allowance := m.EffectiveHostMemoryBudgetGB
	if allowance <= 0 && m.CapabilityProfile != nil {
		allowance = float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
	}
	if allowance <= 0 {
		return 0, "", 0, false
	}
	first := "exploratory first run "
	if estimate.source != "" {
		first = estimate.source + " "
	}
	host, source := allowance*coldMemoryFraction, first+boundedFirstRun+"25% of host allowance)"
	peak, evidence := q.similarMemoryPeak(j, machineTarget(m).OS)
	if evidence > 0 {
		host, source = peak*similarMemoryFactor, first+boundedFirstRun+"similar work peak × 2.5)"
	}
	if lower := q.constrainedMemoryLower(j, estimate); lower*2 > host {
		host, source = lower*2, first+boundedFirstRun+"constrained peak × 2)"
	}
	if f := j.MemoryOOMFloor; f != nil {
		floor = max(floor, f.HostMemoryGB)
	}
	host = max(host, floor, minimumHostMemoryGB)
	if !validRate(host) || host >= allowance/2 {
		return 0, "", 0, false
	}
	return host, source, evidence, true
}

// The largest retained constrained host peak of this work or job. It is a lower
// bound: it only ever raises a first-run bound.
func (q *Queue) constrainedMemoryLower(j *Job, estimate memoryEstimate) float64 {
	lower := estimate.constrainedGB
	if estimate.key != nil {
		lower = max(lower, q.sameOSMemoryLowerBound(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)))
	}
	for _, facts := range j.MemoryAttempts {
		if p := facts.MemoryPeak; p.constrained() && p.HostGB != nil && validRate(*p.HostGB) {
			lower = max(lower, hostMemoryPeakGB(p))
		}
	}
	return lower
}

// Plain GPU runs on unified-memory hosts without an accepted estimate (#596).
// There the card's memory is host RAM, CUDA allocations escape the job's
// cgroup, and neither a device sample nor a classified GPU allocation failure
// exists, so no device bound can be checked or grown. Host-wide exploration
// instead reserved most of the machine until an accepted run. These runs take
// what `myce run --gpu` declared before #592: the stated device figure or
// 1 GiB, and the stated host figure, else similar GPU work's accepted host peak
// × 2.5, else 1 GiB. The cgroup-enforced host member still doubles on OOM.
// Cost: device use above the figure is unreserved, as before #592; admission's
// live host-memory checks still see it. Measured --mem avoids that. Extension
// jobs and discrete cards keep their exploration. At half the allowance the
// existing exploration applies.
const unifiedGPUDefaultGB = 1.0

func (q *Queue) unifiedGPUFirstRun(j *Job, m *Machine, estimate memoryEstimate, device, host, floor float64) (float64, float64, string, int, bool) {
	if !m.UnifiedMemory || j.Spec.Needs.cpuOnly() || j.Spec.extensionJob() || !q.learnedMemory() {
		return 0, 0, "", 0, false
	}
	allowance := m.EffectiveHostMemoryBudgetGB
	if allowance <= 0 && m.CapabilityProfile != nil {
		allowance = float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
	}
	why, evidence := "stated device", 0
	if device <= 0 {
		device, why = unifiedGPUDefaultGB, "1 GiB device default"
	}
	switch peak, similar := q.similarMemoryPeak(j, machineTarget(m).OS); {
	case host > 0:
		why += ", stated host"
	case similar > 0:
		host, evidence, why = peak*similarMemoryFactor, similar, why+", similar work host peak × 2.5"
	default:
		host, why = unifiedGPUDefaultGB, why+", 1 GiB host default"
	}
	if lower := q.constrainedMemoryLower(j, estimate); lower*2 > host {
		host, why = lower*2, why+", constrained peak × 2"
	}
	if f := j.MemoryOOMFloor; f != nil {
		floor = max(floor, f.HostMemoryGB)
	}
	host = max(host, floor, minimumHostMemoryGB)
	if allowance <= 0 || !validRate(host+device) || host+device >= allowance/2 {
		return 0, 0, "", 0, false
	}
	return device, host, boundedFirstRun + "unified GPU: " + why + ")", evidence, true
}

// Fresh accepted unconstrained peaks of the same family on the same OS: CPU
// peaks for CPU work, unified-layout GPU host peaks for GPU work. Rows written
// before memory sample counting count once.
func (q *Queue) similarMemoryPeak(j *Job, os string) (float64, int) {
	family, peak, evidence, gpu := memoryFamily(j), 0.0, 0, !j.Spec.Needs.cpuOnly()
	for _, point := range q.s.WorkObservations {
		p, age := point.MemoryPeak, q.clock.Now().Sub(point.MemoryRecordedAt)
		if point.MemoryFamily != family || (point.Key.Component != "cpu") != gpu || gpu && point.MemoryLayout != "unified" || point.Key.Target.OS != os || !memoryIdentityUsable(point) || point.MemoryRecordedAt.IsZero() || age < 0 || age > workCurveMaxAge || p == nil || p.constrained() || p.PressureUnavailable || p.HostGB == nil || !validRate(*p.HostGB) {
			continue
		}
		peak, evidence = max(peak, hostMemoryPeakGB(p)), evidence+max(1, point.MemorySamples)
	}
	return peak, evidence
}

// Origin names the figure's basis for status, job timing and waiting reasons.
func (e *JobMemoryEnvelope) Origin() string {
	switch {
	case e.Basis == "learned":
		return fmt.Sprintf("learned from %d comparable accepted attempts: %s", e.Evidence, e.Source)
	case e.Basis == "exploratory" && e.Evidence > 0:
		return fmt.Sprintf("exploratory, bounded by %d similar accepted attempts: %s", e.Evidence, e.Source)
	case e.Basis == "exploratory" && !strings.Contains(e.Source, "exploratory"):
		return "exploratory: " + e.Source
	}
	return e.Source
}
