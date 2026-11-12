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
		work := j.Spec.workType()
		if j.Spec.ExtensionCheck != nil {
			work = "check:" + work // checks and workloads learn separately (#618)
		}
		return identityHash(struct{ Extension, Work string }{digest, work})
	}
	return identityHash(struct{ Project, Name string }{j.Spec.Project, j.Spec.Name})
}

// The marker preserves the existing OOM retry protocol for an exploratory
// envelope smaller than physical capacity. It is a protocol label, not a size.
const boundedFirstRun = "(bounded: "

// With no usable measurements an arbitrary command has no finite demand bound.
// Reserve measured physical headroom for one probe instead of guessing a
// fraction. The cost is local concurrency until a clean completion teaches us;
// other hosts remain independent. Explicit containment and OOM floors win.
func (q *Queue) coldMemoryBound(j *Job, m *Machine, estimate memoryEstimate, floor float64) (float64, string, int, bool) {
	host, source, evidence, _, ok := q.coldMemoryBoundOrProbe(j, m, estimate, floor)
	return host, source, evidence, ok
}

// A positive probe peak declines the bound in favour of that physical probe,
// floored at the peak (see below); the caller keeps it as the probe floor.
func (q *Queue) coldMemoryBoundOrProbe(j *Job, m *Machine, estimate memoryEstimate, floor float64) (float64, string, int, float64, bool) {
	if !q.learnedMemory() {
		return 0, "", 0, 0, false
	}
	peak, margin, evidence := q.similarMemoryPeak(j, m)
	host := peak + margin
	source := "exploratory first run"
	if estimate.source != "" {
		source = estimate.source
	}
	lower := q.constrainedMemoryLowerFor(j, estimate, !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory)
	if lower > 0 && estimate.key != nil {
		facts := q.memoryConstraintEvidence(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program), !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory)
		if facts.recovery > 0 {
			bound, why := q.recoveryMemoryBound(j, m, estimate, host, 0)
			if why != "" {
				return 0, "", 0, 0, false // Keep the physical probe's refusal diagnostic.
			}
			return bound, source, 0, 0, true
		}
	}
	// A constrained completion is not an upper bound, even with a prior. Do
	// not invent headroom by multiplying it. Physical probing remains subject
	// to normal admission, release custody and the retained OOM requirement.
	if host <= lower || evidence == 0 {
		return 0, "", 0, 0, false
	}
	if f := j.MemoryOOMFloor; f != nil {
		floor = max(floor, f.HostMemoryGB)
	}
	host = max(host, floor, minimumHostMemoryGB)
	if !validRate(host) {
		return 0, "", 0, 0, false
	}
	// The margin is the spread between runs of differently sized work, so it
	// can exceed a host's whole allowance while every accepted run fitted it.
	// Refusing then makes the evidence cost more than having none: a host
	// without evidence probes its whole allowance. Probe that way here too,
	// never below the measured peak. Placement then stays on dedicated hosts
	// that already ran this work rather than only on hosts that never have.
	if allowance := hostMemoryAllowanceGB(m); j.Spec.Needs.cpuOnly() && allowance > 0 && hostNeed(m, 0, host) > allowance && hostNeed(m, 0, max(peak, floor)) <= allowance {
		return 0, "", 0, max(peak, floor), false
	}
	return host, source + " " + boundedFirstRun + "accepted family peaks + measured uncertainty)", evidence, 0, true
}

// Overriding a stated hint needs at least two accepted family runs: the
// fewest that expose spread between items. With no figure at all, one run
// already bounds a first run (coldMemoryBound). Host RAM only: the family
// bound has no device vector, so GPU hints keep their device figure.
const memoryFamilyHintRuns = 2

type familyMemoryBound struct {
	host     float64
	source   string
	evidence int
}

func (q *Queue) familyMemoryOverHint(j *Job, m *Machine, estimate memoryEstimate, floor float64, out *familyMemoryBound) bool {
	if j.MemoryFallback == nil || !j.MemoryFallback.valid() || !j.Spec.Needs.cpuOnly() {
		return false
	}
	host, source, evidence, ok := q.coldMemoryBound(j, m, estimate, floor)
	if !ok || evidence < memoryFamilyHintRuns {
		return false
	}
	*out = familyMemoryBound{host, source, evidence}
	return true
}

// The host RAM this worker may ever give jobs: its allowance, else physical RAM.
func hostMemoryAllowanceGB(m *Machine) float64 {
	if m.EffectiveHostMemoryBudgetGB > 0 || m.CapabilityProfile == nil {
		return m.EffectiveHostMemoryBudgetGB
	}
	return float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
}

// The largest retained constrained host peak of this work or job. It is a lower
// bound: it only ever raises a first-run bound.
func (q *Queue) constrainedMemoryLower(j *Job, estimate memoryEstimate) float64 {
	return q.constrainedMemoryLowerFor(j, estimate, false)
}
func (q *Queue) constrainedMemoryLowerFor(j *Job, estimate memoryEstimate, discrete bool) float64 {
	if cache := q.memoryPlanning; cache != nil {
		id := constrainedMemoryIdentity(j, estimate)
		id.lower.discrete = discrete
		if lower, ok := cache.constrained[id]; ok {
			return lower
		}
		lower := q.constrainedMemoryLowerUncached(j, estimate, discrete)
		cache.constrained[id] = lower
		return lower
	}
	return q.constrainedMemoryLowerUncached(j, estimate, discrete)
}
func (q *Queue) constrainedMemoryLowerUncached(j *Job, estimate memoryEstimate, discrete bool) float64 {
	lower := estimate.constrainedGB
	if estimate.key != nil {
		lower = max(lower, q.memoryLowerBound(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program), discrete))
	}
	for _, facts := range j.MemoryAttempts {
		if p := facts.MemoryPeak; p.constrained() && p.HostGB != nil && validRate(*p.HostGB) && memoryConstraintFacts(facts) {
			lower = max(lower, hostMemoryPeakGB(p))
		}
	}
	return lower
}

// Unknown host/device overlap cannot justify two independent size guesses.
// A cold unified probe owns the card exclusively and reserves the whole shared
// physical headroom. With neither vector known, one supported kernel page is
// a nonzero device/exclusivity marker, with the remaining pool available to the
// enforced host member. It is not a device-use estimate or sharing evidence.
// Explicit vectors and member floors retain their original meaning.
func (q *Queue) unifiedGPUFirstRun(j *Job, m *Machine, estimate memoryEstimate, device, host, floor float64) (float64, float64, string, int, bool) {
	if !m.UnifiedMemory || j.Spec.Needs.cpuOnly() || !q.learnedMemory() {
		return 0, 0, "", 0, false
	}
	room, why := q.exploratoryMemory(j, m)
	if why != "" {
		return 0, 0, "", 0, false
	}
	floor = max(floor, q.constrainedMemoryLower(j, estimate), minimumHostMemoryGB)
	if f := j.MemoryOOMFloor; f != nil {
		floor = max(floor, f.HostMemoryGB)
	}
	switch {
	case host > 0 && device <= 0:
		device = max(minimumHostMemoryGB, room-host)
	case device > 0 && host <= 0:
		host = max(floor, room-device)
	default:
		device = max(device, minimumHostMemoryGB)
		host = max(host, floor, room-device)
	}
	if !validRate(host+device) || host+device > room {
		return 0, 0, "", 0, false
	}
	return device, host, "exploratory shared physical headroom; exclusive device", 0, true
}

// Fresh accepted unconstrained peaks of the same family on the same OS: CPU
// peaks for CPU work, matching-layout GPU host peaks for GPU work. Rows written
// before memory sample counting count once.
func (q *Queue) similarMemoryPeak(j *Job, m *Machine) (float64, float64, int) {
	family, gpu, layout := memoryFamily(j), !j.Spec.Needs.cpuOnly(), memoryLayout(j, m)
	if !gpu {
		layout = ""
	}
	var points []*WorkObservation
	for _, point := range q.memoryObservationCatalog().family[similarMemoryKey{family, machineTarget(m).OS, gpu, layout}] {
		if q.acceptedMemoryPoint(point) && point.Key.Host == hostID(m) && point.Key.Fingerprint == m.ExtensionFingerprint {
			points = append(points, point)
		}
	}
	return observedMemoryPeakAndMargin(points, false)
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
