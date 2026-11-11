package myce

import "time"

// Owned by one immutable planning pass, shared across its target machines.
// Never retained across placements, inventory changes, or transaction boundaries.
type memoryPlanningCache struct {
	observations *memoryObservationCatalog
	history      *memoryHistoryCache
	constrained  map[constrainedMemoryKey]float64
	components   map[memoryComponentKey]*Machine
	allocations  map[allocationViewKey]*Machine
}

type memoryComponentKey struct {
	owner  *Machine
	device string
	cpu    bool
}

// Observation identity reads inventory, not projected free room. Component
// copies are private to this immutable pass, including permanent-capacity views.
func (q *Queue) memoryObservationMachine(j *Job, owner *Machine) *Machine {
	key := memoryComponentKey{owner: owner, cpu: j.Spec.Needs.cpuOnly()}
	if len(j.AllocatedDevices) == 1 {
		key.device = j.AllocatedDevices[0]
	}
	cache := q.memoryPlanning
	if cache != nil && len(j.AllocatedDevices) <= 1 {
		if m := cache.components[key]; m != nil {
			return m
		}
	}
	m := allocationMachine(owner, j.AllocatedDevices)
	if key.cpu && owner.Devices != nil {
		m = cpuMachineView(owner)
	}
	if cache != nil && len(j.AllocatedDevices) <= 1 {
		if cache.components == nil {
			cache.components = map[memoryComponentKey]*Machine{}
		}
		cache.components[key] = m
	}
	return m
}

// Permanent-capacity views share retained execution evidence and the origin's
// observation catalog. Admission-dependent caches stay private to each view.
type memoryHistoryCache struct {
	index  retainedMemoryIndex
	owners map[retainedMemoryOwner]*Machine
	lower  map[memoryLowerKey]memoryConstraintEvidence
}

// Keys separate the full comparable work/configuration and target OS.
// Raw cold facts may be reused; lower bounds and owner views belong to a pass.
type memoryHistoryKey struct{ work, os string }

type memoryLowerKey struct {
	work     memoryWorkKey
	os       string
	discrete bool
	after    time.Time
}
type constrainedMemoryKey struct {
	lower       memoryLowerKey
	job         string
	constrained float64
}
type similarMemoryKey struct {
	family, os string
	gpu        bool
	layout     string
}

func newMemoryPlanningCache() *memoryPlanningCache {
	return &memoryPlanningCache{
		history: &memoryHistoryCache{lower: map[memoryLowerKey]memoryConstraintEvidence{}}, constrained: map[constrainedMemoryKey]float64{},
	}
}
func memoryLowerIdentity(key ObservationKey, configuration string) memoryLowerKey {
	return memoryLowerKey{work: memoryWorkIdentity(key, configuration), os: key.Target.OS}
}
func constrainedMemoryIdentity(j *Job, estimate memoryEstimate) constrainedMemoryKey {
	key := constrainedMemoryKey{job: j.ID, constrained: estimate.constrainedGB}
	if estimate.key != nil {
		key.lower = memoryLowerIdentity(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program))
	}
	return key
}

// The Darwin RSS guard stops a job after three over-limit samples across two
// sampler periods; a late sample extends that by up to one more period. The
// measured reaction was 0.2–0.8 s at the 200 ms cadence (native-isolation.md),
// so a job can overshoot its last sample by its growth rate times this window.
// attemptMemorySamplePeriod is platform independent, so the queue and the
// Darwin worker compute the same charge.
const darwinGuardReaction = 4 * attemptMemorySamplePeriod

// usedMemoryCharge charges a running job the memory it uses, not its whole
// declared envelope, where a guard contains its growth (#1044, audit item 3):
//
//   - native CPU-only work with a positive declaration: accelerator memory
//     (Metal, unified CUDA, driver-pinned host pages) is outside the cgroup and
//     the RSS guard, so those jobs keep their envelope;
//   - a guarded dedicated host: Linux cgroup v2 ceilings, or the Darwin RSS
//     guard. Interactive hosts keep their envelope (MY-45: harsher limits);
//   - learned evidence for this work on this target (MY-34): a first run with no
//     accepted history keeps its envelope, and later runs never drop below
//     what history says the job reaches;
//   - a fresh exact-attempt sample. Missing or stale data grants nothing.
//
// The charge is max(learned need, attempt high water, live forecast) plus the
// measured overshoot before containment acts, bounded by the enforced ceiling.
// Actual observation above that bound is kept: it is real occupancy.
// Remaining growth credits current resident memory once, because live host
// availability already excludes it. When lenders outgrow what they were
// charged, yieldMemoryOvercommit stops the lowest-tier work before the kernel.
func usedMemoryCharge(j *Job, m *Machine, ceiling float64, sample hostMemorySample, now time.Time) (charge, growth float64, ok bool) {
	if !usedMemoryChargeEligible(j, m, sample, now) {
		return 0, 0, false
	}
	resident := max(sample.gb, sample.footprintGB)
	observed := max(sample.peakGB, resident)
	learned := hostNeed(m, 0, j.MemoryEnvelope.Learned.HostGB)
	// hostForecast already carries the last measured increment and the rate
	// through the sample's age; the drift margin covers snapshot-to-launch.
	margin := exploratoryDriftMarginGB(m)
	bound := ceiling
	if m.MemoryEnforcement == darwinRSSGuardStatus {
		overshoot := sample.rateGB * darwinGuardReaction.Seconds()
		margin += overshoot
		bound += overshoot // RSS can pass the limit before the guard stops it.
	}
	charge = max(min(max(learned, observed, sample.hostForecast(now))+margin, bound), observed)
	credit := sample.gb
	if m.MemoryEnforcement == darwinRSSGuardStatus {
		// Darwin availability excludes the job's resident pages, but RSS gets
		// no resident field. The charge is built from the same RSS, so credit
		// it once while always protecting the measured margin.
		credit = sample.footprintGB
	}
	return charge, max(0, charge-credit, min(margin, charge)), true
}

func usedMemoryChargeEligible(j *Job, m *Machine, sample hostMemorySample, now time.Time) bool {
	if j == nil || m == nil || j.StartedAt == nil || (j.State != "running" && j.State != "stopping") {
		return false
	}
	n := j.Spec.Needs
	if j.Spec.Run.Docker || !n.cpuOnly() || n.Memory == "auto" || n.HostMemoryGB <= 0 || m.SharedHost != nil {
		return false
	}
	if m.MemoryEnforcement != "memory ceilings enforced (cgroup v2)" && m.MemoryEnforcement != darwinRSSGuardStatus {
		return false
	}
	e := j.MemoryEnvelope
	if e == nil || e.Learned == nil || e.Learned.Evidence < 1 || !validMemoryValue(e.Learned.HostGB) || e.Learned.HostGB <= 0 {
		return false
	}
	if _, fresh := sample.hostGrowth(now); !fresh || !validMemoryValue(sample.footprintGB) || !validMemoryValue(sample.rateGB) {
		return false
	}
	return max(sample.gb, sample.footprintGB) > 0
}
