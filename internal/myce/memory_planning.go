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
// declared envelope, where a guard contains its growth or, for unified CUDA
// device memory, where it is measured per process (#1044, audit item 3):
//
//   - native CPU-only work with a positive declaration on a guarded dedicated
//     host: Linux cgroup v2 ceilings, or the Darwin RSS guard. Interactive
//     hosts keep their envelope (MY-45: harsher limits);
//   - native GPU work on a unified CUDA host with cgroup v2 ceilings (see
//     unifiedUsedMemoryCharge). Metal and discrete accelerators keep their
//     envelope: the Darwin guard samples RSS, which never sees Metal buffers,
//     and no per-process Metal sample exists yet;
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
	if unifiedDeviceWork(j, m) {
		charge, growth = unifiedUsedMemoryCharge(j, m, ceiling, sample, now)
		return charge, growth, true
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

func unifiedDeviceWork(j *Job, m *Machine) bool {
	return unifiedCUDA(m) && !j.Spec.Needs.cpuOnly()
}

// Measured on a GB10 unified host on 2026-11-11 (#1044): the attempt cgroup
// charges and contains host, managed (cudaMallocManaged) and pinned
// (cudaMallocHost) allocations, but device allocations (cudaMalloc, which the
// PyTorch caching allocator uses) escape both the cgroup and RSS. NVML still
// reports them per process, the worker samples that every
// deviceMemorySamplePeriod, and host availability falls by the same amount.
// So the host share is contained and the device share is measured.
//
// Each share is charged max(accepted peak of this work on this target, attempt
// high water, live forecast). The peaks drop the learned envelope's measured
// uncertainty, which on this host was as large as the peaks themselves; growth
// past them is what yieldMemoryOvercommit stops before the host runs out. The
// sum, plus the launch drift margin, is bounded by the envelope; observed use
// above it is kept. Per-process device counters can count one allocation more
// than once and may overlap cgroup pages (#424), so the sum only overstates,
// and only the host vector uniquely charged to the cgroup earns credit.
func unifiedUsedMemoryCharge(j *Job, m *Machine, ceiling float64, sample hostMemorySample, now time.Time) (charge, growth float64) {
	l := j.MemoryEnvelope.Learned
	host := max(l.PeakHostGB, sample.peakGB, sample.gb, sample.footprintGB, sample.hostForecast(now))
	device := max(l.PeakDeviceGB, sample.devicePeakGB, sample.deviceGB, deviceForecast(sample, now))
	observed := max(sample.peakGB, sample.gb, sample.footprintGB) + max(sample.devicePeakGB, sample.deviceGB)
	margin := exploratoryDriftMarginGB(m)
	charge = max(min(host+device+margin, ceiling), observed)
	return charge, max(0, charge-sample.gb, min(margin, charge))
}

func deviceForecast(s hostMemorySample, now time.Time) float64 {
	return s.deviceGB + s.deviceGrowthGB + s.deviceRateGB*now.Sub(s.deviceAt).Seconds()
}

func usedMemoryChargeEligible(j *Job, m *Machine, sample hostMemorySample, now time.Time) bool {
	if j == nil || m == nil || j.StartedAt == nil || (j.State != "running" && j.State != "stopping") {
		return false
	}
	n, e := j.Spec.Needs, j.MemoryEnvelope
	if j.Spec.Run.Docker || m.SharedHost != nil || e == nil || e.Learned == nil || e.Learned.Evidence < 1 {
		return false
	}
	if _, fresh := sample.hostGrowth(now); !fresh || !validMemoryValue(sample.footprintGB) || !validMemoryValue(sample.rateGB) {
		return false
	}
	if unifiedDeviceWork(j, m) {
		return unifiedUsedMemoryEligible(j, m, sample, now)
	}
	if !n.cpuOnly() || n.Memory == "auto" || n.HostMemoryGB <= 0 {
		return false
	}
	if m.MemoryEnforcement != "memory ceilings enforced (cgroup v2)" && m.MemoryEnforcement != darwinRSSGuardStatus {
		return false
	}
	if !validMemoryValue(e.Learned.HostGB) || e.Learned.HostGB <= 0 {
		return false
	}
	return max(sample.gb, sample.footprintGB) > 0
}

// The envelope is fixed at assignment, learned or declared, and the worker
// applies hostNeed (host plus device) as the attempt's memory.max. One card's
// NVML sample describes the attempt; RSS and host-delta fallbacks, which
// require exclusive execution, are estimates and grant nothing.
func unifiedUsedMemoryEligible(j *Job, m *Machine, sample hostMemorySample, now time.Time) bool {
	n, l := j.Spec.Needs, j.MemoryEnvelope.Learned
	if m.MemoryEnforcement != "memory ceilings enforced (cgroup v2)" || !m.HostMemoryMeasured || n.MemoryGB <= 0 || n.HostMemoryGB <= 0 || len(j.AllocatedDevices) > 1 {
		return false
	}
	if l.PeakDeviceGB <= 0 || !validMemoryValue(l.PeakDeviceGB) || !validMemoryValue(l.PeakHostGB) {
		return false
	}
	if sample.source != "" || sample.exclusive || sample.deviceAt.IsZero() || now.Before(sample.deviceAt) || now.Sub(sample.deviceAt) >= deviceMemorySampleFreshFor {
		return false
	}
	return validMemoryValue(sample.deviceGB) && validMemoryValue(sample.deviceGrowthGB) && validMemoryValue(sample.deviceRateGB)
}
