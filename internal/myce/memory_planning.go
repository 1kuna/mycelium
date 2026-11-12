package myce

import (
	"slices"
	"time"
)

// Owned by one immutable planning pass, shared across its target machines.
// Never retained across placements, inventory changes, or transaction boundaries.
type memoryPlanningCache struct {
	observations *memoryObservationCatalog
	history      *memoryHistoryCache
	constrained  map[constrainedMemoryKey]float64
	components   map[memoryComponentKey]*Machine
	allocations  map[allocationViewKey]*Machine
	// Storage observations grouped by family for this pass.
	storageFamilies map[string][]StorageObservation
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

func metalFootprintWork(j *Job, m *Machine) bool {
	return m != nil && backendFor(m.GPU).processFootprint && !j.Spec.Needs.cpuOnly() && slices.Contains(m.Capabilities, metalFootprintGuardCapability)
}

func unifiedDeviceWork(j *Job, m *Machine) bool {
	return unifiedCUDA(m) && !j.Spec.Needs.cpuOnly()
}

// Measured on a unified host on 2026-11-11 (#1044): the attempt cgroup
// charges and contains host, managed (cudaMallocManaged) and pinned
// (cudaMallocHost) allocations, but device allocations (cudaMalloc, which the
// PyTorch caching allocator uses) escape both the cgroup and RSS. NVML still
// reports them per process, the worker samples that every
// deviceMemorySamplePeriod, and host availability falls by the same amount.
// So the host share is contained and the device share is measured: by-use
// unified work is charged both (memoryCharge). Per-process device counters can
// count one allocation more than once and may overlap cgroup pages (#424), so
// the sum only overstates, and only the host vector earns resident credit.
func deviceForecast(s hostMemorySample, now time.Time) float64 {
	return s.deviceGB + s.deviceGrowthGB + s.deviceRateGB*now.Sub(s.deviceAt).Seconds()
}
