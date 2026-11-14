package myce

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// SharedInputCapacity is a worker-local cache snapshot. Heartbeats only copy
// it: discovery/accounting runs at startup and cache mutation, never in q.mu.
// FreeBytes excludes allocated bytes and outstanding staging reservations.
// Protected identifies reusable entries that are not included in EvictableBytes.
type SharedInputCapacity struct {
	CapacityBytes  uint64            `json:"capacity_bytes"`
	FreeBytes      uint64            `json:"free_bytes"`
	EvictableBytes uint64            `json:"evictable_bytes"`
	Protected      map[string]uint64 `json:"protected,omitempty"`
}

func (c *SharedInputCache) capacityReport() *SharedInputCapacity {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.capacity == nil {
		return nil
	}
	report := *c.capacity
	report.Protected = maps.Clone(report.Protected)
	return &report
}

func (c *SharedInputCache) refreshCapacityIfTracked() {
	c.mu.Lock()
	tracked := c.capacity != nil
	c.mu.Unlock()
	if tracked {
		c.refreshCapacity()
	}
}

func (c *SharedInputCache) refreshCapacity() {
	if err := c.cache.init(); err != nil {
		return
	}
	gate, err := c.cache.lock(context.Background(), "admission", syscall.LOCK_EX)
	if err != nil {
		return
	}
	defer unlockEnvironment(gate)
	c.mu.Lock()
	initial := c.capacity == nil
	c.mu.Unlock()
	if initial {
		// Existing admission already removes abandoned reservations/partials
		// using their build locks. Otherwise a crash could advertise no free
		// space forever, preventing the acquire that would reclaim them.
		_ = c.admit(context.Background(), "", 0)
	}
	c.refreshCapacityLocked()
}

// The existing cache admission lock serializes accounting with reservations and
// eviction. A failed inventory withholds space; it never invents free capacity.
func (c *SharedInputCache) refreshCapacityLocked() {
	report := &SharedInputCapacity{Protected: map[string]uint64{}}
	defer func() { c.mu.Lock(); c.capacity = report; c.mu.Unlock() }()
	used, err := c.cache.DiskUsage()
	if err != nil {
		return
	}
	entries, err := os.ReadDir(c.cache.Root)
	if err != nil {
		return
	}
	var reserved, evictable uint64
	for _, entry := range entries {
		key := entry.Name()
		if strings.HasSuffix(key, ".reservation") {
			b, err := os.ReadFile(filepath.Join(c.cache.Root, key))
			if err != nil {
				return
			}
			n, err := strconv.ParseUint(string(b), 10, 63)
			if err != nil {
				return
			}
			var written uint64
			stage := strings.TrimSuffix(key, ".reservation") + ".partial"
			for _, part := range []string{"data", "archive"} {
				size, err := environmentSize(filepath.Join(c.cache.Root, stage, part))
				if err != nil && !os.IsNotExist(err) {
					return
				}
				written = addDiskBytes(written, uint64(max(0, size)))
			}
			if n > written {
				reserved = addDiskBytes(reserved, n-written)
			}
			continue
		}
		if entry.IsDir() && strings.HasSuffix(key, ".partial") {
			partial := filepath.Join(c.cache.Root, key)
			if st, err := os.Lstat(filepath.Join(partial, "resume")); err == nil && st.Mode().IsRegular() {
				lock, err := c.cache.tryLock(strings.TrimSuffix(key, ".partial") + ".build")
				if err != nil {
					return
				}
				if lock != nil {
					n, err := environmentDiskUsage(partial)
					unlockEnvironment(lock)
					if err != nil {
						return
					}
					evictable = addDiskBytes(evictable, uint64(n))
				}
			}
			continue
		}
		if !entry.IsDir() || (FileRef{SHA256: key}).validate() != nil {
			continue
		}
		n, err := environmentDiskUsage(filepath.Join(c.cache.Root, key))
		if err != nil {
			return
		}
		lock, err := c.cache.tryLock(key + ".use")
		if err != nil {
			return
		}
		pinned, err := sharedInputPinned(c.cache.Root, key)
		if lock != nil {
			unlockEnvironment(lock)
		}
		if err != nil {
			return
		}
		if lock == nil || pinned {
			report.Protected[key] = uint64(n)
		} else {
			evictable = addDiskBytes(evictable, uint64(n))
		}
	}
	// Measure after reading growing prefixes; writes during the scan only make
	// remaining-byte accounting more conservative.
	used, err = c.cache.DiskUsage()
	if err != nil {
		return
	}
	explicit := c.cache.Limit
	if c.automatic {
		explicit = 0
	}
	limit, free, err := inputCacheBudget(c.cache.Root, used, explicit, c.diskProbe, c.budget.foreignHeadroom(c.cache.Root))
	if err != nil {
		return
	}
	report.CapacityBytes = uint64(max(0, limit))
	if c.automatic && c.budget != nil {
		// Permanent feasibility uses physical capacity. Other caches are
		// reclaimable by the shared ledger, rather than permanent occupancy.
		probe := c.diskProbe
		if probe == nil {
			probe = workerDiskProbe
		}
		if disk, err := probe(c.cache.Root); err == nil && disk.TotalBytes > diskFreeFloor(disk.TotalBytes) {
			report.CapacityBytes = disk.TotalBytes - diskFreeFloor(disk.TotalBytes)
		}
	}
	occupied := addDiskBytes(uint64(max(0, used)), reserved)
	if report.CapacityBytes > occupied {
		report.FreeBytes = report.CapacityBytes - occupied
	}
	physical := uint64(max(0, free))
	if physical > reserved {
		physical -= reserved
	} else {
		physical = 0
	}
	report.FreeBytes = min(report.FreeBytes, physical)
	report.EvictableBytes = min(evictable, report.CapacityBytes-report.FreeBytes)
}

// Include archive staging alongside the expanded payload. Saturation makes an
// overflowing collection impossible rather than letting it wrap into a fit.
func sharedInputRequired(s Spec, m *Machine) uint64 {
	sizes := map[string]uint64{}
	for _, input := range s.SharedInputs {
		key := input.key()
		size := uint64(max(0, input.bytes()))
		if input.Archive != "" && (m == nil || !slices.Contains(m.CachedSharedInputs, key)) {
			size = addDiskBytes(size, uint64(max(0, input.SizeBytes)))
		}
		// Aliases share storage; differing upper bounds must not make the
		// answer depend on map iteration order.
		sizes[key] = max(sizes[key], size)
	}
	var required uint64
	for _, size := range sizes {
		required = addDiskBytes(required, size)
	}
	return required
}

func sharedInputCacheReason(s Spec, m *Machine, capacityOnly bool) string {
	if s.ephemeral() || len(s.SharedInputs) == 0 {
		return ""
	}
	required := sharedInputRequired(s, m)
	if m == nil {
		return fmt.Sprintf("shared_input_cache: required=%d bytes capacity=unknown; worker capacity report required", required)
	}
	// Capacity reports are optional across worker upgrades. A worker without
	// one retains staging-time admission, whose local cache enforces its actual
	// limit and reports required/available bytes. Requiring the newer report
	// here would permanently strand work on an otherwise compatible old worker.
	if m.SharedInputCache == nil {
		return ""
	}
	report := m.SharedInputCache
	if required > report.CapacityBytes {
		return fmt.Sprintf("shared_input_cache: required=%d bytes capacity=%d bytes", required, report.CapacityBytes)
	}
	if capacityOnly {
		return ""
	}
	inputs := maps.Clone(s.SharedInputs)
	keys := map[string]bool{}
	for _, input := range inputs {
		keys[input.key()] = true
	}
	for key, input := range m.sharedInputClaims {
		if !keys[key] {
			required = addDiskBytes(required, sharedInputRequired(Spec{SharedInputs: map[string]SharedInput{key: input}}, m))
			inputs[key] = input
			keys[key] = true
		}
	}
	available := addDiskBytes(min(report.FreeBytes, report.CapacityBytes), min(report.EvictableBytes, report.CapacityBytes))
	seen := map[string]bool{}
	for _, input := range inputs {
		key := input.key()
		if !seen[key] && slices.Contains(m.CachedSharedInputs, key) {
			available = addDiskBytes(available, report.Protected[key])
			seen[key] = true
		}
	}
	available = min(available, report.CapacityBytes)
	if required > available {
		return fmt.Sprintf("shared_input_cache: required=%d bytes free=%d bytes evictable=%d bytes available=%d bytes capacity=%d bytes", required, report.FreeBytes, report.EvictableBytes, available, report.CapacityBytes)
	}
	return ""
}

func (q *Queue) sharedInputCapacityRefusal(j *Job) error {
	if j.Spec.ephemeral() || len(j.Spec.SharedInputs) == 0 {
		return nil
	}
	var largest uint64
	reported := false
	unreported := false
	for _, m := range q.plannerMachines() {
		// Disabled/retired targets cannot make an impossible submission look viable.
		if q.maintenanceBlocked(hostID(m)) || q.deviceAdmissionBlocked(m) || !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		if m.SharedInputCache == nil {
			unreported = true
			continue
		}
		for _, candidate := range sharedInputCandidates(j, m) {
			reported = true
			largest = max(largest, m.SharedInputCache.CapacityBytes)
			if sharedInputCacheReason(candidate.Spec, m, true) == "" {
				return nil
			}
		}
	}
	// Missing reports during startup or a mixed-version rollout cannot prove
	// impossibility. Legacy workers enforce capacity when staging the inputs.
	if unreported || !reported {
		return nil
	}
	code := "shared_input_cache_capacity"
	return &HTTPError{Status: 409, Code: code, Message: fmt.Sprintf("%s: shared inputs require %d bytes; largest enabled matching worker cache capacity=%d bytes", code, sharedInputRequired(j.Spec, nil), largest)}
}

// Shared-input cache space that holds this job on every allowed host, or the
// missing capacity reports that keep the queue from knowing.
func sharedInputWait(j *Job, machines map[string]*Machine) *Hold {
	if j.Spec.ephemeral() || len(j.Spec.SharedInputs) == 0 {
		return nil
	}
	var reasons []string
	for _, m := range machines {
		if !j.Spec.allowsMachine(m.ID) {
			continue
		}
		for _, candidate := range sharedInputCandidates(j, m) {
			reason := sharedInputCacheReason(candidate.Spec, m, false)
			if reason == "" {
				return nil
			}
			reasons = append(reasons, reason)
		}
	}
	if len(reasons) == 0 {
		return newHold("shared_input_unreported", fmt.Sprintf("shared_input_cache_unreported: required=%d bytes; waiting for worker capacity reports", sharedInputRequired(j.Spec, nil)))
	}
	slices.Sort(reasons)
	return newHold("shared_input", strings.Join(slices.Compact(reasons), "; "))
}

// Queue-owned claims cover the interval between assignment and the next worker
// snapshot. Shared identities count once across concurrent attempts.
func (q *Queue) sharedInputClaims(m *Machine) map[string]SharedInput {
	claims := map[string]SharedInput{}
	for _, j := range q.liveJobs() {
		if j.Machine != m.ID || j.Session != m.Session || j.ReleaseConfirmed {
			continue
		}
		for _, input := range j.Spec.SharedInputs {
			claims[input.key()] = input
		}
	}
	return claims
}

// Match target-specific extension needs before deciding whether a CPU/GPU
// target is relevant. The unresolved platform union is not a CPU-only test.
func sharedInputCandidates(j *Job, m *Machine) []*Job {
	var out []*Job
	for _, candidate := range extensionJobs(j, m, false) {
		s := candidate.Spec
		if !executionTargetMatches(s, m) || !deviceAllowed(candidate, m) {
			continue
		}
		if !s.Needs.cpuOnly() && (m.GPU == "cpu" || len(s.Needs.Platform.Required) > 0 && !slices.Contains(s.Needs.Platform.Required, m.GPU)) {
			continue
		}
		out = append(out, candidate)
	}
	return out
}
