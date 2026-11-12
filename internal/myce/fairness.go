package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// MaxConcurrent covers all classes; legacy experiment caps retain their scope.
type ProjectPolicy struct {
	DefaultCallPriority      string              `json:"default_call_priority,omitempty" yaml:"default_call_priority,omitempty"`
	AllowedMachines          []string            `json:"allowed_machines,omitempty" yaml:"allowed_machines,omitempty"`
	ProductionParityHosts    map[string][]string `json:"production_parity_hosts,omitempty" yaml:"production_parity_hosts,omitempty"`
	MaxConcurrent            int                 `json:"max_concurrent,omitempty" yaml:"max_concurrent,omitempty"`
	Weight                   float64             `json:"weight" yaml:"weight"`
	MaxConcurrentExperiments int                 `json:"max_concurrent_experiments" yaml:"max_concurrent_experiments"`
	MaxMemoryShare           float64             `json:"max_memory_share" yaml:"max_memory_share"`
	// Optional queue file-store limits for non-production submissions; zero
	// means no limit. Production is admitted; the store's reserve still applies.
	MaxRetainedFileBytes uint64 `json:"max_retained_file_bytes,omitempty" yaml:"max_retained_file_bytes,omitempty"`
	MaxAddedFileBytes24h uint64 `json:"max_added_file_bytes_24h,omitempty" yaml:"max_added_file_bytes_24h,omitempty"`
	ReservedFileBytes    uint64 `json:"reserved_file_bytes,omitempty" yaml:"reserved_file_bytes,omitempty"`
	// Optional Go duration after which this project's terminal job inputs and
	// results expire. It can only shorten the queue-wide --file-retention; empty
	// keeps the queue-wide policy (zero there retains until explicit release).
	FileRetention string `json:"file_retention,omitempty" yaml:"file_retention,omitempty"`
}
type FairUsage struct {
	Seconds float64   `json:"seconds"`
	At      time.Time `json:"at"`
	// Measured demand/service cycle, retained across idle time and restart.
	HorizonSeconds float64 `json:"horizon_seconds,omitempty"`
	WaitSeconds    float64 `json:"wait_seconds,omitempty"`
	ServiceSeconds float64 `json:"service_seconds,omitempty"`
	QuantumSeconds float64 `json:"quantum_seconds,omitempty"`
	// Only current allocations; values are covered seconds, never elapsed age.
	Active map[string]float64 `json:"active,omitempty"`
}

func decayed(u FairUsage, now time.Time) float64 {
	if u.HorizonSeconds <= 0 || u.QuantumSeconds <= 0 {
		return u.Seconds // Initial learning awaits a completed service observation.
	}
	return u.Seconds * math.Exp2(-max(0, now.Sub(u.At).Seconds())/u.HorizonSeconds)
}
func (q *Queue) weight(project string) float64 {
	if p := q.s.Policies[project]; p.Weight > 0 {
		return p.Weight
	}
	return 1
}

// Resource families have separate ledgers: GPU seconds cannot be converted to
// CPU core-seconds. CPU admission/accounting is supplied by the core ledger slice.
func fairResourceFamily(s Spec) string {
	if !s.Needs.cpuOnly() && (s.Needs.MemoryGB > 0 || s.Needs.Model != "") {
		return "gpu"
	}
	return "cpu"
}
func fairUsageKey(tier, family, project string) string {
	key, _ := json.Marshal([3]string{tier, family, project})
	return string(key)
}
func (q *Queue) fairKey(s Spec) string {
	return fairUsageKey(q.priorityMatch(s).Tier, fairResourceFamily(s), s.Project)
}

func ReadProjectPolicies(path string) (policies map[string]ProjectPolicy, err error) {
	defer func() { err = policyFileError(path, err) }()
	if path == "" {
		return nil, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	var p map[string]ProjectPolicy
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	e = d.Decode(&p)
	if e == nil {
		var extra any
		if d.Decode(&extra) != io.EOF {
			e = errors.New("expected one project policy document")
		}
	}
	return p, e
}
func validateProjectPolicies(p map[string]ProjectPolicy) error {
	for name, v := range p {
		if err := validateProjectIdentifier(name); err != nil {
			return err
		}
		if v.DefaultCallPriority != "" && v.DefaultCallPriority != "experiment" && v.DefaultCallPriority != "production" {
			return errors.New("default_call_priority must be experiment or production")
		}

		for _, machine := range v.AllowedMachines {
			if strings.TrimSpace(machine) == "" {
				return errors.New("project allowed_machines must contain nonempty machine names")
			}
		}
		for work, hosts := range v.ProductionParityHosts {
			prefix, name, _ := strings.Cut(work, ":")
			if (prefix != "kind" && prefix != "program" && prefix != "extension") || strings.TrimSpace(name) == "" || work != strings.TrimSpace(work) {
				return errors.New("production_parity_hosts keys must be exact kind:<name>, program:<type> or extension:<name> work identifiers")
			}
			for _, host := range hosts {
				if strings.TrimSpace(host) == "" || host != strings.TrimSpace(host) {
					return errors.New("production_parity_hosts must contain nonempty exact host names")
				}
			}
		}
		if _, err := parseProjectFileRetention(v.FileRetention); err != nil {
			return err
		}
		if name == "" || !validMemoryValue(v.Weight) || v.Weight <= 0 || v.MaxConcurrent < 0 || v.MaxConcurrentExperiments < 0 || !validMemoryValue(v.MaxMemoryShare) || v.MaxMemoryShare > 1 {
			return errors.New("project requires positive finite weight, nonnegative concurrency caps, and memory share in [0,1]")
		}
	}
	return nil
}

func cloneProductionParityHosts(approvals map[string][]string) map[string][]string {
	if approvals == nil {
		return nil
	}
	copy := make(map[string][]string, len(approvals))
	for work, hosts := range approvals {
		copy[work] = slices.Clone(hosts)
	}
	return copy
}

// A physical host may be configured before its first worker check-in.
func (q *Queue) validateProductionParityHosts(approvals map[string][]string) error {
	for _, hosts := range approvals {
		for _, host := range hosts {
			known := q.s.Hosts[host] != nil
			for _, m := range q.s.Machines {
				known = known || hostID(m) == host && m.Replacement == nil
			}
			if !known {
				return fmt.Errorf("production_parity_hosts: unknown host %q; use a configured or registered physical host ID", host)
			}
		}
	}
	return nil
}

// SeedProjects applies startup configuration only before any policy is stored.
// Later operator edits use SetPriorityPolicy with the current revision.
func (q *Queue) SeedProjects(p map[string]ProjectPolicy) error {
	return q.configureProjects(p, true)
}

func (q *Queue) ConfigureProjects(p map[string]ProjectPolicy) error {
	return q.configureProjects(p, false)
}

func (q *Queue) configureProjects(p map[string]ProjectPolicy, seedOnly bool) error {
	if err := validateProjectPolicies(p); err != nil {
		return err
	}
	var previous map[string]ProjectPolicy
	return q.updateTransaction(context.Background(), func() error {
		previous = q.s.Policies
		if seedOnly && (q.s.PriorityPolicy != nil || len(q.s.Policies) > 0) {
			return nil
		}
		copy := map[string]ProjectPolicy{}
		for k, v := range p {
			if err := q.validateMachineNames(v.AllowedMachines); err != nil {
				return err
			}
			if err := q.validateProductionParityHosts(v.ProductionParityHosts); err != nil {
				return err
			}
			v.AllowedMachines = slices.Clone(v.AllowedMachines)
			v.ProductionParityHosts = cloneProductionParityHosts(v.ProductionParityHosts)
			copy[k] = v
		}
		if q.policyRevision() == ^uint64(0) {
			return errors.New("policy revision exhausted")
		}
		policy := q.effectivePriorityPolicy()
		q.s.PriorityPolicy = &PriorityPolicyRecord{Revision: q.policyRevision() + 1, Policy: policy}
		q.s.Policies = copy
		q.seedProjectParity(copy)
		q.mirrorProjectParity()
		return nil
	}, nil, false, func() { q.reindexProjectFileDeadlines(previous) })
}
func (q *Queue) capReason(j *Job) string { return q.capReasonFor(j, nil) }
func (q *Queue) capReasonFor(j *Job, target *Machine) string {
	p := q.s.Policies[j.Spec.Project]
	if p.MaxConcurrent > 0 {
		count := 0
		for _, r := range q.liveJobs() {
			if r.ID != j.ID && r.Spec.Project == j.Spec.Project && (r.State == "running" || r.State == "stopping") {
				count++
			}
		}
		for _, c := range q.calls {
			if c.ID != j.ID && c.Project == j.Spec.Project && c.Machine != "" && c.Error == "" {
				count++
			}
		}
		if count >= p.MaxConcurrent {
			return fmt.Sprintf("%s has reached its concurrent work cap (%d)", j.Spec.Project, p.MaxConcurrent)
		}
	}
	if j.Spec.Priority != "experiment" || p.MaxConcurrentExperiments == 0 && p.MaxMemoryShare == 0 {
		return ""
	}
	need := j.Spec.Needs.MemoryGB
	if j.Spec.Needs.Model != "" && need == 0 {
		model, _ := q.model(j.Spec.Needs.Model)
		need = model.MemoryGB
	}
	count := 0
	used, total := 0.0, 0.0
	for _, m := range q.plannerMachines() {
		if !m.Gone && !m.Leaving {
			total += m.TotalMemoryGB
		}
	}
	for _, r := range q.liveJobs() {
		if r.ID != j.ID && r.Spec.Project == j.Spec.Project && r.Spec.Priority == "experiment" && (r.State == "running" || r.State == "stopping") {
			count++
			used += r.Spec.Needs.MemoryGB
		}
	}
	// Model calls consume their model reservation once per project/machine/model.
	seen := map[string]bool{}
	for _, c := range q.calls {
		if c.Project == j.Spec.Project && c.Priority == "experiment" && c.Machine != "" && c.Error == "" {
			count++
			if j.Spec.Needs.MemoryGB == 0 && j.Spec.Needs.Model == c.Model && (target == nil || target.ID == c.Machine) {
				need = 0
			}
			key := slotKey(c.Machine, c.Model, c.Device)
			if !seen[key] {
				model, _ := q.model(c.Model)
				used += model.MemoryGB
				seen[key] = true
			}
		}
	}
	if p.MaxConcurrentExperiments > 0 && count >= p.MaxConcurrentExperiments {
		return fmt.Sprintf("%s has reached its concurrent experiment cap (%d)", j.Spec.Project, p.MaxConcurrentExperiments)
	}
	if p.MaxMemoryShare > 0 && !j.Spec.Needs.cpuOnly() && (total <= 0 || used+need > total*p.MaxMemoryShare+1e-9) {
		return fmt.Sprintf("%s has reached its GPU memory share cap (%.0f%%)", j.Spec.Project, p.MaxMemoryShare*100)
	}
	return ""
}

func (q *Queue) batchCandidateWithResources(j *Job, m *Machine, resources map[batchResourceKey]batchResourceRoom, scans ...*placementScan) *Job {
	if !q.admissionPass(j, m, !q.environmentFailureBlocked(j, m), predicateEnvironmentFailure) {
		return nil
	}
	if !q.admissionPass(j, m, q.preemptionPlacementAllows(j, m), predicatePreemptionPlacement) || !q.admissionPass(j, m, q.yieldPlacementAllows(j, m), predicateYieldPlacement) {
		return nil
	}
	if j.Spec.FanOut != nil {
		return q.fanOutCandidateWithResources(j, m, resources)
	}
	if j.State != "queued" || len(m.groupMembers) > 0 && !q.admissionPass(j, m, q.mayNeedCards(j, m), predicateCardGroup) || !q.placementAllowed(j, m) || !q.admissionPass(j, m, q.checkpointReady(j, m), predicateCheckpoint) {
		return nil
	}
	variants := extensionJobs(j, m, true)
	q.admissionPass(j, m, len(variants) > 0, predicateExtensionCandidates)
	for _, candidate := range variants {
		if j.Batch != nil && j.Batch.Run {
			candidate = q.retryWaveCandidate(candidate, m)
			if candidate == nil {
				q.admissionPass(j, m, false, predicateRetryWave)
				continue
			}
		}
		q.applyLearnedNeeds(candidate)
		// These final eligibility predicates cannot be changed by memory
		// sizing. Reject incompatible routes before reducing their evidence;
		// live room, reclaim, fairness and all remaining checks still follow.
		if !q.admissionPass(j, m, memorySizingTargetFits(candidate, m), predicateMemoryTarget) || !q.admissionPass(j, m, toolsFit(candidate, m), predicateTools) || !q.admissionPass(j, m, nativeNetworkFits(candidate, m), predicateNativeNetwork) || !q.admissionPass(j, m, q.batchCPUCeilingFits(candidate, m, scans...), predicateCPUCeiling) {
			continue
		}
		candidate = q.memoryCandidate(candidate, m)
		if q.admissionPass(j, m, q.capReasonFor(candidate, m) == "", predicateProjectCapacity) && q.batchResourcesFitWithScan(candidate, m, resources) {
			return candidate
		}
	}
	return nil
}

// A card group takes only work no single card holds. Decide that before the
// costly placement checks, which every queued job would otherwise pay again
// for each multi-card host. A declared plain need is final; learned and
// extension needs are resolved for this target first.
func (q *Queue) mayNeedCards(j *Job, m *Machine) bool {
	if !j.Spec.extensionJob() && j.Spec.Needs.Memory != "auto" {
		return groupFits(j, m)
	}
	for _, c := range q.memoryCandidates(j, m) {
		if groupFits(c, m) && (c.MemoryEnvelope == nil || c.MemoryEnvelope.Refusal == "") {
			return true
		}
	}
	return false
}

// Ordinary projections depend on the concrete vector and tier; capacity evidence
// and scarce yields also depend on the exact demand. Reuse within one read-only device scan and only
// without preparations, whose sharing decision also depends on the recipe.
type batchResourceKey struct {
	project, class, priority string
	yieldDemand              string
	gpu, host                float64
	cores                    int
	shares                   bool
	xpuSharing               bool
}
type batchResourceRoom struct {
	machine  Machine
	memory   float64
	ok       bool
	plan     *placementPlan
	tryYield bool
}

func resourceKey(j *Job) batchResourceKey {
	id := ""
	if len(j.LaunchCapacityWait) > 0 || len(j.Spec.RequirementReasons) > 0 || len(j.Extensions) > 0 {
		id = j.ID
	}
	return batchResourceKey{j.Spec.Project, j.Spec.Class, j.Spec.Priority, id, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB, j.Spec.Needs.cores(), sharesDevice(j), fanOutDeviceSharing(j)}
}

func (q *Queue) batchResourcesFitWithScan(j *Job, m *Machine, resources map[batchResourceKey]batchResourceRoom) bool {
	if !q.admissionHold(j, m, q.variantParityHold(j, m), predicateVariantParity) {
		return false
	}
	key := resourceKey(j)
	room, found := resources[key]
	if !found {
		// #740 made every annotated/extension job miss the whole resource
		// projection cache, even when no scarce-host yield was possible.
		// Share ordinary release work; capacity deficits and the yield fallback
		// retain exact-demand identity.
		ordinaryKey := key
		if len(j.LaunchCapacityWait) == 0 {
			ordinaryKey.yieldDemand = ""
		}
		var ok bool
		room, ok = resources[ordinaryKey]
		if !ok {
			room = q.batchResourceProjection(j, m)
			if resources != nil && len(q.s.PreparationPlans) == 0 {
				resources[ordinaryKey] = room
			}
		}
		if room.tryYield && key.yieldDemand != "" {
			if plan, _ := q.scarceYieldPlan(j, m); plan != nil {
				room = q.batchResourceProjectionWithPlan(j, m, plan)
			}
		}
		if resources != nil && len(q.s.PreparationPlans) == 0 {
			resources[key] = room
		}
	}
	return q.admissionPass(j, m, room.ok, predicateReleasePlan) && q.admissionPass(j, m, fitsResources(j, &room.machine, room.memory), predicateResources)
}
func (q *Queue) batchResourceProjection(j *Job, m *Machine) batchResourceRoom {
	plan, yield := q.ordinaryRoomPlan(j, m, true)
	room := q.batchResourceProjectionWithPlan(j, m, plan)
	room.tryYield = yield
	return room
}

func (q *Queue) batchResourceProjectionWithPlan(j *Job, m *Machine, plan *placementPlan) batchResourceRoom {
	var projected Machine
	if plan != nil {
		projected = q.cpuAfterReleases(m, plan.jobs, plan.slots, plan.preparationCores)
	} else {
		// Future queue order still includes work behind an occupied command
		// slot. Only roomPlan authorizes stops; this estimate cannot admit it.
		jobs, slots, ok := q.hostReleases(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB, j.Spec.Priority, j.Spec)
		if !ok {
			return batchResourceRoom{}
		}
		projected = q.cpuAfterReleases(m, jobs, slots, 0)
	}
	memory := q.batchReclaimableGB(j, m)
	if plan != nil && len(plan.yields) > 0 {
		memory = m.FreeMemoryGB
		for _, v := range plan.jobs {
			if batchOnTarget(v, m) {
				memory += deviceShareGB(v)
			}
		}
		for _, slot := range plan.slots {
			if slotOnTarget(slot, m) {
				memory += q.slotMemory(slot)
			}
		}
		// Each victim returns its ledger charge, as in every release plan.
		room := q.hostRoomAfter(m, plan.jobs, nil)
		projected.FreeHostMemoryGB, projected.hostReservationDebtGB = max(0, room), max(0, -room)
	}
	return batchResourceRoom{machine: projected, memory: memory, ok: true, plan: plan}
}

// A card group offers its smallest card's room once per card.
func (q *Queue) batchReclaimableGB(j *Job, m *Machine) float64 {
	if len(m.groupMembers) > 0 {
		least := math.Inf(1)
		for _, card := range m.groupMembers {
			least = min(least, q.batchReclaimableGB(j, card))
		}
		return least * float64(len(m.groupMembers))
	}
	memory := q.reclaimableGPUMemory(m, j.Spec.Priority, j.Spec)
	if release, ok := q.s.BatchReleases[batchReleaseKey(m)]; ok && release.Session == m.Session && q.clock.Now().Before(release.Until) {
		memory += release.MemoryGB
	}
	// Status may already project a running batch's memory as free.
	if m.TotalMemoryGB > 0 {
		memory = min(memory, m.TotalMemoryGB)
	}
	return memory
}

// candidate is the device-aware planner for batch dispatch, model admission and
// status. Exclusions enumerate later candidates without changing the competing
// projects used to calculate their deficits.
func (q *Queue) candidate(m *Machine, excluded map[string]bool, scans ...*placementScan) *Job {
	return q.candidates(m, scans...).pick(excluded)
}
func (q *Queue) pick(m *Machine, scans ...*placementScan) *Job {
	return q.immediateCandidate(q.candidate(m, nil, scans...), m)
}

func (q *Queue) plannedPick(m *Machine, scan *placementScan) *Job {
	if t := q.admissionTrace; t != nil {
		previous := t.selecting
		t.selecting = true
		defer func() { t.selecting = previous }()
	}
	return q.immediateCandidate(q.assignmentPlan(scan)[targetKey(m)], m)
}

func (q *Queue) immediateCandidate(best *Job, m *Machine) *Job {
	if best != nil && (!q.admissionGate(best, m, !q.preemptionReleasesPending(best), predicateReleasePending) || !q.admissionGate(best, m, !q.preparationActive(m), predicatePreparationActive) || !q.admissionGate(best, m, !q.preparationStopping(m), predicatePreparationStopping) || !q.admissionGate(best, m, q.calls[best.ID] == nil, predicateModelCall) || !q.admissionGate(best, m, fits(best, m, m.FreeMemoryGB), predicateResources) || !q.admissionGate(best, m, environmentReady(best, m), predicateEnvironment)) {
		return nil
	}
	return best
}

func (q *Queue) plannerMachines() []*Machine {
	machines := make([]*Machine, 0, len(q.s.Machines))
	for _, m := range q.s.Machines {
		machines = append(machines, q.placementTargets(m)...)
	}
	slices.SortFunc(machines, func(a, b *Machine) int { return strings.Compare(targetKey(a), targetKey(b)) })
	return machines
}

// queueProjection uses the dispatch comparator on each eligible machine with
// current batch reservations released. Order is conditional on current inventory
// and usage: future arrivals, usage and which worker frees first can change it.
// The holds are the scheduler's wait for each queued job, computed once here;
// status reads them rather than evaluating the wait again.
func (q *Queue) queueProjection() (map[string]int, map[string]string, map[string]*Hold) {
	order, reasons, holds := map[string]int{}, map[string]string{}, map[string]*Hold{}
	scan := q.placementScan()
	sets := make([]*candidateSet, 0, len(q.s.Machines))
	immediate := map[string]*Machine{}
	for _, m := range scan.machines {
		// Explain the current admissible target, even when the queue-order
		// projection also includes a cheaper host whose capacity is occupied.
		if j := q.plannedPick(m, scan); j != nil && q.workerIdle(m, j) && q.placementAllowed(j, m) && q.checkpointReady(j, m) {
			if previous := immediate[j.ID]; previous == nil || q.targetPlacementBetter(j, m, previous, scan) {
				immediate[j.ID] = m
			}
		}
		copy := *m // Projection changes only this scalar memory value; inventory stays read-only.
		cards := m.groupMembers
		if len(cards) == 0 {
			cards = []*Machine{m}
		}
		least := math.Inf(1)
		for _, card := range cards {
			free := card.FreeMemoryGB
			for _, r := range q.liveJobs() {
				if batchOnTarget(r, card) && batchReservesCapacity(r) {
					free += deviceShareGB(r)
				}
			}
			least = min(least, free)
		}
		copy.FreeMemoryGB = least * float64(len(cards))
		// Projection only changes exclusions, never queue state or inventory.
		// Reuse the same read-only planning pass for every projected position.
		sets = append(sets, q.buildCandidates(&copy, scan))
	}
	for _, j := range q.liveJobs() {
		if j.State != "queued" {
			continue
		}
		holds[j.ID] = q.waitHold(j, scan)
		reasons[j.ID] = holds[j.ID].Detail
	}
	excluded := map[string]bool{}
	for n := 1; ; n++ {
		best, selected := q.nextProjection(sets, excluded, scan, false)
		if best == nil {
			break
		}
		order[best.ID] = n
		excluded[best.ID] = true
		target := selected.m
		if current := immediate[best.ID]; current != nil {
			target = current
		}
		// Explain the first competing job, not a separate UI ordering heuristic.
		winner := selected.pick(nil)
		base := "waiting for a worker to pull this job; " + valueReason(q.placementValues(scan)[hostID(target)], target)
		live := q.s.Machines[target.ID]
		for _, view := range q.placementTargets(live) {
			if sameTarget(view, target) {
				live = view
				break
			}
		}
		busy := !q.workerBatchIdle(live)
		batchSlotAllocated := busy
		sharedWith := 0
		if busy {
			if n, ok := q.deviceCoTenants(live, best, true); ok && q.cardSettled(live) {
				sharedWith, busy = n, false
			}
		}
		if call := q.calls[best.ID]; call != nil {
			busy = !q.callCanUse(live, call)
		}
		if busy || !fits(best, live, live.FreeMemoryGB) {
			base = reservationWaitReason(live, batchSlotAllocated && sharedWith == 0, sharedWith)
			if why := q.cardWaitingReason(best, live); why != "" {
				base = why
			}
		}
		if !environmentReady(best, target) {
			if busy {
				base += "; " + strings.Replace(environmentWaitingReason(best, target), "building environment:", "environment not ready:", 1)
			} else {
				base = environmentWaitingReason(best, target)
			}
		}
		if h := holds[best.ID]; h != nil && h.Kind == "waiting_for_builder" {
			base = h.Detail
		}
		base += "; " + selected.fairExplanation(best)
		if winner.ID == best.ID {
			reasons[best.ID] = base + "; next in line"
			continue
		}
		ahead := winner.Spec.Name + " goes first"
		if q.comparePriority(winner.Spec, best.Spec) < 0 {
			ahead = q.priorityMatch(winner.Spec).Tier + " work goes first"
		} else if winner.Spec.Project != best.Spec.Project && fairResourceFamily(winner.Spec) == fairResourceFamily(best.Spec) {
			if selected.score(winner, target) < selected.score(best, target)-1e-6 {
				ahead = fmt.Sprintf("%s goes first (%s is ahead of its fair share)", winner.Spec.Project, best.Spec.Project)
			} else if selected.active(winner) < selected.active(best)-1e-6 {
				ahead = winner.Spec.Project + " goes first (same-tier fair turn)"
			}
		}
		reasons[best.ID] = base + "; " + ahead
	}
	return order, reasons, holds
}

// Admission reservations do not measure compute load. Keep the selected
// planner target and its measured RAM evidence visible without claiming busy CPUs.
func reservationWaitReason(m *Machine, batchSlot bool, sharedWith int) string {
	parts := []string{"waiting for reserved capacity on " + m.ID}
	if batchSlot {
		parts = append(parts, "batch slot allocated")
	}
	if sharedWith > 0 {
		parts = append(parts, fmt.Sprintf("device %s shared with %d attempts; %.1f GiB device memory free", m.Device, sharedWith, m.FreeMemoryGB))
	}
	parts = append(parts, fmt.Sprintf("CPU %d/%d cores reserved; utilization unknown", m.CPUInUseCores, m.EffectiveCPUCores))
	if m.HostMemoryMeasured && m.HostMemoryError == "" {
		parts = append(parts, fmt.Sprintf("RAM live %.1f GiB available; %.1f GiB reserved; %.1f GiB admission headroom", m.HostAvailableMemoryGB, m.HostMemoryInUseGB, m.FreeHostMemoryGB))
	} else {
		parts = append(parts, "RAM live availability unknown")
	}
	return strings.Join(parts, "; ")
}
func (q *Queue) waitReason(j *Job, scans ...*placementScan) string {
	return q.waitHold(j, scans...).Detail
}

// waitHold is the scheduler's current reason for a queued job, tagged with the
// mechanism that produced it. Each branch names its kind where it decides, so
// status and resolve never infer the mechanism from the reason text.
func (q *Queue) waitHold(j *Job, scans ...*placementScan) *Hold {
	if c := q.calls[j.ID]; c != nil {
		return q.callWait(c)
	}
	// Every target's checkpoint admission fails while the queue's own root is
	// unreachable, so that is the wait for runnable work, whatever else holds it.
	if j.State == "queued" && needsCheckpoint(j) && !q.checkpointsAvailable() && dependencyReady(j) {
		return newHold("checkpoint_root", "NAS checkpoint root is unreachable")
	}
	if reason := q.deviceFaultRetryWaitingReason(j, scans...); reason != "" {
		return newHold(deviceFaultRetryHold, reason)
	}
	if j.State == "queued" && q.launchCapacityPending(j) {
		return launchCapacityHold(j)
	}
	if why := q.yieldWaitReason(j); why != "" {
		return newHold("scarce_host_yield", why)
	}
	if plan := q.yieldProjection(j); plan != nil && plan.Reason != "" {
		return newHold("scarce_host_yield", plan.Reason)
	}
	if h := q.hostRetryStorageWait(j); h != nil {
		return h
	}
	if !dependencyReady(j) {
		if j.Spec.After != nil && j.Spec.After.Piece == "" {
			return newHold("upstream", "waiting for upstream fan-out parent success")
		}
		return newHold("upstream", "waiting for accepted producer piece")
	}
	if h := q.variantParityWait(j); h != nil {
		return h
	}
	if h := q.fleetPauseHold(j); h != nil {
		return h
	}
	if g := j.Spec.Needs.GPU; g != nil && g.Devices != nil {
		found := false
		for _, target := range q.planningMachines(scans...) {
			if !target.Gone && q.jobAllowsMachine(j, target.ID) && deviceAllowed(j, target) && target.MemoryError == "" && (len(j.Spec.Needs.Platform.Required) == 0 || slices.Contains(j.Spec.Needs.Platform.Required, target.GPU)) {
				found = true
				break
			}
		}
		if !found {
			return newHold("no_matching_device", "no_matching_device: no selected device is online and compatible")
		}
	}
	if reason := q.logsWaitingReason(j); reason != "" {
		return newHold("log_policy", reason)
	}
	if reason := q.capReason(j); reason != "" {
		return newHold("project_cap", reason)
	}
	authored := j // Explained as submitted; the model's sizing serves placement checks.
	if model, ok := q.model(j.Spec.Needs.Model); ok && j.Spec.Needs.MemoryGB == 0 {
		copy := cloneJob(j)
		copy.Spec.Needs.MemoryGB = model.MemoryGB
		copy.Spec.Needs.HostMemoryGB = model.HostMemoryGB
		j = &copy
	}
	if h := q.checkpointTargetWait(j, scans...); h != nil {
		return h
	}
	if h := q.holdWait(j); h != nil {
		return h
	}
	if h := q.targetWait(j, scans...); h != nil {
		return h
	}
	machines := map[string]*Machine{}
	for _, m := range q.planningMachines(scans...) {
		if q.projectAllows(j.Spec.Project, m.ID) {
			machines[targetKey(m)] = m
		}
	}
	if reason := q.environmentFailureWaitingReason(j, machines); reason != "" {
		return newHold("deterministically_failed", reason)
	}
	if reason := q.preparationWaitingReason(j, machines); reason != "" {
		return newHold("waiting_for_builder", reason)
	}
	if h := q.memoryWait(j, machines); h != nil {
		return h
	}
	if j.Spec.Needs.cpuOnly() {
		if reason := q.hostBackfillWaitingReason(j, q.placementScan(scans...)); reason != "" {
			return newHold("host_backfill", reason)
		}
	}
	h := waitingHold(j, machines, q.s.Jobs)
	if h.Kind == "waiting_for_worker" || h.Kind == "no_eligible_host" {
		h.Detail = q.noFitDetail(authored, h.Kind == "waiting_for_worker")
	}
	return h
}

// Deficit uses only competing runnable projects. An absent project reserves no
// capacity; an otherwise idle device is always available to its sole project.
func (q *Queue) fairScore(j *Job, m *Machine) float64 {
	if m.ID != "" {
		return q.candidates(m).score(j, m)
	}
	projects := map[string]bool{j.Spec.Project: true}
	for _, r := range q.liveJobs() {
		if dependencyReady(r) && r.State == "queued" && q.comparePriority(r.Spec, j.Spec) == 0 && fairResourceFamily(r.Spec) == fairResourceFamily(j.Spec) && q.capReason(r) == "" {
			projects[r.Spec.Project] = true
		}
	}
	for _, c := range q.calls {
		r := callJob(c)
		if c.Machine == "" && c.Error == "" && q.comparePriority(r.Spec, j.Spec) == 0 && fairResourceFamily(r.Spec) == fairResourceFamily(j.Spec) && q.capReason(r) == "" && (m.ID == "" || q.callCanUse(m, c)) {
			projects[c.Project] = true
		}
	}
	return q.fairScoreProjects(j, projects)
}

func (q *Queue) fairScoreProjects(j *Job, projects map[string]bool) float64 {
	total, weights, largest := 0.0, 0.0, 1.0
	for p := range projects {
		largest = max(largest, q.weight(p))
	}
	for p := range projects {
		key := fairUsageKey(q.priorityMatch(j.Spec).Tier, fairResourceFamily(j.Spec), p)
		total += decayed(q.s.FairUsage[key], q.clock.Now())
		weights += q.weight(p) / largest
	}
	return decayed(q.s.FairUsage[q.fairKey(j.Spec)], q.clock.Now()) - total*(q.weight(j.Spec.Project)/largest)/weights
}

// Equal usage must also share freshly freed capacity before the clock advances.
// This is a tie-break, not a reservation or same-tier preemption mechanism.
func (q *Queue) fairActive(j *Job) float64 {
	count := 0.0
	for _, m := range q.plannerMachines() {
		for _, row := range q.deviceUsage(m) {
			if row.Fair > 0 && row.Project == j.Spec.Project && row.Tier == q.priorityMatch(j.Spec).Tier && row.Family == fairResourceFamily(j.Spec) {
				if row.Family == "cpu" {
					count += row.Fair
				} else {
					count++
				}
			}
		}
	}
	return count / q.weight(j.Spec.Project)
}

// Machine restrictions are policy, independent of temporary availability or capacity.
func machineAllowed(allowed []string, machine string) bool {
	return len(allowed) == 0 || contains(allowed, machine)
}

func (s Spec) allowsMachine(machine string) bool {
	return machineAllowed(s.AllowedMachines, machine) && !contains(s.ExcludedMachines, machine)
}

func (q *Queue) validateJobExclusions(s Spec) error {
	if len(s.ExcludedMachines) == 0 {
		return nil
	}
	if err := q.validateMachineNames(s.ExcludedMachines); err != nil {
		return fmt.Errorf("excluded_machines: %w", err)
	}
	allowed := q.s.Policies[s.Project].AllowedMachines
	if len(allowed) > 0 {
		for _, name := range allowed {
			if s.allowsMachine(name) {
				return nil
			}
		}
		return errors.New("excluded_machines leaves no overlap with project allowed_machines")
	}
	return nil
}

func (q *Queue) jobAllowsMachine(j *Job, machine string) bool {
	return j.Spec.allowsMachine(machine) && q.projectAllows(j.Spec.Project, machine) && q.entryCoverageAllows(j, machine)
}

func (q *Queue) projectAllows(project, machine string) bool {
	v := q.s.Policies[project].AllowedMachines
	return machineAllowed(v, machine)
}

func (q *Queue) validateProjectPlacement(project string, allowed []string) error {
	if err := q.validateMachineNames(allowed); err != nil {
		return err
	}
	p := q.s.Policies[project].AllowedMachines
	if len(p) > 0 && len(allowed) > 0 {
		for _, m := range allowed {
			if contains(p, m) {
				return nil
			}
		}
		return fmt.Errorf("project %s allowed_machines has no overlap with job allowed_machines", project)
	}
	return nil
}

// Missing inventory can return later; reject only restrictions whose entire
// last-known fleet is structurally incompatible, never temporary occupancy.
func (q *Queue) validateProjectCapacity(j *Job) error {
	if g := j.Spec.Needs.GPU; g != nil && g.Devices != nil {
		return nil
	} // Selected devices may return later.
	if model, ok := q.model(j.Spec.Needs.Model); ok && j.Spec.Needs.MemoryGB == 0 {
		copy := cloneJob(j)
		copy.Spec.Needs.MemoryGB = model.MemoryGB
		copy.Spec.Needs.Platform = Platform{Required: model.Platforms}
		j = &copy
	}
	allowed := q.s.Policies[j.Spec.Project].AllowedMachines
	if len(allowed) == 0 {
		return nil
	}
	for _, name := range allowed {
		if !j.Spec.allowsMachine(name) {
			continue
		}
		m := q.s.Machines[name]
		if m == nil || m.TotalMemoryGB == 0 {
			return nil
		}
		if _, reason := cpuCapacity(m); reason != "" {
			return nil // Unknown telemetry can return; it is not impossibility.
		}
		for _, target := range q.placementTargets(m) {
			copy := *target
			copy.DiskVolumes = diskCapacityVolumes(copy.DiskVolumes)
			copy.Gone = false
			copy.Leaving = false
			copy.MemoryError = ""
			copy.CheckpointUnavailable = false
			// Submit validation uses total admitted capacity, never current occupancy.
			copy.CPUInUseCores = 0
			copy.FreeCPUCores = copy.EffectiveCPUCores
			copy.CPUCapacityReason = "" // Acknowledgement affects dispatch only.
			possible := false
			for _, candidate := range extensionJobs(j, &copy, false) {
				value := *candidate
				// Preserve this candidate's frozen policy before suppressing extension
				// qualification. Public isolation text and other alternatives grant nothing.
				value.Spec.Isolation, value.Spec.Reason = "sandboxed", ""
				if nativeCIRequired(candidate) {
					value.Spec.Isolation = nativeIsolationCI
				}
				if reasons := nativeUnsandboxedReasons(candidate); len(reasons) > 0 {
					value.Spec.Isolation, value.Spec.Reason = "unsandboxed", reasons[0]
				}
				value.Spec.Kind, value.Spec.Program = "", nil
				value.Spec.Extension, value.Spec.ExtensionDigest, value.Spec.ExtensionCheck = "", "", nil
				var fit targetFit
				fitsResourcesChecked(&value, &copy, copy.TotalMemoryGB, &fit)
				unknownCacheOnly := copy.SharedInputCache == nil && fit.failures == 1 && fit.first == "shared input cache"
				if fit.failures == 0 || unknownCacheOnly {
					possible = true
					break
				}
			}
			if !possible {
				continue
			}
			if model, ok := q.model(j.Spec.Needs.Model); ok && !q.modelFitsMachine(model, target) {
				continue
			}
			return nil
		}
	}
	return fmt.Errorf("no compatible machine in project %s allowed_machines (last-known platform, capacity and environment)", j.Spec.Project)
}
