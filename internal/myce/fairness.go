package myce

import (
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
	DefaultCallPriority      string   `json:"default_call_priority,omitempty" yaml:"default_call_priority,omitempty"`
	AllowedMachines          []string `json:"allowed_machines,omitempty" yaml:"allowed_machines,omitempty"`
	MaxConcurrent            int      `json:"max_concurrent,omitempty" yaml:"max_concurrent,omitempty"`
	Weight                   float64  `json:"weight" yaml:"weight"`
	MaxConcurrentExperiments int      `json:"max_concurrent_experiments" yaml:"max_concurrent_experiments"`
	MaxMemoryShare           float64  `json:"max_memory_share" yaml:"max_memory_share"`
}
type FairUsage struct {
	Seconds float64   `json:"seconds"`
	At      time.Time `json:"at"`
}

const FairHalfLife = 24 * time.Hour

func decayed(u FairUsage, now time.Time) float64 {
	return u.Seconds * math.Exp2(-max(0, now.Sub(u.At).Hours())/FairHalfLife.Hours())
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
		if name == "" || !validMemoryValue(v.Weight) || v.Weight <= 0 || v.MaxConcurrent < 0 || v.MaxConcurrentExperiments < 0 || !validMemoryValue(v.MaxMemoryShare) || v.MaxMemoryShare > 1 {
			return errors.New("project requires positive finite weight, nonnegative concurrency caps, and memory share in [0,1]")
		}
	}
	return nil
}
func (q *Queue) ConfigureProjects(p map[string]ProjectPolicy) error {
	if err := validateProjectPolicies(p); err != nil {
		return err
	}
	return q.update(func() error {
		copy := map[string]ProjectPolicy{}
		for k, v := range p {
			if err := q.validateMachineNames(v.AllowedMachines); err != nil {
				return err
			}
			v.AllowedMachines = slices.Clone(v.AllowedMachines)
			copy[k] = v
		}
		if q.policyRevision() == ^uint64(0) {
			return errors.New("policy revision exhausted")
		}
		policy := q.effectivePriorityPolicy()
		q.s.PriorityPolicy = &PriorityPolicyRecord{Revision: q.policyRevision() + 1, Policy: policy}
		q.s.Policies = copy
		return nil
	})
}
func (q *Queue) capReason(j *Job) string { return q.capReasonFor(j, nil) }
func (q *Queue) capReasonFor(j *Job, target *Machine) string {
	p := q.s.Policies[j.Spec.Project]
	if p.MaxConcurrent > 0 {
		count := 0
		for _, r := range q.s.Jobs {
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
	for _, r := range q.s.Jobs {
		if r.Spec.Project == j.Spec.Project && r.Spec.Priority == "experiment" && (r.State == "running" || r.State == "stopping") {
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

// batchCanUse includes memory that the existing release planner can reclaim.
// This lets a batch's fair turn precede an idle resident model, without admitting
// the batch until the worker has acknowledged the release.
func (q *Queue) batchCanUse(j *Job, m *Machine) bool {
	return q.batchCandidate(j, m) != nil
}

func (q *Queue) batchCandidate(j *Job, m *Machine) *Job {
	if j.State != "queued" || !q.placementAllowed(j, m) || !q.checkpointReady(j, m) {
		return nil
	}
	for _, candidate := range extensionJobs(j, m, true) {
		q.applyLearnedNeeds(candidate)
		if q.capReasonFor(candidate, m) == "" && q.batchResourcesFit(candidate, m) {
			// Leave accelerator workers for GPU work when CPU placement is free.
			// Filter here so the same planner can consider the next candidate.
			if candidate.Spec.Needs.cpuOnly() && m.GPU != "cpu" && q.betterPlacement(candidate, m) {
				continue
			}
			return candidate
		}
	}
	return nil
}

func (q *Queue) batchResourcesFit(j *Job, m *Machine) bool {
	plan := q.roomPlan(j, m, true)
	var projected Machine
	if plan != nil {
		projected = q.cpuAfterReleases(m, plan.jobs, plan.slots, plan.preparationCores)
	} else {
		// Future queue order still includes work behind an occupied command
		// slot. Only roomPlan authorizes stops; this estimate cannot admit it.
		jobs, slots, ok := q.hostReleases(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB, j.Spec.Priority, j.Spec)
		if !ok {
			return false
		}
		projected = q.cpuAfterReleases(m, jobs, slots, 0)
	}
	memory := q.reclaimableGPUMemory(m, j.Spec.Priority, j.Spec)
	if release, ok := q.s.BatchReleases[batchReleaseKey(m)]; ok && release.Session == m.Session && q.clock.Now().Before(release.Until) {
		memory += release.MemoryGB
	}
	// Status may already project a running batch's memory as free.
	if m.TotalMemoryGB > 0 {
		memory = min(memory, m.TotalMemoryGB)
	}
	return fitsResources(j, &projected, memory)
}

// candidate is the device-aware planner for batch dispatch, model admission and
// status. Exclusions enumerate later candidates without changing the competing
// projects used to calculate their deficits.
func (q *Queue) candidate(m *Machine, excluded map[string]bool) *Job {
	// Fair score is constant within a project on this worker. Compare each
	// project's best ordinary candidate instead of rescanning for every queued job.
	byProject := map[string]*Job{}
	for _, j := range q.s.Jobs {
		if excluded[j.ID] {
			continue
		}
		if j := q.batchCandidate(j, m); j != nil {
			old := byProject[j.Spec.Project]
			if old == nil || q.before(j, old, m) {
				byProject[j.Spec.Project] = j
			}
		}
	}
	for _, c := range q.calls {
		if excluded[c.ID] || !q.callCanUse(m, c) {
			continue
		}
		j := callJob(c)
		j.State = "queued"
		j.Spec.Name = "model call: " + c.Model
		old := byProject[j.Spec.Project]
		if old == nil || q.before(j, old, m) {
			byProject[j.Spec.Project] = j
		}
	}
	projects := make([]string, 0, len(byProject))
	for p := range byProject {
		projects = append(projects, p)
	}
	slices.Sort(projects)
	var best *Job
	for _, p := range projects {
		j := byProject[p]
		if best == nil || q.before(j, best, m) {
			best = j
		}
	}

	return best
}
func (q *Queue) pick(m *Machine) *Job {
	best := q.candidate(m, nil)
	if best != nil && (q.preparationActive(m) || q.preparationStopping(m) || q.calls[best.ID] != nil || !fits(best, m, m.FreeMemoryGB) || !environmentReady(best, m)) {
		return nil
	}
	return best
}

func (q *Queue) plannerMachines() []*Machine {
	machines := make([]*Machine, 0, len(q.s.Machines))
	for _, m := range q.s.Machines {
		machines = append(machines, q.machineTargets(m)...)
	}
	slices.SortFunc(machines, func(a, b *Machine) int { return strings.Compare(targetKey(a), targetKey(b)) })
	return machines
}

// queueProjection uses the dispatch comparator on each eligible machine with
// current batch reservations released. Order is conditional on current inventory
// and usage: future arrivals, usage and which worker frees first can change it.
func (q *Queue) queueProjection() (map[string]int, map[string]string) {
	order, reasons := map[string]int{}, map[string]string{}
	machines := make([]*Machine, 0, len(q.s.Machines))
	for _, m := range q.plannerMachines() {
		copy := *m // Projection changes only this scalar memory value; inventory stays read-only.
		for _, r := range q.s.Jobs {
			if batchOnTarget(r, m) && (r.State == "running" || r.State == "stopping") {
				copy.FreeMemoryGB += r.Spec.Needs.MemoryGB
			}
		}
		machines = append(machines, &copy)
	}
	for _, j := range q.s.Jobs {
		if j.State != "queued" {
			continue
		}
		reasons[j.ID] = q.waitReason(j)
	}
	excluded := map[string]bool{}
	for n := 1; ; n++ {
		var best *Job
		var target *Machine
		for _, m := range machines {
			winner := q.candidate(m, excluded)
			if winner != nil && (best == nil || q.before(winner, best, m)) {
				best, target = winner, m
			}
		}
		if best == nil {
			break
		}
		order[best.ID] = n
		excluded[best.ID] = true
		// Explain the first competing job, not a separate UI ordering heuristic.
		winner := q.candidate(target, nil)
		base := "waiting for a worker to pull this job"
		live := q.s.Machines[target.ID]
		for _, view := range q.machineTargets(live) {
			if sameTarget(view, target) {
				live = view
				break
			}
		}
		busy := false
		for _, r := range q.s.Jobs {
			if batchOnTarget(r, live) && (r.State == "running" || r.State == "stopping") {
				busy = true
			}
		}
		if call := q.calls[best.ID]; call != nil {
			busy = !q.callCanUse(live, call)
		}
		if busy || !fits(best, live, live.FreeMemoryGB) {
			base = "waiting for a free machine"
		}
		if !environmentReady(best, target) {
			if busy {
				base += "; environment not ready: " + best.Spec.Environment.Key()
			} else {
				base = "building environment: " + best.Spec.Environment.Key()
			}
		}
		if winner.ID == best.ID {
			reasons[best.ID] = base + "; next in line"
			continue
		}
		ahead := winner.Spec.Name + " goes first"
		if q.comparePriority(winner.Spec, best.Spec) < 0 {
			ahead = q.priorityMatch(winner.Spec).Tier + " work goes first"
		} else if winner.Spec.Project != best.Spec.Project && fairResourceFamily(winner.Spec) == fairResourceFamily(best.Spec) {
			if q.fairScore(winner, target) < q.fairScore(best, target)-1e-6 {
				ahead = fmt.Sprintf("%s goes first (%s is ahead of its fair share)", winner.Spec.Project, best.Spec.Project)
			} else if q.fairActive(winner) < q.fairActive(best)-1e-6 {
				ahead = winner.Spec.Project + " goes first (same-tier fair turn)"
			}
		}
		reasons[best.ID] = base + "; " + ahead
	}
	return order, reasons
}
func (q *Queue) waitReason(j *Job) string {
	if g := j.Spec.Needs.GPU; g != nil && g.Devices != nil {
		found := false
		for _, target := range q.plannerMachines() {
			if !target.Gone && q.jobAllowsMachine(j, target.ID) && deviceAllowed(j, target) && target.MemoryError == "" && (len(j.Spec.Needs.Platform.Required) == 0 || slices.Contains(j.Spec.Needs.Platform.Required, target.GPU)) {
				found = true
				break
			}
		}
		if !found {
			return "no_matching_device: no selected device is online and compatible"
		}
	}
	if reason := q.logsWaitingReason(j); reason != "" {
		return reason
	}
	if reason := q.capReason(j); reason != "" {
		return reason
	}
	if model, ok := q.model(j.Spec.Needs.Model); ok && j.Spec.Needs.MemoryGB == 0 {
		copy := cloneJob(j)
		copy.Spec.Needs.MemoryGB = model.MemoryGB
		copy.Spec.Needs.HostMemoryGB = model.HostMemoryGB
		j = &copy
	}
	if r := q.holdWaitReason(j); r != "" {
		return r
	}
	machines := map[string]*Machine{}
	for _, m := range q.plannerMachines() {
		if q.projectAllows(j.Spec.Project, m.ID) {
			machines[targetKey(m)] = m
		}
	}
	return waitingReason(j, machines, q.s.Jobs)
}

// Deficit uses only competing runnable projects. An absent project reserves no
// capacity; an otherwise idle device is always available to its sole project.
func (q *Queue) fairScore(j *Job, m *Machine) float64 {
	projects := map[string]bool{j.Spec.Project: true}
	for _, r := range q.s.Jobs {
		if m.ID != "" {
			r = q.batchCandidate(r, m)
			if r == nil {
				continue
			}
		}
		if r.State == "queued" && q.comparePriority(r.Spec, j.Spec) == 0 && fairResourceFamily(r.Spec) == fairResourceFamily(j.Spec) && q.capReason(r) == "" {
			projects[r.Spec.Project] = true
		}
	}
	for _, c := range q.calls {
		r := callJob(c)
		if c.Machine == "" && c.Error == "" && q.comparePriority(r.Spec, j.Spec) == 0 && fairResourceFamily(r.Spec) == fairResourceFamily(j.Spec) && q.capReason(r) == "" && (m.ID == "" || q.callCanUse(m, c)) {
			projects[c.Project] = true
		}
	}
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

func (q *Queue) jobAllowsMachine(j *Job, machine string) bool {
	return machineAllowed(j.Spec.AllowedMachines, machine) && q.projectAllows(j.Spec.Project, machine)
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
		if len(j.Spec.AllowedMachines) > 0 && !contains(j.Spec.AllowedMachines, name) {
			continue
		}
		m := q.s.Machines[name]
		if m == nil || m.TotalMemoryGB == 0 {
			return nil
		}
		if _, reason := cpuCapacity(m); reason != "" {
			return nil // Unknown telemetry can return; it is not impossibility.
		}
		for _, target := range q.machineTargets(m) {
			copy := *target
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
				value.Spec.Kind, value.Spec.Program = "", nil
				value.Spec.Extension, value.Spec.ExtensionDigest, value.Spec.ExtensionCheck = "", "", nil
				if fitsResources(&value, &copy, copy.TotalMemoryGB) {
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
