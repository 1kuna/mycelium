package myce

import (
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

// Caps limit experiments; allowed_machines restricts every priority and model call.
type ProjectPolicy struct {
	AllowedMachines          []string `json:"allowed_machines,omitempty" yaml:"allowed_machines,omitempty"`
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
func (q *Queue) fairBefore(a, b *Job, m *Machine) bool {
	// Production and deadlines retain their established order.
	if a.Spec.Priority == "experiment" && b.Spec.Priority == "experiment" && a.Spec.Project != b.Spec.Project {
		x := q.fairScore(a, m)
		y := q.fairScore(b, m)
		if math.Abs(x-y) > 1e-6 {
			return x < y
		}
	}
	return before(a, b, m)
}
func ReadProjectPolicies(path string) (map[string]ProjectPolicy, error) {
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
func (q *Queue) ConfigureProjects(p map[string]ProjectPolicy) error {
	for name, v := range p {
		for _, machine := range v.AllowedMachines {
			if strings.TrimSpace(machine) == "" {
				return errors.New("project allowed_machines must contain nonempty machine names")
			}
		}
		if name == "" || !validMemoryValue(v.Weight) || v.Weight <= 0 || v.MaxConcurrentExperiments < 0 || !validMemoryValue(v.MaxMemoryShare) || v.MaxMemoryShare > 1 {
			return errors.New("project requires positive finite weight, nonnegative experiment cap, and memory share in [0,1]")
		}
	}
	return q.update(func() error {
		copy := map[string]ProjectPolicy{}
		for k, v := range p {
			v.AllowedMachines = slices.Clone(v.AllowedMachines)
			copy[k] = v
		}
		q.s.Policies = copy
		return nil
	})
}
func (q *Queue) capReason(j *Job) string { return q.capReasonFor(j, nil) }
func (q *Queue) capReasonFor(j *Job, target *Machine) string {
	if j.Spec.Priority != "experiment" {
		return ""
	}
	p := q.s.Policies[j.Spec.Project]
	if p.MaxConcurrentExperiments == 0 && p.MaxMemoryShare == 0 {
		return ""
	}
	need := j.Spec.Needs.MemoryGB
	if j.Spec.Needs.Model != "" && need == 0 {
		model, _ := q.model(j.Spec.Needs.Model)
		need = model.MemoryGB
	}
	count := 0
	used, total := 0.0, 0.0
	for _, m := range q.s.Machines {
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
			key := slotKey(c.Machine, c.Model)
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
	if p.MaxMemoryShare > 0 && (total <= 0 || used+need > total*p.MaxMemoryShare+1e-9) {
		return fmt.Sprintf("%s has reached its GPU memory share cap (%.0f%%)", j.Spec.Project, p.MaxMemoryShare*100)
	}
	return ""
}

// candidate is shared by dispatch and the status projection. Exclusions only
// remove jobs already listed in the projection; they never change dispatch.
func (q *Queue) candidate(m *Machine, excluded map[string]bool) *Job {
	// Fair score is constant within a project on this worker. Compare each
	// project's best ordinary candidate instead of rescanning for every queued job.
	byProject := map[string]*Job{}
	for _, j := range q.s.Jobs {
		if !excluded[j.ID] && j.State == "queued" && q.placementAllowed(j, m) && q.checkpointReady(j, m) && fits(j, m, m.FreeMemoryGB) && q.capReason(j) == "" {
			old := byProject[j.Spec.Project]
			if old == nil || before(j, old, m) {
				byProject[j.Spec.Project] = j
			}
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
		if best == nil || q.fairBefore(j, best, m) {
			best = j
		}
	}

	return best
}
func (q *Queue) pick(m *Machine) *Job {
	best := q.candidate(m, nil)
	if best != nil && !environmentReady(best, m) {
		return nil
	}
	return best
}

// projectionCandidate also includes model calls that dispatch permits on this
// device; a waiting call must not disappear from a command's next-in-line reason.
func (q *Queue) projectionCandidate(m *Machine, excluded map[string]bool) *Job {
	best := q.candidate(m, excluded)
	for _, c := range q.calls {
		if excluded[c.ID] || !q.callCanUse(m, c) {
			continue
		}
		j := callJob(c)
		j.State = "queued"
		j.Spec.Name = "model call: " + c.Model
		if best == nil || q.fairBefore(j, best, m) {
			best = j
		}
	}
	return best
}

// queueProjection uses the dispatch comparator on each eligible machine with
// current batch reservations released. Order is conditional on current inventory
// and usage: future arrivals, usage and which worker frees first can change it.
func (q *Queue) queueProjection() (map[string]int, map[string]string) {
	order, reasons := map[string]int{}, map[string]string{}
	machines := make([]*Machine, 0, len(q.s.Machines))
	for _, m := range q.s.Machines {
		copy := *m // Projection changes only this scalar memory value; inventory stays read-only.
		for _, r := range q.s.Jobs {
			if r.Machine == m.ID && (r.State == "running" || r.State == "stopping") {
				copy.FreeMemoryGB += r.Spec.Needs.MemoryGB
			}
		}
		machines = append(machines, &copy)
	}
	slices.SortFunc(machines, func(a, b *Machine) int { return strings.Compare(a.ID, b.ID) })
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
			winner := q.projectionCandidate(m, excluded)
			if winner != nil && (best == nil || q.fairBefore(winner, best, m)) {
				best, target = winner, m
			}
		}
		if best == nil {
			break
		}
		order[best.ID] = n
		excluded[best.ID] = true
		// Explain the first competing job, not a separate UI ordering heuristic.
		winner := q.projectionCandidate(target, nil)
		base := "waiting for a worker to pull this job"
		live := q.s.Machines[target.ID]
		busy := false
		for _, r := range q.s.Jobs {
			if r.Machine == live.ID && (r.State == "running" || r.State == "stopping") {
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
		if rank(winner.Spec.Priority) < rank(best.Spec.Priority) {
			ahead = winner.Spec.Priority + " work goes first"
		} else if best.Spec.Priority == "experiment" && winner.Spec.Project != best.Spec.Project && q.fairScore(winner, target) < q.fairScore(best, target)-1e-6 {
			ahead = fmt.Sprintf("%s goes first (%s is ahead of its fair share)", winner.Spec.Project, best.Spec.Project)
		}
		reasons[best.ID] = base + "; " + ahead
	}
	return order, reasons
}
func (q *Queue) waitReason(j *Job) string {
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
	for id, m := range q.s.Machines {
		if q.projectAllows(j.Spec.Project, id) {
			machines[id] = m
		}
	}
	return waitingReason(j, machines, q.s.Jobs)
}

// Deficit uses only competing runnable projects. An absent project reserves no
// capacity; an otherwise idle device is always available to its sole project.
func (q *Queue) fairScore(j *Job, m *Machine) float64 {
	projects := map[string]bool{j.Spec.Project: true}
	for _, r := range q.s.Jobs {
		if r.State == "queued" && r.Spec.Priority == "experiment" && q.capReason(r) == "" && (m.ID == "" || q.placementAllowed(r, m) && fits(r, m, m.FreeMemoryGB)) {
			projects[r.Spec.Project] = true
		}
	}
	for _, c := range q.calls {
		if c.Machine == "" && c.Error == "" && c.Priority == "experiment" && q.capReason(callJob(c)) == "" && (m.ID == "" || q.callCanUse(m, c)) {
			projects[c.Project] = true
		}
	}
	total, weights := 0.0, 0.0
	largest := 1.0
	for p := range projects {
		largest = max(largest, q.weight(p))
	}
	for p := range projects {
		total += decayed(q.s.FairUsage[p], q.clock.Now())
		weights += q.weight(p) / largest
	}
	return decayed(q.s.FairUsage[j.Spec.Project], q.clock.Now()) - total*(q.weight(j.Spec.Project)/largest)/weights
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
		copy := *m
		copy.Gone = false
		copy.Leaving = false
		copy.MemoryError = ""
		copy.CheckpointUnavailable = false
		structural := j
		if j.Spec.extensionJob() {
			value := cloneJob(j)
			value.Spec.Kind = ""
			value.Spec.Program = nil
			structural = &value
		}
		if !fitsWithoutHostMemory(structural, &copy, copy.TotalMemoryGB) {
			continue
		}
		if model, ok := q.model(j.Spec.Needs.Model); ok && !modelFitsMachine(model, m) {
			continue
		}
		return nil
	}
	return fmt.Errorf("no compatible machine in project %s allowed_machines (last-known platform, capacity and environment)", j.Spec.Project)
}
