package myce

import (
	"errors"
	"math"
	"sort"
	"time"
)

type CopyReport struct {
	Attempt        string  `json:"attempt,omitempty"`
	BytesPerSecond float64 `json:"bytes_per_second,omitempty"`
	Model          string  `json:"model"`
	State          string  `json:"state"`
	Bytes          int64   `json:"bytes"`
	Total          int64   `json:"total"`
	Error          string  `json:"error,omitempty"`
}
type CopyTask struct {
	Launch *HostLaunch `json:"launch,omitempty"`
	Model  string      `json:"model"`
	Score  int         `json:"score"`
}
type SyncRequest struct {
	Model  string `json:"model"`
	To     string `json:"to"`
	Cancel bool   `json:"cancel,omitempty"`
}
type ModelUse struct {
	Count int       `json:"count"`
	At    time.Time `json:"at"`
}

func targetMatches(to string, m *Machine) bool { return to == "all" || to == m.ID || to == m.GPU }
func (q *Queue) SyncModel(r SyncRequest) error {
	return q.update(func() error {
		model, ok := q.model(r.Model)
		if !ok || model.SizeBytes <= 0 || model.DirectNAS {
			return errors.New("sync requires a catalog model with verified local-cache bytes")
		}
		if r.To == "" {
			return errors.New("sync requires --to machine, platform, or all")
		}
		known := r.To == "all" || validGPU(r.To)
		if _, ok := q.s.Machines[r.To]; ok {
			known = true
		}
		if !known {
			return errors.New("unknown sync target (join machine first)")
		}
		for i, s := range q.s.SyncRequests {
			if s.Model == r.Model && s.To == r.To {
				q.s.SyncRequests[i] = r
				return nil
			}
		}
		q.s.SyncRequests = append(q.s.SyncRequests, r)
		return nil
	})
}

// Pins protect and warm all possible landing machines, including outside a window.
func (q *Queue) cachePins(m *Machine) []string {
	out := []string{}
	for _, p := range q.s.Pins {
		model, ok := q.model(p.Model)
		if ok && q.projectAllows(p.Project, m.ID) && q.modelFitsMachine(model, m) && (p.Machine == "" || p.Machine == m.ID) && (p.Platform == "" || p.Platform == m.GPU) && !contains(out, p.Model) {
			out = append(out, p.Model)
		}
	}
	sort.Strings(out)
	return out
}
func (q *Queue) modelScores(m *Machine) map[string]int {
	scores := map[string]int{}
	for _, j := range q.s.Jobs {
		if j.Spec.Needs.Model == "" || len(j.Spec.AllowedMachines) > 0 && !contains(j.Spec.AllowedMachines, m.ID) || len(j.Spec.Needs.Platform.Required) > 0 && !contains(j.Spec.Needs.Platform.Required, m.GPU) {
			continue
		}
		if j.State == "queued" || j.State == "running" || j.State == "stopping" {
			scores[j.Spec.Needs.Model]++
		}
	}
	for model, use := range q.s.ModelUses {
		if q.clock.Now().Sub(use.At) < 24*time.Hour {
			scores[model] += use.Count
		}
	}
	for _, c := range q.calls {
		if c.Error == "" {
			scores[c.Model]++
		}
	}
	return scores
}
func (q *Queue) copyPlan(m *Machine) ([]CopyTask, []CopyTask, []string) {
	if q.maintenanceBlocked(hostID(m)) || m.Leaving || availabilityBlocked(m) || q.machineLimitReason(m) != "" {
		return nil, nil, nil
	}
	targets := q.machineTargets(m)
	scores, pins := map[string]int{}, []string{}
	for _, target := range targets {
		for name, score := range q.modelScores(target) {
			scores[name] = max(scores[name], score)
		}
		for _, name := range q.cachePins(target) {
			if !contains(pins, name) {
				pins = append(pins, name)
			}
		}
	}
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") && j.Spec.Needs.Model != "" && !contains(pins, j.Spec.Needs.Model) {
			pins = append(pins, j.Spec.Needs.Model)
		}
	}
	wanted := map[string]bool{}
	cancelled := map[string]bool{}
	for model, score := range scores {
		if score > 0 {
			wanted[model] = true
		}
	}
	for _, s := range q.s.SyncRequests {
		matches := false
		for _, target := range targets {
			matches = matches || targetMatches(s.To, target)
		}
		if matches {
			if s.Cancel {
				cancelled[s.Model] = true
			} else {
				wanted[s.Model] = true
			}
		}
	}
	for _, name := range pins {
		wanted[name] = true
		delete(cancelled, name)
	}
	sort.Strings(pins)
	usage := []CopyTask{}
	for _, model := range q.models.Models {
		usage = append(usage, CopyTask{Model: model.Name, Score: scores[model.Name]})
	}
	sort.Slice(usage, func(i, j int) bool { return usage[i].Model < usage[j].Model })
	plan := []CopyTask{}
	busy := q.dockerBlocker(hostID(m)) != "" || q.held(hostID(m)) || m.Gone || m.Leaving || q.unreported[m.ID]
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") {
			busy = true
		}
	}
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && (s.State == "loading" || s.InFlight > 0) {
			busy = true
		}
	}
	if !busy {
		for name := range wanted {
			model, ok := q.model(name)
			if ok && !cancelled[name] && model.SizeBytes > 0 && !model.DirectNAS && !contains(m.CachedModels, name) {
				for _, target := range targets {
					if q.modelFitsMachine(model, target) {
						l := q.issuePreparation(target, "copy", name)
						plan = append(plan, CopyTask{Model: name, Score: scores[name], Launch: &l})
						break // The verified cache is shared by every component.
					}
				}
			}
		}
	}
	sort.Slice(plan, func(i, j int) bool {
		a, b := contains(pins, plan[i].Model), contains(pins, plan[j].Model)
		if a != b {
			return a
		}
		if plan[i].Score != plan[j].Score {
			return plan[i].Score > plan[j].Score
		}
		return plan[i].Model < plan[j].Model
	})
	return plan, usage, pins
}

// Cold estimates use measured throughput, with a visible conservative 40 MB/s
// prior until the first copy. Cached copies have no NAS transfer time.
func (q *Queue) loadSeconds(model string, m *Machine) float64 {
	if contains(m.Models, model) || contains(m.CachedModels, model) {
		return 0
	}
	mm, ok := q.model(model)
	if !ok || mm.SizeBytes <= 0 {
		return 0
	}
	remaining := mm.SizeBytes
	for _, r := range m.CopyReports {
		if r.Model == model {
			remaining = max(0, remaining-min(mm.SizeBytes, r.Bytes))
		}
	}
	speed := m.CopyBytesPerSecond
	if speed <= 0 || math.IsNaN(speed) || math.IsInf(speed, 0) {
		speed = 40_000_000
	}
	return float64(remaining) / speed
}
func (q *Queue) workerIdle(m *Machine, demand *Job) bool {
	if !q.workerBatchIdle(m) || q.untrackedPreparations(m) > 0 {
		return false
	}
	for _, l := range q.s.PreparationPlans {
		// A copy already serving this model demand remains useful preparation.
		if l.Kind == "copy" && l.Key == demand.Spec.Needs.Model {
			continue
		}
		if l.Machine == m.ID && l.Session == m.Session && l.Started && !q.preparationShared(m, demand, &l) {
			return false
		}
	}
	return true
}

// Preparation offers must continue while a build runs, even though that
// worker is busy for placement steering.
func (q *Queue) workerBatchIdle(m *Machine) bool {
	if m.Gone || m.Leaving || q.unreported[m.ID] {
		return false
	}
	for _, j := range q.s.Jobs {
		if batchOnTarget(j, m) && (j.State == "running" || j.State == "stopping") {
			return false
		}
	}
	return true
}

func (q *Queue) recordModelUse(name string) {
	if name == "" {
		return
	}
	if q.s.ModelUses == nil {
		q.s.ModelUses = map[string]ModelUse{}
	}
	use := q.s.ModelUses[name]
	if q.clock.Now().Sub(use.At) >= 24*time.Hour {
		use.Count = 0
	}
	use.Count++
	use.At = q.clock.Now()
	q.s.ModelUses[name] = use
}

// placementBetter applies the same rule to foreground placement and build plans.
func (q *Queue) placementBetter(j *Job, a, b *Machine) bool {
	if j.Spec.Needs.cpuOnly() && (a.GPU == "cpu") != (b.GPU == "cpu") {
		return a.GPU == "cpu"
	}
	aa, bb := affinity(j, a), affinity(j, b)
	if aa != bb {
		return aa > bb
	}
	if len(j.Spec.Needs.Platform.Preferred) > 0 {
		ap, bp := contains(j.Spec.Needs.Platform.Preferred, a.GPU), contains(j.Spec.Needs.Platform.Preferred, b.GPU)
		if ap != bp {
			return ap
		}
	}
	if better, known := q.completionBetter(j, a, b); known {
		return better
	}
	if modelLoaded(j, a) != modelLoaded(j, b) {
		return modelLoaded(j, a)
	}
	return q.loadSeconds(j.Spec.Needs.Model, a)+environmentSeconds(j, a) < q.loadSeconds(j.Spec.Needs.Model, b)+environmentSeconds(j, b)
}
func (q *Queue) betterPlacement(j *Job, m *Machine) bool {
	if j.Spec.FanOut != nil {
		return false
	} // The discrete plan already selected this component.
	// Plain commands and component targets use the same preferred-platform
	// deferral as legacy workers, before the specialized placement ranking.
	if len(j.Spec.Needs.Platform.Preferred) > 0 && !contains(j.Spec.Needs.Platform.Preferred, m.GPU) {
		for _, other := range q.plannerMachines() {
			if contains(j.Spec.Needs.Platform.Preferred, other.GPU) && q.placementAllowed(j, other) && fits(j, other, other.FreeMemoryGB) && affinity(j, other) >= affinity(j, m) && q.workerBatchIdle(other) {
				return true
			}
		}
	}
	if j.Spec.extensionJob() {
		for _, other := range q.plannerMachines() {
			if !sameTarget(other, m) && !q.unreported[other.ID] && q.workerIdle(other, j) && q.placementAllowed(j, other) && q.checkpointReady(j, other) && fits(j, other, other.FreeMemoryGB) && q.capabilityPlacementBetter(j, other, m) {
				return true
			}
		}
		return false
	}
	if !j.Spec.Needs.cpuOnly() && j.Spec.Needs.Model == "" && j.Spec.Environment.Key() == "" {
		return false
	}
	for _, other := range q.plannerMachines() {
		if sameTarget(other, m) || !q.workerIdle(other, j) || !q.placementAllowed(j, other) || !fits(j, other, other.FreeMemoryGB) {
			continue
		}
		if q.placementBetter(j, other, m) {
			return true
		}
	}
	return false
}
