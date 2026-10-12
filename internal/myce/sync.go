package myce

import (
	"errors"
	"math"
	"sort"
	"time"
)

type CopyReport struct {
	BytesPerSecond float64 `json:"bytes_per_second,omitempty"`
	Model          string  `json:"model"`
	State          string  `json:"state"`
	Bytes          int64   `json:"bytes"`
	Total          int64   `json:"total"`
	Error          string  `json:"error,omitempty"`
}
type CopyTask struct {
	Model string `json:"model"`
	Score int    `json:"score"`
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
		if ok && modelFitsMachine(model, m) && (p.Machine == "" || p.Machine == m.ID) && (p.Platform == "" || p.Platform == m.GPU) && !contains(out, p.Model) {
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
	if m.EnablePending {
		return nil, nil, nil
	}
	scores := q.modelScores(m)
	pins := q.cachePins(m)
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
		if targetMatches(s.To, m) {
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
	usage := []CopyTask{}
	for _, model := range q.models.Models {
		usage = append(usage, CopyTask{model.Name, scores[model.Name]})
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
			if ok && !cancelled[name] && model.SizeBytes > 0 && !model.DirectNAS && modelFitsMachine(model, m) && !contains(m.CachedModels, name) {
				plan = append(plan, CopyTask{name, scores[name]})
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
func (q *Queue) workerIdle(m *Machine) bool {
	if m.Gone || m.Leaving || q.unreported[m.ID] {
		return false
	}
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") {
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
	if modelLoaded(j, a) != modelLoaded(j, b) {
		return modelLoaded(j, a)
	}
	return q.loadSeconds(j.Spec.Needs.Model, a)+environmentSeconds(j, a) < q.loadSeconds(j.Spec.Needs.Model, b)+environmentSeconds(j, b)
}
func (q *Queue) betterPlacement(j *Job, m *Machine) bool {
	if j.Spec.Needs.Model == "" && j.Spec.Environment.Key() == "" {
		return false
	}
	for _, other := range q.s.Machines {
		if other.ID == m.ID || !q.workerIdle(other) || !q.placementAllowed(j, other) || !fits(j, other, other.FreeMemoryGB) {
			continue
		}
		if q.placementBetter(j, other, m) {
			return true
		}
	}
	return false
}
