package myce

import (
	"errors"
	"sort"
	"time"
)

type ModelPin struct {
	PreemptionCount int    `json:"preemption_count,omitempty"`
	Project         string `json:"project"`
	Model           string `json:"model"`
	Machine         string `json:"machine,omitempty"`
	Platform        string `json:"platform,omitempty"`
	Weekdays        []int  `json:"weekdays,omitempty"`
	Start           string `json:"start,omitempty"`
	End             string `json:"end,omitempty"`
	Timezone        string `json:"timezone,omitempty"`
	Assigned        string `json:"assigned,omitempty"`
}

func (p ModelPin) active(now time.Time) bool {
	if p.Start == "" {
		return true
	}
	loc, _ := time.LoadLocation(p.Timezone)
	now = now.In(loc)
	if len(p.Weekdays) > 0 {
		ok := false
		for _, d := range p.Weekdays {
			if d == int(now.Weekday()) {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	hm := now.Format("15:04")
	return hm >= p.Start && hm < p.End
}
func (q *Queue) Pin(p ModelPin) error {
	if err := validateProjectIdentifier(p.Project); err != nil {
		return err
	}
	return q.update(func() error {
		model, ok := q.model(p.Model)
		if !ok {
			return errors.New("unknown model")
		}
		if p.Project == "" || (p.Machine == "") == (p.Platform == "") {
			return errors.New("pin requires project and exactly one machine or platform")
		}
		if e := q.validateProjectPlacement(p.Project, []string{p.Machine}); p.Machine != "" && e != nil {
			return e
		}
		if p.Platform != "" && !contains(model.Platforms, p.Platform) {
			return errors.New("model cannot run on pin platform")
		}
		if p.Start != "" || p.End != "" {
			if _, e := time.Parse("15:04", p.Start); e != nil {
				return e
			}
			if _, e := time.Parse("15:04", p.End); e != nil {
				return e
			}
			if p.Start >= p.End {
				return errors.New("pin window must end after start (split overnight windows)")
			}
			if p.Timezone == "" {
				p.Timezone = "UTC"
			}
			if _, e := time.LoadLocation(p.Timezone); e != nil {
				return e
			}
		}
		for _, d := range p.Weekdays {
			if d < 0 || d > 6 {
				return errors.New("weekdays must be 0 (Sunday) through 6")
			}
		}
		for _, existing := range q.s.Pins {
			if existing.Project == p.Project && existing.Model == p.Model {
				return errors.New("project/model already pinned; unpin first")
			}
		}
		p.Assigned = ""
		q.s.Pins = append(q.s.Pins, p)
		q.maintain()
		return nil
	})
}
func (q *Queue) Unpin(project, model string) error {
	return q.update(func() error {
		for i, p := range q.s.Pins {
			if p.Project == project && p.Model == model {
				q.s.Pins = append(q.s.Pins[:i], q.s.Pins[i+1:]...)
				q.maintain()
				return nil
			}
		}
		return errors.New("pin not found")
	})
}
func (q *Queue) slotPinned(s *ModelSlot) bool {
	for _, p := range q.s.Pins {
		if p.Model == s.Model && p.Assigned == s.Machine && p.active(q.clock.Now()) {
			return true
		}
	}
	return false
}
func (q *Queue) maintainPins() {
	now := q.clock.Now()
	for i := range q.s.Pins {
		p := &q.s.Pins[i]
		if !p.active(now) {
			p.Assigned = ""
			continue
		}
		model, _ := q.model(p.Model)
		candidates := []*Machine{}
		for _, m := range q.s.Machines {
			if q.hostBenchmarkPending(hostID(m)) {
				// Keep an existing pin's engine protected while onboarding waits
				// for an idle host. Do not plan a new engine alongside a benchmark.
				if s := q.s.ModelSlots[slotKey(m.ID, p.Model)]; s == nil || s.State == "failed" {
					continue
				}
			}
			if !availabilityBlocked(m) && q.projectAllows(p.Project, m.ID) && !m.Gone && !m.Leaving && m.MemoryError == "" && m.ModelURL != "" && !q.unreported[m.ID] && q.modelUsableMachine(model, m) && (p.Machine == "" || p.Machine == m.ID) && (p.Platform == "" || p.Platform == m.GPU) {
				candidates = append(candidates, m)
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			if (candidates[i].ID == p.Assigned) != (candidates[j].ID == p.Assigned) {
				return candidates[i].ID == p.Assigned
			}
			a, b := contains(candidates[i].Models, p.Model), contains(candidates[j].Models, p.Model)
			if a != b {
				return a
			}
			a, b = contains(candidates[i].CachedModels, p.Model), contains(candidates[j].CachedModels, p.Model)
			if a != b {
				return a
			}
			return candidates[i].ID < candidates[j].ID
		})
		p.Assigned = ""
		demand := &Job{ID: "pin/" + p.Project + "/" + p.Model, Spec: Spec{Project: p.Project, Priority: "production"}}
		demand.Spec.Needs.Model = p.Model
		var best *placementPlan
		for _, m := range candidates {
			key := slotKey(m.ID, p.Model)
			if s := q.s.ModelSlots[key]; s != nil {
				if s.State != "draining" && s.State != "unloading" && s.State != "unload_failed" {
					best = &placementPlan{machine: m}
					break
				}
				continue
			}
			if winner := q.candidate(m, nil); winner != nil && q.comparePriority(winner.Spec, demand.Spec) < 0 {
				continue
			}
			if !q.placementAllowed(demand, m) {
				continue
			}
			if plan := q.roomPlan(demand, m, false); plan != nil && plan.cheaper(best) {
				best = plan
			}
		}
		if best == nil {
			continue
		}
		m := best.machine
		key := slotKey(m.ID, p.Model)
		if q.s.ModelSlots[key] != nil {
			p.Assigned = m.ID
			continue
		}
		q.applyPlacement(best, demand)
		if m.FreeMemoryGB < model.MemoryGB || !cpuAllocationFits(m, 1) || !hostFits(m, model.MemoryGB, model.HostMemoryGB) || q.preparationActive(m) || q.preparationStopping(m) {
			continue
		}
		q.s.ModelSlots[key] = &ModelSlot{Project: p.Project, Model: p.Model, Machine: m.ID, Session: m.Session, State: "loading", LoadUntil: q.engineLoadDeadline(), Attempt: ID(), Priority: "production", LastUsed: now}
		q.initEnginePreemption(q.s.ModelSlots[key])
		m.FreeMemoryGB -= model.MemoryGB
		q.refreshHostMemory()
		p.Assigned = m.ID
	}
}
func (q *Queue) SetProductionGrace(d time.Duration) error {
	return q.SetPreemptionGrace(d)
}
func (q *Queue) SetPreemptionGrace(d time.Duration) error {
	if d <= 0 {
		return errors.New("preemption grace must be positive")
	}
	q.mu.Lock()
	q.productionGrace = d
	q.mu.Unlock()
	return nil
}
func (q *Queue) stopFor(j *Job, priority, kind string) {
	j.PriorityPolicy = q.priorityMatch(j.Spec)
	if j.State == "running" {
		q.s.Preemptions = append(q.s.Preemptions, Preemption{At: q.clock.Now(), Job: j.ID, Project: j.Spec.Project, For: priority})
	}
	q.stop(j, "making room for "+priority+" "+kind)
	if j.State == "stopping" {
		grace := q.preemptionGrace()
		if priority == "deadline" {
			grace = StopGrace
		}
		if !j.Spec.Resumable {
			grace = 0
		}
		t := q.clock.Now().Add(grace)
		if j.StopAt == nil || t.Before(*j.StopAt) {
			j.StopAt = &t
			if !cancelled(j) {
				j.Reason = "making room for " + priority + " " + kind
			}
		}
	}
}
