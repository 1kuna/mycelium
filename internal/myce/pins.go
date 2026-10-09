package myce

import (
	"errors"
	"sort"
	"time"
)

type ModelPin struct {
	Project  string `json:"project"`
	Model    string `json:"model"`
	Machine  string `json:"machine,omitempty"`
	Platform string `json:"platform,omitempty"`
	Weekdays []int  `json:"weekdays,omitempty"`
	Start    string `json:"start,omitempty"`
	End      string `json:"end,omitempty"`
	Timezone string `json:"timezone,omitempty"`
	Assigned string `json:"assigned,omitempty"`
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
	return q.update(func() error {
		model, ok := q.model(p.Model)
		if !ok {
			return errors.New("unknown model")
		}
		if p.Project == "" || (p.Machine == "") == (p.Platform == "") {
			return errors.New("pin requires project and exactly one machine or platform")
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
			if !m.Gone && !m.Leaving && m.MemoryError == "" && m.ModelURL != "" && !q.unreported[m.ID] && modelFitsMachine(model, m) && (p.Machine == "" || p.Machine == m.ID) && (p.Platform == "" || p.Platform == m.GPU) {
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
		for _, m := range candidates {
			key := slotKey(m.ID, p.Model)
			if s := q.s.ModelSlots[key]; s != nil && s.State != "draining" {
				p.Assigned = m.ID
				break
			}
			if m.FreeMemoryGB >= model.MemoryGB {
				q.s.ModelSlots[key] = &ModelSlot{Model: p.Model, Machine: m.ID, Session: m.Session, State: "loading", Priority: "production", LastUsed: now}
				m.FreeMemoryGB -= model.MemoryGB
				p.Assigned = m.ID
				break
			}
			q.makeModelRoom(m, model.MemoryGB, "production")
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") && rank(j.Spec.Priority) > 0 && m.FreeMemoryGB+j.Spec.Needs.MemoryGB >= model.MemoryGB {
					q.stopFor(j, "production", "pin")
				}
			}
		}
	}
}
func (q *Queue) SetProductionGrace(d time.Duration) error {
	if d <= 0 {
		return errors.New("production grace must be positive")
	}
	q.mu.Lock()
	q.productionGrace = d
	q.mu.Unlock()
	return nil
}
func (q *Queue) stopFor(j *Job, priority, kind string) {
	q.stop(j, "making room for "+priority+" "+kind)
	if j.State == "stopping" && priority == "production" {
		grace := q.productionGrace
		if grace <= 0 {
			grace = 10 * time.Second
		}
		if !j.Spec.Resumable {
			grace = 0
		}
		t := q.clock.Now().Add(grace)
		if j.StopAt == nil || t.Before(*j.StopAt) {
			j.StopAt = &t
			if j.Reason != "cancel requested" {
				j.Reason = "making room for " + priority + " " + kind
			}
		}
	}
}
