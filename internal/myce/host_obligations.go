package myce

import (
	"fmt"
	"sort"
)

// HostObligation identifies exact retained work; enumeration never releases custody.
type HostEngineExecution struct {
	Machine string `json:"machine"`
	Session string `json:"session"`
	Model   string `json:"model"`
	Attempt string `json:"attempt"`
}

type HostCallExecution struct {
	HostEngineExecution
	ID string `json:"id"`
}

type HostObligation struct {
	Launch      *HostLaunch          `json:"launch,omitempty"`
	Call        *HostCallExecution   `json:"call,omitempty"`
	Engine      *HostEngineExecution `json:"engine,omitempty"`
	Preparation bool                 `json:"preparation,omitempty"`
	Result      Result               `json:"execution"`
}

func obligationKey(p HostObligation) string {
	if p.Launch != nil {
		return "preparation/" + p.Launch.Attempt
	}
	if p.Call != nil {
		c := p.Call
		return fmt.Sprintf("call/%s/%s/%s/%s/%s", c.Machine, c.Session, c.Model, c.Attempt, c.ID)
	}
	if p.Engine != nil {
		e := p.Engine
		return fmt.Sprintf("engine/%s/%s/%s/%s", e.Machine, e.Session, e.Model, e.Attempt)
	}
	return fmt.Sprintf("execution/%t/%s", p.Preparation, releaseKey(HostRelease{Job: p.Result.ID, Machine: p.Result.Machine, Session: p.Result.Session, Attempt: p.Result.Attempt}))
}
func (q *Queue) executionHost(machine string) string {
	if m := q.s.Machines[machine]; m != nil {
		return hostID(m)
	}
	return ""
}
func (q *Queue) hostObligations(host string) []HostObligation {
	pending := map[string]HostObligation{}
	add := func(p HostObligation) { pending[obligationKey(p)] = p }
	jobs := map[string]*Job{}
	for id, j := range q.s.Jobs {
		jobs[id] = j
	}
	for _, r := range q.s.Submissions {
		if r.Job != nil && jobs[r.JobID] == nil {
			jobs[r.JobID] = r.Job
		}
	}
	for _, j := range jobs {
		for _, c := range j.Copies {
			if !c.ReleaseConfirmed && q.executionHost(c.Machine) == host {
				add(HostObligation{Result: Result{Machine: c.Machine, Session: c.Session, Execution: Execution{ID: j.ID, Attempt: c.Attempt}}})
			}
		}
	}
	for _, r := range q.s.HostReleases {
		if r.Host == host {
			add(HostObligation{Preparation: r.Preparation, Result: Result{Machine: r.Machine, Session: r.Session, Execution: Execution{ID: r.Job, Attempt: r.Attempt}}})
		}
	}
	for _, s := range q.s.ModelSlots {
		// An exact failed receipt confirms cleanup. A pin's retry reservation
		// is desired state, not engine custody; unloading/unload_failed still is.
		if s.State != "failed" && q.executionHost(s.Machine) == host {
			add(HostObligation{Engine: &HostEngineExecution{Machine: s.Machine, Session: s.Session, Model: s.Model, Attempt: s.Attempt}})
		}
	}
	for _, c := range q.calls {
		if c.Machine != "" && q.executionHost(c.Machine) == host {
			add(HostObligation{Call: &HostCallExecution{HostEngineExecution: HostEngineExecution{Machine: c.Machine, Session: c.Session, Model: c.Model, Attempt: c.Attempt}, ID: c.ID}})
		}
	}
	for _, l := range q.s.PreparationPlans {
		if q.executionHost(l.Machine) == host {
			copy := l
			add(HostObligation{Launch: &copy, Preparation: true, Result: Result{Machine: l.Machine, Session: l.Session, Execution: Execution{ID: "preparation"}}})
		}
	}
	for _, m := range q.s.Machines {
		if hostID(m) == host && q.untrackedPreparations(m) > 0 {
			add(HostObligation{Preparation: true, Result: Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: "preparation"}}})
		}
	}
	keys := make([]string, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]HostObligation, 0, len(keys))
	for _, key := range keys {
		out = append(out, pending[key])
	}
	return out
}
func obligationSession(p HostObligation) (string, string) {
	if p.Launch != nil {
		return p.Launch.Machine, p.Launch.Session
	}
	if p.Call != nil {
		return p.Call.Machine, p.Call.Session
	}
	if p.Engine != nil {
		return p.Engine.Machine, p.Engine.Session
	}
	return p.Result.Machine, p.Result.Session
}

func (q *Queue) obligationCall(e *HostCallExecution) *ModelCall {
	c := q.calls[e.ID]
	if c != nil && c.Machine == e.Machine && c.Session == e.Session && c.Model == e.Model && c.Attempt == e.Attempt {
		return c
	}
	return nil
}

func hostPreparing(m *Machine) bool {
	for _, r := range m.EnvironmentReports {
		if r.State == "building" {
			return true
		}
	}
	for _, r := range m.CopyReports {
		if r.State == "copying" {
			return true
		}
	}
	return false
}
