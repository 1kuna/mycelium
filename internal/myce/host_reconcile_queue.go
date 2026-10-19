package myce

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// One queue fence per host. Released keys make exact retries idempotent without
// interpreting an empty payload as a blanket release.
type QueueHostIntent struct {
	HostRevision
	Desired       string                    `json:"desired"`
	Boot          string                    `json:"boot_id"`
	Reason        string                    `json:"reason"`
	AcceptedAt    time.Time                 `json:"accepted_at"`
	Released      map[string]HostObligation `json:"released,omitempty"`
	ClosedLeaving map[string]string         `json:"closed_leaving,omitempty"`
}
type HostExchange struct {
	Simulated bool   `json:"simulated,omitempty"`
	Host      string `json:"host_id"`
	HostRevision
	Desired    string        `json:"desired"`
	Boot       string        `json:"boot_id"`
	Request    string        `json:"request_id"`
	AcceptedAt time.Time     `json:"accepted_at"`
	Reason     string        `json:"reason"`
	LastClose  *HostClose    `json:"last_close"`
	Sessions   []HostSession `json:"discharged_sessions"`
}
type HostAcknowledgement struct {
	Host string `json:"host_id"`
	HostRevision
	Desired string           `json:"desired"`
	Boot    string           `json:"boot_id"`
	Request string           `json:"request_id"`
	Pending []HostObligation `json:"pending"`
	Covered []HostObligation `json:"covered"`
}

func obligationKey(p HostObligation) string {
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
func (q *Queue) hostClosedReason(host string) string {
	s := q.s.HostIntents[host]
	if s.Desired != "closed" {
		return ""
	}
	if s.Reason != "" {
		return s.Reason
	}
	return "host closed"
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
		if q.executionHost(s.Machine) == host {
			add(HostObligation{Engine: &HostEngineExecution{Machine: s.Machine, Session: s.Session, Model: s.Model, Attempt: s.Attempt}})
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
func (q *Queue) ReconcileHost(c HostExchange) (HostAcknowledgement, error) {
	var ack HostAcknowledgement
	err := q.updateScoped(func() string { return c.Host }, false, func() error {
		var err error
		ack, err = q.reconcileHost(c)
		if err == nil {
			q.maintain()
			ack.Pending = q.hostObligations(c.Host)
			ack.Covered = q.hostCoverage(c.Host)
		}
		return err
	})
	if err != nil {
		q.mu.Lock()
		revision := q.s.HostIntents[c.Host].HostRevision
		q.mu.Unlock()
		code := "host_reconcile_rejected"
		if revision.Revision > c.Revision {
			code = "host_revision_behind_queue"
		}
		return HostAcknowledgement{}, &hostReconcileError{cause: err, revision: revision, code: code}
	}
	return ack, nil
}
func (q *Queue) reconcileHost(c HostExchange) (HostAcknowledgement, error) {
	var zero HostAcknowledgement
	if _, dev := q.clock.(*devClock); c.Simulated && !dev {
		return zero, errors.New("simulated_exclusion_unknown")
	}
	if c.Host == "" || c.Revision == 0 || c.Operation == "" || c.Boot == "" || c.Request == "" || c.AcceptedAt.IsZero() || (c.Desired != "closed" && c.Desired != "open") {
		return zero, errors.New("incomplete host reconciliation identity")
	}
	old := q.s.HostIntents[c.Host]
	if c.Revision < old.Revision || c.Revision == old.Revision && (c.Operation != old.Operation || c.Desired != old.Desired || !c.AcceptedAt.Equal(old.AcceptedAt)) {
		return zero, ErrHostRevision
	}
	if c.Desired == "open" && c.Revision > old.Revision && old.Revision > 0 {
		if old.Desired != "closed" || c.Revision != old.Revision+1 || c.LastClose == nil || c.LastClose.HostRevision != old.HostRevision || c.LastClose.Settlement != "confirmed" || len(q.hostObligations(c.Host)) > 0 {
			return zero, errors.New("previous_close_unsettled")
		}
	}
	if c.Desired == "open" && len(c.Sessions) > 0 {
		return zero, errors.New("open carries release evidence")
	}
	var releases []HostObligation
	for _, session := range c.Sessions {
		if session.Machine == "" || session.Session == "" {
			return zero, errors.New("incomplete session discharge")
		}
		if savedHost := q.executionHost(session.Machine); savedHost != "" && savedHost != c.Host {
			return zero, errors.New("session host mismatch")
		}
		for _, p := range q.hostObligations(c.Host) {
			machine, savedSession := p.Result.Machine, p.Result.Session
			if p.Engine != nil {
				machine, savedSession = p.Engine.Machine, p.Engine.Session
			}
			if machine == session.Machine && savedSession == session.Session {
				releases = mergeObligations(releases, []HostObligation{p})
			}
		}
	}
	if q.s.HostIntents == nil {
		q.s.HostIntents = map[string]QueueHostIntent{}
	}
	released := old.Released
	if released == nil || c.Revision != old.Revision {
		released = map[string]HostObligation{}
	}
	closedLeaving := old.ClosedLeaving
	if closedLeaving == nil {
		closedLeaving = map[string]string{}
	}
	q.s.HostIntents[c.Host] = QueueHostIntent{ClosedLeaving: closedLeaving, HostRevision: c.HostRevision, Desired: c.Desired, Boot: c.Boot, Reason: c.Reason, AcceptedAt: c.AcceptedAt, Released: released}
	for _, m := range q.s.Machines {
		if hostID(m) != c.Host {
			continue
		}
		m.Disabled = q.hostClosedReason(c.Host)
		if c.Desired == "closed" {
			if !m.Leaving {
				closedLeaving[m.ID] = m.Session
			}
			m.Leaving = true
		} else if old.Desired == "closed" && closedLeaving[m.ID] == m.Session {
			m.Leaving = false
			delete(closedLeaving, m.ID)
		}
		if c.Desired == "closed" {
			delete(q.unreported, m.ID)
			preparing := false
			for _, r := range m.EnvironmentReports {
				preparing = preparing || r.State == "building"
			}
			for _, r := range m.CopyReports {
				preparing = preparing || r.State == "copying"
			}
			if preparing {
				if q.s.HostReleases == nil {
					q.s.HostReleases = map[string]HostRelease{}
				}
				r := HostRelease{Host: c.Host, Job: "preparation", Machine: m.ID, Session: m.Session, Preparation: true}
				q.s.HostReleases[releaseKey(r)] = r
			}
		}
	}
	if c.Desired == "closed" {
		for _, j := range q.s.Jobs {
			if q.executionHost(j.Machine) != c.Host || j.State != "running" && j.State != "stopping" {
				continue
			}
			q.stop(j, "host closed")
			q.retainRelease(j)
		}
		for _, call := range q.calls {
			if q.executionHost(call.Machine) == c.Host {
				call.Error = "host closed"
			}
		}
	}
	for _, p := range releases {
		released[obligationKey(p)] = p
		if p.Engine != nil {
			e := p.Engine
			key := slotKey(e.Machine, e.Model)
			s := q.s.ModelSlots[key]
			if s != nil && s.Session == e.Session && s.Attempt == e.Attempt {
				delete(q.s.ModelSlots, key)
			}
			continue
		}
		r := p.Result
		q.settleRelease(r)
		if p.Preparation {
			if m := q.s.Machines[r.Machine]; m != nil && m.Session == r.Session {
				m.EnvironmentReports = nil
				m.CopyReports = nil
			}
			continue
		}
		j := q.retainedJob(r.ID)
		if j == nil {
			continue
		}
		// Planned cleanup refunds only the exact saved copy. Other attempts keep
		// their infrastructure losses; host and queue wall clocks do not order them.
		matched := false
		for i := range j.Copies {
			copy := &j.Copies[i]
			if copy.Machine != r.Machine || copy.Session != r.Session || copy.Attempt != r.Attempt {
				continue
			}
			matched = true
			if copy.LossCharged {
				j.Losses = max(0, j.Losses-1)
				copy.LossCharged = false
			}
		}
		if j.State == "queued" && !cancelled(j) && j.Spec.Priority != "production" {
			j.RequeuedByOperator = true
		}
		current := j.Machine == r.Machine && j.Session == r.Session && j.Attempt == r.Attempt
		revive := matched && !cancelled(j) && j.State == "failed" && j.Losses < 3 && strings.HasPrefix(j.Reason, "failed after 3 infrastructure losses:")
		if current && (j.State == "running" || j.State == "stopping" || j.State == "queued") || revive {
			if cancelled(j) {
				j.State = "cancelled"
				j.StopAt = nil
			} else {
				q.retry(j, "requeued: host closed")
				j.RequeuedByOperator = true
			}
			// The close interrupted this attempt. Its audit copy remains, but no
			// current execution can accept a late result from that placement.
			if current {
				j.Machine, j.Session = "", ""
			}
		}
	}
	return HostAcknowledgement{Host: c.Host, HostRevision: c.HostRevision, Desired: c.Desired, Boot: c.Boot, Request: c.Request, Pending: q.hostObligations(c.Host), Covered: q.hostCoverage(c.Host)}, nil
}

func (q *Queue) hostCoverage(host string) []HostObligation {
	out := []HostObligation{}
	for _, p := range q.s.HostIntents[host].Released {
		out = mergeObligations(out, []HostObligation{p})
	}
	add := func(j *Job) {
		for _, copy := range j.Copies {
			if copy.ReleaseConfirmed && q.executionHost(copy.Machine) == host {
				out = mergeObligations(out, []HostObligation{{Result: Result{Machine: copy.Machine, Session: copy.Session, Execution: Execution{ID: j.ID, Attempt: copy.Attempt}}}})
			}
		}
	}
	for _, j := range q.s.Jobs {
		add(j)
	}
	for _, r := range q.s.Submissions {
		if r.Job != nil {
			add(r.Job)
		}
	}
	sort.Slice(out, func(i, j int) bool { return obligationKey(out[i]) < obligationKey(out[j]) })
	return out
}

type hostReconcileError struct {
	cause    error
	code     string
	revision HostRevision
}

func (e *hostReconcileError) Error() string { return e.code }
func (e *hostReconcileError) Unwrap() error { return e.cause }
