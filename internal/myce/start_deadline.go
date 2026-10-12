package myce

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// Allow ordinary stop grace and the two-minute cleanup escalation, plus reporting.
const ProductionStartTimeout = 150 * time.Second

func startupPending(j *Job) bool {
	return j.Spec.StartBy != nil && !terminal(j.State) && j.Reason != "cancel requested" && (j.State == "queued" || j.StartedAt == nil)
}

func (q *Queue) startWaitingReason(j *Job) string {
	machines := []*Machine{}
	for _, m := range q.s.Machines {
		machines = append(machines, m)
	}
	sort.Slice(machines, func(a, b int) bool { return machines[a].ID < machines[b].ID })
	for _, m := range machines {
		if !q.projectAllows(j.Spec.Project, m.ID) || len(j.Spec.AllowedMachines) > 0 && !slices.Contains(j.Spec.AllowedMachines, m.ID) || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU) {
			continue
		}
		if j.Machine != "" && j.Machine != m.ID {
			continue
		}
		host := hostID(m)
		if b := q.startupReleaseBlocker(host); b != "" {
			return b
		}
		if m.Disabled != "" || q.s.DisabledHosts[host] != "" {
			return "host " + host + " disabled by operator"
		}
		if b := q.dockerBlocker(host); b != "" {
			return "waiting for Docker cleanup to be confirmed on " + host + "; " + b
		}
		if b := q.hostBlockers(host); b != "" && q.held(host) {
			return "waiting for lower-priority work to stop on " + host + "; " + b
		}
		if j.State == "queued" {
			continue
		}
		if m.Gone || q.unreported[m.ID] {
			return "waiting for worker " + m.ID + " to report"
		}
		return "waiting for command start on " + m.ID + " (preparing environment or command)"
	}
	return q.waitReason(j)
}

// Expire before planning any new reclaim or assignment. Release custody is independent.
func (q *Queue) maintainStartDeadlines() {
	now := q.clock.Now()
	for _, j := range q.s.Jobs {
		if j.Spec.Priority == "production" && j.Spec.StartBy == nil && !terminal(j.State) {
			deadline := j.Queued.Add(ProductionStartTimeout)
			j.Spec.StartBy = &deadline
		}
		if !startupPending(j) || !expired(j.Spec.StartBy, now) {
			continue
		}
		reason := q.startWaitingReason(j)
		if j.State == "queued" && expired(j.Spec.GiveUpAt, now) && j.Spec.GiveUpAt.Before(*j.Spec.StartBy) {
			j.State = "failed"
			j.Reason = "give-up time reached"
			j.FinishedAt = &now
			continue
		}
		if j.State == "running" || j.State == "stopping" {
			j.StopAt = &now
			j.LastStopOutcome = "release pending"
			q.retainRelease(j)
			key := releaseKey(HostRelease{Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt})
			if r, ok := q.s.HostReleases[key]; ok {
				r.CleanupFailed = true
				q.s.HostReleases[key] = r
			}
		}
		j.State = "failed"
		j.Reason = "production start deadline exceeded: " + reason
		j.FinishedAt = &now
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
	}
}

// Keep a failed startup's resources fenced until its exact execution is released.
func (q *Queue) startupReleaseBlocker(host string) string {
	for _, r := range q.s.HostReleases {
		j := q.s.Jobs[r.Job]
		if r.Host == host && j != nil && terminal(j.State) && j.Spec.StartBy != nil {
			return fmt.Sprintf("host %s: production startup release pending", host)
		}
	}
	return ""
}

type StartPermit struct {
	RemainingSeconds float64 `json:"remaining_seconds"`
}

// A permit authorizes dispatch, not successful startup. Expiry/start receipts are serialized.
func (q *Queue) PermitStart(r Result) (StartPermit, error) {
	var permit StartPermit
	var rejected error
	err := q.update(func() error {
		q.maintainStartDeadlines()
		j, e := q.current(r)
		if e != nil {
			rejected = e
			return nil
		}
		m := q.s.Machines[j.Machine]
		if j.State != "running" || m == nil || !q.placementAllowed(j, m) {
			rejected = ErrStale
			return nil
		}
		if j.Spec.StartBy != nil {
			permit.RemainingSeconds = max(0, j.Spec.StartBy.Sub(q.clock.Now()).Seconds())
		}
		return nil
	})
	if err != nil {
		return permit, err
	}
	return permit, rejected
}

func (q *Queue) startStatusReason(j *Job) string {
	return q.startWaitingReason(j) + fmt.Sprintf("; %.0fs left to start", max(0, j.Spec.StartBy.Sub(q.clock.Now()).Seconds()))
}
