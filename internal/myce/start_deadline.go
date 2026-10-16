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
		if !startupPending(j) {
			continue
		}
		disabled := j.State == "queued" && q.allStartHostsDisabled(j)
		if !expired(j.Spec.StartBy, now) && !disabled {
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
		if disabled && !expired(j.Spec.StartBy, now) {
			j.Reason = "production cannot start: no eligible host is enabled; " + reason
		}
		j.FinishedAt = &now
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
	}
}

// Keep a failed startup's resources fenced until its exact execution is released.
func (q *Queue) startupReleaseBlocker(host string) string { return q.startupReleaseDetail(host).Text }

func (q *Queue) startupReleaseDetail(host string) HoldDetail {
	detail := HoldDetail{}
	for _, r := range q.s.HostReleases {
		j := q.s.Jobs[r.Job]
		if r.Host == host && j != nil && terminal(j.State) && j.Spec.StartBy != nil {
			detail.Host, detail.Kind = host, "startup_release"
			detail.Text = fmt.Sprintf("host %s: production startup release pending", host)
			m := q.s.Machines[r.Machine]
			detail.OperatorActionRequired = detail.OperatorActionRequired || m == nil || m.Gone
		}
	}
	return detail
}

type StartPermit struct {
	RemainingSeconds float64 `json:"remaining_seconds"`
}

// Legacy preparation probe; only Started commits the execution hand-off.
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
		if j.State != "running" || !q.startPlacementAllowed(j) {
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

// An unknown fleet may still join; an explicit operator disable is not incoming room.
func (q *Queue) allStartHostsDisabled(j *Job) bool {
	if model, ok := q.model(j.Spec.Needs.Model); ok && j.Spec.Needs.MemoryGB == 0 {
		copy := cloneJob(j)
		copy.Spec.Needs.MemoryGB = model.MemoryGB
		copy.Spec.Needs.Platform = Platform{Required: model.Platforms}
		j = &copy
	}
	found := false
	for _, m := range q.s.Machines {
		if !q.projectAllows(j.Spec.Project, m.ID) || len(j.Spec.AllowedMachines) > 0 && !slices.Contains(j.Spec.AllowedMachines, m.ID) || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU) {
			continue
		}
		capacity := *m
		capacity.Gone, capacity.Leaving, capacity.CheckpointUnavailable = false, false, false
		capacity.MemoryError = ""
		memory := m.TotalMemoryGB
		if memory == 0 {
			memory = j.Spec.Needs.MemoryGB
		}
		if !fitsWithoutHostMemory(j, &capacity, memory) {
			continue
		}
		found = true
		if m.Disabled == "" && q.s.DisabledHosts[hostID(m)] == "" {
			return false
		}
	}
	return found
}

func (q *Queue) startPlacementAllowed(j *Job) bool {
	m := q.s.Machines[j.Machine]
	if m == nil {
		return false
	}
	if j.Spec.Run.Docker && (m.DockerCleanup == "creating" || m.DockerCleanup == "create pending") {
		// Own completed create is safe; sibling custody must still block the hand-off.
		previous := m.DockerCleanup
		m.DockerCleanup = ""
		defer func() { m.DockerCleanup = previous }()
	}
	return q.placementAllowed(j, m)
}
