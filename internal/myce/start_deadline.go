package myce

import (
	"fmt"
	"slices"
	"time"
)

// Budget includes up to 15s accelerator TERM grace, 120s cleanup escalation,
// and 15s for reporting and replacement launch; grace never extends StartBy.
const ProductionStartTimeout = maxGPUStopGrace + StopGrace + 15*time.Second

func startupPending(j *Job) bool {
	if j.Spec.FanOut != nil && j.FanOut != nil && j.FanOut.Next > 0 {
		return false
	}
	return j.Spec.StartBy != nil && !terminal(j.State) && !cancelled(j) && (j.State == "queued" || j.StartedAt == nil)
}

func (q *Queue) startWaitingReason(j *Job) string {
	if reason := q.diskWaitingReason(j, j.ProductionHost); reason != "" {
		return reason
	}
	machines := q.plannerMachines()
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
		if q.maintenanceBlocked(host) {
			return "host " + host + " in planned maintenance"
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
	for _, j := range q.liveJobs() {
		if j.Spec.Priority == "production" && j.Spec.StartBy == nil && dependencyReady(j) && !terminal(j.State) {
			deadline := j.Queued.Add(ProductionStartTimeout)
			j.Spec.StartBy = &deadline
		}
		if !startupPending(j) {
			continue
		}
		disabled := dependencyReady(j) && j.State == "queued" && j.Attempt == 0 && q.allStartHostsDisabled(j)
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
	for _, m := range q.plannerMachines() {
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
		if !q.maintenanceBlocked(hostID(m)) {
			return false
		}
	}
	return found
}

func (q *Queue) startPlacementAllowed(j *Job) bool { return q.startPlacementRejection(j) == "" }

// startPlacementRejection names the failed start-fence predicate, or "". The
// names are fixed labels, safe to log without host, path or volume detail.
func (q *Queue) startPlacementRejection(j *Job) string {
	if q.startDiskFloorReason(j, q.s.Machines[j.Machine]) != "" {
		return "disk_floor"
	}
	return q.startPlacementPolicyRejection(j)
}

// A worker reporting Started has already executed; retain the policy fences
// without treating its existing files as a new disk allocation.
func (q *Queue) startPlacementPolicyAllowed(j *Job) bool {
	return q.startPlacementPolicyRejection(j) == ""
}

func (q *Queue) startPlacementPolicyRejection(j *Job) string {
	owner := q.s.Machines[j.Machine]
	m := allocationMachine(owner, j.AllocatedDevices)
	if m == nil {
		return "machine_unavailable"
	}
	if q.preparationStopping(m) {
		return "preparation_stopping"
	}
	if j.Spec.Run.Docker && dockerStartingOrRunning(m.DockerCleanup) {
		// A heartbeat can retain a healthy pending operation after its receipt.
		// Reuse the exact-custody classifier; sibling and old-copy holds still fence.
		previous := owner.DockerCleanup
		m.DockerCleanup, owner.DockerCleanup = "", ""
		defer func() { m.DockerCleanup, owner.DockerCleanup = previous, previous }()
	}
	switch {
	case !executionTargetMatches(j.Spec, m):
		return "execution_target"
	case !cpuFits(j, m):
		return "cpu_fit"
	case !q.placementPolicyAllowed(j, m, false):
		return "placement_policy"
	}
	return ""
}
