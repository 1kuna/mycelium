package myce

import (
	"fmt"
	"log"
	"slices"
	"time"
)

// Retained names describe historical records, not new lifecycle defaults.
const ProductionStartTimeout = maxGPUStopGrace + StopGrace + 15*time.Second
const MaxProductionStartWindow = time.Hour

func productionStartWindow(s Spec) time.Duration {
	window, _ := time.ParseDuration(s.StartWindow) // validated at submission
	return window
}

// A caller's absolute cutoff wins; a relative window starts at dependency
// readiness/assignment as before. Without either, only the submitted deadline
// bounds admission. An installation's observed latency is an estimate, never
// evidence that a healthy command must fail during a larger overload.
func initializeStartDeadline(j *Job, since time.Time) {
	if j.Spec.ExtensionCheck != nil && j.Spec.ExtensionCheck.Timeout != "" {
		if j.Spec.StartBy == nil {
			deadline := since.Add(MaxProductionStartWindow)
			j.Spec.StartBy = &deadline
		}
		return
	}
	if j.Spec.Priority != "production" {
		return
	}
	if j.Spec.StartBy == nil {
		j.StartWindow = productionStartWindow(j.Spec)
		if j.StartWindow > 0 {
			deadline := since.Add(j.StartWindow)
			j.Spec.StartBy = &deadline
		} else if j.Spec.Deadline != nil {
			deadline := *j.Spec.Deadline
			j.Spec.StartBy = &deadline
		}
	} else if j.StartWindow == 0 && (absoluteStartBy(j) || j.Spec.StartWindow != "") {
		j.StartWindow = j.Spec.StartBy.Sub(j.Queued)
	}
	if j.Spec.Deadline != nil && (j.Spec.StartBy == nil || j.Spec.Deadline.Before(*j.Spec.StartBy)) {
		deadline := *j.Spec.Deadline
		j.Spec.StartBy = &deadline
	}
}

// Older records did not distinguish generated and caller-owned cutoffs. Keep
// their persisted timestamp authoritative; new submissions always store the bit,
// including false, so generated relative windows remain distinguishable.
func absoluteStartBy(j *Job) bool {
	return j.Spec.StartBy != nil && (j.StartByExplicit == nil || *j.StartByExplicit)
}

func (q *Queue) startupPending(j *Job) bool {
	if j.Spec.FanOut != nil && j.FanOut != nil {
		// A logical parent holds no launch slot. Its relative window belongs
		// to each physical child from assignment, including the first wave.
		if (!absoluteStartBy(j) && j.Spec.Deadline == nil) || j.FanOut.Next == len(j.Spec.FanOut.Pieces) {
			return false
		}
		// An expired tail cannot dispatch, but already materialized children
		// retain their own startup/recovery windows and accepted-start rights.
		// Settle them before failing the parent, whose failure cancels children.
		for _, id := range j.FanOut.Children {
			if child := q.s.Jobs[id]; child != nil && !terminal(child.State) {
				return false
			}
		}
	}
	return (j.Spec.StartBy != nil || j.Spec.Priority == "production") && !terminal(j.State) && !cancelled(j) && (j.State == "queued" || j.StartedAt == nil)
}

func (q *Queue) startWaitingReason(j *Job, scans ...*placementScan) string {
	if reason := q.diskWaitingReason(j, j.ProductionHost); reason != "" {
		return reason
	}
	machines := q.plannerMachines()
	for _, m := range machines {
		if !q.projectAllows(j.Spec.Project, m.ID) || !j.Spec.allowsMachine(m.ID) || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU) {
			continue
		}
		if j.Machine != "" && j.Machine != m.ID {
			continue
		}
		// A matching platform alone does not qualify an extension or satisfy
		// its OS/architecture target. Explain only routes dispatch can use.
		if !executionTargetMatches(j.Spec, m) || len(extensionJobs(j, m, true)) == 0 {
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
	return q.waitReason(j, scans...)
}

// Expire before planning any new reclaim or assignment. Release custody is independent.
func (q *Queue) maintainStartDeadlines() {
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		if j.Spec.Priority == "production" && !terminal(j.State) && (j.Spec.StartBy != nil || dependencyReady(j)) {
			initializeStartDeadline(j, j.Queued)
		}
		// Disabled-host rejection is an admission decision, independent of the
		// relative timeout exemption for logical fan-out capacity waits.
		pending := q.startupPending(j)
		firstWave := j.Spec.FanOut != nil && j.FanOut != nil && j.FanOut.Next == 0
		disabled := (pending || firstWave) && j.Spec.Priority == "production" && !cancelled(j) && dependencyReady(j) && j.State == "queued" && j.Attempt == 0 && q.allStartHostsDisabled(j)
		executionExpired := false
		if check := j.Spec.ExtensionCheck; check != nil && check.Timeout != "" && j.StartedAt != nil && !terminal(j.State) && !cancelled(j) {
			timeout, _ := time.ParseDuration(check.Timeout) // validated at submission
			executionExpired = !now.Before(j.StartedAt.Add(timeout))
		}
		if !pending && !disabled && !executionExpired {
			continue
		}
		if !expired(j.Spec.StartBy, now) && !disabled && !executionExpired {
			continue
		}
		reason := q.startWaitingReason(j)
		outcome := attemptOutcome(j, Result{Interrupted: true})
		if j.State == "queued" && expired(j.Spec.GiveUpAt, now) && (j.Spec.StartBy == nil || j.Spec.GiveUpAt.Before(*j.Spec.StartBy)) {
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
		if j.Spec.ExtensionCheck != nil {
			j.Reason = "never started: " + reason
			if executionExpired {
				j.Reason = "check execution timeout exceeded"
			}
		}
		if disabled && (!pending || !expired(j.Spec.StartBy, now)) {
			j.Reason = "production cannot start: no eligible host is enabled; " + reason
		}
		j.FinishedAt = &now
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
		if !executionExpired && outcome.Attempt > 0 && outcome.Machine != "" && expired(j.Spec.StartBy, now) {
			q.alertAttemptLoss(j, outcome, true)
		}
		// Public receipts deliberately redact internal placement detail. Keep
		// one queue-local diagnostic even when no attempt was ever assigned.
		event := "production_start_failed"
		if j.Spec.ExtensionCheck != nil {
			event = "extension_check_timeout"
		}
		log.Printf("event=%s job=%s attempt=%d reason=%q", event, j.ID, j.Attempt, j.Reason)
		q.recordAttemptOutcome(j, outcome)
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

func (q *Queue) startStatusReason(j *Job, scans ...*placementScan) string {
	reason := q.startWaitingReason(j, scans...)
	if j.Spec.StartBy != nil {
		return reason + fmt.Sprintf("; %.0fs left to start", max(0, j.Spec.StartBy.Sub(q.clock.Now()).Seconds()))
	}
	if observed := q.observedPlacementLatency(j); observed > 0 {
		return reason + fmt.Sprintf("; observed placement latency %.3fs (estimate, no submitted start cutoff)", observed.Seconds())
	}
	return reason + "; no submitted start cutoff; placement latency not yet observed"
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
		if !q.projectAllows(j.Spec.Project, m.ID) || !j.Spec.allowsMachine(m.ID) || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU) {
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
	if parkedStorage(j) {
		return "storage_protection_parked"
	}
	if j.StartedAt == nil && !j.Spec.Needs.cpuOnly() && q.deviceAdmissionBlocked(allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)) {
		return "device_disabled"
	}
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
	case j.StartedAt == nil && j.ScarceHostYield != nil && !q.yieldResumeStartAllowed(j, m):
		return "scarce_host_yield"
	case j.StartedAt == nil && !sharedHostJobFits(j, m):
		return "shared_host_profile"
	case !executionTargetMatches(j.Spec, m):
		return "execution_target"
	case !cpuFits(j, m):
		return "cpu_fit"
	case !q.placementPolicyAllowed(j, m, false):
		return "placement_policy"
	}
	return ""
}
