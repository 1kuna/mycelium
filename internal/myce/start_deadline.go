package myce

import (
	"fmt"
	"log"
	"slices"
	"time"
)

// Budget includes up to 15s accelerator TERM grace, 120s cleanup escalation,
// and 15s for reporting and replacement launch; grace never extends StartBy.
const ProductionStartTimeout = maxGPUStopGrace + StopGrace + 15*time.Second

// Bound unavailable-route and stuck-preparation detection. Qualified capacity
// waiting is separate from this relative startup budget.
const MaxProductionStartWindow = time.Hour

func startCapacityBudget(j *Job) time.Duration {
	if j.StartCapacityBudget != nil {
		return *j.StartCapacityBudget
	}
	return j.StartWindow
}

func productionStartWindow(s Spec) time.Duration {
	if s.StartWindow != "" {
		window, _ := time.ParseDuration(s.StartWindow) // validated at submission
		return window
	}
	return ProductionStartTimeout
}

// Keep the durable original length separate from the advancing recovery cutoff.
// An absolute caller deadline always wins over a relative declaration.
func initializeStartDeadline(j *Job, since time.Time) {
	if j.Spec.ExtensionCheck != nil && j.Spec.ExtensionCheck.Timeout != "" {
		// Reuse the startup fence without promoting qualification to production.
		// One hour bounds unavailable-route/preparation waits at the existing
		// maximum start window. Capacity waiting pauses this startup budget,
		// independently of the execution budget.
		if j.Spec.StartBy == nil {
			j.StartWindow = MaxProductionStartWindow
			deadline := since.Add(j.StartWindow)
			j.Spec.StartBy = &deadline
		} else if j.StartWindow == 0 {
			j.StartWindow = j.Spec.StartBy.Sub(j.Queued)
		}
		return
	}
	if j.Spec.Priority != "production" {
		return
	}
	if j.Spec.StartBy == nil {
		j.StartWindow = productionStartWindow(j.Spec)
		deadline := since.Add(j.StartWindow)
		j.Spec.StartBy = &deadline
	} else if j.StartWindow == 0 {
		j.StartWindow = j.Spec.StartBy.Sub(j.Queued)
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
		if !absoluteStartBy(j) || j.FanOut.Next == len(j.Spec.FanOut.Pieces) {
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
	if q.startCapacityPaused(j) {
		return false // Capacity is not failed startup; absolute cutoffs still bind.
	}
	return j.Spec.StartBy != nil && !terminal(j.State) && !cancelled(j) && (j.State == "queued" || j.StartedAt == nil)
}

func (q *Queue) startCapacityPaused(j *Job) bool {
	return j.Spec.StartBy != nil && !absoluteStartBy(j) && !terminal(j.State) && !cancelled(j) &&
		(q.launchCapacityPending(j) || j.State == "queued" && (j.StartCapacityWaiting || j.DeviceFaultStartWait != nil))
}

func (q *Queue) startWaitingReason(j *Job, scans ...*placementScan) string {
	if h := q.diskWait(j, j.ProductionHost); h != nil {
		return h.Detail
	}
	machines := q.planningMachines(scans...)
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
		if h := q.startupReleaseHold(host); h.Kind != "" {
			return h.Detail
		}
		if q.maintenanceBlocked(host) {
			return "host " + host + " in planned maintenance"
		}
		if b, _ := q.hostBlockerState(host); b != "" && q.held(host) {
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
			if j.Spec.StartBy == nil || j.StartWindow == 0 {
				q.touchJob(j.ID)
				initializeStartDeadline(j, j.Queued)
			}
		}
	}
	// Resolve the backlog in one immutable planning pass. Deadline transitions
	// below can change jobs, so discard this evidence cache before applying them.
	type capacityStatus struct{ waiting, ready bool }
	capacity := func() map[*Job]capacityStatus {
		previous := q.memoryPlanning
		q.memoryPlanning = newMemoryPlanningCache()
		defer func() { q.memoryPlanning = previous }()
		status := map[*Job]capacityStatus{}
		for _, j := range q.liveJobs() {
			if j.State == "queued" && j.Spec.StartBy != nil && !absoluteStartBy(j) {
				waiting, ready := q.queuedCapacityStatus(j)
				status[j] = capacityStatus{waiting, ready}
			}
		}
		return status
	}()
	for _, j := range q.liveJobs() {
		faultWaiting := q.maintainDeviceFaultStartWait(j)
		if status, ok := capacity[j]; ok && !faultWaiting {
			if j.StartCapacityWaiting != status.waiting || j.StartCapacityWaiting && status.ready {
				q.touchJob(j.ID)
			}
			if j.StartCapacityWaiting && status.ready && j.Spec.StartBy != nil {
				// Capacity recovered before dispatch. Restore startup time without
				// hiding a later unavailable/disabled-route diagnosis.
				deadline := now.Add(startCapacityBudget(j))
				j.Spec.StartBy = &deadline
			}
			j.StartCapacityWaiting = status.waiting
		}
		// Disabled-host rejection is an admission decision, independent of the
		// relative timeout exemption for logical fan-out capacity waits.
		pending := q.startupPending(j)
		firstWave := j.Spec.FanOut != nil && j.FanOut != nil && j.FanOut.Next == 0
		disabled := (pending || firstWave) && j.Spec.Priority == "production" && j.Spec.StartBy != nil && !cancelled(j) && dependencyReady(j) && j.State == "queued" && j.Attempt == 0 && q.allStartHostsDisabled(j)
		executionExpired := false
		if check := j.Spec.ExtensionCheck; check != nil && check.Timeout != "" && j.StartedAt != nil && !terminal(j.State) && !cancelled(j) {
			timeout, _ := time.ParseDuration(check.Timeout) // validated at submission
			executionExpired = !now.Before(j.StartedAt.Add(timeout))
		}
		if !pending && !disabled && !executionExpired {
			continue
		}
		if pending && !disabled && !executionExpired && j.State == "running" && absoluteStartBy(j) && recordedStartRefusal(j) != "" {
			continue // The refusal report requeues it and restores the caller's remainder.
		}
		if !expired(j.Spec.StartBy, now) && !disabled && !executionExpired {
			continue
		}
		q.touchJob(j.ID)
		reason := q.startWaitingReason(j)
		outcome := attemptOutcome(j, Result{Interrupted: true})
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
		if j.Spec.ExtensionCheck != nil {
			j.Reason = "never started: " + reason
			if executionExpired {
				j.Reason = "check execution timeout exceeded"
			}
		}
		if disabled && (!pending || !expired(j.Spec.StartBy, now)) {
			j.Reason = "production cannot start: no eligible host is enabled; " + reason
		}
		if disabled {
			j.DiagnosticCode = "host_unavailable"
			j.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "host_unavailable", Predicate: "no_eligible_host_enabled"}
			j.ExecutionEvidence = unstartedExecution(0)
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
// The zero Hold means no fence.
func (q *Queue) startupReleaseHold(host string) Hold {
	h, operator := Hold{}, false
	for _, r := range q.s.HostReleases {
		j := q.s.Jobs[r.Job]
		if r.Host == host && j != nil && terminal(j.State) && j.Spec.StartBy != nil {
			m := q.s.Machines[r.Machine]
			operator = operator || m == nil || m.Gone
			h = scopedHold("startup_release", host, fmt.Sprintf("host %s: production startup release pending", host), operator)
		}
	}
	return h
}

func pausedStartBudget(j *Job) string {
	if wait := j.DeviceFaultStartWait; wait != nil {
		return fmt.Sprintf("%.0fs left to start (paused for device recovery)", wait.Remaining.Seconds())
	}
	return fmt.Sprintf("%.0fs left to start (paused for capacity)", startCapacityBudget(j).Seconds())
}

func (q *Queue) startStatusReason(j *Job, scans ...*placementScan) string {
	if q.startCapacityPaused(j) {
		// Assignment restores the saved relative budget. The old saved
		// cutoff is suspended, so subtracting wall time would report zero.
		return q.startWaitingReason(j, scans...) + "; " + pausedStartBudget(j)
	}
	return q.startWaitingReason(j, scans...) + fmt.Sprintf("; %.0fs left to start", max(0, j.Spec.StartBy.Sub(q.clock.Now()).Seconds()))
}

// An unknown fleet may still join; an explicit operator disable is not incoming room.
// A drain owned by the running fleet upgrade is temporary: the host returns
// when its step verifies, so production waits under its ordinary start deadline
// instead of failing. A stopped operation's retained drain is not temporary.
func (q *Queue) allStartHostsDisabled(j *Job) bool {
	hosts := q.startEligibleHosts(j)
	for _, host := range hosts {
		if !q.maintenanceBlocked(host) || q.fleetDrainTemporary(host) {
			return false
		}
	}
	return len(hosts) > 0
}

// startEligibleHosts lists the physical hosts whose policy and capacity could
// start j, independent of maintenance mode and temporary availability.
func (q *Queue) startEligibleHosts(j *Job) []string {
	if model, ok := q.model(j.Spec.Needs.Model); ok && j.Spec.Needs.MemoryGB == 0 {
		copy := cloneJob(j)
		copy.Spec.Needs.MemoryGB = model.MemoryGB
		copy.Spec.Needs.Platform = Platform{Required: model.Platforms}
		j = &copy
	}
	var hosts []string
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
		if host := hostID(m); !slices.Contains(hosts, host) {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

// startPlacementRejection names the failed start-fence predicate, or "". The
// names are fixed labels, safe to log without host, path or volume detail.
func (q *Queue) startPlacementRejection(j *Job) string {
	if j.StartedAt == nil && q.deviceFaultRetryBlocked(j, allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)) {
		return deviceFaultRetryHold
	}
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
