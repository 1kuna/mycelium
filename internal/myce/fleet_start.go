package myce

import (
	"fmt"
	"slices"
)

func fleetStartProbeSpec(machine string) Spec {
	return Spec{Name: "fleet-start-probe", Project: "mycelium", Class: "maintenance", AllowedMachines: []string{machine}, Needs: ResourceNeeds{Platform: Platform{Required: []string{"cpu"}}, CPUCores: 1, HostMemoryGB: 0.125}, Run: RunSpec{Command: []string{"sh", "-c", `test "${MYCE_TOKEN+x}" != x`}}}
}

// Even an owner policy that ranks maintenance highly cannot make an upgrade
// smoke interrupt useful work. Apply this after the shared resource-plan cache:
// another job's identical resource vector must not lend it interruption power.
func (q *Queue) fleetStartPlan(j *Job, plan *placementPlan) *placementPlan {
	if u := q.s.FleetUpgrade; u != nil && plan != nil && plan.stops > 0 {
		for _, h := range u.Hosts {
			if j.ID == h.StartProbe {
				return nil
			}
		}
	}
	return plan
}

func (q *Queue) fleetSnapshotStarts(h *fleetUpgradeHost) {
	h.StartBefore = nil
	for _, j := range q.liveJobs() {
		if j.Machine == h.Machine && j.Attempt > 0 {
			h.StartBefore = append(h.StartBefore, Execution{ID: j.ID, Attempt: j.Attempt})
		}
	}
}

// Health and RunnerSmoke qualify the service process, not its detached
// assignment launcher (#1709). Use an ordinary, queue-owned CPU assignment to
// prove that path, including on an otherwise idle host. There is no scheduler,
// resource, maintenance, custody or start-authorization exception for the probe.
func (q *Queue) fleetVerifyStart(h *fleetUpgradeHost) bool {
	m := q.s.Machines[h.Machine]
	if m == nil || m.CapabilityProfile == nil || m.Fleet != nil && (m.Fleet.Session != m.Session || !q.fleetCurrent(m.Fleet, m.CapabilityProfile.OS, m.CapabilityProfile.Arch) || fleetHealthIdentity(m.Fleet) != h.StartIdentity) {
		q.fleetStop(h, "worker_start_runtime_changed")
		return false
	}
	if m.Fleet != nil && fleetProofFailure(m.Fleet) != "" {
		q.fleetStop(h, "worker_start_runtime_changed")
		return false
	}
	if !q.fleetProofFresh(m.Fleet) {
		if q.clock.Now().Sub(h.StepAt) >= fleetStepTimeout {
			q.fleetStop(h, "worker_start_not_verified")
			return false
		}
		h.Wait = "waiting for the upgraded worker's next healthy runtime proof before accepting its start probe"
		return false
	}
	j := q.s.Jobs[h.StartProbe]
	if h.StartProbe != "" && !q.fleetStartProbeValid(h, m, j) {
		return false
	}
	// Useful work may win ordinary admission ahead of the smoke. A newly
	// assigned execution reaching Started on this verified session proves the
	// same start path without waiting for customer work to finish. Retained
	// pre-upgrade attempts and their replays cannot supply this evidence.
	for _, j := range q.liveJobs() {
		if j.ID != h.StartProbe && j.Machine == m.ID && j.Session == m.Session && j.StartedAt != nil && !slices.Contains(h.StartBefore, Execution{ID: j.ID, Attempt: j.Attempt}) {
			if probe := q.s.Jobs[h.StartProbe]; probe != nil && !terminal(probe.State) {
				q.cancelJob(probe)
			}
			h.StartAccepted = j.ID
			return true
		}
	}
	if h.StartProbe == "" {
		if h.Owned && h.Policy.Mode == "drained" {
			if err := q.changeHostMaintenance(h.Host, HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &h.Policy.UpdatedAt, IfDrainOwner: h.Policy.DrainOwner}); err != nil {
				q.fleetStop(h, "resume_conflict")
				return false
			}
			h.Policy = q.s.HostMaintenance[h.Host]
		}
		if q.maintenanceBlocked(h.Host) {
			q.fleetStop(h, "worker_start_maintenance_blocked")
			return false
		}
		id := ID()
		now := q.clock.Now()
		s := fleetStartProbeSpec(m.ID)
		q.putJob(id, &Job{ID: id, Spec: s, State: "queued", Queued: now, WaitingSince: now, Checkpoint: q.checkpointPath(id), FileTransport: q.fileCheckpoints, PriorityPolicy: q.priorityMatch(s)})
		h.StartProbe = id
		h.ProgressAt = now
	}
	j = q.s.Jobs[h.StartProbe]
	if !q.fleetStartProbeValid(h, m, j) {
		return false
	}
	if j.State == "succeeded" && j.StartedAt != nil && j.ReleaseConfirmed {
		h.StartAccepted = j.ID
		return true
	}
	if reason := q.fleetStartAdmissionReason(m, j); reason != "" {
		h.Phase, h.Outcome, h.DeferredReason = "deferred", FleetHostDeferred, reason
		h.Wait = fmt.Sprintf("start probe %s deferred: %s; will revisit after the other hosts", h.StartProbe, reason)
		h.ProgressAt = q.clock.Now()
		return false
	}
	// Once offered, a never-starting launcher still needs a finite failure
	// budget. Keep the existing service-step bound for this liveness check;
	// measured admission pressure above consumes no such budget.
	if q.clock.Now().Sub(h.StepAt) >= fleetStepTimeout {
		q.fleetStop(h, "worker_start_not_verified")
		return false
	}
	h.Wait = fmt.Sprintf("waiting for start probe %s on upgraded worker session to start, succeed and release", h.StartProbe)
	return false
}

// Deferral never excuses an execution failure, retry or stale runtime fence.
func (q *Queue) fleetStartProbeValid(h *fleetUpgradeHost, m *Machine, j *Job) bool {
	if j == nil || cancelled(j) || j.Attempt > 1 || j.Starts > 1 || j.InfrastructureRetries > 0 || j.Losses > 0 || terminal(j.State) && j.State != "succeeded" || j.Attempt > 0 && (j.Machine != m.ID || j.Session != m.Session) {
		q.fleetStop(h, "worker_start_probe_failed")
		return false
	}
	return true
}

// Use current scheduler accounting, never asynchronous diagnostic snapshots.
// Only measured owner pressure or an occupied CPU/RAM budget can defer a
// never-assigned probe. Unknown telemetry and structural/policy refusals retain
// the ordinary failure path. All work here is in-memory under the queue lock.
func (q *Queue) fleetStartAdmissionReason(m *Machine, j *Job) string {
	if j.State != "queued" || j.Attempt != 0 || j.StartedAt != nil || j.Starts != 0 {
		return ""
	}
	q.refreshHostMemory()
	m = cpuMachineView(m)
	if !m.HostMemoryMeasured || m.HostMemoryError != "" || m.CPUCapacityReason != "" {
		return ""
	}
	pressure := m.SharedHost != nil && q.sharedPressureFresh(m) && m.SharedHostYieldReason != "" && m.SharedHostReason == q.hostPressureReason(m)
	if why := q.sharedHostAdmissionRefusal(j, m); why != "" && !pressure {
		return ""
	}
	copy := *m
	if pressure {
		copy.SharedHostReason = ""
	}
	var fit targetFit
	fitsResourcesChecked(j, &copy, copy.FreeMemoryGB, &fit)
	for _, failed := range fit.failed {
		if failed != "cores" || !m.cpuLedger || m.EffectiveCPUCores <= 0 || m.FreeCPUCores >= j.Spec.Needs.cores() {
			return ""
		}
	}
	if pressure {
		return "shared_host_owner_pressure: " + m.SharedHostYieldReason
	}
	if why := hostFitFailure(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB); why == "host memory" {
		return fmt.Sprintf("no_capacity: start probe needs %.3f GiB host RAM; %.3f GiB admissible", hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), m.FreeHostMemoryGB)
	}
	if len(fit.failed) > 0 {
		return fmt.Sprintf("no_capacity: start probe needs %d CPU core; %d admissible", j.Spec.Needs.cores(), m.FreeCPUCores)
	}
	return ""
}
