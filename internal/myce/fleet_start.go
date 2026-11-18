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
	if h := q.fleetActiveHost(); h != nil && j.ID == h.StartProbe && plan != nil && plan.stops > 0 {
		return nil
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
	if q.clock.Now().Sub(h.StepAt) >= fleetStepTimeout {
		q.fleetStop(h, "worker_start_not_verified")
		return false
	}
	if !q.fleetProofFresh(m.Fleet) {
		h.Wait = "waiting for the upgraded worker's next healthy runtime proof before accepting its start probe"
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
	j := q.s.Jobs[h.StartProbe]
	if j == nil || cancelled(j) || j.Attempt > 1 || j.Starts > 1 || j.InfrastructureRetries > 0 || j.Losses > 0 || terminal(j.State) && j.State != "succeeded" || j.Attempt > 0 && (j.Machine != m.ID || j.Session != m.Session) {
		q.fleetStop(h, "worker_start_probe_failed")
		return false
	}
	if j.State == "succeeded" && j.StartedAt != nil && j.ReleaseConfirmed {
		h.StartAccepted = j.ID
		return true
	}
	// A blocked/full host waits within the finite service-step budget rather
	// than interrupting existing work to manufacture rollout acceptance.
	h.Wait = fmt.Sprintf("waiting for start probe %s on upgraded worker session to start, succeed and release", h.StartProbe)
	return false
}
