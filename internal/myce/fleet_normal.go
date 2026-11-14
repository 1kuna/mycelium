package myce

import (
	"fmt"
	"time"
)

// A plan is a deadline, not evidence of a delivered signal or released custody.
// It is persisted with the rollout so observations/restarts cannot renew it.
type FleetPlannedStop struct {
	Obligation HostObligation `json:"obligation"`
	StopAfter  time.Time      `json:"stop_after"`
	Reason     string         `json:"reason"`
}

// Allow natural completion for the last measured planned stop-to-release cost
// of this job, at most its checkpoint grace. Waiting longer costs more host
// downtime than the stop budget itself. With no completed measurement, use that
// existing budget: it prevents indefinite drains without inventing a new cap or
// forecasting completion (MY-34, MY-49). The cost is up to one grace of idle
// admission before signalling; exact custody can still delay the service update.
func fleetNormalWait(j *Job) (time.Duration, string) {
	for i := len(j.Copies) - 1; i >= 0; i-- {
		c := j.Copies[i]
		if c.StoppedAt == nil || c.ReleasedAt == nil || !c.ReleaseConfirmed || c.LossCharged || (c.StopCause != maintenanceStopCause && c.StopCause != deviceMaintenanceStopCause) {
			continue
		}
		if cost := c.ReleasedAt.Sub(*c.StoppedAt); cost > 0 {
			wait := min(cost, StopGrace)
			return wait, fmt.Sprintf("resumable normal upgrade: last planned stop-to-release %s; natural completion wait %s (capped at checkpoint grace %s)", cost, wait, StopGrace)
		}
	}
	return StopGrace, fmt.Sprintf("resumable normal upgrade: no completed planned-stop measurement; allow one checkpoint grace (%s) for natural completion to avoid an indefinite drain", StopGrace)
}

func (q *Queue) fleetNormalJob(p HostObligation) *Job {
	if p.Launch != nil || p.Call != nil || p.Engine != nil || p.Preparation {
		return nil
	}
	j := q.retainedJob(p.Result.ID)
	if j == nil || j.Attempt != p.Result.Attempt || j.Machine != p.Result.Machine || j.Session != p.Result.Session || j.State != "running" || j.StartedAt == nil || j.Spec.Priority == "production" || !j.Spec.Resumable || cancelled(j) || (!j.FileTransport && j.Checkpoint == "") {
		return nil
	}
	return j
}

func (q *Queue) maintainFleetNormalStops(h *fleetUpgradeHost) bool {
	obligations := q.hostObligations(h.Host)
	seen := map[string]bool{}
	for _, request := range h.StopRequests {
		seen[obligationKey(request.Obligation)] = true
	}
	// Do not interrupt even resumable work while a neighboring non-resumable
	// or production obligation remains. Old cleanup and model custody wait too.
	for _, p := range obligations {
		if !seen[obligationKey(p)] && q.fleetNormalJob(p) == nil {
			h.PlannedStops = nil
			h.Wait = "waiting for running work on the host to finish (non-resumable, production, model work or cleanup)"
			return false
		}
	}
	active := map[string]bool{}
	for _, p := range obligations {
		active[obligationKey(p)] = true
	}
	plans := h.PlannedStops[:0]
	for _, plan := range h.PlannedStops {
		if active[obligationKey(plan.Obligation)] {
			plans = append(plans, plan)
		}
	}
	h.PlannedStops = plans
	for _, p := range obligations {
		key := obligationKey(p)
		if seen[key] {
			continue
		}
		planned := false
		for _, plan := range h.PlannedStops {
			planned = planned || obligationKey(plan.Obligation) == key
		}
		if !planned {
			wait, reason := fleetNormalWait(q.fleetNormalJob(p))
			h.PlannedStops = append(h.PlannedStops, FleetPlannedStop{Obligation: p, StopAfter: q.clock.Now().Add(wait), Reason: reason})
		}
	}
	for _, plan := range h.PlannedStops {
		key := obligationKey(plan.Obligation)
		if seen[key] || q.clock.Now().Before(plan.StopAfter) {
			continue
		}
		j := q.fleetNormalJob(plan.Obligation)
		if j == nil {
			continue
		}
		deadline := q.clock.Now().Add(StopGrace)
		h.StopRequests = append(h.StopRequests, FleetStopRequest{Obligation: plan.Obligation, RequestedAt: q.clock.Now(), Resumable: true, ProgressBefore: j.Progress, Reason: plan.Reason})
		q.maintainHostObligationGrace(plan.Obligation, &deadline, true)
	}
	if len(obligations) > 0 {
		h.Wait = "waiting for running work: planned checkpoint stops are listed in planned_stops; stop_requests records requested stops; exact release is required"
		return false
	}
	return true
}
