package myce

import "time"

// StopRequests record intent, not proof of signal delivery, a saved checkpoint
// or physical release. Exact attempt history/checkpoint receipts remain owned
// by the existing worker/result protocol and lifecycle classifier.
type FleetStopRequest struct {
	Obligation     HostObligation `json:"obligation"`
	RequestedAt    time.Time      `json:"requested_at"`
	Resumable      bool           `json:"resumable"`
	ProgressBefore int64          `json:"progress_before"`
}

// Critical keeps the host-by-host order but starts the existing maintenance
// stop as soon as a host is drained, rather than waiting for its work to
// finish: resumable work checkpoints and is requeued (MY-39). StopGrace is the existing checkpoint/signal-delivery budget, not idle
// evidence: even after it expires all custody gates still bind. Operator policy
// is not rewritten, and its drain/disable is never resumed by this operation.
func (q *Queue) maintainFleetCriticalStops(h *fleetUpgradeHost) bool {
	if h.CriticalStopAt == nil {
		for _, worker := range q.hostMaintenanceStatus(h.Host).Workers {
			if !worker.Fresh {
				h.Wait = "waiting for fresh worker contact before the critical checkpoint stop"
				return false
			}
		}
		if h.Queue && !h.QueueDone && (q.fleetQueue == nil || !q.fleetProofFresh(q.fleetQueue.Proof)) {
			h.Wait = "waiting for fresh queue runtime proof before the critical checkpoint stop"
			return false
		}
		deadline := q.clock.Now().Add(StopGrace)
		h.CriticalStopAt = &deadline
	}
	obligations := q.hostObligations(h.Host)
	if h.Queue && !h.QueueDone {
		// The queue owns every model HTTP stream, including remote engines.
		// Other hosts' batch work is outside a queue-only stop's scope.
		for _, c := range q.calls {
			if q.executionHost(c.Machine) != h.Host {
				obligations = append(obligations, HostObligation{Call: &HostCallExecution{HostEngineExecution: HostEngineExecution{Machine: c.Machine, Session: c.Session, Model: c.Model, Device: c.Device, Attempt: c.Attempt}, ID: c.ID}})
			}
		}
	}
	seen := map[string]bool{}
	for _, request := range h.StopRequests {
		seen[obligationKey(request.Obligation)] = true
	}
	for _, obligation := range obligations {
		key := obligationKey(obligation)
		if !seen[key] {
			request := FleetStopRequest{Obligation: obligation, RequestedAt: q.clock.Now()}
			if j := q.retainedJob(obligation.Result.ID); j != nil {
				request.Resumable, request.ProgressBefore = j.Spec.Resumable, j.Progress
			}
			h.StopRequests = append(h.StopRequests, request)
			seen[key] = true
		}
		q.maintainHostObligation(obligation, h.CriticalStopAt)
	}
	return true
}
