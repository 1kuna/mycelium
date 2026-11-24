package myce

import (
	"runtime"
	"slices"
	"time"
)

// No local PIDs or paths enter this proof. The worker verifies private process
// births/scope records; the queue compares only exact execution obligations.
type WorkerAdoptionProof struct {
	Protocol int         `json:"protocol"`
	Attempts []Execution `json:"attempts"`
	Reason   string      `json:"reason,omitempty"`
}

func workerAdoptionProof(actors map[attemptKey]*attemptSupervisor, now time.Time, interval time.Duration) *WorkerAdoptionProof {
	proof := &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{}}
	for _, a := range actors {
		a.mu.Lock()
		g, released, finished := a.custodian, a.released, a.finished
		shared := a.worker.Machine.SharedHost != nil
		a.mu.Unlock()
		if finished || released && g == nil {
			continue
		}
		if g == nil {
			proof.Reason = "attempt has no detached custodian"
			if runtime.GOOS == "linux" && shared {
				proof.Reason = "Linux shared-host aggregate CPU quota requires natural drain"
			}
			return proof
		}
		g.mu.Lock()
		v, observed := g.last, g.observed
		g.mu.Unlock()
		// The independent owner exists before input staging or environment
		// acquisition begins. Started describes the payload, not adoptability:
		// g.read already verified the exact custodian birth and manager scope.
		// A locally released attempt can still owe the queue logs or results.
		// Its durable snapshot survives the swap and needs no live heartbeat.
		record := attemptRecordFor(&a.job, a.job.Machine)
		resultReady := v.Finished && record.readyToRelease(v.State.Result, false)
		live := !v.Finished && v.State.Result == nil && !released && !now.Before(v.At) && now.Sub(v.At) <= max(3*interval, attemptLivenessMaxAge)
		if !observed || !resultReady && !live {
			if released {
				continue // An unrelated, already released actor cannot veto takeover.
			}
			proof.Reason = "detached attempt readiness or release is unconfirmed"
			return proof
		}
		proof.Attempts = append(proof.Attempts, Execution{ID: a.job.ID, Attempt: a.job.Attempt})
	}
	return proof
}

func (q *Queue) fleetAdoptionReason(h *fleetUpgradeHost) string {
	m := q.s.Machines[h.Machine]
	if m == nil || !q.fleetProofFresh(m.Fleet) || m.Fleet.Session != m.Session || m.Fleet.Adoption == nil || m.Fleet.Adoption.Protocol != custodianProtocol {
		return "worker has no current live takeover proof"
	}
	proof := m.Fleet.Adoption
	if proof.Reason != "" {
		switch proof.Reason {
		case "attempt has no detached custodian", "Linux shared-host aggregate CPU quota requires natural drain", "detached attempt readiness or release is unconfirmed":
			return proof.Reason
		}
		return "worker cannot prove detached attempt readiness"
	}
	for _, p := range q.hostObligations(h.Host) {
		if p.Engine != nil || p.Call != nil {
			if reason := q.modelAdoptionReason(m, p); reason != "" {
				return reason
			}
			continue
		}
		if p.Launch != nil && !p.Launch.Started {
			continue // AuthorizeLaunch fences the still-unstarted offer at cutover.
		}
		if p.Launch != nil || p.Preparation {
			// Standalone background preparation belongs to the worker's
			// goroutine/cache writer, unlike staging inside an exact attempt.
			return "preparation has no detached custodian"
		}
		owner := attemptRecord{key: attemptKey{p.Result.ID, p.Result.Attempt, p.Result.Session}, machine: p.Result.Machine}
		if !owner.sameWorker(m.ID, m.Session) {
			return "an older or different worker session retains execution custody"
		}
		if !slices.ContainsFunc(proof.Attempts, owner.key.matches) {
			return "a retained attempt is absent from the live takeover proof"
		}
	}
	return ""
}

// This short cutover fence changes no maintenance policy and leaves running
// custody intact. New engines/preparations cannot race final authorization.
func (q *Queue) fleetTakeoverFence(host string) bool {
	for machine := range q.s.WorkerHandovers {
		if m := q.s.Machines[machine]; m != nil && hostID(m) == host {
			return true
		}
	}
	h := q.fleetActiveHost()
	return h != nil && h.Host == host && h.Machine != "" && !h.WorkerDone && h.Takeover && h.Phase != "worker_start"
}
