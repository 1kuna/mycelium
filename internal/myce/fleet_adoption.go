package myce

import (
	"runtime"
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
		g, released := a.custodian, a.released || a.finished
		shared := a.worker.Machine.SharedHost != nil
		a.mu.Unlock()
		if released {
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
		if !observed || (!v.Started && !v.Finished) || now.Sub(v.At) > max(3*interval, attemptLivenessMaxAge) || v.State.Result != nil && v.State.Result.ReleasePending {
			proof.Reason = "detached attempt readiness or release is unconfirmed"
			return proof
		}
		proof.Attempts = append(proof.Attempts, Execution{ID: a.job.ID, Attempt: a.job.Attempt})
	}
	return proof
}

func (q *Queue) fleetAdoptionReason(h *fleetUpgradeHost) string {
	m := q.s.Machines[h.Machine]
	if m == nil || !q.fleetProofFresh(m.Fleet) || m.Fleet.Adoption == nil || m.Fleet.Adoption.Protocol != custodianProtocol {
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
		if p.Launch != nil || p.Engine != nil || p.Call != nil || p.Preparation {
			return "model, preparation or cleanup custody requires natural drain"
		}
		if p.Result.Machine != m.ID || p.Result.Session != m.Session {
			return "an older or different worker session retains execution custody"
		}
		found := false
		for _, e := range proof.Attempts {
			found = found || e.ID == p.Result.ID && e.Attempt == p.Result.Attempt
		}
		if !found {
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
	return h != nil && h.Host == host && h.Takeover && h.Phase != "worker_start"
}
