package myce

import "time"

// A runtime proof is fresh against its own process's measured proof cadence
// (#1450). The proof a check-in carries was collected about one worker
// heartbeat before it was sent, and the queue keeps it until the next check-in
// replaces it, so a healthy proof reaches about two heartbeat cycles of age.
// Under host load those cycles stretch: on 2026-11-12 the queue saw healthy
// proofs 5–8 s old against the fixed 5 s bound and refused every rollout.
//
// So the queue learns, per process, the age each proof had reached when its
// successor arrived, with the same RFC 6298 estimator as worker loss (#1465),
// and accepts a proof younger than that mean plus four deviations. It is never
// stricter than fleetRuntimeFreshFor, and a new process starts there, so a
// replaced process is judged against the strict bound until its own cadence
// is measured. fleetProofMaxAge is the ceiling: a process silent beyond it is
// treated as gone however slow its cadence was.
type fleetProofCadence struct {
	pid int
	age probeEstimate // seconds; age reached when the next delivery replaced it
}

// fleetProofMaxAge is the measured filesystem-operation stall backstop (#289),
// the same bound that limits how long a live poll can cover check-in silence
// (#1465). A process whose proof has not been delivered for that long has
// stalled beyond any measured healthy stall, or is dead.
const fleetProofMaxAge = onboardingFilesystemStallBudget

// noteFleetProof records the delivery of next, replacing prev, at now. Only
// consecutive proofs of one process teach its cadence; a different process
// forgets the old one. Caller holds q.mu for writing.
func (q *Queue) noteFleetProof(prev, next *FleetRuntimeProof, now time.Time) {
	if next == nil || next.Session == "" {
		return
	}
	if prev != nil && prev.Session != next.Session {
		delete(q.fleetCadence, prev.Session)
	}
	if q.fleetCadence == nil {
		q.fleetCadence = map[string]*fleetProofCadence{}
	}
	c := q.fleetCadence[next.Session]
	if c == nil || c.pid != next.PID {
		c = &fleetProofCadence{pid: next.PID}
		q.fleetCadence[next.Session] = c
	}
	if prev == nil || prev.Session != next.Session || prev.PID != next.PID || prev.ObservedAt.IsZero() {
		return
	}
	// A proof that already outlived the ceiling taught nothing about health.
	if age := now.Sub(prev.ObservedAt); age > 0 && age < fleetProofMaxAge {
		c.age.observe(age.Seconds())
	}
}

// fleetProofBound is how old p may be and still count as fresh.
func (q *Queue) fleetProofBound(p *FleetRuntimeProof) time.Duration {
	bound := fleetRuntimeFreshFor
	if c := q.fleetCadence[p.Session]; c != nil && c.pid == p.PID && c.age.Mean > 0 {
		bound = max(bound, time.Duration(c.age.upper()*float64(time.Second)))
	}
	return min(bound, fleetProofMaxAge)
}

func (q *Queue) fleetProofAge(p *FleetRuntimeProof) (time.Duration, bool) {
	if !fleetProofReady(p) || p.ObservedAt.IsZero() {
		return 0, false
	}
	age := q.clock.Now().Sub(p.ObservedAt)
	return age, age >= 0
}

func (q *Queue) fleetProofFresh(p *FleetRuntimeProof) bool {
	age, ok := q.fleetProofAge(p)
	return ok && age < q.fleetProofBound(p)
}

// fleetProofLate reports one late proof from the process whose health window
// is running: younger than one further bound, so a single slow delivery keeps
// the window while a process that stopped proving still resets it.
func (q *Queue) fleetProofLate(h *fleetUpgradeHost, p *FleetRuntimeProof) bool {
	age, ok := q.fleetProofAge(p)
	return ok && !h.HealthSince.IsZero() && h.HealthIdentity == fleetHealthIdentity(p) && age < min(2*q.fleetProofBound(p), fleetProofMaxAge)
}
