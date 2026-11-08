package myce

import (
	"context"
	"time"
)

// Receipt metadata is immutable; its request lifecycle is queue-owned under
// contactMu. Worker-local age still uses
// wall time: an unchanged sensor cannot be renewed by later check-ins. Only
// time consumed by this queue accepting a report, or unavailable for its next
// replacement, is excluded from the queue's freshness decision.
type hostHealthDelivery struct {
	order       uint64
	ageSeconds  float64
	opportunity time.Duration
	preparation *hostHealthPreparation
}

func (q *Queue) acceptHostHealthDelivery(m, old *Machine, order uint64, reportedAge float64, preparation *hostHealthPreparation) {
	// Overlapping retries can finish bulk preparation in reverse order. Keep
	// newer accepted health (and its original age/baseline), including refusal.
	if old != nil && old.Session == m.Session && old.HostHealth != nil && old.HostHealth.delivery != nil && old.HostHealth.delivery.order > order {
		health := cloneHostHealth(*old.HostHealth)
		// LastSeen belongs to this check-in; retain physical diagnostic age
		// from the newer evidence's original acceptance rather than reset it.
		if elapsed := m.LastSeen.Sub(old.LastSeen); elapsed >= 0 {
			health.AgeSeconds += elapsed.Seconds()
		} else {
			health.AgeSeconds = -1
		}
		m.HostHealth = &health
		return
	}
	if h := m.HostHealth; h != nil {
		h.delivery = &hostHealthDelivery{order: order, ageSeconds: reportedAge, opportunity: q.controlNow(), preparation: preparation}
	}
}

func (q *Queue) hostHealthEvidenceValid(m *Machine, now time.Time) bool {
	h := m.HostHealth
	if h == nil || h.delivery == nil || q.mu.control == nil {
		// Restarts retain physical wall age until another report is accepted.
		return hostHealthEvidenceValid(h, now.Sub(m.LastSeen))
	}
	copy := *h
	copy.AgeSeconds = h.delivery.ageSeconds
	return !now.Before(m.LastSeen) && validMemoryValue(h.AgeSeconds) && hostHealthEvidenceValid(&copy, q.hostHealthServiceElapsed(m))
}

// A fresh report pauses only its session's accepted service age while this
// queue prepares it and its reply. It cannot revive evidence expired at receipt.
// Successful completion retains the actual processing credit on that receipt;
// rejection/cancellation removes it. contactMu protects the request lifecycle,
// independently of transactional contact/loss bookkeeping.
type hostHealthPreparation struct {
	machine, session  string
	order             uint64
	opportunity       time.Duration
	ctx               context.Context
	finished, success bool
	completed         time.Duration
}

func (q *Queue) beginHostHealthPreparation(ctx context.Context, m Machine, order uint64) *hostHealthPreparation {
	h := m.HostHealth
	if h == nil || h.Probe == nil || !hostHealthEvidenceValid(h, 0) || h.DrainReason != "" {
		return nil
	}
	preparation := &hostHealthPreparation{machine: m.ID, session: m.Session, order: order, opportunity: q.controlNow(), ctx: ctx}
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if q.healthPreparing == nil {
		q.healthPreparing = make(map[string]map[*hostHealthPreparation]struct{})
	}
	if q.healthPreparing[m.ID] == nil {
		q.healthPreparing[m.ID] = make(map[*hostHealthPreparation]struct{})
	}
	q.healthPreparing[m.ID][preparation] = struct{}{}
	return preparation
}

func (q *Queue) finishHostHealthPreparation(preparation *hostHealthPreparation, success bool) {
	if preparation == nil {
		return
	}
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if preparation.finished {
		return
	}
	preparation.finished = true
	preparation.success = success && preparation.ctx.Err() == nil
	preparation.ctx = nil
	preparation.completed = q.controlNow()
	delete(q.healthPreparing[preparation.machine], preparation)
	if len(q.healthPreparing[preparation.machine]) == 0 {
		delete(q.healthPreparing, preparation.machine)
	}
}

func (q *Queue) hostHealthServiceElapsed(m *Machine) time.Duration {
	delivery := m.HostHealth.delivery
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	baseline := delivery.opportunity
	if preparation := delivery.preparation; preparation != nil && preparation.finished && preparation.success {
		baseline = max(baseline, preparation.completed)
	}
	elapsed := q.controlNow() - baseline
	if elapsed < 0 {
		return elapsed
	}
	for preparation := range q.healthPreparing[m.ID] {
		if preparation.session == m.Session && preparation.order >= delivery.order && preparation.ctx.Err() == nil {
			elapsed = min(elapsed, max(0, preparation.opportunity-baseline))
		}
	}
	return elapsed
}

// Detached reads retain effective service age, never a live request pointer.
// Their clock is frozen at the same opportunity while q.mu is held.
func (q *Queue) freezeHostHealthDelivery(m *Machine) *hostHealthDelivery {
	if m.HostHealth == nil || m.HostHealth.delivery == nil || q.mu.control == nil {
		return nil
	}
	frozen := *m.HostHealth.delivery
	frozen.opportunity = q.controlNow() - q.hostHealthServiceElapsed(m)
	frozen.preparation = nil
	return &frozen
}
