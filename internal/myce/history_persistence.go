package myce

import (
	"maps"
	"reflect"
	"time"
)

// Retain efficiency readings until their own validity ends; a clean close
// flushes the latest integral. Launch-drift histories no longer size first runs.
type historySnapshot struct {
	efficiency        map[string]EfficiencyHost
	contacts          map[string]contactEvidence
	lastWorkerContact time.Time
}

func cloneEfficiencyHistory(efficiency map[string]EfficiencyHost) map[string]EfficiencyHost {
	if efficiency == nil {
		return nil
	}
	return copyObservationValue(reflect.ValueOf(efficiency)).Interface().(map[string]EfficiencyHost)
}

// persistentBase is the state root as SQLite should hold it. It also stages
// the snapshot that persistUsage adopts only after the commit succeeds.
func (q *Queue) persistentBase() state {
	base := queueBase(q.s, false)
	q.historiesPending = nil
	if q.persisted == nil || q.histories == nil || q.flushHistoriesNow {
		q.historiesPending = &historySnapshot{
			efficiency: cloneEfficiencyHistory(q.s.EfficiencyHosts),
			contacts:   maps.Clone(q.s.ContactEvidence), lastWorkerContact: q.s.LastWorkerContact,
		}
		return base
	}
	writeEfficiency := efficiencyNeedsWrite(q.histories.efficiency, q.s.EfficiencyHosts, q.clock.Now())
	next := *q.histories
	if writeEfficiency {
		next.efficiency = cloneEfficiencyHistory(q.s.EfficiencyHosts)
	}
	writeContacts := q.inLockCommit || q.s.LastWorkerContact.Sub(next.lastWorkerContact) >= RestartGrace
	var custodyContacts map[string]contactEvidence
	if writeContacts {
		next.contacts, next.lastWorkerContact = maps.Clone(q.s.ContactEvidence), q.s.LastWorkerContact
	} else if custodyContacts = q.checkpointCustodyContacts(); custodyContacts != nil {
		next.contacts = custodyContacts
	}
	if writeEfficiency || writeContacts || custodyContacts != nil {
		q.historiesPending = &next
	}
	base.EfficiencyHosts = next.efficiency
	base.ContactEvidence, base.LastWorkerContact = next.contacts, next.lastWorkerContact
	return base
}

// A restart uses saved allowances to protect unreported attempts. Save the
// matching report with assignment, start or recovery, and save increases for
// existing custody before acknowledging them. Ordinary timestamps still use
// the periodic checkpoint, whose deadline is independent of these writes.
func (q *Queue) checkpointCustodyContacts() map[string]contactEvidence {
	var next map[string]contactEvidence
	for id, j := range q.records.live {
		if j.State != "running" && j.State != "stopping" {
			continue
		}
		live, ok := q.s.ContactEvidence[j.Machine]
		if !ok || live.Session != j.Session {
			continue
		}
		stored := q.histories.contacts[j.Machine]
		before, dirty := q.records.jobs[id]
		acquired := dirty && (before == nil || before.Machine != j.Machine || before.Session != j.Session ||
			before.Attempt != j.Attempt || before.State != "running" && before.State != "stopping" ||
			before.StartedAt == nil && j.StartedAt != nil)
		if stored.Session == live.Session && !acquired &&
			live.Allowance <= stored.Allowance && live.ArrivalAllowance <= stored.ArrivalAllowance {
			continue
		}
		if stored.Session == live.Session {
			live.Allowance = max(live.Allowance, stored.Allowance)
			live.ArrivalAllowance = max(live.ArrivalAllowance, stored.ArrivalAllowance)
		}
		if next == nil {
			next = make(map[string]contactEvidence, len(q.histories.contacts)+1)
			maps.Copy(next, q.histories.contacts)
		}
		next[j.Machine] = live
	}
	return next
}

// Called after the SQLite commit that wrote the staged snapshot.
func (q *Queue) commitHistories() {
	if q.historiesPending != nil {
		q.histories = q.historiesPending
		q.historiesPending = nil
	}
}

// Writes the live histories once, for a clean shutdown. Every other record is
// byte-compared with its stored copy, so only the state root can change.
func (q *Queue) flushHistories() error {
	if q.db == nil || q.simulation || q.persisted == nil || q.histories == nil || q.records == nil {
		return nil
	}
	if !efficiencyNeedsWrite(q.histories.efficiency, q.s.EfficiencyHosts, time.Time{}) &&
		q.histories.lastWorkerContact.Equal(q.s.LastWorkerContact) && maps.Equal(q.histories.contacts, q.s.ContactEvidence) {
		return nil
	}
	q.flushHistoriesNow = true
	defer func() { q.flushHistoriesNow, q.historiesPending = false, nil }()
	var batch queueWriteBatch
	next, err := q.prepareStateWrite(&batch, &queuePersistenceTiming{}, false)
	if err != nil {
		return err
	}
	if err := q.writeQueue(&batch, nil, nil, queueWriteLocked); err != nil {
		return err
	}
	q.commitPersistenceRoot(next)
	for key, value := range next.records {
		if key != "state" {
			q.noteEvidenceRecord(key, q.persisted[key], value)
		}
		if value == nil {
			delete(q.persisted, key)
		} else {
			q.persisted[key] = value
		}
	}
	q.commitHistories()
	return nil
}

// A zero now compares every field, including measured readings and integrals.
func efficiencyNeedsWrite(stored, live map[string]EfficiencyHost, now time.Time) bool {
	if len(stored) != len(live) {
		return true
	}
	for host, h := range live {
		old, ok := stored[host]
		if !ok || old.Identity != h.Identity || old.Host != h.Host || !old.Since.Equal(h.Since) || reservedRate(old.Rate) != reservedRate(h.Rate) {
			return true
		}
		if now.IsZero() {
			if !reflect.DeepEqual(old, h) {
				return true
			}
			continue
		}
		if old.Rate.MeasuredMemoryGB != nil && !now.Before(old.Rate.ValidUntil) || old.Rate.MeasuredDeviceGB != nil && !now.Before(old.Rate.DeviceValidUntil) {
			return true
		}
	}
	return false
}

// The rate without its readings: reservations, plus whether each measured
// dimension exists, so a host that starts or stops reporting is written.
type rateShape struct {
	efficiencyRate
	memory, device bool
}

func reservedRate(r efficiencyRate) rateShape {
	shape := rateShape{memory: r.MeasuredMemoryGB != nil, device: r.MeasuredDeviceGB != nil}
	r.MeasuredMemoryGB, r.MeasuredDeviceGB = nil, nil
	r.ValidUntil, r.DeviceValidUntil = time.Time{}, time.Time{}
	shape.efficiencyRate = r
	return shape
}
