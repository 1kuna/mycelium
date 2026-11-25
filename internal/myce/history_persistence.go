package myce

import (
	"reflect"
	"time"
)

// Retain efficiency readings until their own validity ends; a clean close
// flushes the latest integral. Launch-drift histories no longer size first runs.
type historySnapshot struct {
	efficiency map[string]EfficiencyHost
}

func cloneHistories(efficiency map[string]EfficiencyHost) *historySnapshot {
	h := &historySnapshot{}
	if efficiency != nil {
		h.efficiency = copyObservationValue(reflect.ValueOf(efficiency)).Interface().(map[string]EfficiencyHost)
	}
	return h
}

// persistentBase is the state root as SQLite should hold it. It also stages
// the snapshot that persistUsage adopts only after the commit succeeds.
func (q *Queue) persistentBase() state {
	base := queueBase(q.s, false)
	q.historiesPending = nil
	if q.persisted == nil || q.histories == nil || q.flushHistoriesNow {
		q.historiesPending = cloneHistories(q.s.EfficiencyHosts)
		return base
	}
	writeEfficiency := efficiencyNeedsWrite(q.histories.efficiency, q.s.EfficiencyHosts, q.clock.Now())
	stored := q.histories
	if writeEfficiency {
		next := *q.histories
		next.efficiency = cloneHistories(q.s.EfficiencyHosts).efficiency
		q.historiesPending, stored = &next, &next
	}
	base.EfficiencyHosts = stored.efficiency
	return base
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
	if !efficiencyNeedsWrite(q.histories.efficiency, q.s.EfficiencyHosts, time.Time{}) {
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
