package myce

import (
	"maps"
	"reflect"
	"time"
)

// Two state fields are measurement histories that every check-in advances:
// the per-host efficiency integral and the cold-probe launch headroom samples.
// They live in memory and reach SQLite only when the stored copy would stop
// describing what a restart needs (#1066, MY-43). No interval is configured;
// each trigger is the history's own correctness condition:
//
//   - Efficiency: the host set, identity, start, reserved rates and which
//     measured dimensions exist are written at once, so the reconstructed seconds and reservation integrals are exact.
//     A measured rate is written again once its own ValidUntil (last contact
//     plus the measured worker-loss allowance) lapses. Until then a restart
//     extends the stored measurement, which the queue already treats as current
//     for that span; after a crash nothing beyond it is credited, which shows up
//     as lower measured coverage, never as invented usage.
//   - Launch headroom: machine, session, learned refusals and assigned offer
//     margins are written at once. Samples are written when the reserve they
//     yield (largest adjacent drop) differs from the stored samples' reserve, so
//     a restart sizes the next cold probe exactly as the live queue would. The
//     first post-restart sample pairs with the last stored one; that pair can
//     only raise the reserve.
//
// A clean Close writes the live histories. A crash loses at most the samples
// and measured readings taken since the last write, which by construction did
// not change any decision the stored copy supports.
type historySnapshot struct {
	efficiency map[string]EfficiencyHost
	launch     map[string]*hostLaunchMemory
}

func cloneHistories(efficiency map[string]EfficiencyHost, launch map[string]*hostLaunchMemory) *historySnapshot {
	h := &historySnapshot{}
	if efficiency != nil {
		h.efficiency = copyObservationValue(reflect.ValueOf(efficiency)).Interface().(map[string]EfficiencyHost)
	}
	if launch != nil {
		h.launch = copyObservationValue(reflect.ValueOf(launch)).Interface().(map[string]*hostLaunchMemory)
	}
	return h
}

// persistentBase is the state root as SQLite should hold it. It also stages
// the snapshot that persistUsage adopts only after the commit succeeds.
func (q *Queue) persistentBase() state {
	base := queueBase(q.s, false)
	q.historiesPending = nil
	if q.persisted == nil || q.histories == nil || q.flushHistoriesNow {
		q.historiesPending = cloneHistories(q.s.EfficiencyHosts, q.s.HostLaunchMemory)
		return base
	}
	writeEfficiency := efficiencyNeedsWrite(q.histories.efficiency, q.s.EfficiencyHosts, q.clock.Now())
	writeLaunch := launchNeedsWrite(q.histories.launch, q.s.HostLaunchMemory)
	stored := q.histories
	if writeEfficiency || writeLaunch {
		next := *q.histories
		if writeEfficiency {
			next.efficiency = cloneHistories(q.s.EfficiencyHosts, nil).efficiency
		}
		if writeLaunch {
			next.launch = cloneHistories(nil, q.s.HostLaunchMemory).launch
		}
		q.historiesPending, stored = &next, &next
	}
	base.EfficiencyHosts, base.HostLaunchMemory = stored.efficiency, stored.launch
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
	if !efficiencyNeedsWrite(q.histories.efficiency, q.s.EfficiencyHosts, time.Time{}) && !launchSamplesDiffer(q.histories.launch, q.s.HostLaunchMemory) {
		return nil
	}
	q.flushHistoriesNow = true
	defer func() { q.flushHistoriesNow, q.historiesPending = false, nil }()
	tx, err := q.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	next, err := q.persistState(tx, &queuePersistenceTiming{})
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	q.persistedStateFields = next.stateFields
	for key, value := range next.records {
		q.noteEvidenceRecord(key, q.persisted[key], value)
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

func launchNeedsWrite(stored, live map[string]*hostLaunchMemory) bool {
	if len(stored) != len(live) {
		return true
	}
	for host, h := range live {
		old := stored[host]
		if old == nil || h == nil || old.Machine != h.Machine || old.Session != h.Session || old.RefusalGB != h.RefusalGB || !maps.Equal(old.Refusals, h.Refusals) || !maps.Equal(old.Assigned, h.Assigned) {
			return true
		}
		a, aok := sampleDriftGB(old.Samples)
		b, bok := sampleDriftGB(h.Samples)
		if aok != bok || a != b {
			return true
		}
	}
	return false
}

func launchSamplesDiffer(stored, live map[string]*hostLaunchMemory) bool {
	if launchNeedsWrite(stored, live) {
		return true
	}
	for host, h := range live {
		if !reflect.DeepEqual(stored[host].Samples, h.Samples) {
			return true
		}
	}
	return false
}

// The sample-derived part of coldProbeDriftMarginGB.
func sampleDriftGB(samples []hostHeadroomSample) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	drift := samples[0].AvailableGB - samples[1].AvailableGB + minimumHostMemoryGB
	for i := 2; i < len(samples); i++ {
		drift = max(drift, samples[i-1].AvailableGB-samples[i].AvailableGB+minimumHostMemoryGB)
	}
	return drift, true
}
