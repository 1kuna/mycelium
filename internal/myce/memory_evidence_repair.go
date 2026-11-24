package myce

import (
	"cmp"
	"database/sql"
	"errors"
	"log"
	"maps"
	"slices"
)

type importedMemoryBounds struct {
	invalid, verified, ambiguous float64
}

// The v1 importer discarded provenance when it took the exact maximum. Use
// its own key and peak projection to identify a bad import at that maximum.
// A tie with any other plausible source keeps the floor.
func importedMemoryFloors(legacy map[string]WorkObservation) map[memoryEvidenceKey]importedMemoryBounds {
	bounds := make(map[memoryEvidenceKey]importedMemoryBounds)
	add := func(key memoryEvidenceKey, peak *AttemptMemoryPeak, invalid bool) {
		for _, point := range appendProjectedMemoryPeaks(nil, key, memoryEvidencePoint{}, peak) {
			b := bounds[point.Key]
			if invalid {
				b.invalid = max(b.invalid, point.Peak)
			} else if legacyWorkloadLimited(peak) {
				b.verified = max(b.verified, point.Peak)
			} else {
				b.ambiguous = max(b.ambiguous, point.Peak)
			}
			bounds[point.Key] = b
		}
	}
	for _, p := range legacy {
		key, ok := legacyMemoryEvidenceKey(p)
		if !ok {
			continue
		}
		if prior := p.MemoryEscalation; prior != nil && prior.Peak != nil {
			// The legacy invalidator copied a dated, accepted, unconstrained peak.
			add(key, prior.Peak, !prior.RecordedAt.IsZero() && !prior.Peak.constrained() && !legacyWorkloadLimited(prior.Peak))
		}
		if p.MemoryPeak.constrained() {
			add(key, p.MemoryPeak, p.MemoryRecordedAt.IsZero() && legacyHostOnlyDiagnostic(p.MemoryPeak))
		}
	}
	return bounds
}

func legacyHostOnlyDiagnostic(p *AttemptMemoryPeak) bool {
	if p.Pressure == nil || p.SwapGB == nil || *p.SwapGB != 0 || p.Events == nil ||
		*p.Events != (MemoryEventCounts{}) || p.Pressure.CgroupSwapOutPages != 0 || p.Pressure.CgroupRefaultAnon != 0 {
		return false
	}
	switch p.Pressure.Reason {
	case "host_swap_growth", "host_psi_total", "host_pressure_level", "available_below_floor", "host_oom":
		return true
	default:
		return false
	}
}

// Retained outcomes and measurements are joined by attempt, machine and
// session. A retried attempt may no longer have a reconstructable exact key.
// In that case protect possible matching floors in its work family.
func retainedMemoryLimits(s state, candidates map[memoryEvidenceKey]importedMemoryBounds) (map[memoryEvidenceKey]float64, map[string]bool) {
	limits, uncertain := make(map[memoryEvidenceKey]float64), make(map[string]bool)
	if len(candidates) == 0 {
		return limits, uncertain
	}
	families := make(map[string]bool)
	for key := range candidates {
		families[key.Family] = true
	}
	for _, j := range s.Jobs {
		if len(j.MemoryAttempts) == 0 {
			continue
		}
		possible := map[string]bool{}
		if family := memoryFamily(j); families[family] {
			possible[family] = true
		}
		for _, extension := range j.Extensions {
			identity := *j
			identity.Extension = &ExtensionSelection{Digest: extension.Digest}
			if family := memoryFamily(&identity); families[family] {
				possible[family] = true
			}
		}
		if len(possible) == 0 {
			continue
		}
		for _, facts := range j.MemoryAttempts {
			peak := facts.MemoryPeak
			if peak == nil {
				continue
			}
			assignment := retainedMemoryAssignment(j, facts)
			limited := legacyWorkloadLimited(peak)
			pressure := assignment.StopCause == memoryPressureCause
			for _, outcome := range j.AttemptHistory {
				if outcome.Attempt == facts.Attempt && outcome.Machine == facts.Machine && outcome.Session == facts.Session {
					limited = limited || outcome.OutOfMemory
					pressure = pressure || outcome.Reason == memoryPressureCause ||
						outcome.ExecutionCause != nil && outcome.ExecutionCause.Class == memoryPressureCause
					break
				}
			}
			if !limited && !pressure {
				continue
			}
			key, known := retainedMemoryEvidenceKey(j, s.Machines[facts.Machine], assignment)
			if !known || !possible[key.Family] {
				for family := range possible {
					uncertain[family] = true
				}
				continue
			}
			for _, point := range appendProjectedMemoryPeaks(nil, key, memoryEvidencePoint{}, peak) {
				if _, ok := candidates[point.Key]; !ok {
					continue
				}
				if pools := assignment.Memory; pressure && pools != nil {
					estimate := pools.RAM
					if point.Key.Pool == "card" && pools.Card != nil {
						estimate = *pools.Card
					}
					if point.Peak <= estimate.ChargeGB {
						continue // The pooled learner ignored this bystander stop.
					}
				}
				limits[point.Key] = max(limits[point.Key], point.Peak)
			}
		}
	}
	return limits, uncertain
}

func retainedMemoryAssignment(j *Job, facts AttemptMemoryFacts) Copy {
	for _, copy := range j.Copies {
		if copy.Attempt == facts.Attempt && copy.Machine == facts.Machine && copy.Session == facts.Session {
			return copy
		}
	}
	if facts.Attempt == j.Attempt && facts.Machine == j.Machine && facts.Session == j.Session {
		return Copy{Attempt: j.Attempt, Extension: j.Extension, AllocatedDevices: j.AllocatedDevices, Memory: j.Memory, StopCause: j.Reason}
	}
	return Copy{}
}

func retainedMemoryEvidenceKey(j *Job, m *Machine, assignment Copy) (memoryEvidenceKey, bool) {
	if m == nil || assignment.Attempt == 0 || j.CPUFallbackFrom != nil && j.Attempt != assignment.Attempt {
		return memoryEvidenceKey{}, false
	}
	m = allocationMachine(m, assignment.AllocatedDevices)
	if target := machineTarget(m); target.OS == "" || target.Platform == "" {
		return memoryEvidenceKey{}, false
	}
	var extension *Extension
	if j.Spec.extensionJob() {
		if assignment.Extension == nil {
			return memoryEvidenceKey{}, false
		}
		for i := range j.Extensions {
			e := &j.Extensions[i]
			if e.Digest == assignment.Extension.Digest && e.matchesTarget(m) {
				extension = e
				break
			}
		}
		if extension == nil {
			return memoryEvidenceKey{}, false
		}
	}
	attempt := *j
	attempt.AllocatedDevices = assignment.AllocatedDevices
	return pooledEvidenceKeyForExtension(&attempt, m, extension), true
}

func repairMemoryEvidence(db *sql.DB, e *memoryEvidence, s state) (*memoryEvidence, error) {
	var marker int
	err := db.QueryRow(`SELECT 1 FROM memory_evidence WHERE kind='projection' AND key='v2'`).Scan(&marker)
	if !errors.Is(err, sql.ErrNoRows) {
		return e, err
	}
	bounds := importedMemoryFloors(s.WorkObservations)
	candidates := make(map[memoryEvidenceKey]importedMemoryBounds)
	for key, b := range bounds {
		if lower := e.exact[key].Lower; lower > 0 && lower == b.invalid && lower > max(b.verified, b.ambiguous) {
			candidates[key] = b
		}
	}
	limits, uncertain := retainedMemoryLimits(s, candidates)
	e.writes = make(map[memoryEvidenceRowKey]any)
	var report memoryFloorRepairReport
	for key, series := range e.exact {
		if series.Lower <= 0 {
			continue
		}
		report.examined++
		b, candidate := bounds[key], candidates[key].invalid > 0
		verified := max(b.verified, limits[key])
		if verified >= series.Lower {
			continue
		}
		if !candidate || uncertain[key.Family] {
			report.ambiguous++
			if len(report.examples) < memoryFloorReportLimit {
				report.examples = append(report.examples, key)
			}
			continue
		}
		previous := series.Lower
		series.Lower = max(b.ambiguous, verified)
		e.exact[key] = series
		e.write(memoryEvidenceRowKey{kind: "exact", key: key}, series)
		report.record(e, key, previous, series.Lower)
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := e.persist(tx); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO memory_evidence(kind,key,data) VALUES ('projection','v2','true')`); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	e.writes = nil
	report.log()
	return e, nil
}

const memoryFloorReportLimit = 5

type memoryFloorChange struct {
	key                       memoryEvidenceKey
	beforeCharge, afterCharge float64
	afterLevel                string
}

type memoryFloorRepairReport struct {
	examined, cleared, reduced, ambiguous int
	examples                              []memoryEvidenceKey
	families                              map[string]bool
	changes                               map[memoryEvidenceKey]memoryFloorChange
}

func (r *memoryFloorRepairReport) record(e *memoryEvidence, key memoryEvidenceKey, before, after float64) {
	if after == 0 {
		r.cleared++
	} else {
		r.reduced++
	}
	if r.families == nil {
		r.families = make(map[string]bool)
		r.changes = make(map[memoryEvidenceKey]memoryFloorChange)
	}
	r.families[key.Family] = true
	confidence := e.confidence()
	repaired := e.estimateWithFloor(key, 0, confidence, after)
	change := memoryFloorChange{
		key:          key,
		beforeCharge: e.estimateWithFloor(key, 0, confidence, before).ChargeGB,
		afterCharge:  repaired.ChargeGB,
		afterLevel:   repaired.Evidence.Level,
	}
	family := key.familyKey()
	if existing, ok := r.changes[family]; !ok || compareMemoryFloorChanges(change, existing) < 0 {
		r.changes[family] = change
	}
}

func compareMemoryFloorChanges(a, b memoryFloorChange) int {
	return cmp.Or(cmp.Compare(b.beforeCharge, a.beforeCharge), cmp.Compare(a.key.Family, b.key.Family),
		cmp.Compare(a.key.Platform, b.key.Platform), cmp.Compare(a.key.Pool, b.key.Pool), cmp.Compare(a.key.Exact, b.key.Exact))
}

func (r memoryFloorRepairReport) log() {
	log.Printf("event=memory_floor_repair examined=%d cleared=%d reduced=%d kept_ambiguous=%d affected_families=%d",
		r.examined, r.cleared, r.reduced, r.ambiguous, len(r.families))
	for _, key := range r.examples {
		log.Printf("event=memory_floor_retained reason=ambiguous platform=%s pool=%s family=%s exact=%s", key.Platform, key.Pool, key.Family, key.Exact)
	}
	changes := slices.SortedFunc(maps.Values(r.changes), compareMemoryFloorChanges)
	for _, change := range changes[:min(len(changes), memoryFloorReportLimit)] {
		log.Printf("event=memory_floor_charge platform=%s pool=%s family=%s exact=%s evidence_charge_before_gib=%.6f evidence_charge_after_gib=%.6f evidence_after_level=%s",
			change.key.Platform, change.key.Pool, change.key.Family, change.key.Exact, change.beforeCharge, change.afterCharge, change.afterLevel)
	}
}
