package myce

import (
	"fmt"
	"slices"
	"time"
)

// ProductionBackoff is admission policy, never execution or release evidence.
// Only the last preemption is needed to detect two interrupted opportunities.
type ProductionBackoff struct {
	LastAt time.Time `json:"last_at"`
	Until  time.Time `json:"until,omitempty"`
}

// Use only this recipe's measured preparation history. The placement estimator's
// fleet averages/default 60s are useful for ranking, not for withholding work.
func measuredSetupSeconds(j *Job, m *Machine, cold bool) (float64, bool) {
	if j.Spec.Needs.cpuOnly() && m.Devices != nil {
		m = cpuMachineView(m)
	}
	keys := map[string]bool{}
	for _, r := range extensionEnvironmentRecipes(j, m) {
		keys[r.Key] = true
	}
	if k := environmentKey(j, m); k != "" {
		keys[k] = true
	}
	seconds := 0.0
	for k := range keys {
		if !cold && slices.Contains(m.Environments, k) {
			continue
		}
		v := m.EnvironmentBuildSeconds[k]
		if !validRate(v) {
			return 0, false
		}
		seconds += v
	}
	return seconds, true
}

func (q *Queue) recordProductionBackoff(j *Job, m *Machine, demand *Job) {
	if m == nil || demand.Spec.Priority != "production" || j.Spec.Priority == "production" || q.comparePriority(j.Spec, demand.Spec) <= 0 {
		return
	}
	// One production start window is the minimum useful retry opportunity.
	// Longer measured setup gets a full setup interval. Two preemptions within
	// two such opportunities trigger one interval of back-off: enough to avoid
	// another wasted bootstrap, without an all-day host reservation.
	interval := ProductionStartTimeout
	if seconds, known := measuredSetupSeconds(j, m, true); known {
		interval = max(interval, time.Duration(seconds*float64(time.Second)))
	}
	if j.ProductionBackoff == nil {
		j.ProductionBackoff = map[string]ProductionBackoff{}
	}
	now, host := q.clock.Now(), hostID(m)
	b := j.ProductionBackoff[host]
	if !b.LastAt.IsZero() && !now.Before(b.LastAt) && now.Sub(b.LastAt) <= 2*interval {
		b.Until = now.Add(interval)
	}
	b.LastAt = now
	j.ProductionBackoff[host] = b
}

func (q *Queue) productionAdmissionReason(j *Job, m *Machine) string {
	// Revalidate offers and unstarted payloads, leaving Started custody alone.
	if j.Spec.Priority == "production" || j.StartedAt != nil && j.State != "queued" {
		return ""
	}
	if j.Spec.Needs.cpuOnly() && m.Devices != nil {
		m = cpuMachineView(m)
	}
	host := hostID(m)
	if b := j.ProductionBackoff[host]; q.clock.Now().Before(b.Until) {
		return fmt.Sprintf("host %s: production preemption back-off; retry in %.0fs", host, b.Until.Sub(q.clock.Now()).Seconds())
	}
	if reason := q.unifiedCUDAProductionReason(j, m); reason != "" {
		return reason
	}
	setup, known := measuredSetupSeconds(j, m, false)
	if !known || setup <= 0 {
		return ""
	}
	if q.fleetPausesProduction() {
		return ""
	}
	soonest, id := setup, ""
	for _, demand := range q.liveJobs() {
		if demand.State != "queued" || demand.Spec.Priority != "production" || cancelled(demand) || expired(demand.Spec.GiveUpAt, q.clock.Now()) || q.comparePriority(j.Spec, demand.Spec) <= 0 || !q.jobAllowsMachine(demand, m.ID) || !q.reservationAllows(demand, m) || demand.ProductionHost != "" && demand.ProductionHost != host || !fitsWithoutHostMemory(demand, m, m.TotalMemoryGB) {
			continue
		}
		// A fan-out parent with every piece launched has no new local demand.
		if demand.FanOut != nil && demand.Spec.FanOut != nil && demand.FanOut.Next >= len(demand.Spec.FanOut.Pieces) {
			continue
		}
		seconds, estimated := q.productionArrival(demand)
		if estimated && (seconds < soonest || seconds == soonest && id != "" && demand.ID < id) {
			soonest, id = seconds, demand.ID
		}
	}
	if id != "" {
		return fmt.Sprintf("host %s: predictable production demand %s in ~%.0fs; measured setup %.0fs exceeds free window", host, id, soonest, setup)
	}
	return ""
}

// Only forecast declared pieces already running under active production. An
// unlaunched piece, unknown history, or overrun gives no estimate. Whole-parent
// dependencies require all remaining pieces; a piece edge needs only its wave.
func (q *Queue) productionArrival(j *Job) (float64, bool) {
	if dependencyReady(j) {
		return 0, true
	}
	a := j.Spec.After
	if a == nil {
		return 0, false
	}
	p := q.s.Jobs[a.Job]
	if p == nil || p.Spec.Priority != "production" || terminal(p.State) || cancelled(p) || p.FanOut == nil || p.Spec.FanOut == nil {
		return 0, false
	}
	remaining := map[string]float64{}
	for _, id := range p.FanOut.Children {
		child := q.s.Jobs[id]
		if child == nil || child.Batch == nil {
			continue
		}
		seconds, known := q.productionRemaining(child)
		if known {
			for _, piece := range child.Batch.Pieces {
				remaining[piece.Key] = seconds
			}
		}
	}
	seconds := 0.0
	for _, piece := range p.Spec.FanOut.Pieces {
		if a.Piece != "" && a.Piece != piece.Key {
			continue
		}
		if _, accepted := p.FanOut.Results[piece.Key]; accepted {
			continue
		}
		v, known := remaining[piece.Key]
		if !known {
			return 0, false
		}
		seconds = max(seconds, v)
	}
	return seconds, true
}

func (q *Queue) productionRemaining(j *Job) (float64, bool) {
	if j.State != "running" || j.Spec.Priority != "production" || cancelled(j) || j.StartedAt == nil {
		return 0, false
	}
	key, reason := q.workObservationKey(j)
	// Measured waves deliberately disable throughput refinement, but their
	// qualified concurrency curve still predicts the declared wave's service.
	wave := j.Batch != nil && j.Batch.Run
	if key == nil || !wave && (reason != "" || j.Timing == nil || j.Timing.Reason != "") {
		return 0, false
	}
	point := q.s.WorkObservations[identityHash(key)]
	if !q.usableCurve(point, *key) || j.Spec.Workload == nil {
		return 0, false
	}
	units := j.Spec.Workload.Units
	if j.Batch != nil && j.Batch.Run {
		units *= float64(len(j.Batch.Pieces))
	}
	seconds := units/point.Rate - max(0, q.clock.Now().Sub(*j.StartedAt).Seconds())
	return seconds, validRate(seconds)
}
