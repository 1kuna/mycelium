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
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	if m == nil || demand.Spec.Priority != "production" || j.Spec.Priority == "production" || q.comparePriority(j.Spec, demand.Spec) <= 0 {
		return
	}
	// One production start window is the minimum useful retry opportunity.
	// Longer measured setup gets a full setup interval. Two preemptions within
	// two such opportunities trigger one interval of back-off: enough to avoid
	// another wasted bootstrap, without an all-day host reservation.
	interval := productionBackoffInterval(j, m)
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

func productionBackoffInterval(j *Job, m *Machine) time.Duration {
	interval := ProductionStartTimeout
	if seconds, known := measuredSetupSeconds(j, m, true); known {
		interval = max(interval, time.Duration(seconds*float64(time.Second)))
	}
	return interval
}

// #1299: lower-tier jobs preempted for a production stage were re-placed on the
// same host in the seconds before the pipeline's next stage was placed, then
// evicted again, churning memory while that stage prepared. A queued job that
// production preempted on this host stays off it while outranking production
// work that could start here is pending: a ready queued stage (until it is
// placed or its start deadline ends it) or an open production lease (only
// within one back-off interval of the preemption, so a lease never reserves a
// host indefinitely). Other hosts are unaffected, so the job places elsewhere.
func (q *Queue) productionGapReason(j *Job, m *Machine, host string) string {
	b, preempted := j.ProductionBackoff[host]
	if !preempted || b.LastAt.IsZero() || j.State != "queued" {
		return ""
	}
	for _, d := range q.liveJobs() {
		if d == j || d.State != "queued" || d.Spec.Priority != "production" || cancelled(d) || !dependencyReady(d) || q.comparePriority(j.Spec, d.Spec) <= 0 {
			continue
		}
		if d.FanOut != nil && d.Spec.FanOut != nil && d.FanOut.Next >= len(d.Spec.FanOut.Pieces) {
			continue // A fully launched parent has no new local demand.
		}
		if slices.Contains(q.startEligibleHosts(d), host) {
			return fmt.Sprintf("host %s: production stage %s pending after preemption; placing elsewhere", host, d.ID)
		}
	}
	if !q.clock.Now().Before(b.LastAt.Add(productionBackoffInterval(j, m))) {
		return ""
	}
	for _, l := range q.openProductionLeases() {
		for _, stage := range l.stageJobs() {
			if q.comparePriority(j.Spec, stage.Spec) > 0 && slices.Contains(q.startEligibleHosts(stage), host) {
				return fmt.Sprintf("host %s: production lease %s pending after preemption; placing elsewhere", host, l.Key)
			}
		}
	}
	return ""
}

// productionAdmissionHold is the per-target production admission check. The
// unified-CUDA headroom reservation is its own hold kind.
func (q *Queue) productionAdmissionHold(j *Job, m *Machine) *Hold {
	// Revalidate offers and unstarted payloads, leaving Started custody alone.
	if j.Spec.Priority == "production" || j.StartedAt != nil && j.State != "queued" {
		return nil
	}
	if j.Spec.Needs.cpuOnly() && m.Devices != nil {
		m = cpuMachineView(m)
	}
	host := hostID(m)
	if b := j.ProductionBackoff[host]; q.clock.Now().Before(b.Until) {
		return newHold("production_admission", fmt.Sprintf("host %s: production preemption back-off; retry in %.0fs", host, b.Until.Sub(q.clock.Now()).Seconds()))
	}
	if reason := q.productionGapReason(j, m, host); reason != "" {
		return newHold("production_admission", reason)
	}
	if reason := q.unifiedCUDAProductionReason(j, m); reason != "" {
		return newHold("unified_cuda_headroom", reason)
	}
	setup, known := measuredSetupSeconds(j, m, false)
	if !known || setup <= 0 {
		return nil
	}
	soonest, id := setup, ""
	for _, demand := range q.liveJobs() {
		if demand.State != "queued" || demand.Spec.Priority != "production" || cancelled(demand) || expired(demand.Spec.GiveUpAt, q.clock.Now()) || q.comparePriority(j.Spec, demand.Spec) <= 0 || !q.jobAllowsMachine(demand, m.ID) || !q.reservationAllows(demand, m) {
			continue
		}
		// Forecast only a qualified recipe that can actually run on this host.
		// Parity-held demand must not withhold another project's preparation.
		eligible := false
		for _, candidate := range extensionJobs(demand, m, true) {
			if q.variantParityReason(candidate, m) == "" && fitsResources(candidate, m, m.TotalMemoryGB) {
				eligible = true
				break
			}
		}
		if !eligible {
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
		return newHold("production_admission", fmt.Sprintf("host %s: predictable production demand %s in ~%.0fs; measured setup %.0fs exceeds free window", host, id, soonest, setup))
	}
	return nil
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

// productionRemaining is a running production attempt's last accepted
// duration less its elapsed time; unknown history or an overrun is unknown.
func (q *Queue) productionRemaining(j *Job) (float64, bool) {
	if j.State != "running" || j.Spec.Priority != "production" || cancelled(j) || j.StartedAt == nil {
		return 0, false
	}
	t := j.Timing
	if t == nil || t.Reason != "" || t.LastAcceptedSeconds == nil {
		return 0, false
	}
	seconds := *t.LastAcceptedSeconds - max(0, q.clock.Now().Sub(*j.StartedAt).Seconds())
	return seconds, validRate(seconds)
}
