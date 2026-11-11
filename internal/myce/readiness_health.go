package myce

import "strings"

// Detection uses the same episode-based path as adaptive queue health. No timer
// interprets a cold load as dead, and an alert never changes launch/stop custody.
func (q *Queue) detectReadinessHealth() {
	live := map[string]bool{}
	for _, s := range q.s.ModelSlots {
		key := "cold-model/" + s.Machine + "/" + s.Device + "/" + s.Model + "/" + s.Attempt
		live[key] = true
		inactive := s.State == "loading" && strings.HasPrefix(s.Error, coldLoadInactive)
		waiting := ""
		if inactive {
			waiting = q.coldLoadContender(s)
		}
		q.observeHealth(key, inactive && waiting != "", 0, HealthAlert{Kind: "model_load_needs_attention", Reason: "cold_load_inactive_contended", Host: q.executionHost(s.Machine), Device: s.Device, JobID: waiting})
		if inactive {
			s.Error = coldLoadInactive
			if waiting != "" {
				s.Error = coldLoadContended
			}
		}
	}
	for key := range q.s.Health.Conditions {
		if strings.HasPrefix(key, "cold-model/") && !live[key] {
			delete(q.s.Health.Conditions, key)
		}
	}
}

// Reuse permanent target qualification and the live resource ledger. Alert only
// when releasing this slot would let eligible, prepared demand fit; unrelated,
// policy-excluded, paused and unprepared work must not raise an episode.
func (q *Queue) coldLoadContender(s *ModelSlot) string {
	owner := q.s.Machines[s.Machine]
	if owner == nil {
		return ""
	}
	model, ok := q.model(s.Model)
	if !ok {
		return ""
	}
	blocked := func(j *Job, m *Machine, batch bool) bool {
		if hostID(m) != hostID(owner) || !q.placementPolicyAllowed(j, m, true) || q.capReasonFor(j, m) != "" || !q.checkpointReady(j, m) || !environmentReady(j, m) {
			return false
		}
		projected := q.afterReleases(m, nil, []*ModelSlot{s})
		if slotOnTarget(s, m) {
			projected.FreeMemoryGB += model.MemoryGB
		}
		if !fits(j, &projected, projected.FreeMemoryGB) {
			return false
		}
		// Check actual occupancy, including device exclusivity and pending custody.
		return q.roomPlan(j, m, batch) == nil
	}
	for _, j := range q.s.Jobs {
		if j.State != "queued" || j.Pause != nil || cancelled(j) || !dependencyReady(j) || j.Spec.Needs.Model == s.Model {
			continue
		}
		found := false
		q.evaluateTargets(j, func(fit targetFit, candidate *Job, target *Machine) bool {
			if fit.failures != 0 || hostID(target) != hostID(owner) {
				return true
			}
			for _, m := range q.placementTargets(q.s.Machines[target.ID]) {
				if targetKey(m) == targetKey(target) && blocked(candidate, m, true) {
					found = true
					return false
				}
			}
			return true
		})
		if found {
			return j.ID
		}
	}
	for _, c := range q.calls {
		if c.Model == s.Model || c.Machine != "" || c.Error != "" || q.models.Wait > 0 && q.clock.Now().Sub(c.Queued) >= q.models.Wait {
			continue
		}
		demand, ok := q.model(c.Model)
		if !ok {
			continue
		}
		for _, m := range q.placementTargets(owner) {
			if q.callExcludes(c, m.ID, targetDevice(m)) || !q.callPlacementAllowed(c, m) || !q.modelUsableMachine(demand, m) {
				continue
			}
			j := callJob(c)
			j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB = demand.MemoryGB, demand.HostMemoryGB
			if blocked(j, m, false) {
				return c.ID
			}
		}
	}
	return ""
}
