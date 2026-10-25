package myce

import "slices"

// The reference must explicitly benchmark this exact server, not an extension
// command with a similar name. Recipe qualification still gates every point.
type ModelMeasurementBinding struct {
	ExtensionDigest string `json:"extension_digest" yaml:"extension_digest"`
	Work            string `json:"work" yaml:"work"`
	ServerDigest    string `json:"server_digest" yaml:"server_digest"`
}

// ModelMeasurementDigest binds model bytes, command, context/cache arguments,
// resource/configured caps and resolved environment recipes. It excludes only
// the binding itself, avoiding a recursive digest. Secret values never leave
// their existing mechanism; this exposes only an opaque digest.
func ModelMeasurementDigest(m Model, environments EnvironmentCatalog) string {
	m.Measurement = nil
	keys := map[string]string{}
	for platform, recipe := range environments.Environments[m.Environment] {
		keys[platform] = recipe.Key // Snapshot keys already bind all recipe bytes.
	}
	return identityHash([]any{m, keys})
}

func sameCallGroup(a, b *ModelCall) bool {
	return a.FanOut != "" && a.FanOut == b.FanOut && a.Model == b.Model && a.Project == b.Project && a.Priority == b.Priority && a.Class == b.Class && a.Workload != nil && b.Workload != nil && *a.Workload == *b.Workload
}

func (q *Queue) boundModelPoints(c *ModelCall, m *Machine, model Model) []WorkObservation {
	b := model.Measurement
	if c.FanOut == "" || c.Workload == nil || b == nil || b.ServerDigest != ModelMeasurementDigest(model, q.environments) {
		return nil
	}
	for _, e := range q.s.Extensions {
		if e.Digest != b.ExtensionDigest {
			continue
		}
		key, _ := curveKey(e, m, b.Work)
		if key == nil || key.ServerDigest == "" || key.ServerDigest != b.ServerDigest || key.Reference.InputShape != c.Workload.InputShape {
			return nil
		}
		var points []WorkObservation
		for _, count := range e.variant(machineTarget(m)).Sweeps[b.Work].Counts {
			if model.MaxInFlight > 0 && count > model.MaxInFlight {
				continue
			}
			k := *key
			k.Concurrency = count
			p := q.s.WorkObservations[identityHash(k)]
			// Resident model reservations must already cover the measured envelope.
			// Unknown memory never permits an extra call slot.
			if !q.usableCurve(p, k) || count > 1 && (p.HostMemoryGB <= 0 || p.DeviceMemoryGB <= 0) || p.HostMemoryGB > model.HostMemoryGB || p.DeviceMemoryGB > model.MemoryGB {
				continue
			}
			points = append(points, p)
		}
		return points
	}
	return nil
}

func (q *Queue) declaredCallPlan(c *ModelCall, model Model) []batchAllocation {
	n := 0
	for _, other := range q.calls {
		if sameCallGroup(c, other) && other.Error == "" && other.Machine == "" {
			n++
		}
	}
	var components []batchComponent
	for _, m := range q.plannerMachines() {
		s := q.s.ModelSlots[targetSlotKey(m, c.Model)]
		if s == nil || s.State != "ready" || s.InFlight != 0 || !q.callPlacementAllowed(c, m) || !q.placementPolicyAllowed(callJob(c), m, false) || q.capReasonFor(callJob(c), m) != "" || !q.modelUsableMachine(model, m) || q.unreported[m.ID] || q.callExcludes(c, m.ID, targetDevice(m)) {
			continue
		}
		component := batchComponent{id: targetKey(m), host: hostID(m), cpuBudget: 1, hostBudget: model.HostMemoryGB}
		for _, p := range q.boundModelPoints(c, m, model) {
			component.points = append(component.points, batchPoint{count: p.Key.Concurrency, rate: p.Rate, cpu: 1, host: model.HostMemoryGB})
		}
		// Model residency is already charged by the queue's shared host ledger.
		// This plan adds no host allocation; distinct engines remain independent.
		component.host = component.id
		components = append(components, component)
	}
	return planBatch(components, n, c.Workload.Units)
}

func (q *Queue) declaredCallLimit(c *ModelCall, m *Machine, model Model) int {
	if c.FanOut == "" {
		return model.MaxInFlight
	}
	points := q.boundModelPoints(c, m, model)
	if len(points) == 0 {
		return 1
	}
	if slot := q.s.ModelSlots[targetSlotKey(m, c.Model)]; slot != nil && slot.InFlight > 0 {
		limit := 0
		for _, live := range q.calls {
			if live.Machine == m.ID && live.Device == targetDevice(m) && live.Model == c.Model && live.Error == "" {
				if !sameCallGroup(c, live) {
					return 1
				}
				limit = max(limit, live.BatchConcurrency)
			}
		}
		if slices.ContainsFunc(points, func(p WorkObservation) bool { return p.Key.Concurrency == limit }) {
			return limit
		}
		return 1
	}
	plan := q.declaredCallPlan(c, model)
	if len(plan) == 0 {
		return 1
	}
	for _, a := range plan {
		if a.component == targetKey(m) {
			return a.count
		}
	}
	return -1
}

func (q *Queue) estimateDeclaredCall(c *ModelCall, m *Machine) (completionEstimate, bool) {
	model, ok := q.model(c.Model)
	if !ok || c.FanOut == "" || c.Workload == nil {
		return completionEstimate{}, false
	}
	count := q.declaredCallLimit(c, m, model)
	for _, p := range q.boundModelPoints(c, m, model) {
		if p.Key.Concurrency == count {
			return completionEstimate{group: identityHash([]any{p.Key.ExtensionDigest, p.Key.Reference}), service: c.Workload.Units * float64(count) / p.Rate, setup: q.loadSeconds(c.Model, m), samples: p.Samples, count: count}, true
		}
	}
	return completionEstimate{}, false
}
