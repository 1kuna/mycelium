package myce

import (
	"context"
	"log"
	"maps"
	"reflect"
)

// Pin the committed generation inside the submission transaction. No live
// queue lock is taken after submitContext returns, including on key replay.
func (q *Queue) submitWithDeadlineGuidance(ctx context.Context, s Spec) (*Job, error) {
	var snapshot *submissionAdviceSnapshot
	j, err := q.submitContext(ctx, s, func() { snapshot = q.captureSubmissionAdvice() })
	if err != nil {
		return j, err
	}
	// Production acceptance must not wait for a second planner pass over retained
	// history (or another physical-memory probe) after its durable commit. Return
	// the committed receipt and policy advice; the normal job GET owns full
	// blocker diagnostics. The snapshot pins the exact accepted policy on replay.
	if v := snapshot.view; j.State != "queued" || v.priorityMatch(j.Spec).Tier == "production" {
		j.PriorityPolicy = v.priorityMatch(j.Spec)
		j.DeadlineGuidance = v.deadlineGuidance(j)
		j.DeadlineGuidance.DetailsDeferred = j.State == "queued"
		return j, nil
	}
	timing := newSubmissionAdviceTiming()
	defer func() { timing.finish(q.observeSubmissionAdvice) }()
	// Acceptance is already durable. Request cancellation cannot turn it into
	// an unaccepted submission or omit the existing response advice fields.
	adviceCtx, release, err := q.requestBudgets.projectionContext(context.WithoutCancel(ctx))
	defer release()
	var v *Queue
	if err == nil {
		v, err = snapshot.planner(adviceCtx)
	}
	if err != nil {
		// The durable receipt is authoritative even if measured headroom cannot
		// accommodate optional diagnostics. Never encourage duplicate work with
		// a post-commit failure. Preserve policy/guidance fields, explicitly
		// marking unavailable blocker detail rather than implying no blockers.
		v = snapshot.view
		copy := *j
		copy.State = "advice_unavailable"
		j.PriorityPolicy = v.priorityMatch(j.Spec)
		j.DeadlineGuidance = v.deadlineGuidance(&copy)
		j.DeadlineGuidance.Blockers = []string{"waiting details unavailable within measured queue memory capacity; inspect this accepted job after headroom recovers"}
		log.Print("event=submission_advice_unavailable code=projection_unavailable accepted=true")
		return j, nil
	}
	v.submissionDeadlineGuidance(j, &timing)
	return j, nil
}

type submissionAdviceSnapshot struct {
	generation *readGeneration
	view       *Queue
	ledgers    map[string]observationMachineLedger
	machines   map[string]*Machine // Owned live copy; nil uses the committed generation.
}

// q.mu is held. Only pin immutable records/configuration and small per-worker
// scalar ledgers/contact timestamps; never walk jobs or decode/copy payloads.
func (q *Queue) captureSubmissionAdvice() *submissionAdviceSnapshot {
	v := &Queue{clock: observationClock{q.clock.Now()}, nasAvailable: q.nasAvailable,
		simulation: q.simulation, productionGrace: q.productionGrace, lossTimeouts: q.lossTimeouts,
		productionInfrastructureRetries: q.productionInfrastructureRetries,
		restartUntil:                    q.restartUntil, networkQuiet: q.networkQuiet, root: q.root,
		hostMemorySamples: q.readSamples, detachedRead: true}
	// Policy records are replaced, never mutated. Keep this small fallback even
	// when cold decoding cannot fit the post-commit request allowance.
	v.s.PriorityPolicy = q.s.PriorityPolicy
	snapshot := &submissionAdviceSnapshot{generation: q.readGeneration, view: v,
		ledgers: make(map[string]observationMachineLedger, len(q.s.Machines))}
	for key, m := range q.s.Machines {
		snapshot.ledgers[key] = observationMachineLedger{healthDelivery: q.freezeHostHealthDelivery(m),
			toolInventory: m.toolInventory, cpuLedger: m.cpuLedger,
			modelReservationDebtGB: m.modelReservationDebtGB, hostReservationDebtGB: m.hostReservationDebtGB}
	}
	q.freezeContactView(v)
	if q.simulation {
		// Simulators bypass persistence and own no immutable record generation.
		owned := copyObservation(observationState{State: q.s, Models: q.models, Environments: q.environments, Calls: q.calls, Unreported: q.unreported})
		v.s, v.models, v.environments, v.calls, v.unreported = owned.State, owned.Models, owned.Environments, owned.Calls, owned.Unreported
	} else {
		v.models, v.environments = *q.readModels, *q.readEnvironments
		v.calls, v.unreported = q.readRuntime.Calls, q.readRuntime.Unreported
	}
	return snapshot
}

// Full retained execution facts matter for cold-memory advice. The selected
// job read's minimized context is insufficient here. Retained records remain
// borrowed read-only; own active jobs, machine ledgers and all planner caches.
func (s *submissionAdviceSnapshot) planner(ctx context.Context) (*Queue, error) {
	v := s.view
	if s.generation != nil {
		inputs, _, err := s.generation.inputs(ctx)
		if err != nil {
			return nil, err
		}
		active := 0
		for _, j := range inputs.Jobs {
			if mutableJob(j) {
				active++
			}
		}
		// Shared immutable payloads already belong to measured process RSS.
		// Charge only new map storage, with the same spare-capacity/three-stage
		// representation allowance as chargeProjectionValue, before allocating.
		if a := projectionAllocation(ctx); a != nil {
			width := int64(reflect.TypeFor[string]().Size() + reflect.TypeFor[*Job]().Size() + reflect.TypeFor[uintptr]().Size())
			if !a.charge(6 * int64(len(inputs.Jobs)+active) * width) {
				return nil, projectionRefusal(a.limit - a.held)
			}
		}
		v.s = inputs
		v.s.Jobs = maps.Clone(inputs.Jobs)
		v.observationLive = make(map[string]*Job, active)
		for id, j := range inputs.Jobs {
			if !mutableJob(j) {
				continue
			}
			if err := chargeProjectionValue(ctx, j); err != nil {
				return nil, err
			}
			owned := copyObservationValue(reflect.ValueOf(j)).Interface().(*Job)
			v.s.Jobs[id], v.observationLive[id] = owned, owned
		}
		if s.machines != nil {
			v.s.Machines = s.machines
		} else {
			if err := chargeProjectionValue(ctx, inputs.Machines); err != nil {
				return nil, err
			}
			v.s.Machines = copyObservationValue(reflect.ValueOf(inputs.Machines)).Interface().(map[string]*Machine)
		}
	}
	for key, m := range v.s.Machines {
		l := s.ledgers[key]
		if m.HostHealth != nil {
			m.HostHealth.delivery = l.healthDelivery
		}
		m.toolInventory, m.cpuLedger = l.toolInventory, l.cpuLedger
		m.modelReservationDebtGB, m.hostReservationDebtGB = l.modelReservationDebtGB, l.hostReservationDebtGB
	}
	// Catalog construction copies each curve and builds private lookup maps
	// and slices even when immutable decoding is warm. Reserve the full curve
	// representation with the shared projection/growth allowance first; this
	// also covers the shallower catalog copies and indexes without a new cap.
	if err := chargeProjectionValue(ctx, v.s.WorkObservations); err != nil {
		return nil, err
	}
	v.memoryPlanning = newMemoryPlanningCache()
	v.memoryObservationIndex = v.memoryObservationCatalog()
	v.observationScan = v.placementScan()
	v.observationScan.observationOwner = v
	v.observationScan.memory = v.memoryPlanning
	return v, nil
}

// Resolve plans one hypothetical job, the same question submission advice
// answers. The full status capture copied and charged every retained execution
// into one request lane, so a read-only check could be refused with
// projection_capacity_exceeded once history outgrew that lane. Share
// the committed generation instead: retained records stay borrowed read-only
// and only active jobs, machines and planner indexes are copied and charged.
// Under q.mu it copies only configuration, machines and scalar ledgers.
func (q *Queue) captureResolveView(ctx context.Context) (*Queue, error) {
	unlock, err := requestObservationLock(ctx, &q.mu)
	if err != nil {
		return nil, err
	}
	// Offline imports can install records the committed generation lacks.
	imported := q.records != nil && (q.records.jobCount != len(q.s.Jobs) || q.records.submissionCount != len(q.s.Submissions))
	if imported || q.readGeneration == nil && !q.simulation {
		unlock()
		v, _, err := q.captureObservation("", ctx)
		return v, err
	}
	snapshot := q.captureSubmissionAdvice()
	snapshot.view.evidenceRetention = q.evidenceRetention
	if !q.simulation {
		// Configuration setters do not commit, so the published copies can lag
		// a registered environment or model. Small; charged before copying.
		// Check-ins can update machines (contact, inventory) without a commit,
		// and eligibility depends on them. Machines are few; copy them live.
		config := observationState{State: state{Machines: q.s.Machines}, Models: q.models, Environments: q.environments, Unreported: q.unreported}
		if err := chargeProjectionValue(ctx, config); err != nil {
			unlock()
			return nil, err
		}
		owned := copyObservation(config)
		snapshot.view.models, snapshot.view.environments, snapshot.view.unreported = owned.Models, owned.Environments, owned.Unreported
		snapshot.machines = owned.State.Machines
		if snapshot.machines == nil {
			snapshot.machines = map[string]*Machine{}
		}
	}
	unlock()
	return snapshot.planner(ctx)
}
