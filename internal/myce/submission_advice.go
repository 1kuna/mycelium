package myce

import "context"

// Pin the committed generation inside the submission transaction. No live
// queue lock is taken after submitContext returns, including on key replay.
func (q *Queue) submitWithDeadlineGuidance(ctx context.Context, s Spec) (*Job, error) {
	var snapshot *submissionAdviceSnapshot
	j, err := q.submitContext(ctx, s, func() { snapshot = q.captureSubmissionAdvice() })
	if err != nil {
		return j, err
	}
	timing := newSubmissionAdviceTiming()
	defer func() { timing.finish(q.observeSubmissionAdvice) }()
	// Acceptance is already durable. Request cancellation cannot turn it into
	// an unaccepted submission or omit the existing response advice fields.
	v, err := snapshot.planner(context.WithoutCancel(ctx))
	if err != nil {
		return j, err
	}
	v.submissionDeadlineGuidance(j, &timing)
	return j, nil
}

type submissionAdviceSnapshot struct {
	generation *readGeneration
	view       *Queue
	ledgers    map[string]observationMachineLedger
}

// q.mu is held. Only pin immutable records/configuration and small per-worker
// scalar ledgers/contact timestamps; never walk jobs or decode/copy payloads.
func (q *Queue) captureSubmissionAdvice() *submissionAdviceSnapshot {
	v := &Queue{clock: observationClock{q.clock.Now()}, nasAvailable: q.nasAvailable,
		simulation: q.simulation, productionGrace: q.productionGrace, lossTimeouts: q.lossTimeouts,
		productionInfrastructureRetries: q.productionInfrastructureRetries,
		restartUntil:                    q.restartUntil, networkQuiet: q.networkQuiet, root: q.root,
		hostMemorySamples: q.readSamples, detachedRead: true}
	snapshot := &submissionAdviceSnapshot{generation: q.readGeneration, view: v,
		ledgers: make(map[string]observationMachineLedger, len(q.s.Machines))}
	v.contacts = make(map[string]*workerContact, len(q.s.Machines))
	for key, m := range q.s.Machines {
		snapshot.ledgers[key] = observationMachineLedger{m.toolInventory, m.cpuLedger, m.hostBudgetGB, m.modelReservationDebtGB, m.hostReservationDebtGB}
		v.contacts[key] = &workerContact{session: m.Session, processed: q.lastContact(m, v.clock.Now())}
	}
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
// job read's minimized context is insufficient here. Own the whole diagnostic
// view before building caches: published records must never become planner state.
func (s *submissionAdviceSnapshot) planner(ctx context.Context) (*Queue, error) {
	v := s.view
	if s.generation != nil {
		inputs, _, err := s.generation.inputs(ctx)
		if err != nil {
			return nil, err
		}
		v.s = copyObservation(observationState{State: inputs}).State
	}
	for key, m := range v.s.Machines {
		l := s.ledgers[key]
		m.toolInventory, m.cpuLedger, m.hostBudgetGB = l.toolInventory, l.cpuLedger, l.hostBudgetGB
		m.modelReservationDebtGB, m.hostReservationDebtGB = l.modelReservationDebtGB, l.hostReservationDebtGB
	}
	v.memoryPlanning = newMemoryPlanningCache()
	v.memoryObservationIndex = v.memoryObservationCatalog()
	v.observationScan = v.placementScan()
	v.observationScan.observationOwner = v
	v.observationScan.memory = v.memoryPlanning
	return v, nil
}
