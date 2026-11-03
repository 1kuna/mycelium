package myce

import (
	"context"
	"maps"
)

// Publish only after SQLite commit, under q.mu and before clearing the undo
// journal. Readers keep the previous immutable generation throughout a write.
// Copy changed jobs only: retained history and large ledgers must not become
// per-heartbeat work. The map shells are private to each generation.
func (q *Queue) publishCommittedRead() {
	base := q.s
	base.Jobs, base.Submissions = nil, nil
	inputs := copyObservation(observationState{State: base, Models: q.models,
		Environments: q.environments, Calls: q.calls, Unreported: q.unreported})
	v := &Queue{s: inputs.State, models: inputs.Models, environments: inputs.Environments,
		calls: inputs.Calls, unreported: inputs.Unreported, clock: q.clock,
		nasAvailable: q.nasAvailable, simulation: q.simulation,
		productionGrace: q.productionGrace, lossTimeouts: q.lossTimeouts,
		productionInfrastructureRetries: q.productionInfrastructureRetries,
		restartUntil:                    q.restartUntil, networkQuiet: q.networkQuiet, root: q.root,
		files: q.files, fileRetention: q.fileRetention}
	for id, m := range v.s.Machines {
		original := q.s.Machines[id]
		m.cpuLedger, m.hostBudgetGB = original.cpuLedger, original.hostBudgetGB
		m.modelReservationDebtGB, m.hostReservationDebtGB = original.modelReservationDebtGB, original.hostReservationDebtGB
	}
	jobs, submissions := q.s.Jobs, q.s.Submissions
	if previous := q.committedRead.Load(); previous != nil && q.records != nil && q.records.updating {
		v.s.Jobs, v.s.Submissions = maps.Clone(previous.s.Jobs), maps.Clone(previous.s.Submissions)
		jobs = make(map[string]*Job, len(q.records.jobs))
		for id := range q.records.jobs {
			jobs[id] = q.s.Jobs[id]
		}
		submissions = make(map[string]*submissionRecord, len(q.records.submissions))
		for key := range q.records.submissions {
			submissions[key] = q.s.Submissions[key]
		}
	} else {
		v.s.Jobs, v.s.Submissions = map[string]*Job{}, map[string]*submissionRecord{}
	}
	copyJob := func(j *Job) *Job {
		copy := *j
		copy.LedgerEntries = nil // Never an observation input.
		if !mutableJob(j) {
			copy.Extensions = nil // Frozen payloads are not diagnostic inputs.
		}
		out := copyObservation(observationState{State: state{Jobs: map[string]*Job{j.ID: &copy}}}).State.Jobs[j.ID]
		out.simGatherWait = j.simGatherWait
		return out
	}
	for id, j := range jobs {
		if j == nil {
			delete(v.s.Jobs, id)
		} else {
			v.s.Jobs[id] = copyJob(j)
		}
	}
	for key, r := range submissions {
		if r == nil {
			delete(v.s.Submissions, key)
			continue
		}
		copy := *r
		copy.Job = nil
		out := copyObservation(observationState{State: state{Submissions: map[string]*submissionRecord{key: &copy}}}).State.Submissions[key]
		if r.Job != nil {
			out.Job = v.s.Jobs[r.JobID]
			if out.Job == nil {
				out.Job = copyJob(r.Job)
			}
		}
		v.s.Submissions[key] = out
	}
	q.committedRead.Store(v)
}

func (q *Queue) captureCommittedObservation(id string, options *statusOptions, contexts ...context.Context) (*Queue, fileStoreObservation, error) {
	if err := requestContext(contexts).Err(); err != nil {
		return nil, fileStoreObservation{}, err
	}
	v := q.committedRead.Load()
	if v == nil {
		// Offline/import and simulator callers can construct a Queue directly.
		// Serving queues always publish during Open, before accepting requests.
		return q.captureStatusObservation(id, options, contexts...)
	}
	out, files, err := v.captureStatusInputs(id, options)
	files.reservedOutputs = func(payloads map[string]storedPayload) uint64 {
		return v.outputReservations(payloads, "")
	}
	// No queue mutex was acquired. Keep the observational callback outside
	// capture so test instrumentation can commit another generation here.
	if q.observeRead != nil {
		kind := "status"
		if id != "" {
			kind = "job"
		}
		q.observeRead(kind, 0, 0)
	}
	return out, files, err
}
