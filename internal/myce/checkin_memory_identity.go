package myce

import (
	"context"
	"reflect"
)

type checkinMemoryIdentities struct {
	revision         uint64
	values           map[*Job]string
	retainedRevision uint64
	retained         retainedMemoryIndex
}

// Plain recipe identities include every input reference. Snapshot under the
// mutex, hash outside it, then accept only if no transaction committed meanwhile.
// A stale snapshot falls back to the ordinary read-only planner's computation.
func (q *Queue) snapshotCheckinMemoryIdentities(ctx context.Context) checkinMemoryIdentities {
	unlock := requestLock(ctx, &q.mu)
	snapshot := checkinMemoryIdentities{revision: q.fileReferenceRevision}
	jobs := map[*Job]Job{}
	var retained map[string]*Job
	var learned []learnedMemoryFact
	if ctx.Err() == nil && q.retainedMemoryIndex == nil && q.records != nil {
		snapshot.retainedRevision = q.retainedMemoryRevision
		retained = snapshotRetainedMemoryJobs(q)
		learned = q.expiredMemory // Appended copy-on-write; never mutated.
	}
	if ctx.Err() == nil {
		for _, j := range q.liveJobs() {
			if j.State == "queued" && plainMemoryWork(j.Spec) && j.Spec.FanOut == nil {
				// Copy recipe inputs only, never retained attempts, ledgers or
				// execution payloads. Hashing remains outside the mutex.
				s := j.Spec
				input := Job{Spec: Spec{Project: s.Project, Name: s.Name, ProfileMemory: s.ProfileMemory,
					Needs: s.Needs, Args: s.Args, Program: s.Program, Run: s.Run, Environment: s.Environment, Files: s.Files}, EnvironmentRecipes: j.EnvironmentRecipes}
				jobs[j] = copyObservationValue(reflect.ValueOf(input)).Interface().(Job)
			}
		}
	}
	unlock()
	if retained != nil {
		snapshot.retained = buildRetainedMemoryIndex(retained, nil, learned)
	}
	snapshot.values = make(map[*Job]string, len(jobs))
	for original, copy := range jobs {
		snapshot.values[original] = profileMemoryIdentity(&copy)
	}
	return snapshot
}

// Caller holds q.mu. Pointer keys also prevent a replacement/rollback from
// lending an identity to a different job object with the same logical ID.
func (s checkinMemoryIdentities) current(q *Queue) map[*Job]string {
	if s.revision != q.fileReferenceRevision {
		return nil
	}
	return s.values
}

// The cold-record generation also changes on rollback, which need not advance
// the commit revision. Owner inventory is deliberately not frozen in this index.
func (s checkinMemoryIdentities) installRetainedMemory(q *Queue) {
	if s.retained != nil && s.retainedRevision == q.retainedMemoryRevision && q.retainedMemoryIndex == nil {
		q.retainedMemoryIndex = s.retained
	}
}
