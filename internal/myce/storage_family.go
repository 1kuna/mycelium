package myce

// An entry that runs once per item rarely repeats an exact StorageKey: each
// item has its own inputs, args and often a newer source. Its runs share a
// family instead: the same project, entry/profile or program name, workload
// shape, target and environment. As memory's family bound does for a first
// run, accepted family peaks plus their measured spread size a run that has
// no exact evidence. Two accepted runs are the fewest that can expose any
// spread between items; with fewer, the declared hint stands.
const storageFamilyMinRuns = 2

func storageFamily(project string, k StorageKey) string {
	return identityHash(struct {
		Project, Program, Shape, Environment string
		Variant                              ExecutionTarget
	}{project, k.Program, k.Shape, k.Environment, k.Variant})
}

func (r StorageRange) merge(o StorageRange) StorageRange {
	if o.Samples == 0 {
		return r
	}
	if r.Samples == 0 {
		return o
	}
	r.Min, r.Peak = min(r.Min, o.Min), max(r.Peak, o.Peak)
	r.PositiveError = max(r.PositiveError, o.PositiveError)
	r.Samples = addDiskBytes(r.Samples, o.Samples)
	return r
}

// learnedStorage returns this key's accepted managed peaks ("exact"), else
// its family's ("family") once enough accepted runs exist. Queue memory only.
func (q *Queue) learnedStorage(j *Job, key *StorageKey) (StorageObservation, string, bool) {
	if key == nil || len(q.s.StorageObservations) == 0 {
		return StorageObservation{}, "", false
	}
	now := q.clock.Now()
	exact, ok := q.s.StorageObservations[identityHash(key)]
	if ok && exact.Managed.Samples > 0 && !now.Before(exact.RecordedAt) {
		return exact, "exact", true
	}
	var family StorageObservation
	for _, p := range q.storageFamilyMembers(storageFamily(j.Spec.Project, *key)) {
		if p.Managed.Samples == 0 || now.Before(p.RecordedAt) {
			continue
		}
		family.Output, family.Scratch, family.Spool = family.Output.merge(p.Output), family.Scratch.merge(p.Scratch), family.Spool.merge(p.Spool)
		family.Checkpoint, family.Metadata = family.Checkpoint.merge(p.Checkpoint), family.Metadata.merge(p.Metadata)
		family.Managed, family.Files = family.Managed.merge(p.Managed), family.Files.merge(p.Files)
		family.AllocationUnit = max(family.AllocationUnit, p.AllocationUnit)
	}
	if family.Managed.Samples < storageFamilyMinRuns {
		return StorageObservation{}, "", false
	}
	// This item's own rejected larger sample still forces fresh admission.
	family.Key, family.LowerBound = *key, exact.LowerBound
	return family, "family", true
}

// One planning pass groups observations once; other callers scan.
func (q *Queue) storageFamilyMembers(family string) []StorageObservation {
	if c := q.memoryPlanning; c != nil {
		if c.storageFamilies == nil {
			c.storageFamilies = map[string][]StorageObservation{}
			for _, p := range q.s.StorageObservations {
				if p.Family != "" {
					c.storageFamilies[p.Family] = append(c.storageFamilies[p.Family], p)
				}
			}
		}
		return c.storageFamilies[family]
	}
	var out []StorageObservation
	for _, p := range q.s.StorageObservations {
		if p.Family == family {
			out = append(out, p)
		}
	}
	return out
}
