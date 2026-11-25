package myce

// Memory pressure during a refused launch or infrastructure stop says nothing
// about the payload's need. A positively classified OOM still wins a stop race.
func memoryConstraintResult(started bool, r Result) bool {
	if r.OutOfMemory {
		return true
	}
	if r.ReleaseOnly || !started {
		return false
	}
	if c := r.ExecutionCause; c != nil {
		if c.PrelaunchRefused || c.Predicate == "launch_authorization" {
			return false
		}
		switch c.Operation {
		case "launch", "start_authorization", "start_handoff", "teardown", "start_deadline":
			return false
		}
	}
	return !r.Interrupted || r.ExecutionCause != nil && r.ExecutionCause.ExitCode != nil
}

type eligibleStorageSample struct {
	gateBuilt  bool
	storageKey *StorageKey
	job        *Job
	result     Result
}

func (q *Queue) storageLearningSample(j *Job, r Result) (*eligibleStorageSample, string) {
	switch {
	case r.ReleaseOnly:
		return nil, "release_only"
	case r.OutOfMemory:
		return nil, "oom"
	case !memoryConstraintResult(j.StartedAt != nil, r):
		return nil, "infrastructure_interrupted"
	case !r.Success:
		return nil, "attempt_failed"
	case r.ReleasePending:
		return nil, "release_pending"
	case j.State != "running":
		return nil, "attempt_not_running"
	case r.Interrupted:
		return nil, "interrupted"
	case r.Forced:
		return nil, "forced"
	case r.ID != j.ID || r.Attempt != j.Attempt || r.Machine != j.Machine || r.Session != j.Session:
		return nil, "attempt_changed"
	case len(j.AllocatedDevices) > 1:
		return nil, "multi_card"
	case r.MemoryPeak == nil:
		return nil, "peak_unavailable"
	case r.MemoryPeak.constrained():
		return nil, "constrained"
	case r.MemoryPeak.PressureUnavailable:
		return nil, "pressure_unavailable"
	case !validRate(memoryValue(r.MemoryPeak.HostGB)):
		return nil, "host_peak_unavailable"
	}
	return &eligibleStorageSample{gateBuilt: true, job: j, result: r, storageKey: q.storageKey(j, allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices))}, ""
}

func (q *Queue) recordAttemptLearning(j *Job, r Result) {
	q.recordWorkObservation(j, r)
	q.recordGPUSharingRun(j, r)
	if sample, _ := q.storageLearningSample(j, r); sample != nil {
		q.recordStorageNeeds(*sample)
	}
}

func (q *Queue) recordStorageNeeds(sample eligibleStorageSample) {
	if !sample.gateBuilt {
		return
	}
	reason := storageLearningReason(&sample, "", sample.job, sample.result)
	if !storageOutputEligible(reason) {
		return
	}
	j, r := sample.job, sample.result
	key := *sample.storageKey
	id := identityHash(key)
	old := q.s.StorageObservations[id]
	p := r.StoragePeak
	e := j.StorageEnvelope
	old.Key, old.Family = key, storageFamily(j.Spec.Project, key)
	old.Output = old.Output.add(manifestDiskBytes(r.OutputRefs), e.OutputBytes)
	old.OutputRecordedAt = q.clock.Now()
	old.OutputExecution = r.Execution
	if reason == "eligible" {
		old.Staging = old.Staging.add(p.StagedInputBytes, nil)
		old.Scratch = old.Scratch.add(p.ScratchBytes, e.ScratchBytes)
		old.Spool = old.Spool.add(p.SpoolBytes, e.SpoolBytes)
		old.Checkpoint = old.Checkpoint.add(p.CheckpointBytes, e.CheckpointBytes)
		old.Metadata = old.Metadata.add(p.MetadataBytes, e.MetadataBytes)
		old.Managed = old.Managed.add(p.ManagedBytes, e.ManagedBytes)
		old.Files = old.Files.add(p.Files, e.Files)
		old.AllocationUnit = max(old.AllocationUnit, p.AllocationUnit)
		old.RecordedAt = q.clock.Now()
		old.Execution = r.Execution
	}
	if q.s.StorageObservations == nil {
		q.s.StorageObservations = map[string]StorageObservation{}
	}
	q.touchCatalog("storage_observations/" + id)
	q.s.StorageObservations[id] = old
}
