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

func memoryConstraintFacts(facts AttemptMemoryFacts) bool {
	// Classify once at completion, before retry clears attempt state. Raw event
	// counters remain diagnostics, even when they imply pressure by themselves.
	return facts.LearningReason != "infrastructure_interrupted" && facts.LearningReason != "release_only"
}

// eligibleMemorySample is the input to every successful-attempt memory writer.
// Only memoryLearningSample constructs it, after checking outcome, custody and
// pressure. gateBuilt rejects zero/field-built values at runtime; the source
// regression keeps production construction here (not a Go package boundary).
// Writers may further narrow comparability, never relax eligibility.
// Keep new attempt-memory writers here and call them from recordAttemptLearning.
// Diagnostics, throughput, OOM lower bounds and idle reference reports are not
// successful-attempt memory learning.
type eligibleMemorySample struct {
	storageKey   *StorageKey
	gateBuilt    bool
	job          *Job
	result       Result
	work         bool
	programKey   string
	programFloor SizeHint
}

func (q *Queue) memoryLearningSample(j *Job, r Result) (*eligibleMemorySample, string) {
	switch {
	case r.ReleaseOnly:
		return nil, "release_only"
	case r.OutOfMemory:
		return nil, "oom"
	case !memoryConstraintResult(j.StartedAt != nil, r):
		return nil, "infrastructure_interrupted"
	case !r.Success:
		return nil, "attempt_failed"
	case j.StartedAt == nil:
		return nil, "not_started"
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
		// Per-card evidence from a multi-card attempt cannot size a one-card run.
		return nil, "multi_card"
	case r.MemoryPeak == nil:
		return nil, "peak_unavailable"
	case r.MemoryPeak.constrained():
		return nil, "constrained"
	case r.MemoryPeak.PressureUnavailable:
		return nil, "pressure_unavailable"
	case r.MemoryPeak.HostGB == nil || !validRate(*r.MemoryPeak.HostGB):
		return nil, "host_peak_unavailable"
	}
	workReason := q.workMemoryComparison(j)
	programKey := ""
	var floor SizeHint
	if j.Spec.Program != nil && j.Extension != nil && r.Usage != nil && r.Usage.valid() &&
		validMemoryValue(r.Usage.MemoryGB+deviceMemoryGrowthGB(r.MemoryPeak)) && validMemoryValue(r.Usage.HostMemoryGB+memoryGrowthGB(r.MemoryPeak)) {
		if m := allocationMachine(q.s.Machines[r.Machine], j.AllocatedDevices); m != nil {
			for _, e := range j.Extensions {
				if e.Digest == j.Extension.Digest {
					programKey = programNeedsKey(j, e, m.GPU)
					floor = SizeHint{MemoryGB: e.MemoryGB, HostMemoryGB: e.HostMemoryGB}
					break
				}
			}
		}
	}
	storageKey := q.storageKey(j, allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices))
	if workReason != "eligible" && programKey == "" && storageKey == nil {
		return nil, workReason
	}
	return &eligibleMemorySample{storageKey: storageKey, gateBuilt: true, job: j, result: r, work: workReason == "eligible", programKey: programKey, programFloor: floor}, "eligible"
}

// One dispatch for all successful-attempt learners. Throughput can still learn
// from a rejected memory sample, but gets no token to change memory evidence.
func (q *Queue) recordAttemptLearning(j *Job, r Result) {
	sample, _ := q.memoryLearningSample(j, r)
	q.recordWorkObservation(j, r, sample)
	if sample != nil {
		q.recordProgramNeeds(*sample)
		q.recordStorageNeeds(*sample)
	}
}

func (q *Queue) workMemoryComparison(j *Job) string {
	t := j.Timing
	if t == nil || t.Key == nil {
		return "timing_unavailable"
	}
	if j.Batch != nil && (j.Batch.Concurrency != 1 || len(j.Batch.Pieces) != 1) {
		return "batch_not_comparable"
	}
	current, _ := q.workObservationKey(j)
	if current == nil || *current != *t.Key || t.Execution.ID != j.ID || t.Execution.Attempt != j.Attempt || t.Machine != j.Machine || t.Session != j.Session {
		return "identity_changed"
	}
	return "eligible"
}

// All fitted work-memory fields, including the legacy Usage maxima consumed by
// batch/model placement, require the same token as ProgramNeeds. Readers derive
// envelopes from these accepted fields; they never learn from diagnostic facts.
func (q *Queue) recordWorkMemory(sample eligibleMemorySample, old WorkObservation, point *WorkObservation) {
	if !sample.gateBuilt || !sample.work {
		return
	}
	j, r := sample.job, sample.result
	if old.MemoryPeak.constrained() && !old.MemoryPeak.Constrained {
		peak := *old.MemoryPeak
		peak.Constrained = true
		old.MemoryPeak = &peak
	}
	// Corrected accepted attribution replaces the old aggregate rather than
	// carrying its inflated outside peak/range forward. Rejected attempts never
	// reach this writer. The accepted-sample ledger records the replacement.
	corrected := r.MemoryPeak.GPUHost != nil && r.MemoryPeak.GPUHost.SiblingAdjusted
	oldCorrected := old.MemoryPeak != nil && old.MemoryPeak.GPUHost != nil && old.MemoryPeak.GPUHost.SiblingAdjusted
	if oldCorrected && r.MemoryPeak.GPUHost != nil && !corrected {
		return // A result from a pre-upgrade worker cannot poison corrected history.
	}
	if corrected && !oldCorrected || !memoryIdentityUsable(old) {
		old.MemoryPeak, old.MemoryRange, old.GPUHostRange, old.MemorySamples = nil, nil, nil, 0
	}
	point.MemoryEscalation = nil
	point.MemoryPeak = mergeMemoryPeaks(old.MemoryPeak, r.MemoryPeak)
	if p := r.MemoryPeak; p.NonreclaimableGB != nil && validRate(*p.NonreclaimableGB) && p.Events != nil && p.Source != processGroupMemorySource {
		point.MemoryRange = mergeMemoryPeakRanges(old.MemoryRange, &MemoryPeakRange{Samples: 1, MinGB: *p.NonreclaimableGB, MaxGB: *p.NonreclaimableGB})
	} else {
		point.MemoryRange = nil // Missing counters cannot preserve a reduced estimate.
	}
	if peak, ok := r.MemoryPeak.gpuHostPeakGB(); ok && r.MemoryPeak.Events != nil && r.MemoryPeak.Source != processGroupMemorySource {
		point.GPUHostRange = mergeMemoryPeakRanges(old.GPUHostRange, &MemoryPeakRange{Samples: 1, MinGB: peak, MaxGB: peak})
	} else {
		point.GPUHostRange = nil
	}
	point.MemoryConfiguration = memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
	point.MemoryFamily, point.MemorySamples = memoryFamily(j), old.MemorySamples+1
	point.MemoryLayout = memoryLayout(j, allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices))
	point.MemoryRecordedAt = q.clock.Now()
}

func (q *Queue) recordProgramNeeds(sample eligibleMemorySample) {
	if !sample.gateBuilt || sample.programKey == "" {
		return
	}
	if q.s.ProgramNeeds == nil {
		q.s.ProgramNeeds = map[string]SizeHint{}
	}
	old, floor, usage, peak := q.s.ProgramNeeds[sample.programKey], sample.programFloor, sample.result.Usage, sample.result.MemoryPeak
	// As for charges (memory_room.go), the need is the latest accepted use plus
	// its measured growth, so a past forecast can come back down (#987).
	// Unknown growth keeps the prior need; authored floors never shrink.
	need := SizeHint{MemoryGB: max(floor.MemoryGB, usage.MemoryGB+deviceMemoryGrowthGB(peak)), HostMemoryGB: max(floor.HostMemoryGB, usage.HostMemoryGB+memoryGrowthGB(peak))}
	if peak == nil || peak.DeviceGrowthGB == nil {
		need.MemoryGB = max(need.MemoryGB, old.MemoryGB)
	}
	if peak == nil || peak.GrowthGB == nil {
		need.HostMemoryGB = max(need.HostMemoryGB, old.HostMemoryGB)
	}
	q.s.ProgramNeeds[sample.programKey] = need
	observation := *usage
	sample.job.MeasuredNeeds = &observation
}

// Storage is a further narrowing of the same custody/outcome/pressure gate.
// Keep the runtime marker check here alongside the other accepted-state writers.
func (q *Queue) recordStorageNeeds(sample eligibleMemorySample) {
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
