package myce

// eligibleMemorySample is the input to every successful-attempt memory writer.
// Only memoryLearningSample constructs it, after checking outcome, custody and
// pressure. gateBuilt rejects zero/field-built values at runtime; the source
// regression keeps production construction here (not a Go package boundary).
// Writers may further narrow comparability, never relax eligibility.
// Keep new attempt-memory writers here and call them from recordAttemptLearning.
// Diagnostics, throughput, OOM lower bounds and idle reference reports are not
// successful-attempt memory learning.
type eligibleMemorySample struct {
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
		validMemoryValue(r.Usage.MemoryGB*1.25) && validMemoryValue(r.Usage.HostMemoryGB*1.25) {
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
	if workReason != "eligible" && programKey == "" {
		return nil, workReason
	}
	return &eligibleMemorySample{gateBuilt: true, job: j, result: r, work: workReason == "eligible", programKey: programKey, programFloor: floor}, "eligible"
}

// One dispatch for all successful-attempt learners. Throughput can still learn
// from a rejected memory sample, but gets no token to change memory evidence.
func (q *Queue) recordAttemptLearning(j *Job, r Result) {
	sample, _ := q.memoryLearningSample(j, r)
	q.recordWorkObservation(j, r, sample)
	if sample != nil {
		q.recordProgramNeeds(*sample)
	}
}

func (q *Queue) workMemoryComparison(j *Job) string {
	t := j.Timing
	if t == nil || t.Key == nil {
		return "timing_unavailable"
	}
	if j.Batch != nil && (j.Batch.Run || j.Batch.Concurrency != 1 || len(j.Batch.Pieces) != 1) {
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
	if !memoryIdentityUsable(old) || !old.MemoryRecordedAt.IsZero() && q.clock.Now().Sub(old.MemoryRecordedAt) > workCurveMaxAge {
		old.MemoryPeak, old.MemoryRange, old.GPUHostRange = nil, nil, nil
	}
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
	point.MemoryLayout = "separate"
	if owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices); owner != nil && owner.UnifiedMemory && !j.Spec.Needs.cpuOnly() {
		point.MemoryLayout = "unified"
	}
	point.MemoryRecordedAt = q.clock.Now()
	if r.Usage != nil && r.Usage.valid() {
		point.HostMemoryGB = max(point.HostMemoryGB, r.Usage.HostMemoryGB)
		point.DeviceMemoryGB = max(point.DeviceMemoryGB, r.Usage.MemoryGB)
	}
}

func (q *Queue) recordProgramNeeds(sample eligibleMemorySample) {
	if !sample.gateBuilt || sample.programKey == "" {
		return
	}
	if q.s.ProgramNeeds == nil {
		q.s.ProgramNeeds = map[string]SizeHint{}
	}
	old, floor, usage := q.s.ProgramNeeds[sample.programKey], sample.programFloor, sample.result.Usage
	// Floors and 25% headroom remain conservative; observations never shrink.
	q.s.ProgramNeeds[sample.programKey] = SizeHint{MemoryGB: max(old.MemoryGB, floor.MemoryGB, usage.MemoryGB*1.25), HostMemoryGB: max(old.HostMemoryGB, floor.HostMemoryGB, usage.HostMemoryGB*1.25)}
	observation := *usage
	sample.job.MeasuredNeeds = &observation
}
