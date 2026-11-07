package myce

import "fmt"

// RepeatedWorkTiming uses the existing accepted-attempt sampler and store. Its
// count is separate from Samples, which also includes memory-only observations.
// The range describes observed variability, not a confidence interval/deadline.
type RepeatedWorkTiming struct {
	Samples    int     `json:"samples"`
	Seconds    float64 `json:"seconds_per_job"`
	MinSeconds float64 `json:"min_seconds"`
	MaxSeconds float64 `json:"max_seconds"`
}

func (p *RepeatedWorkTiming) uncertainty() string {
	if p.Samples == 1 {
		return "single clean sample; variability unknown"
	}
	return fmt.Sprintf("observed range %.2f–%.2fs; future duration uncertain", p.MinSeconds, p.MaxSeconds)
}

func addRepeatedTiming(old *RepeatedWorkTiming, seconds float64) *RepeatedWorkTiming {
	p := RepeatedWorkTiming{Samples: 1, Seconds: seconds, MinSeconds: seconds, MaxSeconds: seconds}
	if old != nil && old.Samples > 0 && validRate(old.Seconds) {
		p.Samples += old.Samples
		p.Seconds = old.Seconds + (seconds-old.Seconds)/float64(p.Samples)
		p.MinSeconds, p.MaxSeconds = min(old.MinSeconds, seconds), max(old.MaxSeconds, seconds)
	}
	return &p
}

func plainCommandWork(s Spec) bool {
	return !s.extensionJob() && s.Workload == nil && len(s.Run.Command) > 0
}

func repeatedWorkIdentity(j *Job, m *Machine) (string, string) {
	s := j.Spec
	if !plainCommandWork(s) || j.Batch != nil {
		return "", "ordinary repeated command unavailable"
	}
	if m.ExtensionFingerprint == "" {
		return "", "runtime fingerprint unavailable"
	}
	if len(s.Inputs) > 0 || s.Needs.Model != "" || s.After != nil {
		return "", "unversioned or dynamic input; repeated work unknown"
	}
	if s.Run.Docker || len(s.Run.Sidecars) > 0 || s.Run.Workdir != "" {
		return "", "external runtime or work directory; repeated work unknown"
	}
	// Include the source archive (unlike memory), all staged bytes and frozen
	// recipes. ProfileMemory intentionally does not identify speed. Hashing here
	// serializes only small manifest/configuration metadata, never payload bytes.
	return identityHash(struct {
		Run           RunSpec
		Execution     *ExecutionSpec
		Files         FileManifest
		SharedInputs  map[string]SharedInput
		Models        map[string]ModelSnapshot
		Outputs       map[string]string
		Environment   Environment
		Recipes       map[string]EnvironmentRecipe
		PrepareSource bool
		Isolation     string
	}{s.Run, s.Execution, s.Files, s.SharedInputs, s.ModelSnapshots, s.Outputs, s.Environment, j.EnvironmentRecipes, s.PrepareSource, s.Isolation}), ""
}

// Speed and memory use the same sampler/store but distinct keys: exact source
// bytes must not fragment the existing intentionally broader memory aggregate.
func (q *Queue) repeatedObservationKey(j *Job) (*ObservationKey, string) {
	key, reason := q.workObservationKey(j)
	if key == nil || reason != "" {
		return nil, reason
	}
	m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if j.Spec.Needs.cpuOnly() && q.s.Machines[j.Machine].Devices != nil {
		m = cpuMachineView(q.s.Machines[j.Machine])
	}
	key.RepeatedWork, reason = repeatedWorkIdentity(j, m)
	if reason != "" {
		return nil, reason
	}
	return key, ""
}

func (q *Queue) recordRepeatedObservation(j *Job, r Result) {
	t := j.Timing
	seconds := q.clock.Now().Sub(*j.StartedAt).Seconds()
	if !validRate(seconds) {
		return
	}
	t.ActualSeconds = &seconds
	prior, _ := q.repeatedPoint(t.RepeatedKey)
	p := addRepeatedTiming(prior, seconds)
	id := identityHash(t.RepeatedKey)
	old, exists := q.s.WorkObservations[id]
	point := WorkObservation{Key: *t.RepeatedKey, Repeated: p,
		Samples: old.Samples + 1, RecordedAt: q.clock.Now(), Source: "accepted repeated command",
		Job: j.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt}
	q.recordServiceCalibration(j, r, old, &point)
	q.putWorkObservation(id, point)
	q.recordCurveLearning(j, old, exists, point)
}

// No new pressure threshold: a clean comparison requires measured zero CPU
// scheduling stalls, quota throttling and I/O stalls. This can exclude work
// delayed by internal parallelism or intrinsic I/O; that
// is preferable to teaching external contention as machine speed. Missing or
// lower-bound counters cannot assert zero. Memory uses its existing baselined
// pressure classification. No additional collectors or benchmark jobs run.
func repeatedContentionReason(r Result) string {
	p, a := r.MemoryPeak, r.Resources
	if p == nil || p.PressureUnavailable || p.Pressure == nil || p.Pressure.Reason != "eligible" {
		return "pressure evidence unavailable or contended; speed unknown"
	}
	if p.constrained() {
		return "memory pressure; speed comparison excluded"
	}
	if a == nil || a.CPU.Unavailable != "" || a.CPU.LowerBound || a.CPU.UsageSeconds == nil ||
		!validMemoryValue(*a.CPU.UsageSeconds) || a.CPU.ThrottledSeconds == nil || !validMemoryValue(*a.CPU.ThrottledSeconds) ||
		a.CPU.Pressure == nil || a.CPU.Pressure.Unavailable != "" || a.CPU.Pressure.Some == nil ||
		a.IO.Pressure == nil || a.IO.Pressure.Unavailable != "" || a.IO.Pressure.Some == nil {
		return "CPU or I/O contention evidence unavailable; speed unknown"
	}
	if *a.CPU.ThrottledSeconds > 0 || a.CPU.Pressure.Some.TotalUS > 0 || a.IO.Pressure.Some.TotalUS > 0 {
		return "CPU scheduling/throttling or I/O stalls; speed comparison excluded"
	}
	return ""
}

func (q *Queue) repeatedPoint(key *ObservationKey) (*RepeatedWorkTiming, *WorkObservation) {
	if key == nil || key.RepeatedWork == "" {
		return nil, nil
	}
	p, ok := q.s.WorkObservations[identityHash(key)]
	if !ok || p.Key != *key || p.Forbidden || p.StaleReason != "" || q.clock.Now().Before(p.RecordedAt) ||
		p.Repeated == nil || p.Repeated.Samples <= 0 || !validRate(p.Repeated.Seconds) {
		return nil, nil
	}
	return p.Repeated, &p
}

func (q *Queue) beginRepeatedTiming(j *Job) {
	t := j.Timing
	t.Comparability = "exact command/source/inputs/environment and component configuration; clean serial attempts only"
	t.HistoryReason = "no accepted matching clean command measurement"
	t.Uncertainty = "duration unknown"
	if t.RepeatedKey == nil {
		t.Comparability = "repeated command comparison unavailable"
		return
	}
	if p, old := q.repeatedPoint(t.RepeatedKey); p != nil {
		prediction := p.Seconds
		t.PredictedSeconds, t.Samples = &prediction, p.Samples
		t.HistoryRecordedAt, t.HistorySource = &old.RecordedAt, "matching repeated command"
		t.HistoryReason, t.Uncertainty = "", p.uncertainty()
	}
}

func (q *Queue) estimateRepeatedCompletion(j *Job, m *Machine) (completionEstimate, bool) {
	if j.Batch != nil || j.Spec.Resumable && j.State == "queued" && j.Attempt > 0 {
		return completionEstimate{}, false
	}
	copy := *q.memoryCandidate(j, m)
	copy.Machine, copy.Session = m.ID, m.Session
	copy.AllocatedDevices = nil
	if m.Devices != nil && !copy.Spec.Needs.cpuOnly() {
		copy.AllocatedDevices = []string{m.Device}
	}
	key, reason := q.repeatedObservationKey(&copy)
	if reason != "" {
		return completionEstimate{}, false
	}
	p, _ := q.repeatedPoint(key)
	if p == nil {
		return completionEstimate{}, false
	}
	transfer, known := inputTransferSeconds(&copy, m)
	if !known {
		return completionEstimate{}, false
	}
	e := completionEstimate{group: key.RepeatedWork, service: p.Seconds,
		setup: environmentSeconds(&copy, m), transfer: transfer, samples: p.Samples,
		comparability: "exact repeated command", uncertainty: p.uncertainty()}
	e.service, e.calibration = q.calibratedService(key, e.service, 1)
	return e, validRate(e.seconds()) && validMemoryValue(e.setup) && validMemoryValue(e.transfer)
}
