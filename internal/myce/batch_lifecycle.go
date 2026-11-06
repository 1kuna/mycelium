package myce

import (
	"slices"
	"strings"
	"time"
)

// No elapsed interval supplies a missing measurement. This exact power-of-two
// duration survives wire float conversion; it represents an unknown cutoff,
// not a tuned timeout. Cold work retains custody after cooperative TERM.
const unmeasuredBatchGrace = time.Duration(1 << 62)

// Small queue-owned observations, committed with the same transaction as the
// exact-attempt report. They never establish checkpoint ownership or release.
type batchLifecycle struct {
	ReportAt      time.Time     `json:"report_at"`
	ReportMin     time.Duration `json:"report_min"`
	ReportMax     time.Duration `json:"report_max"`
	Attempt       int           `json:"attempt"`
	Session       string        `json:"session"`
	Machine       string        `json:"machine"`
	CheckpointAt  time.Time     `json:"checkpoint_at"`
	CheckpointMin time.Duration `json:"checkpoint_min"`
	CheckpointMax time.Duration `json:"checkpoint_max"`
}

func (q *Queue) observeBatchProgress(j *Job, generation int64) {
	if q.s.BatchLifecycle == nil {
		q.s.BatchLifecycle = map[string]batchLifecycle{}
	}
	v := q.s.BatchLifecycle[j.ID]
	now := q.clock.Now()
	if v.Attempt != j.Attempt || v.Session != j.Session || v.Machine != j.Machine {
		v = batchLifecycle{Attempt: j.Attempt, Session: j.Session, Machine: j.Machine}
		if j.StartedAt != nil {
			v.CheckpointAt = *j.StartedAt
			v.ReportAt = *j.StartedAt
		}
	}
	if !v.ReportAt.IsZero() && now.After(v.ReportAt) {
		d := now.Sub(v.ReportAt)
		if v.ReportMin == 0 || d < v.ReportMin {
			v.ReportMin = d
		}
		v.ReportMax = max(v.ReportMax, d)
	}
	v.ReportAt = now
	// Assignment clears the current report, but restoring an accepted generation
	// is not a newly saved checkpoint and cannot teach a stop cadence.
	if generation <= max(j.Progress, q.s.BatchRetries[j.ID].Progress) || generation <= 0 {
		q.s.BatchLifecycle[j.ID] = v
		q.boundLearnedBatchStop(j, now)
		return
	}
	if !v.CheckpointAt.IsZero() && now.After(v.CheckpointAt) {
		d := now.Sub(v.CheckpointAt)
		if v.CheckpointMin == 0 || d < v.CheckpointMin {
			v.CheckpointMin = d
		}
		v.CheckpointMax = max(v.CheckpointMax, d)
	}
	v.CheckpointAt = now
	q.s.BatchLifecycle[j.ID] = v
	q.boundLearnedBatchStop(j, now)
}

// Use this attempt's observed checkpoint cadence and this job's confirmed
// cleanup durations on the same machine. The observed range supplies jitter
// headroom; no cross-machine multiplier or universal wall time is assumed.
func (q *Queue) batchStopGrace(j *Job) time.Duration {
	if q.productionGrace > 0 {
		return q.productionGrace
	}
	if !j.Spec.Resumable && j.Spec.Needs.cpuOnly() {
		return 0
	}
	var low, high time.Duration
	add := func(d time.Duration) {
		if d <= 0 {
			return
		}
		if low == 0 || d < low {
			low = d
		}
		high = max(high, d)
	}
	if v := q.s.BatchLifecycle[j.ID]; v.Attempt == j.Attempt && v.Session == j.Session && v.Machine == j.Machine {
		add(v.CheckpointMin)
		add(v.CheckpointMax)
		if !j.Spec.Resumable {
			add(v.ReportMin)
			add(v.ReportMax)
		}
	}
	if !j.Spec.Resumable && j.AssignedAt != nil && j.StartedAt != nil {
		add(j.StartedAt.Sub(*j.AssignedAt))
	}
	for _, c := range j.Copies {
		if c.Machine == j.Machine && c.ReleaseConfirmed && c.StoppedAt != nil && c.ReleasedAt != nil {
			add(c.ReleasedAt.Sub(*c.StoppedAt))
		}
	}
	if high == 0 {
		return unmeasuredBatchGrace
	}
	// Saturate at the representation boundary rather than overflowing a long
	// checkpoint cadence into immediate KILL.
	return high + min(high-low, unmeasuredBatchGrace-high)
}

// Comparable accepted starts describe the installation's actual queue and
// preparation latency. This is advisory: overload cannot create a new caller
// deadline, and unrelated recipes/targets cannot shorten an authored cutoff.
func batchPlacementKey(j *Job) string {
	return identityHash(struct {
		Command     []string
		Environment string
		Docker      bool
	}{j.Spec.Run.Command, j.Spec.Environment.Name, j.Spec.Run.Docker})
}
func (q *Queue) observedPlacementLatency(j *Job) time.Duration {
	return q.s.BatchPlacements[batchPlacementKey(j)]
}
func (q *Queue) recordBatchPlacement(j *Job) {
	if j.AssignedAt == nil || j.StartedAt == nil {
		return
	}
	if q.s.BatchPlacements == nil {
		q.s.BatchPlacements = map[string]time.Duration{}
	}
	key := batchPlacementKey(j)
	q.s.BatchPlacements[key] = max(q.s.BatchPlacements[key], j.StartedAt.Sub(j.Queued))
}

// Unknown cleanup has no predicted release credit. It must not attract more
// demand to an unmeasured target while other targets can make progress.
func unmeasuredBatchStop(j *Job) bool {
	if j.StopAt == nil {
		return false
	}
	for _, c := range j.Copies {
		if c.Attempt == j.Attempt && c.Session == j.Session && c.Machine == j.Machine && c.StoppedAt != nil {
			return j.StopAt.Sub(*c.StoppedAt) == unmeasuredBatchGrace
		}
	}
	return false
}

func (q *Queue) boundLearnedBatchStop(j *Job, now time.Time) {
	if j.State == "stopping" && unmeasuredBatchStop(j) {
		if grace := q.batchStopGrace(j); grace < unmeasuredBatchGrace {
			deadline := now.Add(grace)
			j.StopAt = &deadline
		}
	}
}

// Failed preparation/launch with no useful progress is held on the unchanged
// target. A new worker session or changed preparation/qualification evidence
// permits a probe; elapsed time and routine memory/heartbeat noise do not.
type batchRetryEvidence struct {
	Targets  map[string]string `json:"targets,omitempty"`
	Progress int64             `json:"progress,omitempty"`
}

func batchRetryTarget(m *Machine) string {
	devices := slices.Clone(targetDevices(m))
	slices.Sort(devices)
	return m.ID + "/" + m.GPU + "/" + strings.Join(devices, ",")
}

func batchInfrastructureState(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	if len(m.groupMembers) > 0 {
		// Group placement orders cards by free memory. Its first-card scalar
		// projection is not new runtime evidence when that ordering changes.
		cards := slices.Clone(m.groupMembers)
		slices.SortFunc(cards, func(a, b *Machine) int { return strings.Compare(a.Device, b.Device) })
		m = groupView(cards)
	}
	// Hash only this job's preparation. Heartbeat report order, unrelated
	// builds and diagnostic counters are not evidence that its fault changed.
	keys := map[string]bool{}
	if key := environmentKey(j, m); key != "" {
		keys[key] = true
	}
	digests := map[string]bool{}
	for _, e := range j.Extensions {
		if !e.matchesTarget(m) || !slices.Contains(e.platforms(), m.GPU) {
			continue
		}
		digests[e.Digest] = true
		keys[extensionPreparationKey(e, m)] = true
		if v := e.variant(machineTarget(m)); v != nil {
			for _, r := range v.Environments {
				keys[r.Key] = true
			}
		}
	}
	type preparationState struct{ Key, State, Failure string }
	reports := []string{}
	for _, r := range m.EnvironmentReports {
		if keys[r.Key] {
			reports = append(reports, identityHash(preparationState{r.Key, r.State, r.FailureKind}))
		}
	}
	slices.Sort(reports)
	environments := []string{}
	for _, key := range m.Environments {
		if keys[key] {
			environments = append(environments, key)
		}
	}
	slices.Sort(environments)
	qualification := []string{}
	for _, r := range m.ExtensionReports {
		if digests[r.Digest] && r.Work == j.Spec.workType() && r.Platform == m.GPU && slices.ContainsFunc(targetDevices(m), func(device string) bool {
			return extensionReportDevice(r.Platform, r.Device) == extensionReportDevice(m.GPU, device)
		}) {
			qualification = append(qualification, identityHash(r))
		}
	}
	slices.Sort(qualification)
	// A grouped target needs every member's qualification/runtime evidence.
	members := []string{}
	for _, card := range m.groupMembers {
		members = append(members, batchInfrastructureState(j, card))
	}
	slices.Sort(members)
	return identityHash(struct {
		Session                   string
		Environments, Reports     []string
		Qualification, Members    []string
		Extension, CPU, Inventory string
		Roots                     []UnavailableRoot
		Checkpoint                bool
		CacheError                string
	}{m.Session, environments, reports, qualification, members, m.ExtensionFingerprint, m.CPUIsolation, m.InventoryReason, m.UnavailableRoots, m.CheckpointUnavailable, m.CacheError})
}

func (q *Queue) holdUnproductiveInfrastructure(j *Job) {
	// An unobserved exit/lost heartbeat has no repeatable failure signature.
	// Keep recovering those interruptions; never mistake them for command exits.
	// A fresh capacity admission sample is itself relevant pressure evidence;
	// existing admission checks govern those probes independently of job faults.
	if j.ExecutionCause != nil && j.ExecutionCause.Predicate == "memory_capacity" {
		return
	}
	if j.ExecutionCause == nil || j.Progress > q.s.BatchRetries[j.ID].Progress || j.ApplicationProgress != nil && j.ApplicationProgress.Attempt == j.Attempt && j.ApplicationProgress.Completed > 0 {
		return
	}
	if q.s.BatchRetries == nil {
		q.s.BatchRetries = map[string]batchRetryEvidence{}
	}
	evidence := q.s.BatchRetries[j.ID]
	if evidence.Targets == nil {
		evidence.Targets = map[string]string{}
	}
	// A failed device admission probe withdraws the last healthy inventory.
	// Only a subsequent healthy inventory report can permit another launch probe;
	// repeated unhealthy reports retain the hold. Never alter execution custody.
	if j.ExecutionCause.Predicate == "device_probe_unavailable" || j.ExecutionCause.Predicate == "device_identity_mismatch" {
		if m := q.s.Machines[j.Machine]; m != nil {
			m.InventoryReason = j.ExecutionCause.Predicate
		}
	}
	m := q.s.Machines[j.Machine]
	if m == nil {
		return
	}
	if j.Spec.Needs.cpuOnly() && m.Devices != nil {
		m = cpuMachineView(m)
	} else {
		m = allocationMachine(m, j.AllocatedDevices)
	}
	evidence.Targets[batchRetryTarget(m)] = batchInfrastructureState(j, m)
	q.s.BatchRetries[j.ID] = evidence
}

func (q *Queue) batchRetryBlocked(j *Job, m *Machine) bool {
	if m == nil {
		return false
	}
	evidence := q.s.BatchRetries[j.ID]
	fingerprint, ok := evidence.Targets[batchRetryTarget(m)]
	return ok && fingerprint == batchInfrastructureState(j, m)
}

func (q *Queue) batchRetryWaitingReason(j *Job) string {
	evidence := q.s.BatchRetries[j.ID]
	if len(evidence.Targets) == 0 {
		return ""
	}
	for _, m := range q.plannerMachines() {
		if q.jobAllowsMachine(j, m.ID) && executionTargetMatches(j.Spec, m) && !q.batchRetryBlocked(j, m) {
			return ""
		}
	}
	return "infrastructure recovery waiting for changed worker session, preparation or qualification evidence; no attempt progress"
}

func (q *Queue) beginBatchRetryEvidence(j *Job) {
	if q.s.BatchRetries == nil {
		q.s.BatchRetries = map[string]batchRetryEvidence{}
	}
	evidence := q.s.BatchRetries[j.ID]
	// The last exact attempt's accepted NAS report survives requeue. Restoring
	// the same generation is not new progress, including after queue restart.
	evidence.Progress = max(evidence.Progress, j.Progress)
	for _, c := range j.Copies {
		evidence.Progress = max(evidence.Progress, c.Progress)
	}
	if save := bestFileCheckpoint(j); save != nil {
		evidence.Progress = max(evidence.Progress, save.Generation)
	}
	q.s.BatchRetries[j.ID] = evidence
}
