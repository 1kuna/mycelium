package myce

import (
	"maps"
	"time"
)

// Throughput transfers between jobs of the same family on the same physical
// device and software configuration. Memory allowances are admission inputs,
// so changing one does not discard a measured throughput comparison.
type gpuSharingKey struct {
	Host, Device, Fingerprint string
	Target                    ExecutionTarget
	Project, Name, Extension  string
	Work, InputShape          string
	Units                     float64
}

func sharingKey(j *Job, m *Machine) (gpuSharingKey, bool) {
	if m == nil || !m.UnifiedMemory || m.Device == "" || j.Spec.Needs.cpuOnly() || j.Spec.Needs.Model != "" ||
		j.Spec.ExtensionCheck != nil || len(j.AllocatedDevices) > 1 || j.Batch != nil && (j.Batch.Concurrency != 1 || len(j.Batch.Pieces) != 1) {
		return gpuSharingKey{}, false
	}
	k := gpuSharingKey{Host: hostID(m), Target: machineTarget(m), Project: j.Spec.Project}
	for _, device := range m.Devices {
		if device.ID == m.Device {
			k.Device, k.Fingerprint = device.Identity, device.Fingerprint
			break
		}
	}
	if k.Device == "" {
		return gpuSharingKey{}, false
	}
	if j.Spec.extensionJob() {
		switch {
		case j.Extension != nil:
			k.Extension = j.Extension.Digest
		case len(j.Extensions) == 1:
			k.Extension = j.Extensions[0].Digest
		default:
			return gpuSharingKey{}, false
		}
		k.Work = j.Spec.workType()
	} else {
		k.Name = j.Spec.Name
		if k.Name == "" {
			return gpuSharingKey{}, false
		}
	}
	if work := j.Spec.Workload; work != nil {
		k.InputShape, k.Units = work.InputShape, work.Units
	}
	return k, true
}

type gpuSharingHistory struct {
	SoloSeconds []float64
	PairSeconds float64
	Exclusive   bool
}

// This is evidence about a new placement only. Existing attempts keep their
// custody, including when a queue or worker restarts during an overlap.
func (q *Queue) gpuSharingRestriction(j *Job, m *Machine) gpuSharingKey {
	if q.memoryEvidence == nil || len(q.memoryEvidence.gpuSharing) == 0 {
		return gpuSharingKey{}
	}
	key, ok := sharingKey(j, m)
	if ok && q.memoryEvidence.gpuSharing[key].Exclusive {
		return key
	}
	return gpuSharingKey{}
}

func (k gpuSharingKey) conflicts(holder *Job, m *Machine) bool {
	if k.Device == "" {
		return false
	}
	other, ok := sharingKey(holder, m)
	return ok && k == other
}

func (q *Queue) gpuSharingBlocked(j *Job, m *Machine) bool {
	restriction := q.gpuSharingRestriction(j, m)
	if restriction.Device == "" {
		return false
	}
	for _, holder := range q.liveJobs() {
		if holder.ID != j.ID && batchReservesCapacity(holder) && batchOnTarget(holder, m) && restriction.conflicts(holder, m) {
			return true
		}
	}
	return false
}

// Queue-clock start and exact attempt identity are already fenced by Started.
// A nil run means the duration cannot establish either a solo run or a pair.
// PairStart is passed to the second finisher only by a clean first completion.
type gpuSharingRun struct {
	Key       gpuSharingKey
	Execution Execution
	Session   string
	Peer      Execution
	PairStart time.Time
}

func currentSharingRun(j *Job) *gpuSharingRun {
	r := j.gpuSharingRun
	if r != nil && r.Execution == (Execution{ID: j.ID, Attempt: j.Attempt}) && r.Session == j.Session {
		return r
	}
	return nil
}

func (q *Queue) beginGPUSharingRun(j *Job) {
	j.gpuSharingRun = nil
	if j.Spec.Needs.cpuOnly() || q.memoryEvidence == nil {
		return
	}
	m := q.planningAllocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if m == nil || !m.UnifiedMemory {
		return
	}
	key, comparable := sharingKey(j, m)
	comparable = comparable && !(j.Spec.Resumable && j.Attempt > 1)
	var peer *Job
	for _, holder := range q.liveJobs() {
		if holder == j || !batchOnTarget(holder, m) || !batchReservesCapacity(holder) || holder.StartedAt == nil {
			continue
		}
		other := currentSharingRun(holder)
		if comparable && peer == nil && other != nil && other.Key == key && other.Peer.ID == "" && holder.Session == j.Session && holder.State == "running" {
			peer = holder
			continue
		}
		comparable = false
		if other != nil {
			q.touchJob(holder.ID)
			holder.gpuSharingRun = nil
		}
	}
	if !comparable || q.gpuSharingCompetingCustody(m) {
		if peer != nil {
			q.touchJob(peer.ID)
			peer.gpuSharingRun = nil
		}
		return
	}
	run := &gpuSharingRun{Key: key, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session}
	if peer != nil {
		q.touchJob(peer.ID)
		other := *peer.gpuSharingRun
		run.Peer, other.Peer = other.Execution, run.Execution
		peer.gpuSharingRun = &other
	}
	j.gpuSharingRun = run
}

func (q *Queue) gpuSharingCompetingCustody(m *Machine) bool {
	for _, release := range q.s.HostReleases {
		if !q.batchReleaseOnTarget(release, m) {
			continue
		}
		holder := q.s.Jobs[release.Job]
		if holder == nil || holder.StartedAt == nil || holder.Machine != release.Machine || holder.Attempt != release.Attempt ||
			holder.Session != release.Session || holder.State != "running" {
			return true
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slotOnTarget(slot, m) && slotReservesMemory(slot) {
			return true
		}
	}
	return false
}

// Called only by the accepted result path, using the same elapsed time and
// unified peak projection as ordinary work and memory observations. A failed
// or unmeasured member cannot teach a shared throughput result.
func (q *Queue) recordGPUSharingRun(j *Job, result Result) {
	run := currentSharingRun(j)
	j.gpuSharingRun = nil
	if run == nil || q.memoryEvidence == nil || j.StartedAt == nil || j.State != "running" ||
		!result.Success || result.ReleasePending || result.Interrupted || result.Forced || result.OutOfMemory ||
		result.MemoryPeak == nil || result.MemoryPeak.constrained() || pooledSampleLimited(result.MemoryPeak) {
		return
	}
	m := q.planningAllocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	key, ok := sharingKey(j, m)
	if !ok || key != run.Key || m.Session != j.Session || m.Gone || q.unreported[m.ID] || q.gpuSharingCompetingCustody(m) {
		return
	}
	points := appendProjectedMemoryPeaks(nil, memoryEvidenceKey{Platform: memoryPlatform(key.Target, true)}, memoryEvidencePoint{}, result.MemoryPeak)
	now := q.clock.Now()
	seconds := now.Sub(*j.StartedAt).Seconds()
	if len(points) == 0 || !validRate(seconds) {
		return
	}
	if run.Peer.ID == "" {
		q.observeGPUSharing(key, seconds, false)
		return
	}
	if !run.PairStart.IsZero() {
		q.observeGPUSharing(key, now.Sub(run.PairStart).Seconds(), true)
		return
	}
	peer := q.s.Jobs[run.Peer.ID]
	if peer == nil || !batchReservesCapacity(peer) || peer.State != "running" || peer.StartedAt == nil {
		return
	}
	other := currentSharingRun(peer)
	if other == nil || other.Execution != run.Peer || other.Peer != run.Execution || other.Session != run.Session || other.Key != key {
		return
	}
	q.touchJob(peer.ID)
	next := *other
	next.PairStart = *j.StartedAt
	if peer.StartedAt.Before(next.PairStart) {
		next.PairStart = *peer.StartedAt
	}
	peer.gpuSharingRun = &next
}

func (q *Queue) observeGPUSharing(key gpuSharingKey, seconds float64, paired bool) {
	if !validRate(seconds) {
		return
	}
	e := q.stagedMemoryEvidence()
	if e.gpuSharingWrites == nil {
		e.gpuSharing = maps.Clone(e.gpuSharing)
		if e.gpuSharing == nil {
			e.gpuSharing = make(map[gpuSharingKey]gpuSharingHistory)
		}
		e.gpuSharingWrites = make(map[gpuSharingKey]gpuSharingHistory)
	}
	h := e.gpuSharing[key]
	if paired {
		h.PairSeconds = seconds
	} else {
		h.SoloSeconds = appendMemoryValue(h.SoloSeconds, seconds, memoryPeakWindow)
	}
	h.Exclusive = len(h.SoloSeconds) != 0 && h.PairSeconds > 2*memoryMedian(h.SoloSeconds)
	e.gpuSharing[key], e.gpuSharingWrites[key] = h, h
}
