package myce

import (
	"fmt"
	"slices"
)

// Placement learns from its own accepted work (MY-31, MY-49). A duration
// ranks placements without granting resources, slots or a deadline.
type completionEstimate struct {
	group   string
	seconds float64
	work    string
}

func (e completionEstimate) reason() string {
	return fmt.Sprintf("last accepted duration %.2fs on this target", e.seconds)
}

// estimateCompletion is this kind of work's last accepted duration on m, keyed
// exactly as recordWorkObservation records it.
func (q *Queue) estimateCompletion(j *Job, m *Machine) (completionEstimate, bool) {
	if j.State == "queued" && j.Spec.Resumable && j.Attempt > 0 {
		return completionEstimate{}, false
	}
	candidates := []*Job{j}
	if j.Spec.extensionJob() {
		candidates = extensionJobs(j, m, true)
	} else if !plainCommandWork(j.Spec) {
		return completionEstimate{}, false
	}
	for _, candidate := range candidates {
		copy := *candidate
		copy = *q.memoryCandidate(&copy, m)
		copy.Machine, copy.Session = m.ID, m.Session
		copy.AllocatedDevices = nil
		if m.Devices != nil && !copy.Spec.Needs.cpuOnly() {
			copy.AllocatedDevices = []string{m.Device}
		}
		key, reason := q.workObservationKey(&copy)
		if key == nil || reason != "" {
			continue
		}
		if seconds, ok := q.lastAcceptedSeconds(key); ok {
			return completionEstimate{group: identityHash([]any{key.Work, key.ExtensionDigest, key.Reference}), seconds: seconds, work: key.Work}, true
		}
	}
	return completionEstimate{}, false
}

type completionPeer struct {
	job *Job
	set *candidateSet
}

// Pick the legacy winner first so a missing or incomparable duration cannot
// reorder hosts. Only ready targets offering that same fair winner compete on
// measured speed, and last-resort policy still applies.
func (q *Queue) fasterReadyTarget(j *Job, selected *candidateSet, peers []completionPeer) (*Job, *candidateSet) {
	if j.PreemptionPlacement != nil || !environmentReady(j, selected.m) {
		return j, selected
	}
	best, ok := q.estimateCompletion(j, selected.m)
	if !ok {
		return j, selected
	}
	lastResort := q.s.HostMaintenance[hostID(selected.m)].LastResort
	for _, peer := range peers {
		m := peer.set.m
		if peer.job.ID != j.ID || peer.set == selected || lastResort != q.s.HostMaintenance[hostID(m)].LastResort || !environmentReady(peer.job, m) {
			continue
		}
		if e, ok := q.estimateCompletion(peer.job, m); ok && e.group == best.group && e.seconds < best.seconds {
			j, selected, best = peer.job, peer.set, e
		}
	}
	return j, selected
}

type completionRelease struct {
	job *Job
	in  float64
}

type completionScan struct {
	waiting     []*Job
	releases    map[string]completionRelease
	sameBacklog *bool
}

// Capture current measured releases once per dispatch pass. Past estimates,
// preparations, stops, and attempts from another session cannot delay work.
func (q *Queue) measuredReleases() *completionScan {
	p := &completionScan{releases: map[string]completionRelease{}}
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		if j.State == "queued" {
			p.waiting = append(p.waiting, j)
			continue
		}
		t := j.Timing
		if j.State != "running" || !j.Spec.Needs.cpuOnly() || cancelled(j) || j.ReleaseConfirmed ||
			j.StartedAt == nil || t == nil || t.Key == nil ||
			t.Reason != "" || t.LastAcceptedSeconds == nil || !validRate(*t.LastAcceptedSeconds) ||
			t.Execution != (Execution{ID: j.ID, Attempt: j.Attempt}) || t.Machine != j.Machine || t.Session != j.Session {
			continue
		}
		m := q.s.Machines[j.Machine]
		if m == nil || m.Session != j.Session || now.Before(*j.StartedAt) {
			continue
		}
		left := *t.LastAcceptedSeconds - now.Sub(*j.StartedAt).Seconds()
		if left <= 0 {
			continue
		}
		prior := p.releases[j.Machine]
		if prior.job == nil || left < prior.in || left == prior.in && j.ID < prior.job.ID {
			p.releases[j.Machine] = completionRelease{j, left}
		}
	}
	return p
}

// A mixed backlog can have another fair winner on the faster host. Retain
// normal dispatch when this pass cannot approximate the tail with one duration.
func (p *completionScan) comparableBacklog(j *Job, work string) bool {
	if p.sameBacklog == nil {
		same := true
		for _, other := range p.waiting {
			s, base := other.Spec, j.Spec
			if !plainCommandWork(s) || s.childFanOut() || other.PreemptionPlacement != nil ||
				s.StartBy != nil || maintenanceResume(other) || s.Project != base.Project ||
				s.Class != base.Class || s.Priority != base.Priority ||
				s.RequireTarget != base.RequireTarget || !slices.Equal(s.Targets, base.Targets) ||
				!slices.Equal(s.AllowedMachines, base.AllowedMachines) ||
				!slices.Equal(s.ExcludedMachines, base.ExcludedMachines) || profileMemoryIdentity(other) != work {
				same = false
				break
			}
		}
		p.sameBacklog = &same
	}
	return *p.sameBacklog
}

// A measured CPU backlog can wait for a faster lane if it can finish the whole
// waiting tail sooner. The lane can be free now or held by a measured attempt.
// Larger backlogs still use slow capacity to increase throughput.
// The prediction is rebuilt on each pass and grants no future placement.
func (q *Queue) fasterMeasuredLane(j *Job, m *Machine, scan *placementScan) *Machine {
	if len(q.s.WorkObservations) == 0 || m.GPU != "cpu" || !plainCommandWork(j.Spec) ||
		j.PreemptionPlacement != nil || j.Spec.StartBy != nil || maintenanceResume(j) || q.fleetStartReservation(j) != nil ||
		!environmentReady(j, m) {
		return nil
	}
	slow, ok := q.estimateCompletion(j, m)
	if !ok {
		return nil
	}
	if scan.completion == nil {
		scan.completion = q.measuredReleases()
	}
	p := scan.completion
	// The selected candidate carries the slow host's reservation. Resolve the
	// original queued intent independently on every other host.
	j = q.s.Jobs[j.ID]
	if j == nil {
		return nil
	}
	var best *Machine
	bestEnd := slow.seconds
	for _, fast := range scan.machines {
		if !scan.measuredRoutes[fast][j.ID] || hostID(fast) == hostID(m) || !scan.live[fast.ID] ||
			q.s.HostMaintenance[hostID(fast)].LastResort || !environmentReady(j, fast) {
			continue
		}
		e, ok := q.estimateCompletion(j, fast)
		if !ok || e.group != slow.group {
			continue
		}
		end := float64(len(p.waiting)) * e.seconds
		if end >= bestEnd || !p.comparableBacklog(j, slow.work) {
			continue
		}
		candidate := q.memoryCandidate(j, fast)
		if q.launchCapacityBlocked(candidate, fast) || q.workerRAMGate(candidate, fast) != nil ||
			q.preparationActive(fast) || q.preparationStopping(fast) ||
			!q.poolCustodyAllows(candidate, fast) ||
			q.capReasonFor(candidate, fast) != "" {
			continue
		}
		room := fast
		slotFree := q.batchSlotFree(fast, candidate)
		if !slotFree || !fits(candidate, fast, fast.FreeMemoryGB) {
			release := p.releases[fast.ID]
			end += release.in
			if release.job == nil || end >= bestEnd {
				continue
			}
			if !slotFree && !q.workerBatchIdleExcept(fast, &HostRelease{Job: release.job.ID, Machine: fast.ID, Session: fast.Session, Attempt: release.job.Attempt}) {
				continue
			}
			key, reason := q.workObservationKey(release.job)
			if reason != "" || key == nil || *key != *release.job.Timing.Key {
				continue
			}
			if _, ok := q.lastAcceptedSeconds(key); !ok {
				continue
			}
			projected := q.drainProjection(fast, []*Job{release.job})
			room = &projected
		}
		if fits(candidate, room, room.FreeMemoryGB) &&
			q.hostBackfillAllows(candidate, room, q.hostBackfillFences(scan)[hostID(fast)]) {
			best, bestEnd = fast, end
		}
	}
	return best
}

// Known durations precede unknowns; incomparable kinds of work have a stable
// order. When both are unknown the caller executes its original comparator.
func (q *Queue) completionBetter(j *Job, a, b *Machine, stableTie ...bool) (better, known bool) {
	ae, aok := q.estimateCompletion(j, a)
	be, bok := q.estimateCompletion(j, b)
	if aok != bok {
		return aok, true
	}
	if !aok {
		return false, false
	}
	if ae.group != be.group {
		return ae.group < be.group, true
	}
	if ae.seconds != be.seconds {
		return ae.seconds < be.seconds, true
	}
	return (len(stableTie) == 0 || stableTie[0]) && q.deviceUseTieBetter(a, b, targetKey(a) < targetKey(b)), true
}

// Keep existing useful-victim selection and disruption costs authoritative.
// Only otherwise equivalent release plans use completion as their tie-break.
func (q *Queue) completionPlanBetter(j *Job, p, other *placementPlan, scans ...*placementScan) bool {
	if other != nil && p.poolRecheck != other.poolRecheck {
		return !p.poolRecheck
	}
	if other != nil && p.stops == other.stops && p.cost == other.cost {
		// Preserve call affinity before host value: use an existing engine
		// instead of leaving a call waiting for a cheaper host to load it.
		if c := q.calls[j.ID]; c != nil {
			if a, b := q.modelReadiness(c.Model, p.machine), q.modelReadiness(c.Model, other.machine); a != b {
				return a < b
			}
		}
		if value, _ := q.valueCompare(p.machine, other.machine, scans...); value != 0 {
			return value < 0
		}
		if better, known := q.completionBetter(j, p.machine, other.machine); known {
			return better
		}
	}
	return p.cheaper(other)
}

func (q *Queue) capabilityPlacementBetter(j *Job, a, b *Machine, scans ...*placementScan) bool {
	return q.capabilityPlacementBetterWithTie(j, a, b, true, scans...)
}

func (q *Queue) capabilityPlacementBetterWithTie(j *Job, a, b *Machine, allowTie bool, scans ...*placementScan) bool {
	value, knownValue := q.valueCompare(a, b, scans...)
	if value != 0 {
		return value < 0
	}
	if j.Spec.Needs.cpuOnly() && (a.GPU == "cpu") != (b.GPU == "cpu") {
		return a.GPU == "cpu"
	}
	if a.batchTenants != b.batchTenants {
		return a.batchTenants < b.batchTenants // An idle card beats sharing a busy one (#538).
	}
	if better, known := q.completionBetter(j, a, b, allowTie); known {
		return better
	}
	if knownValue {
		return allowTie && q.deviceUseTieBetter(a, b, targetKey(a) < targetKey(b))
	}
	return false
}
