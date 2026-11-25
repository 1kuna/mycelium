package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

// IdleReport integrates one state per physical host. Durations are clipped to
// the query window and summed before conversion to machine-hours.
type IdleReport struct {
	Since          time.Time                      `json:"since"`
	Through        time.Time                      `json:"through"`
	MachineHours   float64                        `json:"idle_machine_hours_while_runnable_work_waited"`
	ByMachineClass map[string]*IdleClassHours     `json:"by_machine_class"`
	ByReason       map[AdmissionPredicate]float64 `json:"by_refusal_reason"`
	Intervals      []HostIdleInterval             `json:"intervals,omitempty"`
}

type IdleClassHours struct {
	States   map[string]float64             `json:"state_machine_hours"`
	ByReason map[AdmissionPredicate]float64 `json:"by_refusal_reason"`
}

type HostIdleInterval struct {
	Host  string    `json:"host"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	HostIdleEvidence
}

type HostIdleEvidence struct {
	Class          string             `json:"machine_class"`
	State          string             `json:"state"`
	Reason         AdmissionPredicate `json:"refusal_reason,omitempty"`
	Online         *bool              `json:"online,omitempty"`
	Maintenance    string             `json:"maintenance,omitempty"`
	Leaving        bool               `json:"leaving,omitempty"`
	InFlight       *int               `json:"in_flight,omitempty"`
	ReleaseCustody *int               `json:"release_custody,omitempty"`
	ReadyDemand    *int               `json:"dependency_ready_demand,omitempty"`
	Targets        []IdleTarget       `json:"targets,omitempty"`
}

type IdleTarget struct {
	Machine      string       `json:"machine"`
	Session      string       `json:"session"`
	Device       string       `json:"device,omitempty"`
	Platform     string       `json:"platform"`
	FreeCores    *int         `json:"free_cores,omitempty"`
	FreeHostGB   *float64     `json:"free_host_gb,omitempty"`
	FreeDeviceGB *float64     `json:"free_device_gb,omitempty"`
	Demand       []IdleDemand `json:"demand,omitempty"`
}

type IdleDemand struct {
	Job            string        `json:"job"`
	Attempt        int           `json:"attempt"`
	Piece          int           `json:"piece,omitempty"`
	ConsumerPaused bool          `json:"consumer_paused,omitempty"`
	Compatibility  string        `json:"compatibility"`
	Qualification  string        `json:"qualification"`
	NeedsDevice    bool          `json:"needs_device,omitempty"`
	Decision       *IdleDecision `json:"decision,omitempty"`
}

type IdleDecision struct {
	At        time.Time          `json:"at"`
	ExpiresAt time.Time          `json:"expires_at"`
	Outcome   AdmissionOutcome   `json:"outcome"`
	Stage     AdmissionStage     `json:"stage"`
	Predicate AdmissionPredicate `json:"predicate"`
	HoldKind  string             `json:"hold_kind,omitempty"`
}

const (
	idleWorking  = "working"
	idleNoDemand = "idle_no_compatible_ready_demand"
	idleRefused  = "idle_compatible_demand_refused"
	idleHold     = "intentional_hold"
	idleUnknown  = "unknown"
)

type idleCapture struct {
	index    *publishedReadIndex
	trace    *admissionTrace
	revision uint64
}

type idleObservation struct {
	HostIdleEvidence
	validUntil time.Time
}

type idleHistory struct {
	captureMu sync.Mutex
	latest    uint64
	pending   idleCapture
	done      chan struct{}
	stopped   bool

	mu       sync.Mutex
	previous map[string]idleObservation
	at       time.Time
	revision uint64
	open     map[string]idleStored
	closed   []idleStored
	dirty    map[string]bool
	bytes    uint64
	allow    uint64
	measured bool

	db       *sql.DB // Evidence only. Never shares the control writer.
	flushMu  sync.Mutex
	retained uint64
	loaded   bool
}

// A replaceable mailbox bounds speculative projection under pressure. A
// displaced commit is detected by revision and produces an unknown span.
func (q *Queue) offerIdleCapture(c idleCapture) {
	h := &q.idleHistory
	h.captureMu.Lock()
	if h.stopped || c.revision <= h.latest {
		h.captureMu.Unlock()
		return
	}
	h.latest, h.pending = c.revision, c
	done := h.done
	if done == nil {
		done = make(chan struct{})
		h.done = done
		// Deterministic queue fixtures use the synchronous admission evidence
		// option for both projections. Fleet check-ins do neither build inline.
		if q.admissionWaitBuild.synchronous {
			h.captureMu.Unlock()
			h.build()
			return
		}
		go h.build()
	}
	h.captureMu.Unlock()
	if q.admissionWaitBuild.synchronous {
		<-done
	}
}

func (h *idleHistory) build() {
	for {
		h.captureMu.Lock()
		c := h.pending
		h.pending = idleCapture{}
		if c.index == nil {
			close(h.done)
			h.done = nil
			h.captureMu.Unlock()
			return
		}
		h.captureMu.Unlock()

		at := c.index.capture.source.clock.Now()
		next, err := projectIdleCapture(c, at, h.previous)
		h.mu.Lock()
		h.observe(c.revision, at, next, err == nil)
		h.mu.Unlock()
	}
}

func projectIdleCapture(c idleCapture, at time.Time, previous map[string]idleObservation) (map[string]idleObservation, error) {
	source, err := c.index.capture.detachedQueue(context.Background(), at, nil, func(g *readGeneration) (state, *queueRecords, error) {
		s, err := g.controlState()
		if err != nil {
			return state{}, nil, err
		}
		s.Jobs = map[string]*Job{}
		readWalk(g.root, true, func(key string, record *readRecord) {
			if err != nil || !strings.HasPrefix(key, "jobs/") {
				return
			}
			var value any
			value, err = record.decode(key)
			if err == nil {
				job := value.(*Job)
				s.Jobs[job.ID] = job
			}
		})
		return s, nil, err
	})
	if err != nil {
		return nil, err
	}
	return source.idleObservations(c.trace, at, previous), nil
}

func (q *Queue) idleObservations(trace *admissionTrace, now time.Time, previous map[string]idleObservation) map[string]idleObservation {
	hosts := map[string][]*Machine{}
	inFlight, releases := map[string]int{}, map[string]int{}
	var demand []*Job
	for _, m := range q.s.Machines {
		if m.Replacement == nil {
			host := hostID(m)
			hosts[host] = append(hosts[host], m)
			inFlight[host] += q.untrackedPreparations(m)
		}
	}
	for _, j := range q.liveJobs() {
		if batchReservesCapacity(j) {
			inFlight[q.executionHost(j.Machine)]++
		}
		if (j.State == "queued" || j.State == "paused" && j.Pause != nil) && !cancelled(j) &&
			dependencyReady(j) && !expired(j.Spec.GiveUpAt, now) &&
			(!j.Spec.childFanOut() || j.FanOut == nil || j.FanOut.Next < len(j.Spec.FanOut.Pieces)) {
			demand = append(demand, j)
		}
	}
	for _, c := range q.calls {
		if c.Error != "" {
			continue
		}
		if c.Machine != "" {
			inFlight[q.executionHost(c.Machine)]++
		} else {
			j := callJob(c)
			j.State = "queued"
			demand = append(demand, j)
		}
	}
	for _, slot := range q.s.ModelSlots {
		host := q.executionHost(slot.Machine)
		if slot.State == "loading" {
			inFlight[host]++
		}
		if slot.State != "failed" {
			releases[host]++
		}
	}
	for _, plan := range q.s.PreparationPlans {
		inFlight[q.executionHost(plan.Machine)]++
	}
	for _, release := range q.s.HostReleases {
		releases[release.Host]++
	}
	slices.SortFunc(demand, func(a, b *Job) int { return strings.Compare(a.ID, b.ID) })
	ready := 0
	for _, j := range demand {
		if j.State == "queued" {
			ready++
		}
	}

	result := make(map[string]idleObservation, len(hosts))
	for host, members := range hosts {
		slices.SortFunc(members, func(a, b *Machine) int { return strings.Compare(a.ID, b.ID) })
		busy, custody := inFlight[host], releases[host]
		e := idleObservation{HostIdleEvidence: HostIdleEvidence{
			Class: idleMachineClass(members), Maintenance: q.s.HostMaintenance[host].Mode,
			ReadyDemand: &ready, InFlight: &busy, ReleaseCustody: &custody,
		}}
		anyFresh, allObserved, allLeaving := false, true, true
		for _, m := range members {
			until := m.LastSeen.Add(q.machineLossTimeout(m.ID))
			fresh := !q.unreported[m.ID] && now.Before(until) && q.workerFresh(m, now)
			anyFresh = anyFresh || fresh
			allObserved = allObserved && (fresh || m.Gone && !q.unreported[m.ID])
			allLeaving = allLeaving && (m.Leaving || m.Gone)
			if fresh {
				e.expire(until)
			}
			for _, m := range q.placementTargets(m) {
				e.Targets = append(e.Targets, q.idleTarget(m, fresh, demand, trace))
			}
		}
		slices.SortFunc(e.Targets, func(a, b IdleTarget) int {
			if a.Machine != b.Machine {
				return strings.Compare(a.Machine, b.Machine)
			}
			return strings.Compare(a.Device, b.Device)
		})
		e.Leaving = anyFresh && allLeaving
		if anyFresh || allObserved {
			e.Online = &anyFresh
		}
		e.keepDecisions(previous[host], trace, now)
		e.classify(allObserved, anyFresh)
		result[host] = e
	}
	return result
}

func idleMachineClass(members []*Machine) string {
	os, arch := "", ""
	var platforms []string
	for _, m := range members {
		p := machineTarget(m)
		if p.OS != "" && (os == "" || p.OS < os) {
			os, arch = p.OS, p.Arch
		}
		if p.Platform != "" && p.Platform != "cpu" {
			platforms = append(platforms, p.Platform)
		}
		for _, device := range m.Devices {
			if device.Platform != "" && device.Platform != "cpu" {
				platforms = append(platforms, device.Platform)
			}
		}
	}
	slices.Sort(platforms)
	platforms = slices.Compact(platforms)
	if len(platforms) == 0 {
		platforms = []string{"cpu"}
	}
	return MachineKind{OS: os, Arch: arch, Platform: strings.Join(platforms, "+")}.String()
}

func (q *Queue) idleTarget(m *Machine, fresh bool, demand []*Job, trace *admissionTrace) IdleTarget {
	row := IdleTarget{Machine: m.ID, Session: m.Session, Device: m.Device, Platform: m.GPU}
	if fresh && m.EffectiveCPUCores > 0 && m.CPUCapacityReason == "" {
		row.FreeCores = &m.FreeCPUCores
	}
	if fresh && m.HostMemoryMeasured && m.HostMemoryError == "" {
		row.FreeHostGB = &m.FreeHostMemoryGB
	}
	if fresh && m.GPU != "cpu" && m.MemoryMeasured && m.MemoryError == "" {
		row.FreeDeviceGB = &m.FreeMemoryGB
	}
	for _, j := range demand {
		d := IdleDemand{Job: j.ID, Attempt: j.Attempt, ConsumerPaused: j.State == "paused"}
		d.NeedsDevice = m.GPU != "cpu" && !j.Spec.Needs.cpuOnly()
		if j.FanOut != nil {
			d.Piece = j.FanOut.Next
		}
		d.Compatibility, d.Qualification = q.idleCompatibility(j, m, fresh)
		if trace != nil && trace.machine == m.ID && trace.session == m.Session {
			if !d.ConsumerPaused {
				decision, ok := trace.jobs[j.ID][targetKey(m)]
				if !ok && !j.Queued.After(trace.at) && !j.WaitingSince.After(trace.at) {
					decision = trace.skippedDecision(m)
				}
				if !decision.At.IsZero() {
					d.Decision = idleDecision(decision, trace.period)
				}
			}
		}
		row.Demand = append(row.Demand, d)
	}
	// Keep a real selection even though the post-commit attempt is no longer
	// queued. Its assignment already accounts for the occupied host.
	if trace != nil && trace.machine == m.ID && trace.session == m.Session {
		for id, targets := range trace.jobs {
			d, ok := targets[targetKey(m)]
			if !ok || d.Outcome != AdmissionSelected {
				continue
			}
			if j := q.s.Jobs[id]; j != nil && j.State == "running" {
				row.Demand = append(row.Demand, IdleDemand{Job: id, Attempt: j.Attempt,
					Compatibility: "compatible", Qualification: "accepted", Decision: idleDecision(d, trace.period)})
			}
		}
	}
	slices.SortFunc(row.Demand, func(a, b IdleDemand) int { return strings.Compare(a.Job, b.Job) })
	return row
}

func (q *Queue) idleCompatibility(j *Job, m *Machine, fresh bool) (string, string) {
	const compatible, incompatible, unknown = "compatible", "incompatible", "unknown"
	if !q.jobAllowsMachine(j, m.ID) || j.Spec.Needs.cpuOnly() && m.Devices != nil && m.GPU != "cpu" {
		return incompatible, ""
	}
	if !fresh {
		if m.Gone {
			return incompatible, ""
		}
		return unknown, unknown
	}
	if j.State == "paused" {
		copy := *j
		copy.State = "queued"
		j = &copy
	}
	if j.Spec.childFanOut() {
		j = q.fanOutTemplate(j)
		if j == nil {
			return unknown, unknown
		}
	}
	if !j.Spec.Needs.cpuOnly() && inventoryWaitingReason(q.s.Machines[m.ID]) != "" {
		return unknown, unknown
	}
	matched, unqualified := false, false
	for _, candidate := range extensionJobs(j, m, false) {
		if !memorySizingTargetFits(candidate, m) || !groupFits(candidate, m) ||
			!toolsFit(candidate, m) || !nativeNetworkFits(candidate, m) || !logPolicyFits(candidate.Spec.Logs, m) {
			continue
		}
		matched = true
		if candidate.Spec.extensionJob() && !extensionCanRun(candidate, m) || q.variantParityHold(candidate, m) != nil {
			unqualified = true
			continue
		}
		if model, ok := q.model(candidate.Spec.Needs.Model); ok &&
			(!logPolicyFits(model.Logs, m) || !modelFenceAllows(model, m) || !q.projectAllows(model.Project, m.ID)) {
			continue
		}
		if !environmentCanRun(candidate, m) {
			continue
		}
		if _, _, known := jobCapabilityCapacity(candidate, m); !known {
			return unknown, "accepted"
		}
		if capabilityFits(candidate, m) && (candidate.Spec.Needs.cpuOnly() || m.TotalMemoryGB >= candidate.Spec.Needs.MemoryGB) {
			return compatible, "accepted"
		}
	}
	if unqualified {
		return compatible, "unqualified"
	}
	if matched {
		return incompatible, "accepted"
	}
	return incompatible, ""
}

func idleDecision(d AdmissionDecision, period time.Duration) *IdleDecision {
	r := &IdleDecision{At: d.At, ExpiresAt: d.At.Add(period), Outcome: d.Outcome, Stage: d.Stage, Predicate: d.Predicate}
	if d.Hold != nil {
		r.HoldKind = d.Hold.Kind
	}
	return r
}

func (e *idleObservation) expire(at time.Time) {
	if e.validUntil.IsZero() || at.Before(e.validUntil) {
		e.validUntil = at
	}
}

// Unrelated commits may carry the latest real decision within its measured
// opportunity only. Changed resources, demand or worker session invalidate it.
func (e *idleObservation) keepDecisions(previous idleObservation, trace *admissionTrace, now time.Time) {
	if e.Maintenance != previous.Maintenance || !reflect.DeepEqual(e.ReadyDemand, previous.ReadyDemand) ||
		!reflect.DeepEqual(e.InFlight, previous.InFlight) || !reflect.DeepEqual(e.ReleaseCustody, previous.ReleaseCustody) {
		return
	}
	for i := range e.Targets {
		row := &e.Targets[i]
		if i >= len(previous.Targets) || trace != nil && trace.machine == row.Machine {
			continue
		}
		old := previous.Targets[i]
		demand, oldDemand := row.Demand, old.Demand
		probe := *row
		probe.Demand, old.Demand = nil, nil
		if !reflect.DeepEqual(probe, old) || len(demand) != len(oldDemand) {
			continue
		}
		same := true
		for j, before := range oldDemand {
			before.Decision = nil
			if before != demand[j] {
				same = false
				break
			}
		}
		if same {
			for j, before := range oldDemand {
				if d := before.Decision; d != nil && d.ExpiresAt.After(now) {
					demand[j].Decision = d
				}
			}
		}
	}
}

func (e *idleObservation) classify(observed, online bool) {
	e.State = idleUnknown
	if !observed {
		return
	}
	if online && *e.InFlight > 0 {
		e.State = idleWorking
		return
	}
	if e.Leaving || e.Maintenance == "drained" || e.Maintenance == "disabled" {
		e.State = idleHold
		return
	}
	if !online || e.Maintenance == "" {
		return
	}

	var first *IdleDecision
	waiting, paused, unknown := false, false, false
	for _, target := range e.Targets {
		for _, d := range target.Demand {
			unknown = unknown || d.Compatibility == "unknown"
			if d.Compatibility != "compatible" || d.Qualification != "accepted" {
				continue
			}
			if d.ConsumerPaused {
				paused = true
				continue
			}
			waiting = true
			if target.FreeCores == nil || target.FreeHostGB == nil || d.NeedsDevice && target.FreeDeviceGB == nil {
				continue
			}
			if a := d.Decision; a != nil && a.ExpiresAt.After(a.At) && a.refused() &&
				(first == nil || a.At.Before(first.At)) {
				first = a
			}
		}
	}
	switch {
	case first != nil:
		e.State, e.Reason = idleRefused, first.Predicate
		e.expire(first.ExpiresAt)
	case waiting || unknown:
	case paused:
		e.State = idleHold
	default:
		e.State = idleNoDemand
	}
}

func (d IdleDecision) refused() bool {
	switch d.Predicate {
	case predicateCandidateUnavailable, predicateSelectionSkipped, predicateSelectionNotReached, predicateSelectionOrder:
		return false
	}
	return d.Outcome == AdmissionRejected || d.Outcome == AdmissionNotConsidered || d.Predicate == predicatePlacementElsewhere
}

func (h *idleHistory) observe(revision uint64, at time.Time, next map[string]idleObservation, valid bool) {
	if h.open == nil {
		h.open, h.dirty = map[string]idleStored{}, map[string]bool{}
	}
	for host, old := range h.previous {
		if !at.After(h.at) {
			break
		}
		stop := at
		if !valid || revision != h.revision+1 {
			stop = h.at
		} else if !old.validUntil.IsZero() && old.validUntil.Before(stop) {
			stop = old.validUntil
		}
		h.extend(host, h.at, stop, old.HostIdleEvidence)
		h.extend(host, maxTime(h.at, stop), at, HostIdleEvidence{Class: old.Class, State: idleUnknown})
		if _, present := next[host]; valid && !present {
			h.closeSpan(host)
			delete(h.open, host)
			delete(h.dirty, host)
		}
	}
	if !valid {
		next = maps.Clone(h.previous)
		for host, old := range next {
			next[host] = idleObservation{HostIdleEvidence: HostIdleEvidence{Class: old.Class, State: idleUnknown}}
		}
	}
	h.previous, h.at, h.revision = next, maxTime(h.at, at), revision
}

func (h *idleHistory) extend(host string, start, end time.Time, evidence HostIdleEvidence) {
	if !end.After(start) {
		return
	}
	old, ok := h.open[host]
	if ok && old.end == start.UnixNano() && reflect.DeepEqual(old.evidence, evidence) {
		old.end = end.UnixNano()
		h.open[host], h.dirty[host] = old, true
		return
	}
	data, err := json.Marshal(evidence)
	if err != nil {
		return
	}
	if ok {
		h.closeSpan(host)
	}
	h.open[host], h.dirty[host] = idleStored{host: host, start: start.UnixNano(), end: end.UnixNano(), evidence: evidence, data: data}, true
}

func (h *idleHistory) closeSpan(host string) {
	old := h.open[host]
	if h.dirty[host] && old.size() <= h.allow-min(h.allow, h.bytes) {
		// Persist the encoded payload. The decoded form is only needed to
		// extend a host's current interval across equivalent observations.
		old.evidence = HostIdleEvidence{Class: old.evidence.Class, State: old.evidence.State, Reason: old.evidence.Reason}
		h.closed = append(h.closed, old)
		h.bytes += old.size()
	}
}

func (r IdleReport) Lines() []string {
	lines := []string{fmt.Sprintf("idle machine-hours while runnable work waited: %.3f (%s to %s)", r.MachineHours, r.Since.Format(time.RFC3339), r.Through.Format(time.RFC3339))}
	for _, kind := range slices.Sorted(maps.Keys(r.ByMachineClass)) {
		class := r.ByMachineClass[kind]
		lines = append(lines, fmt.Sprintf("  %s: %.3f idle with refused work, %.3f unknown machine-hours", kind, class.States[idleRefused], class.States[idleUnknown]))
		for _, reason := range slices.Sorted(maps.Keys(class.ByReason)) {
			lines = append(lines, fmt.Sprintf("    %s: %.3f", reason, class.ByReason[reason]))
		}
	}
	return lines
}
