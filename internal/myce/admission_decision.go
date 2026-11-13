package myce

import (
	"slices"
	"strings"
	"time"
)

// AdmissionDecision is an observation of a real check-in branch, never a
// reconstructed diagnostic. Times belong to the injected queue clock. Predicate
// values are code-owned enums; worker text and job payloads are never copied.
// Hold is the refusing check's hold row (kind, owner, reason, clears, next)
// without its detail, which stays with the job's wait.
type AdmissionOutcome string
type AdmissionStage string
type AdmissionPredicate string

const (
	AdmissionNotConsidered AdmissionOutcome = "not_considered"
	AdmissionRejected      AdmissionOutcome = "rejected"
	AdmissionNotSelected   AdmissionOutcome = "not_selected"
	AdmissionCheckin       AdmissionStage   = "checkin"
	AdmissionEligibility   AdmissionStage   = "candidate_eligibility"
	AdmissionPlacement     AdmissionStage   = "placement"
)

type AdmissionDecision struct {
	At                 time.Time          `json:"at"`
	CheckinMachine     string             `json:"checkin_machine"`
	CheckinSession     string             `json:"checkin_session"`
	Device             string             `json:"device,omitempty"`
	Outcome            AdmissionOutcome   `json:"outcome"`
	Stage              AdmissionStage     `json:"stage"`
	Predicate          AdmissionPredicate `json:"predicate"`
	Hold               *Hold              `json:"hold,omitempty"`
	OpportunitySeconds float64            `json:"opportunity_seconds"`
}

type admissionTrace struct {
	machine, session string
	at               time.Time
	period           time.Duration
	jobs             map[string]map[string]AdmissionDecision
	selecting        bool
	active           *AdmissionDecision
	activeJob        string
}

// One advertised or measured send interval is the expected next opportunity,
// not a promise to start by a synthetic deadline. Unknown cadence stays unknown.
// Only compact facts are collected under q.mu; all projection/retention work
// uses the committed immutable capture after unlock.
func (q *Queue) beginAdmissionCheckin(m *Machine) {
	q.contactMu.Lock()
	period := time.Duration(0)
	if c := q.contacts[m.ID]; c != nil && c.session == m.Session {
		period = c.period
		if period <= 0 && c.cadence.Mean > 0 {
			period = time.Duration(c.cadence.Mean * float64(time.Second))
		}
	}
	q.contactMu.Unlock()
	if period <= 0 {
		return
	}
	now := q.clock.Now()
	var trace *admissionTrace
	for _, j := range q.liveJobs() {
		since := j.Queued
		if !j.WaitingSince.IsZero() {
			since = j.WaitingSince
		}
		if j.State != "queued" || since.IsZero() || now.Sub(since) < period {
			continue
		}
		if trace == nil {
			trace = &admissionTrace{machine: m.ID, session: m.Session, at: now, period: period, jobs: map[string]map[string]AdmissionDecision{}}
		}
		trace.jobs[j.ID] = map[string]AdmissionDecision{}
	}
	q.admissionTrace = trace
	if trace != nil {
		q.admissionWaitPending = true
		q.skipAdmissionSelection(m, predicateSelectionSkipped)
	}
}

func (q *Queue) skipAdmissionSelection(m *Machine, reason AdmissionPredicate) {
	t := q.admissionTrace
	if t == nil {
		return
	}
	// Inventory bounds the per-job endpoints; optional transitions use the
	// existing measured evidence-byte budget, rather than a sample-count cap.
	for _, target := range q.placementTargets(m) {
		for _, targets := range t.jobs {
			targets[targetKey(target)] = AdmissionDecision{At: t.at, CheckinMachine: t.machine, CheckinSession: t.session,
				Device: target.Device, Outcome: AdmissionNotConsidered, Stage: AdmissionCheckin, Predicate: reason, OpportunitySeconds: t.period.Seconds()}
		}
	}
}

func (q *Queue) beginAdmissionCandidate(j *Job, m *Machine) (*AdmissionDecision, string) {
	t := q.admissionTrace
	if t == nil || !t.selecting {
		return nil, ""
	}
	previous, previousJob := t.active, t.activeJob
	t.active, t.activeJob = nil, ""
	if t.machine == m.ID {
		if d, ok := t.jobs[j.ID][targetKey(m)]; ok {
			d.Outcome, d.Stage, d.Predicate, d.Hold = AdmissionNotSelected, AdmissionEligibility, predicateCandidateUnavailable, nil
			t.active, t.activeJob = &d, j.ID
		}
	}
	return previous, previousJob
}

func (q *Queue) endAdmissionCandidate(j *Job, m *Machine, candidate *Job, previous *AdmissionDecision, previousJob string) {
	t := q.admissionTrace
	if t == nil || !t.selecting {
		return
	}
	if t.active != nil {
		d := *t.active
		if candidate != nil {
			d.Outcome, d.Predicate, d.Hold = AdmissionNotSelected, predicateSelectionOrder, nil
		} else {
			d.Outcome = AdmissionRejected
		}
		t.jobs[j.ID][targetKey(m)] = d
	}
	t.active, t.activeJob = previous, previousJob
}

// Wrapping an already evaluated boolean preserves Go's exact short-circuit
// order, including the expensive predicates that must not run after a failure.
func (q *Queue) admissionPass(j *Job, m *Machine, pass bool, reason AdmissionPredicate) bool {
	if pass {
		return true
	}
	q.admissionRefused(j, m, reason, nil)
	return false
}

// admissionHold is admissionPass for a per-target check that names its hold:
// nil passes, and a refusal records the hold on the decision.
func (q *Queue) admissionHold(j *Job, m *Machine, h *Hold, reason AdmissionPredicate) bool {
	if h == nil {
		return true
	}
	q.admissionRefused(j, m, reason, h)
	return false
}

func (q *Queue) admissionRefused(j *Job, m *Machine, reason AdmissionPredicate, h *Hold) {
	t := q.admissionTrace
	if t != nil && t.active != nil && t.activeJob == j.ID && t.machine == m.ID && t.active.Device == m.Device && t.active.Predicate == predicateCandidateUnavailable {
		t.active.Predicate, t.active.Hold = reason, nil
		if h != nil {
			t.active.Hold = &Hold{Kind: h.Kind, Owner: h.Owner, Reason: h.Reason, Clears: h.Clears, Next: h.Next}
		}
	}
}

func (q *Queue) admissionGate(j *Job, m *Machine, pass bool, reason AdmissionPredicate) bool {
	if pass {
		return true
	}
	t := q.admissionTrace
	if t != nil && t.selecting && t.machine == m.ID {
		targets := t.jobs[j.ID]
		if d, ok := targets[targetKey(m)]; ok && d.Outcome == AdmissionNotSelected {
			d.Outcome, d.Stage, d.Predicate, d.Hold = AdmissionRejected, AdmissionPlacement, reason, nil
			targets[targetKey(m)] = d
		}
	}
	return false
}

func sameAdmissionDecision(a, b *AdmissionDecision) bool {
	if a == nil || b == nil {
		return a == b
	}
	x, y := *a, *b
	x.At, y.At = time.Time{}, time.Time{}
	if (x.Hold == nil) != (y.Hold == nil) || x.Hold != nil && *x.Hold != *y.Hold {
		return false
	}
	x.Hold, y.Hold = nil, nil
	return x == y
}

// Merge latest real per-target decisions with current diagnostic rows. Preserve
// other hosts' last observation for this exact attempt/piece/session; a status
// projection or Tick cannot invent a new check-in. Inventory removal expires
// its endpoint, bounding the record by the current candidate target set.
func (q *Queue) attachAdmissionDecisions(j *Job, snapshot *AdmissionWaitSnapshot, scan *placementScan) {
	decisions := map[string]AdmissionDecision{}
	if old := j.AdmissionWait; old != nil && old.Latest != nil && old.Latest.Attempt == snapshot.Attempt && old.Latest.NextPiece == snapshot.NextPiece {
		for _, target := range old.Latest.Targets {
			if d := target.AdmissionDecision; d != nil {
				if slices.ContainsFunc(scan.machines, func(m *Machine) bool {
					return targetKey(m) == target.Machine+"/"+target.Device && m.Session == target.Session
				}) {
					decisions[target.Machine+"/"+target.Device] = *d
				}
			}
		}
	}
	if q.admissionTrace != nil {
		for key, d := range q.admissionTrace.jobs[j.ID] {
			decisions[key] = d
		}
	}
	for key, d := range decisions {
		index := slices.IndexFunc(snapshot.Targets, func(t AdmissionWaitTarget) bool {
			return t.Machine+"/"+t.Device == key && t.Session == d.CheckinSession
		})
		if index < 0 {
			m := q.s.Machines[d.CheckinMachine]
			if m == nil || m.Session != d.CheckinSession {
				continue
			}
			snapshot.Targets = append(snapshot.Targets, AdmissionWaitTarget{Host: hostID(m), Machine: m.ID, Session: m.Session, Device: d.Device,
				Resolution: "admission_decision_only", Preemption: WaitPreemption{Source: "diagnostic_projection", Reason: "not_scheduler_turn"}})
			index = len(snapshot.Targets) - 1
		}
		snapshot.Targets[index].AdmissionDecision = &d
	}
	slices.SortFunc(snapshot.Targets, func(a, b AdmissionWaitTarget) int {
		return strings.Compare(a.Machine+"/"+a.Device, b.Machine+"/"+b.Device)
	})
}

func sameAdmissionDecisions(a, b map[string]map[string]AdmissionDecision) bool {
	if len(a) != len(b) {
		return false
	}
	for id, targets := range a {
		if len(targets) != len(b[id]) {
			return false
		}
		for key, left := range targets {
			right, ok := b[id][key]
			if !ok || !sameAdmissionDecision(&left, &right) {
				return false
			}
		}
	}
	return true
}

const (
	predicateExtensionCandidates  AdmissionPredicate = "extension_candidates"
	predicateCPUFallback          AdmissionPredicate = "cpu_fallback"
	predicateAllowedMachine       AdmissionPredicate = "allowed_machine"
	predicateAttemptReports       AdmissionPredicate = "attempt_reports"
	predicateAvailability         AdmissionPredicate = "availability"
	predicateBatchSlot            AdmissionPredicate = "batch_slot"
	predicateCPUCeiling           AdmissionPredicate = "cpu_ceiling"
	predicateCancelled            AdmissionPredicate = "cancelled"
	predicateCandidateUnavailable AdmissionPredicate = "candidate_unavailable"
	predicateCardGroup            AdmissionPredicate = "card_group"
	predicateCheckpoint           AdmissionPredicate = "checkpoint"
	predicateCheckpointTarget     AdmissionPredicate = "checkpoint_target"
	predicateCleanup              AdmissionPredicate = "cleanup"
	predicateDependency           AdmissionPredicate = "dependency"
	predicateDeviceAdmission      AdmissionPredicate = "device_admission"
	predicateDeviceFaultRetry     AdmissionPredicate = "device_fault_retry"
	predicateDiskHeadroom         AdmissionPredicate = "disk_headroom"
	predicateEnvironment          AdmissionPredicate = "environment"
	predicateEnvironmentFailure   AdmissionPredicate = "environment_failure"
	predicateExistingAssignment   AdmissionPredicate = "existing_assignment"
	predicateGroupDemand          AdmissionPredicate = "group_demand"
	predicateHostBackfill         AdmissionPredicate = "host_backfill"
	predicateInventory            AdmissionPredicate = "inventory"
	predicateLaunchCapacity       AdmissionPredicate = "launch_capacity"
	predicateLeaving              AdmissionPredicate = "leaving"
	predicateLogPolicy            AdmissionPredicate = "log_policy"
	predicateMachineCapacity      AdmissionPredicate = "machine_capacity"
	predicateMaintenance          AdmissionPredicate = "maintenance"
	predicateMemoryTarget         AdmissionPredicate = "memory_target"
	predicateModelCall            AdmissionPredicate = "model_call"
	predicateModelFence           AdmissionPredicate = "model_fence"
	predicateModelLogPolicy       AdmissionPredicate = "model_log_policy"
	predicateModelProject         AdmissionPredicate = "model_project"
	predicateNativeNetwork        AdmissionPredicate = "native_network"
	predicatePreemptionPlacement  AdmissionPredicate = "preemption_placement"
	predicatePreparationActive    AdmissionPredicate = "preparation_active"
	predicatePreparationStopping  AdmissionPredicate = "preparation_stopping"
	predicateProductionAdmission  AdmissionPredicate = "production_admission"
	predicateProjectCapacity      AdmissionPredicate = "project_capacity"
	predicateRecovering           AdmissionPredicate = "recovering"
	predicateReleasePending       AdmissionPredicate = "release_pending"
	predicateReleasePlan          AdmissionPredicate = "release_plan"
	predicateReservation          AdmissionPredicate = "reservation"
	predicateResources            AdmissionPredicate = "resources"
	predicateSelectionNotReached  AdmissionPredicate = "selection_not_reached"
	predicateSelectionOrder       AdmissionPredicate = "selection_order"
	predicateSelectionSkipped     AdmissionPredicate = "selection_skipped"
	predicateSharedHostBackoff    AdmissionPredicate = "shared_host_backoff"
	predicateStagingHostRetry     AdmissionPredicate = "staging_host_retry"
	predicateStartupRelease       AdmissionPredicate = "startup_release"
	predicateStorageRoot          AdmissionPredicate = "storage_root"
	predicateTools                AdmissionPredicate = "tools"
	predicateVariantParity        AdmissionPredicate = "variant_parity"
	predicateWorkerNotLive        AdmissionPredicate = "worker_not_live"
	predicateWorkerSuspect        AdmissionPredicate = "worker_suspect"
	predicateYieldPlacement       AdmissionPredicate = "yield_placement"
)
