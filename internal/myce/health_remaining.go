package myce

import (
	"slices"
	"strings"
)

// observeCurrentHealth doesn't leave one condition per healthy retained job.
// Healthy history creates no keys; diagnostic events retain delivery IDs.
func (q *Queue) observeCurrentHealth(key string, bad bool, e HealthAlert) {
	if !bad {
		c, ok := q.s.Health.Conditions[key]
		if !ok {
			return
		}
		if len(c.Samples) == 0 {
			delete(q.s.Health.Conditions, key)
			return
		}
	}
	q.observeHealth(key, bad, 0, e)
}

// All inputs are already accepted in-memory reports, frozen intent and exact
// attempt outcomes. No filesystem probes, hashes, network or manifest reads.
// #834 owns host pressure, silence, mount health and collector diagnostics.
func (q *Queue) detectRemainingHealth() {
	previous := q.memoryPlanning
	q.memoryPlanning = newMemoryPlanningCache()
	defer func() { q.memoryPlanning = previous }()
	seen := map[string]bool{}
	toolNeeded := map[string]bool{}
	observe := func(key string, bad bool, e HealthAlert) { seen[key] = true; q.observeCurrentHealth(key, bad, e) }
	for _, m := range q.s.Machines {
		if m.Replacement != nil {
			continue
		}
		mode := q.s.HostMaintenance[hostID(m)].Mode
		intentional := m.Leaving || mode == "disabled" || mode == "drained" || mode == "draining"
		// Positive sensor errors belong to existing host-health alerts. Omitted
		// admission reports are configuration evidence, not a pressure diagnosis.
		hostReportNeeded := m.HostMemoryBudgetGB > 0 || strings.HasPrefix(m.MemoryEnforcement, "memory ceilings enforced")
		for _, j := range q.liveJobs() {
			if !terminal(j.State) && !cancelled(j) && q.jobAllowsMachine(j, m.ID) && executionTargetMatches(j.Spec, m) && (j.Spec.Needs.HostMemoryGB > 0 || j.Spec.Needs.Memory == "auto") {
				hostReportNeeded = true
			}
			if !terminal(j.State) && !cancelled(j) && q.jobAllowsMachine(j, m.ID) && len(j.Spec.Needs.Tools) > 0 {
				toolNeeded[m.ID] = true
			}
		}
		reasons := map[string]bool{
			"cpu_capacity_report_missing":  m.CapabilityProfile == nil || m.CapabilityProfile.LogicalCPUs <= 0,
			"host_memory_report_missing":   hostReportNeeded && !m.HostMemoryMeasured && m.HostMemoryError == "",
			"device_memory_report_missing": slices.ContainsFunc(m.Devices, func(d MachineDevice) bool { return !d.MemoryMeasured && d.Reason == "" }),
		}
		for reason, bad := range reasons {
			observe("reports/"+m.ID+"/"+reason, !intentional && bad, HealthAlert{Kind: "worker_reports_missing", Host: hostID(m), Reason: reason})
		}
		for _, r := range m.EnvironmentReports {
			key := "recipe/" + m.ID + "/" + r.Key
			observe(key, !intentional && r.State == "failed" && environmentFailureKind(r) == "deterministic", HealthAlert{Kind: "environment_recipe_failed", Host: hostID(m), Reason: "deterministic_recipe_failure"})
		}
		// Actual copy/staging refusals, never cache occupancy or mere coldness.
		transferRefused := m.FileTransferError != "" && m.FileTransferError != inputTransferRecoveringReason
		observe("cache/"+m.ID, !intentional && (m.CacheError != "" || transferRefused || slices.ContainsFunc(m.CopyReports, func(r CopyReport) bool { return r.State == "failed" })), HealthAlert{Kind: "staging_cache_refused", Host: hostID(m), Reason: "worker_transfer_refused"})
	}
	for _, j := range q.liveJobs() {
		if terminal(j.State) || cancelled(j) {
			continue
		}
		missing := slices.ContainsFunc(requirementReasonStatus(j), func(r RequirementReasonStatus) bool {
			return (r.Constraint == "host" || r.Constraint == "device") && r.ReasonSource == "missing"
		})
		observe("pin-reason/"+j.ID, missing, HealthAlert{Kind: "pin_reason_missing", Project: j.Spec.Project, JobID: j.ID, Reason: "requirement_reason_missing"})
		if j.State != "queued" {
			continue
		}
		known, fit, unknown := false, false, false
		q.evaluateTargets(j, func(f targetFit, candidate *Job, m *Machine) bool {
			if q.s.HostMaintenance[hostID(m)].Mode == "disabled" || m.Replacement != nil {
				return true
			}
			// Absent reports cannot prove a permanent capacity mismatch. Their own
			// detector owns the fault; gone/busy hosts retain permanent capacity.
			if !q.jobAllowsMachine(j, m.ID) {
				known = true
				return true
			}
			if len(candidate.Spec.Needs.Tools) > 0 {
				toolNeeded[m.ID] = true
			}
			if m.CapabilityProfile == nil || !m.HostMemoryMeasured || toolInventoryUnknown(candidate, m) {
				unknown = true
				return true
			}
			known = true
			fit = fit || f.failures == 0
			return !fit
		})
		observe("target/"+j.ID, known && !fit && !unknown, HealthAlert{Kind: "job_target_unsatisfied", Project: j.Spec.Project, JobID: j.ID, Reason: "no_enabled_target"})
	}
	for _, m := range q.s.Machines {
		if m.Replacement != nil {
			continue
		}
		mode := q.s.HostMaintenance[hostID(m)].Mode
		intentional := m.Leaving || mode == "disabled" || mode == "drained" || mode == "draining"
		observe("reports/"+m.ID+"/tool_inventory_report_missing", !intentional && toolNeeded[m.ID] && m.toolInventory == nil && m.ToolInventory == nil, HealthAlert{Kind: "worker_reports_missing", Host: hostID(m), Reason: "tool_inventory_report_missing"})
	}
	q.detectAttemptEndingHealth()
	if u := q.s.FleetUpgrade; u != nil {
		for _, h := range u.Hosts {
			// A helper's code is allow-listed; otherwise the queue chose the reason.
			reason := h.Failure
			if h.FailureCode != "" {
				reason = fleetFailureCode(h.FailureCode)
			}
			observe("update/"+h.Host, h.Failure != "" && h.Phase != "skipped", HealthAlert{Kind: "update_failed", Host: h.Host, Reason: reason})
		}
	}
	for key := range q.s.Health.Conditions {
		managed := strings.HasPrefix(key, "reports/") || strings.HasPrefix(key, "recipe/") || strings.HasPrefix(key, "cache/") || strings.HasPrefix(key, "pin-reason/") || strings.HasPrefix(key, "target/") || strings.HasPrefix(key, "update/")
		if managed && !seen[key] {
			q.observeCurrentHealth(key, false, HealthAlert{})
		}
	}
}

// The two latest non-planned endings are enough to prove repetition. Store
// replaceable evidence first; detect only after the queue transaction's final
// classifier has overwritten any provisional requeue record. Two is a protocol
// definition of repeated loss, not a timed fleet threshold.
type healthEnding struct {
	Memory  *AdmissionDetail `json:"memory,omitempty"`
	JobID   string           `json:"job_id"`
	Attempt int              `json:"attempt"`
	Session string           `json:"session"`
	Infra   bool             `json:"infra,omitempty"`
	Staged  bool             `json:"staged,omitempty"`
	Refused bool             `json:"refused,omitempty"`
}

func sameHealthEnding(a healthEnding, j *Job, o AttemptOutcome) bool {
	return a.JobID == j.ID && a.Attempt == o.Attempt && a.Session == o.Session
}

func (q *Queue) recordRemainingAttemptHealth(j *Job, o AttemptOutcome) {
	key := "attempt-endings/" + o.Machine
	c := q.s.Health.Conditions[key]
	c.Endings = slices.Clone(c.Endings)
	i := slices.IndexFunc(c.Endings, func(e healthEnding) bool { return sameHealthEnding(e, j, o) })
	planned := cancelled(j) || o.PoliteStop || o.OperatorStop || o.PriorityStop != nil || plannedStop(j) || historyPlannedStop(j)
	if planned {
		if i >= 0 {
			c.Endings = slices.Delete(c.Endings, i, i+1)
			q.s.Health.Conditions[key] = c
		}
		return
	}
	cause := o.ExecutionCause
	e := healthEnding{JobID: j.ID, Attempt: o.Attempt, Session: o.Session, Infra: o.Interrupted && !o.OutOfMemory && !launchRefusalOutcome(o), Staged: o.inputsStaged, Refused: cause != nil && cause.Operation == "input_staging" && cause.Class != "cancelled"}
	e.Memory = memoryRefusalEvidence(j, o)
	if i >= 0 {
		c.Endings[i] = e
	} else {
		c.Endings = append(c.Endings, e)
		if len(c.Endings) > 2 {
			c.Endings = c.Endings[len(c.Endings)-2:]
		}
	}
	if q.s.Health.Conditions == nil {
		q.s.Health.Conditions = map[string]healthCondition{}
	}
	q.s.Health.Conditions[key] = c
}

func (q *Queue) detectAttemptEndingHealth() {
	for key, c := range q.s.Health.Conditions {
		machine, ok := strings.CutPrefix(key, "attempt-endings/")
		if !ok {
			continue
		}
		host := machine
		if m := q.s.Machines[machine]; m != nil {
			host = hostID(m)
		}
		e := healthEnding{}
		if len(c.Endings) > 0 {
			e = c.Endings[len(c.Endings)-1]
		}
		repeated := len(c.Endings) == 2 && c.Endings[0].Infra && c.Endings[1].Infra
		q.observeCurrentHealth("infrastructure-streak/"+machine, repeated, HealthAlert{Kind: "infrastructure_loss_streak", Host: host, Machine: machine, JobID: e.JobID, Attempt: e.Attempt, Reason: "consecutive_infrastructure_losses"})
		// Missing staging facts cannot establish recovery. Use the latest positive
		// refusal or successful staging/start fact; unknown endings leave it alone.
		for i := len(c.Endings) - 1; i >= 0; i-- {
			ending := c.Endings[i]
			if ending.Refused || ending.Staged {
				q.observeCurrentHealth("attempt-staging/"+machine, ending.Refused, HealthAlert{Kind: "staging_cache_refused", Host: host, JobID: ending.JobID, Attempt: ending.Attempt, Reason: "input_staging_refused"})
				break
			}
		}
	}
}

func toolInventoryUnknown(j *Job, m *Machine) bool {
	if len(j.Spec.Needs.Tools) == 0 {
		return false
	}
	inventory := m.ToolInventory
	if m.toolInventory != nil {
		inventory = m.toolInventory.tools
	}
	return inventory == nil
}
