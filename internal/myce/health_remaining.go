package myce

import (
	"fmt"
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
	seen := map[string]bool{}
	observe := func(key string, bad bool, e HealthAlert) { seen[key] = true; q.observeCurrentHealth(key, bad, e) }
	for _, m := range q.s.Machines {
		if m.Replacement != nil || q.migrationHistoricalMachine(m) {
			continue
		}
		mode := q.s.HostMaintenance[hostID(m)].Mode
		intentional := m.Leaving || mode == "disabled" || mode == "drained" || mode == "draining"
		// Positive sensor errors belong to existing host-health alerts. Omitted
		// admission reports are configuration evidence, not a pressure diagnosis.
		toolReportNeeded := false
		hostReportNeeded := m.HostMemoryBudgetGB > 0 || strings.HasPrefix(m.MemoryEnforcement, "memory ceilings enforced")
		for _, j := range q.liveJobs() {
			if !terminal(j.State) && !cancelled(j) && q.jobAllowsMachine(j, m.ID) && executionTargetMatches(j.Spec, m) && (j.Spec.Needs.HostMemoryGB > 0 || j.Spec.Needs.Memory == "auto") {
				hostReportNeeded = true
			}
			if !terminal(j.State) && !cancelled(j) && q.jobAllowsMachine(j, m.ID) && len(j.Spec.Needs.Tools) > 0 {
				toolReportNeeded = true
			}
		}
		reasons := map[string]bool{
			"tool_inventory_report_missing": toolReportNeeded && m.toolInventory == nil,
			"cpu_capacity_report_missing":   m.CapabilityProfile == nil || m.CapabilityProfile.LogicalCPUs <= 0,
			"host_memory_report_missing":    hostReportNeeded && !m.HostMemoryMeasured && m.HostMemoryError == "",
			"device_memory_report_missing":  slices.ContainsFunc(m.Devices, func(d MachineDevice) bool { return !d.MemoryMeasured && d.Reason == "" }),
		}
		for reason, bad := range reasons {
			observe("reports/"+m.ID+"/"+reason, !intentional && bad, HealthAlert{Kind: "worker_reports_missing", Host: hostID(m), Reason: reason})
		}
		for _, r := range m.EnvironmentReports {
			key := "recipe/" + m.ID + "/" + r.Key
			observe(key, !intentional && r.State == "failed" && environmentFailureKind(r) == "deterministic", HealthAlert{Kind: "environment_recipe_failed", Host: hostID(m), Reason: "deterministic_recipe_failure"})
		}
		// Actual copy/staging refusals, never cache occupancy or mere coldness.
		observe("cache/"+m.ID, !intentional && (m.CacheError != "" || m.FileTransferError != "" || slices.ContainsFunc(m.CopyReports, func(r CopyReport) bool { return r.State == "failed" })), HealthAlert{Kind: "staging_cache_refused", Host: hostID(m), Reason: "worker_transfer_refused"})
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
		q.evaluateTargets(j, func(f targetFit, _ *Job, m *Machine) bool {
			if q.s.HostMaintenance[hostID(m)].Mode == "disabled" || m.Replacement != nil || q.migrationHistoricalMachine(m) {
				return true
			}
			// Absent reports cannot prove a permanent capacity mismatch. Their own
			// detector owns the fault; gone/busy hosts retain permanent capacity.
			if !q.jobAllowsMachine(j, m.ID) {
				known = true
				return true
			}
			if m.CapabilityProfile == nil || !m.HostMemoryMeasured {
				unknown = true
				return true
			}
			known = true
			fit = fit || f.failures == 0
			return !fit
		})
		observe("target/"+j.ID, known && !fit && !unknown, HealthAlert{Kind: "job_target_unsatisfied", Project: j.Spec.Project, JobID: j.ID, Reason: "no_enabled_target"})
	}
	if u := q.s.FleetUpgrade; u != nil {
		for _, h := range u.Hosts {
			reason := h.FailureCode
			if reason == "runner_smoke_failed" {
				reason = "runner_smoke_failed"
			} else if reason != "" {
				reason = fleetFailureCode(reason)
			}
			observe("update/"+h.Host, h.Phase == "failed" || h.Failure != "", HealthAlert{Kind: "update_failed", Host: h.Host, Reason: reason})
		}
	}
	for key := range q.s.Health.Conditions {
		managed := strings.HasPrefix(key, "reports/") || strings.HasPrefix(key, "recipe/") || strings.HasPrefix(key, "cache/") || strings.HasPrefix(key, "pin-reason/") || strings.HasPrefix(key, "target/") || strings.HasPrefix(key, "update/")
		if managed && !seen[key] {
			q.observeCurrentHealth(key, false, HealthAlert{})
		}
	}
}

// A streak is consecutive exact executions, not elapsed minutes or heartbeat
// counts. Two losses is the definition of a repeated loss; one is already
// covered by the production-attempt alert. A successful or workload-caused
// ending breaks it. Planned stops neither count nor manufacture recovery.
func (q *Queue) recordRemainingAttemptHealth(j *Job, o AttemptOutcome) {
	duplicate := false
	for _, old := range j.AttemptHistory {
		if old.Attempt == o.Attempt && old.Machine == o.Machine && old.Session == o.Session {
			duplicate = true
			break
		}
	}
	if cancelled(j) || o.PoliteStop || o.OperatorStop || o.PriorityStop != nil || plannedStop(j) || historyPlannedStop(j) {
		return
	}
	host := o.Machine
	if m := q.s.Machines[o.Machine]; m != nil {
		host = hostID(m)
	}
	if !duplicate {
		key := "infrastructure-streak/" + o.Machine
		c := q.s.Health.Conditions[key]
		infra := o.Interrupted && !o.OutOfMemory
		if infra {
			c.Samples = append(slices.Clone(c.Samples), healthSample{ID: fmt.Sprintf("%s/%d/%s", j.ID, o.Attempt, o.Session), At: q.clock.Now()})
			if len(c.Samples) > 2 {
				c.Samples = c.Samples[len(c.Samples)-2:]
			}
		} else {
			c.Samples = nil
		}
		if infra || len(c.Samples) > 0 || !c.LastAlert.IsZero() {
			if q.s.Health.Conditions == nil {
				q.s.Health.Conditions = map[string]healthCondition{}
			}
			q.s.Health.Conditions[key] = c
		}
		q.observeCurrentHealth(key, len(c.Samples) >= 2, HealthAlert{Kind: "infrastructure_loss_streak", Host: host, JobID: j.ID, Attempt: o.Attempt, Reason: "consecutive_infrastructure_losses"})
	}
	staging := o.ExecutionCause
	refused := staging != nil && staging.Operation == "input_staging" && staging.Class != "cancelled"
	if !duplicate || refused {
		q.observeCurrentHealth("attempt-staging/"+o.Machine, refused, HealthAlert{Kind: "staging_cache_refused", Host: host, JobID: j.ID, Attempt: o.Attempt, Reason: "input_staging_refused"})
	}
}
