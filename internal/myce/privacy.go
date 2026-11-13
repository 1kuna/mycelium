package myce

import (
	"context"
	"sort"
	"strings"
)

// Public diagnostics use fixed messages: truncation cannot remove confidential
// text from an arbitrary upstream error.
func diagnostic(value, message string) string {
	if value == "" {
		return ""
	}
	return message
}

func deviceReasonDiagnostic(reason string) string {
	if deviceFaultReason(reason) {
		return reason
	}
	switch reason {
	case "cuda: device_probe_unavailable", "xpu: device_probe_unavailable", "cuda: device_probe_unavailable; xpu: device_probe_unavailable", "device_identity_mismatch", "device_selector_unmatched", "device_probe_unavailable", "device_budget_unavailable", "device_memory_unavailable":
		return reason
	default:
		return diagnostic(reason, "memory probe failed")
	}
}

func (q *Queue) publicSnapshot(contexts ...context.Context) (Snapshot, error) {
	return q.publicStatusSnapshot(nil, contexts...)
}

func (q *Queue) publicStatusSnapshot(options *statusOptions, contexts ...context.Context) (Snapshot, error) {
	s, err := q.statusSnapshotOptions(options, contexts...)
	for i := range s.LoadedModels {
		s.LoadedModels[i].EnvironmentRecipe = nil
		s.LoadedModels[i].Error = modelReadinessDiagnostic(s.LoadedModels[i].Error)
	}
	for _, m := range s.Machines {
		publicWorkerCollection(m)
		m.Disabled = diagnostic(m.Disabled, "disabled by operator")
		if m.AdmissionReason != "" {
			if strings.HasPrefix(m.AdmissionReason, "maximum simultaneous jobs reached") {
				m.AdmissionReason = "maximum simultaneous jobs reached"
			} else {
				m.AdmissionReason = safeAvailabilityReason(m.AdmissionReason)
			}
		}
		if a := machineAvailability(m); a != nil && !a.Available {
			a.Reason = safeAvailabilityReason(a.Reason)
		}
		for i := range m.ExtensionReports {
			m.ExtensionReports[i].Reason = extensionReportDiagnostic(m.ExtensionReports[i].Reason)
		}
		for i, name := range m.Environments {
			m.Environments[i] = statusEnvironmentName(name)
		}
		builds := map[string]float64{}
		for name, seconds := range m.EnvironmentBuildSeconds {
			builds[statusEnvironmentName(name)] = max(builds[statusEnvironmentName(name)], seconds)
		}
		m.EnvironmentBuildSeconds = builds
		sizes := map[string]uint64{}
		for name, bytes := range m.EnvironmentBytes {
			sizes[statusEnvironmentName(name)] = max(sizes[statusEnvironmentName(name)], bytes)
		}
		m.EnvironmentBytes = sizes
		for i := range m.EnvironmentReports {
			m.EnvironmentReports[i].Key = statusEnvironmentName(m.EnvironmentReports[i].Key)
		}
		m.MemoryEnforcement = memoryEnforcementDiagnostic(m.MemoryEnforcement)
		m.CPUIsolation = cpuIsolationDiagnostic(m.CPUIsolation)
		m.HostMemoryError = diagnostic(m.HostMemoryError, "host memory probe failed")
		m.InventoryReason = deviceReasonDiagnostic(m.InventoryReason)
		m.MemoryError = deviceReasonDiagnostic(m.MemoryError)
		for i := range m.Devices {
			m.Devices[i].Reason = deviceReasonDiagnostic(m.Devices[i].Reason)
		}
		if m.FileTransferError != inputTransferRecoveringReason {
			m.FileTransferError = diagnostic(m.FileTransferError, "file transfer pending; only published progress is available elsewhere")
		}
		m.FaultMonitorError = diagnostic(m.FaultMonitorError, "kernel log not readable")
		m.CacheError = diagnostic(m.CacheError, "cache operation failed")
		for i := range m.CopyReports {
			m.CopyReports[i].Error = diagnostic(m.CopyReports[i].Error, "model copy failed")
		}
		for i := range m.EnvironmentReports {
			m.EnvironmentReports[i].Error = diagnostic(m.EnvironmentReports[i].Error, "environment build failed")
			m.EnvironmentReports[i].Log = ""
			m.EnvironmentReports[i].PathDiagnostic = safeWorkerPathDiagnostic(m.EnvironmentReports[i].PathDiagnostic)
		}
	}
	return s, err
}

func statusEnvironmentName(name string) string {
	if strings.ContainsAny(name, "/\\") {
		return "required software"
	}
	return name
}

func safeAvailabilityReason(reason string) string {
	switch reason {
	case "host CPU thermal coverage unavailable", "waiting for 5 idle minutes", "on battery", "cooling down", "owner signals stale", "power source unavailable", "external power required", "owner activity unavailable", "heat sensing unavailable":
		return reason
	default:
		return "availability unknown"
	}
}

// Called on snapshot copies or a detached read view, never on persisted jobs.
func (q *Queue) statusEligibility(j *Job) {
	j.EligibleHosts, j.EligibleUnrestricted, j.EligibleUnknownMachines = nil, false, nil
	if terminal(j.State) {
		return
	}
	allowed := j.Spec.AllowedMachines
	if len(allowed) == 0 {
		allowed = q.s.Policies[j.Spec.Project].AllowedMachines
	}
	// This marker is open-ended eligibility for maintenance gates, not proof
	// that each machine passes every authored constraint. Exclusions must not
	// hide future non-excluded workers from older fail-closed gates.
	j.EligibleUnrestricted = len(allowed) == 0
	hosts := []string{}
	seen := map[string]bool{}
	addHost := func(host string) {
		if host != "" && !seen[host] {
			hosts = append(hosts, host)
			seen[host] = true
		}
	}
	if !j.EligibleUnrestricted {
		for _, m := range q.s.Machines {
			if q.jobAllowsMachine(j, m.ID) {
				addHost(hostID(m))
			}
		}
		unknown := map[string]bool{}
		for _, name := range allowed {
			if q.jobAllowsMachine(j, name) && q.s.Machines[name] == nil && !unknown[name] {
				j.EligibleUnknownMachines = append(j.EligibleUnknownMachines, name)
				unknown[name] = true
			}
		}
		sort.Strings(j.EligibleUnknownMachines)
	}
	// A policy change cannot release an existing execution or in-flight call.
	if j.State == "running" || j.State == "stopping" {
		if m := q.s.Machines[j.Machine]; m != nil {
			addHost(hostID(m))
		}
	}
	if !j.EligibleUnrestricted || len(hosts) > 0 {
		sort.Strings(hosts)
		j.EligibleHosts = &hosts
	}
}

func statusJobCopy(job *Job) Job {
	j := statusJobValue(job)
	return cloneJob(&j)
}

// Final redaction; nested fields remain borrowed. Observation projection already
// owns its detached inputs; other callers use statusJobCopy before publishing.
func statusJobValue(job *Job) Job {
	value := statusJobInputs(job)
	j := &value
	j.ExecutionCause = safeRuntimeCause(j.ExecutionCause)
	j.CleanupCause = safeRuntimeCause(j.CleanupCause)
	j.Finding = jobFinding(j, j.Reason)
	if strings.HasPrefix(j.Reason, "hypha waiting: ") {
		j.Reason = "hypha waiting: " + availabilityWords(strings.TrimPrefix(j.Reason, "hypha waiting: "))
	}
	if j.State == "failed" {
		switch {
		case strings.HasPrefix(j.Reason, "failed after 3 infrastructure losses:"):
			// Queue-owned exhaustion must precede the last attempt's diagnostic.
			// Never retain its suffix, which may contain private worker errors.
			j.Reason = "stopped after 3 infrastructure interruptions (Mycelium-side, not a command failure)"
			if j.ExecutionCause != nil {
				j.Reason += "; last: " + j.ExecutionCause.String()
			}
			j.Reason += "; next: check saved progress and resubmit"
		case j.Spec.ExtensionCheck != nil && strings.HasPrefix(j.Reason, "never started: "):
			j.Reason = "never started: " + extensionCheckAdmissionReason(strings.TrimPrefix(j.Reason, "never started: "))
		case j.Spec.ExtensionCheck != nil && j.Reason == "check execution timeout exceeded":
			// Fixed queue-owned execution timeout, with no worker error suffix.
		case j.ExecutionCause != nil && j.ExecutionCause.Operation == "launch" && j.ExecutionCause.Class == "host_unavailable" && strings.HasPrefix(j.Reason, "production cannot start: no eligible host is enabled;"):
			j.Reason = "production cannot start: no eligible host is enabled"
		case j.ExecutionCause != nil && j.ExecutionCause.Operation == "launch" && j.ExecutionCause.Class == "host_unavailable" && strings.HasPrefix(j.Reason, "production start deadline exceeded:"):
			j.Reason = "production start deadline exceeded"
		case j.ExecutionCause != nil && (j.ExecutionCause.Finalization != nil || j.ExecutionCause.Operation == "input_staging" || j.ExecutionCause.Class == machoLibraryMissing || j.ExecutionCause.Predicate != ""):
			j.Reason = "execution failed: " + j.ExecutionCause.String()
		case j.ExecutionCause == nil && j.CleanupCause != nil:
			j.Reason = "finalization failed: " + j.CleanupCause.String()
		case j.Reason == "command failed: "+execFormatMessage || j.Reason == "native check failed: "+execFormatMessage:
			// Fixed exec failure guidance contains no payload or machine paths.
		case j.ExecutionCause != nil && j.ExecutionCause.Class == cpuIsolationUnavailable:
			j.Reason = cpuIsolationUnavailable + ": " + rejectionMessages[cpuIsolationUnavailable]
		case j.Reason == "infrastructure_retries_exhausted", j.Reason == "out of memory after one retry on a larger hypha", j.Reason == "out of memory; previous capability unavailable":
			// Fixed scheduler diagnostics contain no payload or machine paths.
		case strings.HasPrefix(j.Reason, "production start deadline exceeded:"):
			j.Reason = "production start deadline exceeded"
		case strings.HasPrefix(j.Reason, "environment failed:"):
			j.Hold = newHold("deterministically_failed", j.Reason)
			// Authenticated job status deliberately includes the preparation
			// diagnostic, like the corresponding job log. Logs:none omits output.
		case strings.HasPrefix(j.Reason, "production cannot start: no eligible host is enabled;"):
			j.Reason = "production cannot start: no eligible host is enabled"
		default:
			j.Reason = ceilingDiagnostic(j.Reason, "execution failed")
		}
	}
	return *j
}

// A failed check keeps its admission category on the supported job API. Raw
// placement/preparation details can contain worker errors and private paths.
func extensionCheckAdmissionReason(reason string) string {
	switch {
	case strings.Contains(reason, "RAM"):
		return "waiting for host RAM"
	case strings.Contains(reason, "production hold"):
		return "waiting for production hold to end"
	case strings.Contains(reason, "maintenance"), strings.Contains(reason, "disabled by operator"):
		return "waiting for host maintenance to end"
	case strings.Contains(reason, "cleanup"), strings.Contains(reason, "release"), strings.Contains(reason, "work to stop"):
		return "waiting for resource release"
	case strings.Contains(reason, "environment"), strings.Contains(reason, "command start"):
		return "waiting for environment or command preparation"
	case strings.Contains(reason, "disk_headroom"), strings.Contains(reason, "disk space"):
		return "waiting for disk space"
	case strings.Contains(reason, "checkpoint"), strings.Contains(reason, "NAS"):
		return "waiting for checkpoint storage"
	case strings.Contains(reason, "worker"), strings.Contains(reason, "machine"):
		return "waiting for an available worker"
	default:
		return "waiting for admission or command preparation"
	}
}

// Omit execution payload from captures that need only identity and lifecycle
// diagnostics. Keep raw diagnostic inputs until the final redaction pass:
// sanitizing an already-sanitized reason can erase the established wording.
// The returned value borrows fields until the caller deep-copies it.
func statusJobInputs(job *Job) Job {
	j := *job
	// A compact capture omits the frozen maps but carries their markers.
	if j.SpecProjection == nil && j.RequirementReasons == nil {
		j.RequirementReasons = requirementReasonStatus(job)
	}
	placementStatus(&j)
	j.LedgerEntries = nil
	if j.Ledger == nil {
		j.Ledger = &JobLedgerInfo{}
	}
	j.Isolation = jobNativeIsolationStatus(job)
	j.Advanced = AdvancedStatus(&job.Spec)
	spec := Spec{Logs: j.Spec.Logs, AllowedMachines: j.Spec.AllowedMachines, ExcludedMachines: j.Spec.ExcludedMachines, Name: j.Spec.Name, Project: j.Spec.Project, Priority: j.Spec.Priority, Class: j.Spec.Class, Kind: j.Spec.Kind, Resumable: j.Spec.Resumable, Preemptible: j.Spec.Preemptible, Deadline: j.Spec.Deadline, StartBy: j.Spec.StartBy, StartWindow: j.Spec.StartWindow}
	if j.Spec.Program != nil {
		spec.Program = &Program{Type: j.Spec.Program.Type}
	}
	if j.Spec.ExtensionCheck != nil {
		spec.ExtensionCheck = &ExtensionCheckRequest{Work: j.Spec.ExtensionCheck.Work, Timeout: j.Spec.ExtensionCheck.Timeout}
	}
	spec.QualityTolerance = j.Spec.QualityTolerance
	spec.AuthoringOrigin = j.Spec.Origin()
	spec.Workload = j.Spec.Workload
	spec.Needs = j.Spec.Needs
	j.Spec = spec
	// This policy list is independent of submitted values, including on a
	// second redaction pass. Empty wire placeholders still omit original values.
	j.SpecProjection = &SpecProjection{Projected: true, Omitted: []string{
		"after", "args", "diagnostics", "disk", "environment", "execution",
		"expected_minutes", "extension", "extension_check.automatic",
		"extension_check.directory", "extension_check.reserve", "extension_digest",
		"fan_out", "files", "give_up_at", "inputs", "isolation",
		"output_delivery", "output_destination", "output_retention", "outputs", "prepare_source", "profile_memory",
		"program.args", "program.file", "reason", "require_target", "requirement_reasons", "resume_compatibility", "run",
		"shared_inputs", "size_hint", "submission_key", "targets",
	}}
	j.Extensions = nil
	j.ModelEnvironmentKey = ""
	j.ModelEnvironmentNeedsSource = false
	j.EnvironmentRecipes = nil
	j.SidecarEnvironmentRecipes = nil
	// Single-job replies restore this compact identity; lists stay small.
	j.EnvironmentLock = nil
	j.EnvironmentFailureLog = ""
	j.Checkpoint = ""
	return j
}
