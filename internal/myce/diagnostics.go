package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os/exec"
	"syscall"
)

// Stable rejection codes and default messages. Submission validation returns
// the specific field rule; other wrapped causes stay local.
var rejectionMessages = map[string]string{
	"job_tools_invalid":                     "needs.tools requires executable basenames for native command prerequisites",
	"status_query_invalid":                  "status requires jobs=recent|active|all, limit=1..1000, an RFC3339 since, an existing before cursor and detail=full when requested",
	"recovery_invalid":                      "recovery requires valid metadata, declared outputs and matching execution identity",
	"recovery_stale":                        "recovery requires the exact retained failed execution",
	"recovery_conflict":                     "recovery key already identifies different evidence; use a new key to append another record",
	"recovery_verification_failed":          "recovery bytes are missing or do not match SHA-256 and size",
	"recovery_unknown":                      "recovered result not found",
	"native_workdir_containment":            "native isolation requires workdir beneath {work}; stage source with inputs or a profile",
	"queue_stopping":                        "queue is shutting down; retry the request",
	"shared_input_cache_capacity":           "declared shared inputs exceed every enabled worker cache capacity",
	"disk_headroom":                         "projected disk need plus free floor exceeds known disk capacity",
	"result_expired":                        "stored job files expired after the retention window",
	"no_qualified_target":                   "no known qualified target with the declared platform, environment and capacity; intent retained locally",
	"device_fault_xe":                       "device quarantined after an xe kernel driver fault; clear after recovery",
	"device_fault_i915":                     "device quarantined after an i915 kernel driver fault; clear after recovery",
	"device_fault_nvidia_xid":               "device quarantined after an NVIDIA Xid; clear after recovery",
	"release_unavailable":                   "queue administrator must configure a signed release and publisher URL before inviting",
	"release_approval_invalid":              "release approval requires a valid signed manifest, independently provisioned Ed25519 public key, HTTPS publisher URL and optional public CA certificates",
	"release_publisher_key_unavailable":     "queue publisher key is not provisioned; on the queue host restart with --release-manifest, --release-key and --release-url to provision trust locally",
	"release_publisher_key_mismatch":        "release key does not match the queue's provisioned publisher key; key rotation requires local queue-host provisioning",
	"release_approval_failed":               "queue could not persist release approval; retry after restoring queue storage",
	"release_downgrade_refused":             "queue approval requires a newer version or identical approval; use --allow-downgrade explicitly to replace an equal or older release",
	"invite_rejected":                       "invite expired, already used, or does not match this queue",
	"machine_renamed":                       "machine retired; use its replacement machine and device selector",
	"no_matching_device":                    "no matching device is available",
	"device_selector_unmatched":             "a configured device selector matches no card; check --device against the machine inventory",
	"device_probe_unavailable":              "accelerator inventory probe unavailable; CPU work remains available with qualified select-none visibility",
	"device_budget_unavailable":             "accelerator memory budget unavailable",
	"machine_worker_conflict":               "physical host already has a machine worker; run one machine worker per host",
	"gpu_count_unsupported":                 "an explicit GPU count above one is unsupported; a job's memory_gb decides its cards, and model servers use one card",
	"gpu_selector_invalid":                  "GPU count must be a positive integer",
	"device_identity_mismatch":              "device runtime mapping does not match saved identity",
	"cpu_accelerator_isolation_unavailable": "CPU execution withheld: accelerator select-none runtime visibility is unqualified; check installed CUDA/XPU runtimes",
	"request_method_invalid":                "method must be POST for /api/jobs",
	"request_invalid":                       "body must be one JSON object with known fields and valid field types",
	"authentication_required":               "authentication requires a valid fleet token",
	"job_fields_required":                   "name, project and run.command are required",
	"job_project_invalid":                   "project cannot contain slash",
	"job_class_invalid":                     "class must be valid and agree with legacy priority",
	"job_worker_data_unsupported":           "ephemeral worker data requires a supported non-resumable managed command and capable worker",
	"worker_data_cleanup_pending":           "ephemeral attempt release requires verified data cleanup",
	"job_execution_invalid":                 "run or execution must describe valid managed execution",
	"job_logs_invalid":                      "logs must be keep or none",
	"job_priority_invalid":                  "priority must be production, deadline or experiment",
	"job_deadline_invalid":                  "deadline priority requires deadline; start_by requires production and must not be in the past; start_window requires production and a duration greater than 0 and at most 1h",
	"job_files_invalid":                     "files must reference valid available input files in the queue file store",
	"job_program_invalid":                   "kind/program, inputs, outputs, args and size_hint must describe a valid extension job",
	"job_memory_invalid":                    "needs.memory_gb must be positive and needs.host_memory_gb finite and nonnegative",
	"job_platform_invalid":                  "needs.platform requires one valid platform string, list, or required/preferred/any mapping",
	"job_docker_retired":                    "managed Docker was retired; use a native environment and native sidecars (docs/services.md)",
	"job_environment_invalid":               "environment must identify one configured environment or supported lock_file recipe",
	"job_environment_platform_mismatch":     "job platform and environment recipes have no compatible execution platform",
	"job_placement_invalid":                 "allowed_machines must satisfy the project's placement policy",
	"job_profile_invalid":                   "profile_memory requires a profile and SHA-256 identity",
	"job_diagnostics_invalid":               "diagnostics must name known checks with valid settings",
	"job_capacity_invalid":                  "needs must fit the project's configured capacity",
	"request_stale":                         "attempt or input custody is stale; refresh before retrying",
	"artifact_unknown":                      "accepted artifact not found",
	"request_failed":                        "request failed",
	"file_release_invalid":                  "release requires a project, explicit job IDs from that project, an accepted reviewed plan and a configured file store",
	"extension_version_conflict":            "extension version is frozen; bump the version",
	"extension_invalid":                     "extension folder or schema is invalid; run extension check --static",
	"extension_unknown":                     "extension digest is not registered",
	"environment_unknown":                   "unknown environment",
	"job_id_ambiguous":                      "job ID prefix is ambiguous; use a longer ID",
	"policy_revision_conflict":              "policy revision changed",
	"policy_invalid":                        "policy or expected_revision is invalid; export the current policy before editing",
	"usage_invalid":                         "since must be a positive duration up to 2160h",
	"usage_unavailable":                     "usage could not be read",
	"status_unavailable":                    "queue status could not be read",
	"model_sync_invalid":                    "model sync request is invalid or unavailable",
	"model_pin_invalid":                     "model pin request is invalid",
	"model_unpin_invalid":                   "model unpin request is invalid",
	"job_submit_failed":                     "job submission could not be stored",
	"job_cancel_invalid":                    "job cancellation requires a known job or project and submission_key",
	"job_pause_invalid":                     "pause requires a reason and an optional future absolute time",
	"job_resume_invalid":                    "resume requires the current pause revision",
	"job_pause_conflict":                    "only queued jobs can pause; assigned or running attempts cannot be paused",
	"job_pause_revision_conflict":           "resume requires the current pause revision; parent-owned children resume with their parent",
	"job_logs_invalid_cursor":               "log cursor must be a nonnegative integer",
	"job_logs_unavailable":                  "job logs could not be read",
	"worker_checkin_invalid":                "worker checkin is invalid or stale",
	"worker_launch_invalid":                 "worker launch is invalid or stale",
	"worker_start_invalid":                  "worker start is invalid or stale",
	"worker_complete_invalid":               "worker completion or output manifest is invalid",
	"worker_environment_log_invalid":        "environment log report is invalid",
	"worker_log_invalid":                    "worker log report is invalid",
	"worker_leave_invalid":                  "worker is unknown or cannot leave",
	"checkpoint_invalid":                    "checkpoint publication is invalid or stale",
	"reservation_invalid":                   "reservation binding, target, owner, reason or maximum lifetime is invalid",
	"host_control_invalid":                  "host control request is invalid",
	"dev_clock_invalid":                     "clock advance must be positive, at least 1ns and at most 90d",
}

type rejectionError struct {
	code  string
	cause error
}

func (e *rejectionError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return rejectionMessages[e.code]
}
func (e *rejectionError) Unwrap() error     { return e.cause }
func reject(code string, cause error) error { return &rejectionError{code: code, cause: cause} }

// fieldRuleError is an explicit allowlist marker for Mycelium's field rules.
// Only pure validation/resolution rules may be marked, never OS or I/O errors.
// Keep the original error chain local; publish only the marked rule's message.
type fieldRuleError struct{ error }

func (e *fieldRuleError) Unwrap() error { return e.error }
func rejectFieldRule(code string, err error) error {
	return reject(code, &fieldRuleError{err})
}

// Keep the field-specific validation code; the fallback is only for uncoded rules.
func specRejection(err error) error {
	var rule *rejectionError
	if errors.As(err, &rule) {
		return rejectFieldRule(rule.code, err)
	}
	return rejectFieldRule("job_spec_invalid", err)
}

// RuntimeCause retains safe operation/class information, never daemon stderr,
// command arguments, environment values, endpoint URLs, or worker filesystem paths.
// Linkage explicitly retains install names from payload load commands.
type RuntimeCause struct {
	Finalization     *FinalizationDetail `json:"finalization,omitempty"`
	Linkage          *MachOLinkage       `json:"linkage,omitempty"`
	RSSLimit         *RSSLimitEvidence   `json:"rss_limit,omitempty"`
	MissingTools     []string            `json:"missing_tools,omitempty"`
	Host             string              `json:"host,omitempty"`
	Staging          *StagingCause       `json:"staging,omitempty"`
	PathDiagnostic   *PathDiagnostic     `json:"path_diagnostic,omitempty"`
	Admission        *AdmissionDetail    `json:"admission,omitempty"`
	PrelaunchRefused bool                `json:"prelaunch_refused,omitempty"`
	Operation        string              `json:"operation"`
	Class            string              `json:"class"`
	ExitCode         *int                `json:"exit_code,omitempty"`
	Predicate        string              `json:"predicate,omitempty"`
	Reason           string              `json:"reason,omitempty"`
}

func (c RuntimeCause) String() string {
	text := c.Operation + ": " + c.Class
	if d := safeFinalization(c.Finalization); d != nil {
		text += "; " + d.String()
	}
	if c.Class == machoLibraryMissing {
		if d := safeMachOLinkage(c.Linkage); d != nil {
			text += "; " + d.String()
		}
	}
	if c.Class == rssLimitExceeded {
		if e := safeRSSLimit(c.RSSLimit); e != nil {
			text += "; " + e.String()
		}
	}
	if c.Class == "tools_missing" {
		text += "; " + missingToolsReason(c.Host, c.MissingTools)
	}
	if c.Operation == "input_staging" {
		if d := safeStagingCause(c.Staging); d != nil {
			text += "; " + d.String()
		}
		if d := safeInputStagingPathDiagnostic(c.PathDiagnostic); d != nil {
			text += "; " + d.String()
			if d.Cause == "missing" {
				text += "; NAS paths are relative to worker --checkpoint-root; import existing bytes with profile local: or myce file put"
			}
		}
	}
	if c.ExitCode != nil && *c.ExitCode == 127 {
		text += "; command unavailable: check the runtime PATH and declare prerequisites in needs.tools"
	}
	if c.ExitCode != nil {
		text = fmt.Sprintf("%s (exit %d)", text, *c.ExitCode)
	}
	if reason := launchPredicateMessages[c.Predicate]; reason != "" {
		text += "; " + c.Predicate + ": " + reason
	}
	if d := safeAdmissionDetail(c.Admission); d != nil {
		available := "unavailable"
		if d.Available != nil {
			available = fmt.Sprintf("%g", *d.Available)
		}
		text += fmt.Sprintf("; %s required=%g available=%s unit=%s envelope_source=%q host_gb=%g device_gb=%g", d.Subcheck, d.Required, available, d.Unit, d.EnvelopeSource, d.EnvelopeHostGB, d.EnvelopeDeviceGB)
	}
	return text
}

type runtimeError struct {
	diagnostic RuntimeCause
	cause      error
}

func (e *runtimeError) Error() string { return e.diagnostic.String() }
func (e *runtimeError) Unwrap() error { return e.cause }
func runtimeErrorFor(operation string, err error) error {
	if err == nil {
		return nil
	}
	var typed *runtimeError
	if errors.As(err, &typed) {
		// The outer error may add raw transport/decoder text. Preserve that chain
		// for errors.Is/As, while rendering only the existing safe diagnostic.
		if _, direct := err.(*runtimeError); direct {
			return err
		}
		return &runtimeError{diagnostic: typed.diagnostic, cause: err}
	}
	c := RuntimeCause{Operation: operation, Class: "io_error"}
	if operation == "input_staging" {
		c.PathDiagnostic = safeInputStagingPathDiagnostic(pathDiagnostic(err))
		var staging *stagingError
		if errors.As(err, &staging) {
			c.Staging = safeStagingCause(&staging.detail)
		}
	}
	if errors.Is(err, errWorkerStartHandoff) {
		c.Operation = "start_handoff"
	} else if errors.Is(err, errWorkerStartAuthorization) {
		c.Operation = "start_authorization"
	}
	var exit *exec.ExitError
	var syntax *json.SyntaxError
	var mismatch *json.UnmarshalTypeError
	var rejection *rejectionError
	var response *HTTPError
	switch {
	case errors.Is(err, errWorkerControlUnreachable):
		c.Class = "control_unreachable"
	case (c.Operation == "start_authorization" || c.Operation == "start_handoff") && errors.As(err, &response) && response.Status == 409 && response.Code == "request_stale":
		c.Class = "admission_changed"
	case errors.As(err, &rejection) && rejection.code == cpuIsolationUnavailable:
		c.Class = cpuIsolationUnavailable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, exec.ErrWaitDelay), errors.Is(err, errFileTransferStalled), errors.Is(err, errCheckpointIOTimeout), errors.Is(err, syscall.ETIMEDOUT):
		c.Class = "timeout"
	case errors.Is(err, context.Canceled):
		c.Class = "cancelled"
	case errors.Is(err, fs.ErrPermission):
		c.Class = "permission_denied"
	case errors.Is(err, fs.ErrNotExist):
		c.Class = "not_found"
	case errors.Is(err, syscall.EDQUOT):
		c.Class = diskLimitExceeded
		if operation == "input_staging" {
			c.Class = "storage_full"
		}
	case errors.Is(err, syscall.ENOSPC):
		c.Class = "storage_full"
	case errors.Is(err, syscall.ECONNREFUSED):
		c.Class = "connection_refused"
	case errors.As(err, &exit):
		c.Class = "command_exit"
		code := exit.ExitCode()
		c.ExitCode = &code
	case errors.As(err, &syntax), errors.As(err, &mismatch):
		c.Class = "invalid_response"
	default:
		switch err.Error() {
		case errWorkerMemory.Error() + ": host allowance changed since queue admission":
			c.Class = "admission_changed"
		}
	}
	if c.Staging != nil {
		switch c.Staging.Error {
		case "timeout":
			c.Class = "timeout"
		case "cache_space", "cache_limit":
			c.Class = "resource_limit"
		case "blob_missing", "blob_expired":
			c.Class = "input_unavailable"
		case "invalid_source", "invalid_input":
			c.Class = "invalid_input"
		}
	}
	return &runtimeError{diagnostic: c, cause: err}
}
func runtimeDiagnostic(err error) *RuntimeCause {
	if err == nil {
		return nil
	}
	var typed *runtimeError
	errors.As(runtimeErrorFor("reconcile", err), &typed)
	c := typed.diagnostic
	return &c
}

// Reports are untrusted even when authenticated. Reconstruct both enum fields
// before showing them publicly; arbitrary worker text never becomes a cause.
func safeRuntimeCause(c *RuntimeCause) *RuntimeCause {
	if c == nil {
		return nil
	}
	operations := map[string]bool{"main": true, "group": true, "command": true, "process_group": true, "outputs": true, "launch": true, "readiness": true, "reconcile": true, "persist": true, "ps": true, "inspect": true, "create": true, "start": true, "update": true, "unpause": true, "kill": true, "stop": true, "logs": true, "network.ls": true, "network.inspect": true, "network.rm": true, "network.create": true, "network.disconnect": true, "retain_evidence": true, "observe_group": true, "remove_networks": true}
	classes := map[string]bool{cpuIsolationUnavailable: true, "admission_changed": true, "resource_limit": true, "stage_failed": true, "oom": true, "io_error": true, "timeout": true, "cancelled": true, "permission_denied": true, "not_found": true, "storage_full": true, "connection_refused": true, "command_exit": true, "invalid_response": true}
	operations["execution"], classes[wallTimeLimit] = true, true
	classes["tools_missing"] = true
	if c.Operation == "outputs" && safeFinalization(c.Finalization) != nil {
		classes[c.Finalization.Error] = true
	}
	classes["host_unavailable"] = c.Operation == "launch"
	classes[machoLibraryMissing] = true
	classes[diskLimitExceeded] = true
	classes[rssLimitExceeded] = true
	classes["input_unavailable"], classes["invalid_input"] = true, true
	classes["control_unreachable"] = c.Operation == "start_authorization" || c.Operation == "start_handoff"
	operations["start_authorization"] = true
	operations["start_handoff"] = true
	operations["input_staging"] = true
	if !operations[c.Operation] || !classes[c.Class] {
		if c.Operation != "launch" || (c.Class != launchRefused && c.Class != launchFailed && c.Class != environmentFailed) {
			return nil
		}
	}
	out := *c
	out.Finalization = nil
	if c.Operation == "outputs" {
		out.Finalization = safeFinalization(c.Finalization)
	}
	out.Linkage = nil
	if c.Operation == "launch" && c.Class == machoLibraryMissing {
		out.Linkage = safeMachOLinkage(c.Linkage)
	}
	out.RSSLimit = nil
	if c.Class == rssLimitExceeded {
		out.RSSLimit = safeRSSLimit(c.RSSLimit)
	}
	out.Host, out.MissingTools = "", nil
	if c.Class == "tools_missing" && validToolName(c.Host) {
		out.Host = c.Host
		for _, tool := range c.MissingTools {
			if validToolName(tool) {
				out.MissingTools = append(out.MissingTools, tool)
			}
		}
	}
	out.Staging = nil
	if c.Operation == "input_staging" {
		out.Staging = safeStagingCause(c.Staging)
	}
	out.PathDiagnostic = nil
	if c.Operation == "input_staging" {
		out.PathDiagnostic = safeInputStagingPathDiagnostic(c.PathDiagnostic)
	}
	out.Admission = nil
	out.PrelaunchRefused = c.PrelaunchRefused && c.Operation == "launch" && (c.Class == launchRefused || c.Class == "resource_limit") && launchPredicateMessages[c.Predicate] != ""
	if c.Operation == "launch" && c.Predicate == "memory_capacity" {
		out.Admission = safeAdmissionDetail(c.Admission)
	}
	out.Predicate, out.Reason = "", ""
	if reason := launchPredicateMessages[c.Predicate]; reason != "" {
		out.Predicate, out.Reason = c.Predicate, reason
	}
	if out.Class != "command_exit" {
		out.ExitCode = nil
	}
	return &out
}

func rejectIf(code string, err error) error {
	if err == nil {
		return nil
	}
	return reject(code, err)
}
