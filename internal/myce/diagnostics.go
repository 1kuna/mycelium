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
	"queue_stopping":                        "queue is shutting down; retry the request",
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
	"machine_worker_conflict":               "physical host already has a machine worker; run one machine worker per host; see M4 install conversion",
	"gpu_count_unsupported":                 "only one GPU per attempt is supported; see #105",
	"gpu_selector_invalid":                  "GPU count must be a positive integer",
	"device_identity_mismatch":              "device runtime mapping does not match saved identity",
	"cpu_accelerator_isolation_unavailable": "CPU execution withheld: accelerator select-none runtime visibility is unqualified; check installed CUDA/XPU runtimes",
	"request_method_invalid":                "method must be POST for /api/jobs",
	"request_invalid":                       "body must be one JSON object with known fields and valid field types",
	"authentication_required":               "authentication requires a valid fleet token",
	"job_fields_required":                   "name, project and run.command are required",
	"job_project_invalid":                   "project cannot contain slash",
	"job_class_invalid":                     "class must be valid and agree with legacy priority",
	"job_execution_invalid":                 "run or execution must describe valid managed execution",
	"job_logs_invalid":                      "logs must be keep or none",
	"job_priority_invalid":                  "priority must be production, deadline or experiment",
	"job_deadline_invalid":                  "deadline priority requires deadline; start_by requires production and must not be in the past; start_window requires production and a duration greater than 0 and at most 1h",
	"job_files_invalid":                     "files must reference valid available input files in the queue file store",
	"job_program_invalid":                   "kind/program, inputs, outputs, args and size_hint must describe a valid extension job",
	"job_memory_invalid":                    "needs.memory_gb must be positive and needs.host_memory_gb finite and nonnegative",
	"job_platform_invalid":                  "needs.platform requires one valid platform string, list, or required/preferred/any mapping",
	"job_docker_invalid":                    "run Docker command, environment, network, sidecars and memory must satisfy managed Docker rules",
	"job_environment_invalid":               "environment must identify one configured environment or supported lock_file recipe",
	"job_environment_platform_mismatch":     "job platform and environment recipes have no compatible execution platform",
	"job_placement_invalid":                 "allowed_machines must satisfy the project's placement policy",
	"job_capacity_invalid":                  "needs must fit the project's configured capacity",
	"request_stale":                         "attempt or input custody is stale; refresh before retrying",
	"artifact_unknown":                      "accepted artifact not found",
	"request_failed":                        "request failed",
	"extension_version_conflict":            "extension version is frozen; bump the version",
	"extension_invalid":                     "extension folder or schema is invalid; run extension check --static",
	"extension_unknown":                     "extension digest is not registered",
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
// command arguments, environment values, endpoint URLs, or filesystem paths.
type RuntimeCause struct {
	Operation string `json:"operation"`
	Class     string `json:"class"`
	ExitCode  *int   `json:"exit_code,omitempty"`
}

func (c RuntimeCause) String() string {
	if c.ExitCode != nil {
		return fmt.Sprintf("%s: %s (exit %d)", c.Operation, c.Class, *c.ExitCode)
	}
	return c.Operation + ": " + c.Class
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
	if errors.Is(err, errWorkerStartHandoff) {
		c.Operation = "start_handoff"
	} else if errors.Is(err, errWorkerStartAuthorization) {
		c.Operation = "start_authorization"
	}
	var exit *exec.ExitError
	var syntax *json.SyntaxError
	var mismatch *json.UnmarshalTypeError
	var rejection *rejectionError
	switch {
	case errors.As(err, &rejection) && rejection.code == cpuIsolationUnavailable:
		c.Class = cpuIsolationUnavailable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, errFileTransferStalled):
		c.Class = "timeout"
	case errors.Is(err, context.Canceled):
		c.Class = "cancelled"
	case errors.Is(err, fs.ErrPermission):
		c.Class = "permission_denied"
	case errors.Is(err, fs.ErrNotExist):
		c.Class = "not_found"
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
		case "Docker inspection unconfirmed", "incomplete Docker inventory", "incomplete Docker network inventory":
			c.Class = "inventory_incomplete"
		case "Docker ownership mismatch", "Docker network ownership mismatch":
			c.Class = "ownership_mismatch"
		case errWorkerMemory.Error() + ": host allowance changed since queue admission":
			c.Class = "admission_changed"
		case "Docker restart policy unknown":
			c.Class = "restart_policy_unknown"
		case "legacy Docker assertion has unknown operation disposition":
			c.Class = "disposition_unknown"
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
	classes := map[string]bool{cpuIsolationUnavailable: true, "admission_changed": true, "resource_limit": true, "stage_failed": true, "oom": true, "io_error": true, "timeout": true, "cancelled": true, "permission_denied": true, "not_found": true, "storage_full": true, "connection_refused": true, "command_exit": true, "invalid_response": true, "inventory_incomplete": true, "ownership_mismatch": true, "restart_policy_unknown": true, "disposition_unknown": true}
	operations["start_authorization"] = true
	operations["start_handoff"] = true
	operations["input_staging"] = true
	if !operations[c.Operation] || !classes[c.Class] {
		if c.Operation != "launch" || (c.Class != launchRefused && c.Class != launchFailed && c.Class != environmentFailed) {
			return nil
		}
	}
	out := *c
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
