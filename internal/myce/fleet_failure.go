package myce

import (
	"encoding/json"
	"errors"
	"fmt"
)

// FleetProofIssue describes why a worker could not report its managed runtime
// proof. It is advisory and never establishes managed identity or readiness.
type FleetProofIssue string

func (issue FleetProofIssue) Error() string { return string(issue) }

const fleetProofIssueHeader = "X-Myce-Fleet-Proof-Issue"

const (
	fleetProofConfigMissing    FleetProofIssue = "installed_service_identity_unavailable"
	fleetProofImageOpen        FleetProofIssue = "running_image_open_failed"
	fleetProofImageStat        FleetProofIssue = "running_image_stat_failed"
	fleetProofImageFormat      FleetProofIssue = "running_image_format_invalid"
	fleetProofImageMapping     FleetProofIssue = "running_image_mapping_unavailable"
	fleetProofImageMismatch    FleetProofIssue = "running_image_mapping_mismatch"
	fleetProofImageHash        FleetProofIssue = "running_image_hash_failed"
	fleetProofImageChanged     FleetProofIssue = "running_image_changed"
	fleetProofReceiptInvalid   FleetProofIssue = "fleet_receipt_invalid"
	fleetProofSmokeCleanup     FleetProofIssue = "runner_smoke_cleanup_failed"
	fleetProofSmokeFailed      FleetProofIssue = "runner_smoke_failed"
	fleetProofManagerUnhealthy FleetProofIssue = "service_manager_unhealthy"
	fleetProofManagerPID       FleetProofIssue = "service_manager_pid_mismatch"
	fleetProofCollectionFailed FleetProofIssue = "proof_collection_failed"
	fleetProofPending          FleetProofIssue = "proof_pending"
	fleetProofSessionMissing   FleetProofIssue = "proof_session_missing"
	fleetProofSessionMismatch  FleetProofIssue = "proof_session_mismatch"
	fleetProofTimeMissing      FleetProofIssue = "proof_observation_time_missing"
	fleetProofTimeFuture       FleetProofIssue = "proof_observation_time_future"
	fleetProofExpired          FleetProofIssue = "proof_observation_expired"
	fleetProofAgeInvalid       FleetProofIssue = "proof_reported_age_invalid"
)

// Accept only fixed codes. Neither received headers nor errors from the OS may
// inject arbitrary text into status or logs.
func safeFleetProofIssue(issue FleetProofIssue) FleetProofIssue {
	switch issue {
	case fleetProofConfigMissing, fleetProofImageOpen, fleetProofImageStat, fleetProofImageFormat,
		fleetProofImageMapping, fleetProofImageMismatch, fleetProofImageHash, fleetProofImageChanged,
		fleetProofReceiptInvalid, fleetProofSmokeCleanup, fleetProofSmokeFailed, fleetProofManagerUnhealthy,
		fleetProofManagerPID, fleetProofCollectionFailed, fleetProofPending, fleetProofSessionMissing,
		fleetProofSessionMismatch, fleetProofTimeMissing, fleetProofTimeFuture, fleetProofExpired:
		return issue
	}
	return ""
}

// Fixed predicate names only: never include received field values. Keep this
// shared with fleetProofReady so diagnostics describe the actual readiness gate.
func fleetProofFailure(p *FleetRuntimeProof) string {
	switch {
	case p == nil:
		return "proof_missing"
	case p.PID <= 0:
		return "proof_pid_missing"
	case p.Session == "":
		return "proof_session_missing"
	case !validPin(p.SHA256):
		return "proof_sha256_invalid"
	case !p.ServiceHealthy:
		return "proof_service_unhealthy"
	case !p.RunnerSmoke:
		return "proof_runner_smoke_failed"
	case p.StartGate != startGateProtocol:
		return "proof_start_gate_mismatch"
	case p.AttemptProtocol != keyedAttemptProtocol:
		return "proof_attempt_protocol_mismatch"
	}
	return ""
}

func fleetAuthorizationCause(err error, p *FleetRuntimeProof, session string) string {
	if err != nil {
		var syntax *json.SyntaxError
		var mismatch *json.UnmarshalTypeError
		if errors.As(err, &syntax) || errors.As(err, &mismatch) {
			return "response_decode_failed"
		}
		var stall *QueueStalledError
		if errors.As(err, &stall) {
			return "queue_stalled"
		}
		var rejection *HTTPError
		if errors.As(err, &rejection) {
			// A syntactically valid server code can still contain private
			// text. Retain only codes this control endpoint is known to emit.
			code := "request_failed"
			switch rejection.Code {
			case "fleet_instruction_stale", "fleet_ack_stale", "fleet_maintenance_changed",
				"authentication_required", "request_invalid", "queue_stopping",
				"status_unavailable", "request_capacity_unavailable":
				code = rejection.Code
			}
			return fmt.Sprintf("HTTP %d code=%s", rejection.Status, code)
		}
		return ClientErrorCause(err)
	}
	if why := fleetProofFailure(p); why != "" {
		return why
	}
	if p.Session != session {
		return "proof_session_mismatch"
	}
	return "ready"
}

// Never project arbitrary receipt text, OS output or queue error messages. This
// allow-list is shared by live reporting, durable replay and queue status; each
// code is one the helper emits (see docs/reference/fleet-upgrades.md).
func fleetFailureCode(code string) string {
	switch code {
	case "fleet_ack_failed", "fleet_authorization_refused", "fleet_instruction_stale", "fleet_ack_stale",
		"fleet_maintenance_changed",
		"fleet_artifact_download_failed", "fleet_artifact_transfer_failed", "fleet_artifact_refused",
		"fleet_staging_write_failed", "fleet_binary_unavailable", "fleet_candidate_invalid",
		"fleet_config_invalid", "fleet_service_invalid", "fleet_install_invalid", "fleet_state_invalid",
		"fleet_local_file_invalid", "fleet_credentials_unavailable", "fleet_queue_invalid",
		"fleet_darwin_identity_refused", "fleet_existing_service_unavailable", "fleet_health_failed",
		"fleet_helper_busy", "fleet_helper_launch_failed", "fleet_helper_launch_unclassified",
		"fleet_install_changed", "fleet_instruction_invalid", "fleet_preflight_failed",
		"fleet_storage_preflight_failed", "fleet_publish_failed", "fleet_publisher_mismatch",
		"fleet_publisher_unavailable", "fleet_signature_refused", "fleet_target_invalid",
		"fleet_receipt_write_failed", "fleet_recovery_runtime_changed", "fleet_restart_failed",
		"fleet_version_refused", "fleet_failure_code_missing", "fleet_failure_code_unrecognized":
		return code
	case "":
		return "fleet_failure_code_missing"
	default:
		return "fleet_failure_code_unrecognized"
	}
}

func fleetControlFailure(err error, fallback string) error {
	var rejection *HTTPError
	if errors.As(err, &rejection) {
		switch rejection.Code {
		case "fleet_instruction_stale", "fleet_ack_stale", "fleet_maintenance_changed":
			return errors.New(rejection.Code)
		}
	}
	return errors.New(fallback)
}

// Preserve the typed pre-intent stall for the runtime's next-poll recovery,
// while helpers after intent retain the stable, durable failure code.
type fleetAuthorizationStalled struct{ cause error }

func (*fleetAuthorizationStalled) Error() string   { return "fleet_authorization_refused" }
func (e *fleetAuthorizationStalled) Unwrap() error { return e.cause }

// Launch yields to the next runtime poll while a helper still runs or the
// queue's answer was momentarily stale; the instruction is fetched again.
type fleetInstructionRefetchError struct{}

func (*fleetInstructionRefetchError) Error() string { return "fleet_instruction_stale" }

func fleetStaleControl(err error) bool {
	return err != nil && (err.Error() == "fleet_instruction_stale" || err.Error() == "fleet_ack_stale")
}
