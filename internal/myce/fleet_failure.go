package myce

import (
	"encoding/json"
	"errors"
)

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

// Launch yields to the next runtime poll while a helper still runs or the
// queue's answer was momentarily stale; the instruction is fetched again.
type fleetInstructionRefetchError struct{}

func (*fleetInstructionRefetchError) Error() string { return "fleet_instruction_stale" }

func fleetStaleControl(err error) bool {
	return err != nil && (err.Error() == "fleet_instruction_stale" || err.Error() == "fleet_ack_stale")
}
