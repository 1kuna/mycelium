package myce

import "errors"

// Never project arbitrary receipt text, OS output or queue error messages. This
// allow-list is shared by live reporting, durable replay and queue status.
func fleetFailureCode(code string) string {
	switch code {
	case "fleet_ack_failed", "fleet_artifact_refused", "fleet_authorization_refused",
		"fleet_artifact_download_failed", "fleet_artifact_transfer_failed",
		"fleet_staging_exists", "fleet_staging_write_failed",
		"fleet_binary_unavailable", "fleet_candidate_invalid", "fleet_config_invalid",
		"fleet_credentials_unavailable", "fleet_darwin_identity_refused",
		"fleet_existing_service_unavailable", "fleet_health_failed",
		"fleet_helper_absence_unproven", "fleet_helper_busy", "fleet_helper_launch_failed",
		"fleet_install_changed", "fleet_install_invalid", "fleet_instruction_invalid",
		"fleet_local_file_invalid", "fleet_preflight_failed", "fleet_previous_failure",
		"fleet_publish_failed", "fleet_publisher_mismatch", "fleet_publisher_unavailable",
		"fleet_queue_invalid", "fleet_receipt_conflict", "fleet_receipt_invalid",
		"fleet_receipt_write_failed", "fleet_recovery_runtime_changed", "fleet_restart_failed",
		"fleet_service_invalid", "fleet_signature_refused", "fleet_staging_cleanup_failed",
		"fleet_state_invalid", "fleet_storage_preflight_failed", "fleet_target_invalid",
		"fleet_version_invalid", "fleet_version_refused", "fleet_instruction_stale",
		"fleet_ack_stale", "fleet_maintenance_changed", "fleet_helper_launch_unclassified",
		"fleet_failure_code_missing", "fleet_failure_code_unrecognized", "fleet_failure_detail_unavailable":
		return code
	case "helper_launch_failed": // Older workers use the unprefixed spelling.
		return "fleet_helper_launch_failed"
	case "update_failed", "fleet_update_failed":
		return "fleet_failure_detail_unavailable"
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

// Only launch's initial authorization can request a refetch: no receipt has
// been written or helper started. Later refusals must retain failed intent and
// must never automatically repeat publication or activation.
type fleetInstructionRefetchError struct{}

func (*fleetInstructionRefetchError) Error() string { return "fleet_instruction_stale" }
