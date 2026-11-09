package myce

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// FinalizationDetail records the boundary after execution, not a guessed payload
// exit. Output is a declared relative name, bound to the frozen spec by the queue.
// Error is a fixed rule; arbitrary OS/transport text stays in the local chain.
type FinalizationDetail struct {
	Step            string `json:"step"`
	Output          string `json:"output,omitempty"`
	Destination     string `json:"destination,omitempty"`
	Error           string `json:"error"`
	HTTPStatus      int    `json:"http_status,omitempty"`
	CommandExitCode *int   `json:"command_exit_code,omitempty"`
}

var finalizationSteps = map[string]bool{
	"output_path": true, "output_validation": true, "output_check": true,
	"output_publication": true, "output_spool": true, "output_reservation": true,
	"output_preflight": true, "output_upload": true, "checkpoint_publication": true,
	"diagnostic_publication": true, "log_persistence": true,
}

var finalizationErrors = map[string]string{
	"not_found":                "file or parent does not exist",
	"permission_denied":        "permission denied",
	"not_regular":              "file must be regular",
	"unsafe_path":              "output path escapes its root or is invalid",
	"changed":                  "file changed during identification or upload",
	"wrong_type":               "a path component has the wrong file type",
	"spool_conflict":           "saved result spool conflicts with the attempt or declarations",
	"baseline_mismatch":        "output differs from native baseline",
	"invalid_numeric_output":   "numeric outputs must be matching nonempty JSON arrays",
	"variation_exceeded":       "output exceeds declared native variation",
	"check_output_too_large":   "check output too large",
	"check_target_unavailable": "check target unavailable",
	"check_extension_missing":  "frozen check extension missing",
	"timeout":                  "operation timed out",
	"cancelled":                "operation cancelled",
	"storage_full":             "storage is full",
	"connection_refused":       "connection refused",
	"http_error":               "publication request rejected",
	"io_error":                 "I/O failed; exact underlying cause unknown",
}

func safeFinalization(d *FinalizationDetail) *FinalizationDetail {
	if d == nil || !finalizationSteps[d.Step] || finalizationErrors[d.Error] == "" {
		return nil
	}
	out := *d
	if !safeRelative(out.Output) {
		out.Output = ""
	}
	if !safeRelative(out.Destination) {
		out.Destination = ""
	}
	if out.HTTPStatus < 400 || out.HTTPStatus > 599 || out.Error != "http_error" {
		out.HTTPStatus = 0
	}
	if out.CommandExitCode != nil {
		if *out.CommandExitCode != 0 {
			out.CommandExitCode = nil
		} else {
			zero := 0
			out.CommandExitCode = &zero
		}
	}
	return &out
}

func (d FinalizationDetail) String() string {
	text := "finalization " + d.Step
	if d.Output != "" {
		text += fmt.Sprintf(" output %q", d.Output)
	}
	if d.Destination != "" {
		text += fmt.Sprintf(" destination %q", d.Destination)
	}
	text += ": " + finalizationErrors[d.Error]
	if d.HTTPStatus != 0 {
		text += fmt.Sprintf(" (HTTP %d)", d.HTTPStatus)
	}
	if d.CommandExitCode != nil {
		text += "; command exited 0"
	}
	return text
}

func finalizationErrorFor(step, output string, err error) error {
	if err == nil {
		return nil
	}
	c := runtimeDiagnostic(err)
	if c.Finalization != nil {
		return &runtimeError{diagnostic: *c, cause: err}
	}
	d := &FinalizationDetail{Step: step, Output: output, Error: c.Class}
	if finalizationErrors[d.Error] == "" {
		d.Error = "io_error"
	}
	// Compare only complete, owned rule messages. Never scrape arbitrary text.
	switch err.Error() {
	case "file must be regular", "job input/output must be a regular file":
		d.Error = "not_regular"
	case "job file escapes shared root", "job files require a root and clean relative names", "output escapes root":
		d.Error = "unsafe_path"
	case "file changed during identification", "file changed before upload":
		d.Error = "changed"
	case "conflicting local result spool":
		d.Error = "spool_conflict"
	case "output differs from native baseline":
		d.Error = "baseline_mismatch"
	case "numeric outputs must be matching nonempty JSON arrays":
		d.Error = "invalid_numeric_output"
	case "output exceeds declared native variation":
		d.Error = "variation_exceeded"
	case "check output too large":
		d.Error = "check_output_too_large"
	case "check target unavailable":
		d.Error = "check_target_unavailable"
	case "frozen check extension missing":
		d.Error = "check_extension_missing"
	}
	if errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.EISDIR) {
		d.Error = "wrong_type"
	}
	var response *HTTPError
	if errors.As(err, &response) {
		d.Error, d.HTTPStatus = "http_error", response.Status
	}
	c.Operation, c.Class, c.Finalization = "outputs", d.Error, safeFinalization(d)
	return &runtimeError{diagnostic: *c, cause: err}
}

// Retryable publication errors retain Success and the local bytes. Required
// output refusals alone change the terminal verdict. Both carry the same cause.
func finalizationResult(r Result, step, output string, err error, terminal bool) Result {
	c := runtimeDiagnostic(finalizationErrorFor(step, output, err))
	if r.Success && !r.Interrupted {
		zero := 0
		c.Finalization.CommandExitCode = &zero
	}
	if !r.Success && r.ExecutionCause != nil {
		r.CleanupCause = c
	} else {
		r.ExecutionCause, r.Error = c, c.String()
	}
	if terminal {
		r.Success = false
	}
	return r
}

func requiredOutputError(step, name string, err error) error {
	return errors.Join(errRequiredDeliveryOutput, finalizationErrorFor(step, name, err))
}

func regularOutputError(info os.FileInfo, err error) error {
	if err == nil && !info.Mode().IsRegular() {
		return errors.New("file must be regular")
	}
	return err
}

// Strict older queues may reject the additive nested detail. Retain it locally
// and retry only their pre-mutation request_invalid refusal with legacy causes.
func finalizationWireInput(in any, strip bool) (any, bool) {
	switch v := in.(type) {
	case *Result:
		if v != nil {
			return finalizationWireInput(*v, strip)
		}
	case Result:
		has := false
		legacy := func(c *RuntimeCause) *RuntimeCause {
			if c == nil || c.Finalization == nil {
				return c
			}
			has = true
			if !strip {
				return c
			}
			copy := *c
			copy.Finalization = nil
			if safeRuntimeCause(&copy) == nil {
				copy.Class = "io_error"
			}
			return &copy
		}
		v.ExecutionCause, v.CleanupCause = legacy(v.ExecutionCause), legacy(v.CleanupCause)
		return v, has
	case LogChunk:
		result, has := finalizationWireInput(v.Result, strip)
		v.Result = result.(Result)
		return v, has
	}
	return in, false
}
