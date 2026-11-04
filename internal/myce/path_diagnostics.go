package myce

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
)

// PathDiagnostic describes an actual failed read. Path is either the exact
// caller-authored spelling or a logical label, never an expanded private root.
// ResolutionBase is a logical ID whose physical mapping stays in local config.
type PathDiagnostic struct {
	Field          string `json:"field"`
	Side           string `json:"side"`
	Path           string `json:"path"`
	ResolutionBase string `json:"resolution_base"`
	Cause          string `json:"cause"`
	Companion      string `json:"companion,omitempty"`
}

func (d PathDiagnostic) String() string {
	text := fmt.Sprintf("%s: %s read %q (base %s): %s", d.Field, d.Side, d.Path, d.ResolutionBase, d.Cause)
	if d.Companion != "" {
		text += fmt.Sprintf("; required companion %q", d.Companion)
	}
	return text
}

type pathReadError struct {
	diagnostic PathDiagnostic
	cause      error
}

func (e *pathReadError) Error() string { return e.diagnostic.String() }
func (e *pathReadError) Unwrap() error { return e.cause }

// PathReadError wraps the failing operation, retaining its cause locally while
// rendering only supplied diagnostic labels. Do not pass resolved OS paths.
func PathReadError(field, side, path, base, companion string, err error) error {
	if err == nil {
		return nil
	}
	cause := "read_failed"
	switch {
	case errors.Is(err, fs.ErrNotExist):
		cause = "missing"
		if companion != "" {
			cause = "companion_absent"
		}
	case errors.Is(err, fs.ErrPermission):
		cause = "unreadable"
	case errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENOTDIR):
		cause = "wrong_type"
	}
	return &pathReadError{PathDiagnostic{field, side, path, base, cause, companion}, err}
}

func pathDiagnostic(err error) *PathDiagnostic {
	var read *pathReadError
	if !errors.As(err, &read) {
		return nil
	}
	d := read.diagnostic
	return &d
}

// Workers may report only this logical read boundary, never arbitrary text or
// roots. Reconstruct every field before including authenticated readback.
func safeWorkerPathDiagnostic(d *PathDiagnostic) *PathDiagnostic {
	if d == nil || d.Field != "environment.binary" || d.Side != "worker" || d.Path != "binary" || d.ResolutionBase != "worker-environment-library" || d.Companion != "" {
		return nil
	}
	switch d.Cause {
	case "missing", "unreadable", "wrong_type", "read_failed":
	default:
		return nil
	}
	copy := *d
	return &copy
}

// Staging reports identify the authored input and logical root role. Neither
// the caller's NAS spelling nor expanded local paths cross this boundary.
func safeInputStagingPathDiagnostic(d *PathDiagnostic) *PathDiagnostic {
	if d == nil || d.Side != "worker" || d.Path != "input" || d.ResolutionBase != "worker-checkpoint-root" || d.Companion != "" || !strings.HasPrefix(d.Field, "shared_inputs.") || !strings.HasSuffix(d.Field, ".nas") {
		return nil
	}
	name := strings.TrimSuffix(strings.TrimPrefix(d.Field, "shared_inputs."), ".nas")
	if name == "" || strings.Trim(name, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_") != "" {
		return nil
	}
	switch d.Cause {
	case "missing", "unreadable", "wrong_type", "read_failed":
	default:
		return nil
	}
	copy := *d
	return &copy
}

// Only the job's authored NAS inputs may name a worker read failure. Shape
// validation alone cannot establish that a worker-supplied name is public.
func safeJobRuntimeCause(spec Spec, c *RuntimeCause) *RuntimeCause {
	out := safeRuntimeCause(c)
	if out != nil {
		out.Staging = bindStagingCause(spec, out.Staging)
	}
	if out == nil || out.PathDiagnostic == nil {
		return out
	}
	d := out.PathDiagnostic
	out.PathDiagnostic = nil
	for name, input := range spec.SharedInputs {
		field := "shared_inputs." + name + ".nas"
		if input.NAS != "" && d.Field == field {
			out.PathDiagnostic = &PathDiagnostic{Field: field, Side: "worker", Path: "input", ResolutionBase: "worker-checkpoint-root", Cause: d.Cause}
			break
		}
	}
	return out
}
