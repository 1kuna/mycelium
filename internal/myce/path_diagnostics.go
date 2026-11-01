package myce

import (
	"errors"
	"fmt"
	"io/fs"
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
