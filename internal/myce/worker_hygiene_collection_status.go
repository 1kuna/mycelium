package myce

import "slices"

// Worker diagnostics are untrusted even when the stock collector emits codes.
// Sanitize only the detached public view, preserving queue-owned evidence.
func publicWorkerCollection(m *Machine) {
	if m.HygieneCollection == nil {
		return
	}
	r := *m.HygieneCollection
	code := func(value, fallback string, allowed ...string) string {
		if value == "" || slices.Contains(allowed, value) {
			return value
		}
		return fallback
	}
	r.FileCache.Error = code(r.FileCache.Error, "file_cache_collect_failed", "file_cache_collect_failed", "file_cache_collect_interrupted", "file_cache_collect_changed")
	r.Environments.Error = code(r.Environments.Error, "environment_collect_failed", "environment_collect_failed", "environment_collect_interrupted", "environment_references_unavailable")
	r.StagingError = code(r.StagingError, "installer_staging_retained", "installer_staging_retained")
	m.HygieneCollection = &r
}
