package myce

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"time"
)

func readEnvironmentLogTail(path string, start int64) []byte {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil
	}
	start = max(start, st.Size()-32768)
	if start >= st.Size() {
		return nil
	}
	b := make([]byte, st.Size()-start)
	n, _ := f.ReadAt(b, start)
	return b[:n]
}

// Installer output is needed because package managers wrap network and disk
// failures in the same exit status as a bad recipe. Infrastructure wins over
// recipe diagnostics in a combined error. Unknown new worker errors retry;
// legacy untyped receipts retain their existing terminal-build contract.
func classifyEnvironmentFailure(err error, output string) string {
	var network net.Error
	if errors.As(err, &network) || errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) || errors.Is(err, errEnvironmentBuildStalled) || errors.Is(err, errWorkerReleasePending) {
		return "transient"
	}
	r := EnvironmentReport{Error: fmt.Sprint(err), Log: output}
	if environmentInfrastructureFailure(r) {
		return "transient"
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() >= 0 {
		return "deterministic"
	}
	if environmentRecipeFailure(r) {
		return "deterministic"
	}
	return "transient"
}

var environmentHTTPFailure = regexp.MustCompile(`\b(?:http(?:/[0-9.]+)?\s+|(?:status(?:\s+code)?|http\s+error)\s*[:=]?\s*)50[234]\b`)

func environmentInfrastructureFailure(r EnvironmentReport) bool {
	text := strings.ToLower(r.Error + "\n" + r.Log)
	if environmentHTTPFailure.MatchString(text) {
		return true
	}
	for _, clue := range []string{"no space left", "disk space", "disk quota", "environment cache needs", "connection refused", "connection reset", "connection closed", "network is unreachable", "network unreachable", "temporary failure", "timed out", "timeout", "failed to download", "error sending request", "dns error", "name resolution", "signal: killed", "signal: terminated", "build stalled", "service unavailable", "too many requests", "502 bad gateway"} {
		if strings.Contains(text, clue) {
			return true
		}
	}
	return false
}

func environmentRecipeFailure(r EnvironmentReport) bool {
	text := strings.ToLower(r.Error + "\n" + r.Log)
	for _, clue := range []string{"no solution found", "resolutionimpossible", "no matching distribution", "unsatisfiable", "failed to solve", "manifest disagreement", "lockfile is not up-to-date"} {
		if strings.Contains(text, clue) {
			return true
		}
	}
	return false
}

func environmentFailureKind(r EnvironmentReport) string {
	if r.FailureKind != "" {
		return r.FailureKind
	}
	if environmentInfrastructureFailure(r) {
		return "transient"
	}
	return "deterministic"
}

func environmentLogExcerpt(text string) string {
	// Heartbeats carry the final 4 KiB; full logs retain their chunked transport.
	if len(text) > 4096 {
		text = text[len(text)-4096:]
	}
	return strings.TrimSpace(text)
}

func environmentFailureReason(r EnvironmentReport, includeLog bool) string {
	cause := r.Error
	if includeLog && environmentLogExcerpt(r.Log) != "" {
		cause += "; " + environmentLogExcerpt(r.Log)
	}
	if environmentFailureKind(r) == "transient" {
		when := "queue reconciliation"
		if r.RetryAt != nil {
			when = r.RetryAt.Format(time.RFC3339)
		}
		return fmt.Sprintf("environment retry pending: %s; retry after %s", cause, when)
	}
	return "environment failed: " + cause + "; fix the recipe and resubmit with changed inputs"
}

func (q *Queue) acceptEnvironmentFailures(m, old *Machine) {
	for i := range m.EnvironmentReports {
		r := &m.EnvironmentReports[i]
		var previous EnvironmentReport
		if old != nil {
			for _, p := range old.EnvironmentReports {
				if p.Key == r.Key {
					previous = p
					break
				}
			}
		}
		r.RetryAt, r.Failures = nil, previous.Failures
		if r.State == "ready" {
			r.Failures = 0
		}
		if r.State != "failed" && r.State != "uploading" {
			continue
		}
		r.FailureKind = environmentFailureKind(*r)
		if r.FailureKind != "transient" {
			continue
		}
		if previous.Attempt == r.Attempt && previous.RetryAt != nil {
			r.RetryAt = previous.RetryAt
			continue
		}
		r.Failures = min(r.Failures+1, 5)
		// 30s avoids retrying every heartbeat; a five-minute ceiling keeps a
		// recovered network or disk usable without manual cache clearing.
		delay := min(30*time.Second*time.Duration(1<<(r.Failures-1)), 5*time.Minute)
		at := q.clock.Now().Add(delay)
		r.RetryAt = &at
	}
}

func (q *Queue) environmentFailureBlocked(j *Job, m *Machine) bool {
	if environmentReady(j, m) {
		return false
	}
	key := environmentKey(j, m)
	if e := qualifiedExtension(j, m); e != nil {
		key = extensionPreparationKey(*e, m)
	}
	for _, r := range m.EnvironmentReports {
		if r.Key == key && (r.State == "failed" || r.State == "uploading") {
			return q.environmentReportBlocked(r)
		}
	}
	return false
}

func (q *Queue) environmentReportBlocked(r EnvironmentReport) bool {
	return (r.State == "failed" || r.State == "uploading") && (environmentFailureKind(r) != "transient" || r.RetryAt == nil || q.clock.Now().Before(*r.RetryAt))
}

func (q *Queue) environmentFailureWaitingReason(j *Job, machines map[string]*Machine) string {
	reason := ""
	for _, m := range machines {
		if !q.placementAllowed(j, m) || !executionTargetMatches(j.Spec, m) || !environmentCanRun(j, m) || j.Spec.Needs.cpuOnly() != (m.GPU == "cpu") {
			continue
		}
		if !q.environmentFailureBlocked(j, m) {
			return ""
		}
		key := environmentKey(j, m)
		if e := qualifiedExtension(j, m); e != nil {
			key = extensionPreparationKey(*e, m)
		}
		for _, r := range m.EnvironmentReports {
			if r.Key == key && (r.State == "failed" || r.State == "uploading") {
				reason = environmentFailureReason(r, j.Spec.Logs != "none")
			}
		}
	}
	return reason
}
