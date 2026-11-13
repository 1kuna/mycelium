package myce

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"slices"
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
	return "environment failed: " + cause + "; repair this worker and explicitly retry its build, or submit changed recipe inputs"
}

func environmentFingerprint(m *Machine) string {
	return identityHash(struct{ Host, Fingerprint string }{hostID(m), m.ExtensionFingerprint})
}

func environmentReportMatches(r EnvironmentReport, m *Machine) bool {
	return r.Target == nil || *r.Target == machineTarget(m) && r.Fingerprint == environmentFingerprint(m)
}

func (q *Queue) acceptEnvironmentFailures(m, old *Machine) {
	incoming := slices.Clone(m.EnvironmentReports)
	m.EnvironmentReports = nil
	// Reports are a worker map, not a complete ledger. Omission and worker
	// restarts must not erase deterministic evidence or a requested retry.
	if old != nil {
		for _, p := range old.EnvironmentReports {
			if p.State == "retry" || (p.State == "failed" || p.State == "uploading") && environmentFailureKind(p) != "transient" {
				m.EnvironmentReports = append(m.EnvironmentReports, p)
			}
		}
	}
	for _, r := range incoming {
		r.Target, r.Fingerprint, r.LogReference = nil, "", ""
		var previous EnvironmentReport
		if old != nil {
			for _, p := range old.EnvironmentReports {
				if p.Key == r.Key {
					previous = p
					break
				}
			}
		}
		var target *Machine
		launch, offered := q.s.PreparationPlans[r.Attempt]
		if offered && (launch.Machine != m.ID || launch.Session != m.Session || launch.Kind != "environment" || launch.Key != r.Key) {
			continue
		}
		for _, view := range q.machineTargets(m) {
			if offered && targetDevice(view) == launch.Device || !offered && previous.Target != nil && *previous.Target == machineTarget(view) {
				target = view
				break
			}
		}
		if target == nil {
			// Untyped retained receipts predate target metadata. Recover their
			// scope from frozen demand, rather than the machine's primary GPU.
			for _, view := range q.machineTargets(m) {
				for _, j := range q.liveJobs() {
					if slices.Contains(environmentFailureKeys(j, view), r.Key) {
						target = view
						if r.LogReference == "" {
							r.LogReference = "myce logs " + j.ID
						}
						break
					}
				}
				if target != nil {
					break
				}
			}
			if target == nil {
				target = m
			}
		}
		// A cached completion cannot undo a retry or follow an identity change.
		// Only a newly issued exact preparation attempt supplies new evidence.
		if previous.Key != "" && (!environmentReportMatches(previous, target) || previous.State == "retry" || previous.Attempt != r.Attempt) && !offered {
			continue
		}
		if !offered && previous.Attempt == r.Attempt && (previous.State == "ready" && r.State != "ready" || (previous.State == "failed" || previous.State == "uploading") && environmentFailureKind(previous) != "transient" && r.State != "failed" && r.State != "uploading") {
			continue
		}
		t := machineTarget(target)
		r.Target, r.Fingerprint = &t, environmentFingerprint(target)
		if previous.LogReference != "" {
			r.LogReference = previous.LogReference
		}
		if offered && launch.Demand != "" {
			r.LogReference = "myce logs " + launch.Demand
		}
		r.RetryAt, r.Failures = nil, previous.Failures
		if r.State == "ready" {
			r.Failures = 0
		}
		if r.State == "failed" || r.State == "uploading" {
			r.FailureKind = environmentFailureKind(r)
			if r.FailureKind == "transient" {
				if previous.Attempt == r.Attempt && previous.RetryAt != nil {
					r.RetryAt = previous.RetryAt
				} else {
					r.Failures = min(r.Failures+1, 5)
					// Existing network/disk backoff avoids retrying every heartbeat.
					delay := min(30*time.Second*time.Duration(1<<(r.Failures-1)), 5*time.Minute)
					at := q.clock.Now().Add(delay)
					r.RetryAt = &at
				}
			}
		}
		m.EnvironmentReports = slices.DeleteFunc(m.EnvironmentReports, func(p EnvironmentReport) bool { return p.Key == r.Key })
		m.EnvironmentReports = append(m.EnvironmentReports, r)
	}
}

// RetryEnvironment clears only one worker/key/target exclusion. Retaining the
// old attempt as a tombstone fences delayed worker receipts across restart.
func (q *Queue) RetryEnvironment(worker, key string, target ExecutionTarget, contexts ...context.Context) error {
	return q.updateRequest(requestContext(contexts), func() error {
		m := q.s.Machines[worker]
		if m == nil {
			return errors.New("unknown environment worker")
		}
		for i, r := range m.EnvironmentReports {
			if r.Key != key || r.Target == nil || *r.Target != target || (r.State != "failed" && r.State != "uploading" && r.State != "retry") {
				continue
			}
			if r.State == "retry" {
				return nil
			}
			m = q.touchMachine(worker)
			m.EnvironmentReports = slices.Clone(m.EnvironmentReports)
			m.EnvironmentReports[i].State = "retry"
			return nil
		}
		return errors.New("no failed environment build for worker, key and target")
	}, nil)
}

func (q *Queue) environmentFailureBlocked(j *Job, m *Machine) bool {
	if environmentReady(j, m) {
		return false
	}
	keys := environmentFailureKeys(j, m)
	for _, r := range m.EnvironmentReports {
		if environmentReportMatches(r, m) && slices.Contains(keys, r.Key) && q.environmentReportBlocked(r) {
			return true
		}
	}
	return false
}

// A known failed sidecar still blocks this target when an earlier member has
// been evicted. Do not rebuild unrelated missing members of a failed candidate.
func environmentFailureKeys(j *Job, m *Machine) []string {
	if j.CommandDiscovery != nil {
		keys := []string{}
		for _, r := range commandEnvironmentRecipes(j, m) {
			if !slices.Contains(m.Environments, r.Key) {
				keys = append(keys, r.Key)
			}
		}
		return keys
	}
	key := environmentKey(j, m)
	if e := qualifiedExtension(j, m); e != nil {
		key = extensionPreparationKey(*e, m)
	}
	return []string{key}
}

func (q *Queue) environmentReportBlocked(r EnvironmentReport) bool {
	return (r.State == "failed" || r.State == "uploading") && (environmentFailureKind(r) != "transient" || r.RetryAt == nil || q.clock.Now().Before(*r.RetryAt))
}

func (q *Queue) environmentFailureWaitingReason(j *Job, machines map[string]*Machine) string {
	reason := ""
	allFailed := true
	for _, m := range machines {
		if !q.placementAllowed(j, m) || !executionTargetMatches(j.Spec, m) || !environmentCanRun(j, m) || j.Spec.Needs.cpuOnly() != (m.GPU == "cpu") {
			continue
		}
		if !q.inventoryFresh(q.s.Machines[m.ID]) {
			continue
		}
		if !q.environmentFailureBlocked(j, m) {
			return ""
		}
		keys := environmentFailureKeys(j, m)
		for _, r := range m.EnvironmentReports {
			if environmentReportMatches(r, m) && slices.Contains(keys, r.Key) && q.environmentReportBlocked(r) {
				if environmentFailureKind(r) == "transient" {
					allFailed = false
				}
				reason = environmentFailureReason(r, j.Spec.Logs != "none")
			}
		}
	}
	if reason != "" && allFailed {
		return "no-buildable-worker: every freshly observed compatible worker failed; " + reason
	}
	return reason
}
