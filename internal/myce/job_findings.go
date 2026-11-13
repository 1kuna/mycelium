package myce

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// JobFinding is one plain-language explanation of a failed or interrupted job
// for its owner: what happened (with the measured evidence Mycelium holds),
// whose problem it is, and the single action to take. It is derived on every
// read from typed causes and queue-owned reasons; it is never persisted, never
// scraped from worker text, and never states a cause Mycelium did not record.
type JobFinding struct {
	// Code is a stable machine-readable class, e.g. "output_not_regular".
	Code string `json:"code"`
	// Owner is "job" (the command or its declaration), "mycelium" (host or
	// queue infrastructure; not the job's fault) or "unknown" (no typed cause).
	Owner string `json:"owner"`
	// What is one sentence: what happened, how Mycelium knows, what it did.
	What string `json:"what"`
	// Action is the one thing to change or do next.
	Action string `json:"action"`
}

const (
	findingOwnerJob      = "job"
	findingOwnerMycelium = "mycelium"
	findingOwnerUnknown  = "unknown"
)

// Text is the compact CLI form shared by status, wait, run and result.
func (f *JobFinding) Text() string {
	if f == nil {
		return ""
	}
	who := map[string]string{findingOwnerJob: "your job", findingOwnerMycelium: "Mycelium, not your job", findingOwnerUnknown: "cause unknown"}[f.Owner]
	return fmt.Sprintf("finding (%s): %s Next: %s", who, f.What, f.Action)
}

// jobFinding runs inside statusJobValue on a detached copy, after runtime
// causes are sanitized but before the public reason is redacted. rawReason is
// matched only against fixed queue-owned strings; no worker text is copied.
func jobFinding(j *Job, rawReason string) *JobFinding {
	switch j.State {
	case "failed":
		return failedJobFinding(j, rawReason)
	case "queued", "running", "stopping", "paused":
		if j.Losses > 0 {
			return &JobFinding{"infrastructure_retry", findingOwnerMycelium,
				fmt.Sprintf("Mycelium lost %s to a host or worker interruption, not to your command, and requeued the job automatically.", plural(j.Losses, "attempt")),
				"none needed; Mycelium retries on its own (resumable jobs continue from saved progress)."}
		}
		if j.AdmissionRequeues > 0 {
			return &JobFinding{"queue_admission", findingOwnerMycelium,
				fmt.Sprintf("Mycelium's queue withdrew launch authorization before your command started and placed the job again (%s); this is a queue admission event, not an execution start or a lost attempt.", plural(j.AdmissionRequeues, "admission requeue")),
				"none needed; Mycelium places the job again on its own."}
		}
	}
	return nil
}

// terminalJobFinding serves the wait index, which publishes scalar states under
// q.mu without the full status projection. Only the two runtime causes need
// sanitizing; the result is computed once when a job first becomes failed.
func terminalJobFinding(stored *Job) *JobFinding {
	j := *stored
	j.ExecutionCause, j.CleanupCause = safeRuntimeCause(j.ExecutionCause), safeRuntimeCause(j.CleanupCause)
	return jobFinding(&j, j.Reason)
}

func failedJobFinding(j *Job, raw string) *JobFinding {
	cause := j.ExecutionCause
	if f := infrastructureExhaustedFinding(j, raw); f != nil {
		return f
	}
	if cause != nil {
		if d := cause.Finalization; d != nil && cause.Operation == "outputs" {
			return outputFinding(d)
		}
		if f := resourceStopFinding(j, cause); f != nil {
			return f
		}
		if f := launchFinding(cause); f != nil {
			return f
		}
	}
	if f := memoryExhaustedFinding(j, raw); f != nil {
		return f
	}
	if n := len(j.AttemptHistory); n > 0 && j.AttemptHistory[n-1].OutOfMemory && j.AttemptHistory[n-1].Attempt == j.Attempt {
		return &JobFinding{"out_of_memory", findingOwnerJob,
			"Your command ran out of memory on this attempt, and this job's class is not retried automatically.",
			"declare more memory (`needs.host_memory_gb`, or `needs.memory: auto`) or reduce the job's memory use."}
	}
	if f := dependencyFinding(j); f != nil {
		return f
	}
	if strings.HasPrefix(raw, "environment failed:") {
		return &JobFinding{"environment_failed", findingOwnerJob,
			"Preparing the job's declared environment failed before your command ran; the preparation log has the details.",
			"fix the environment recipe (see `myce logs`), then resubmit."}
	}
	if j.CleanupCause != nil && cause == nil {
		return &JobFinding{"finalization_failed", findingOwnerMycelium,
			fmt.Sprintf("Your command finished, but Mycelium failed while cleaning up or saving the attempt (%s).", j.CleanupCause.String()),
			"resubmit; if it repeats, report the job ID to the queue owner."}
	}
	if cause != nil && cause.Class == "command_exit" && cause.ExitCode != nil {
		return &JobFinding{"command_exit", findingOwnerJob,
			fmt.Sprintf("Your command exited with code %d; Mycelium recorded no guard stop or infrastructure fault for this attempt.", *cause.ExitCode),
			"read the job's log (`myce logs`) and fix the command."}
	}
	if f := signalFinding(j); f != nil {
		return f
	}
	if !strings.HasPrefix(raw, "command failed: ") && !strings.HasPrefix(raw, "execution failed") {
		return nil // Lifecycle decisions with their own plain reason (deadlines, checks).
	}
	return &JobFinding{"cause_unknown", findingOwnerUnknown,
		"The attempt failed, but Mycelium recorded no exit code, signal, guard stop or infrastructure fault for it.",
		"read the job's log (`myce logs`); to keep reports from the next failure, resubmit with `diagnostics` declared."}
}

func infrastructureExhaustedFinding(j *Job, raw string) *JobFinding {
	if !strings.HasPrefix(raw, "failed after 3 infrastructure losses:") && raw != "infrastructure_retries_exhausted" {
		return nil
	}
	what := fmt.Sprintf("Mycelium lost %s to host or worker interruptions, not to your command, and stopped retrying.", plural(max(j.Losses, 1), "attempt"))
	if j.ExecutionCause != nil {
		what = strings.TrimSuffix(what, ".") + fmt.Sprintf(" (last: %s).", j.ExecutionCause.String())
	}
	return &JobFinding{"infrastructure_retries_exhausted", findingOwnerMycelium, what,
		"resubmit with a new submission key; reusing this key returns this failed job."}
}

// Output-contract errors that only the job can fix. Every other finalization
// error is publication infrastructure (storage, network, spool, timeouts).
var outputFindingActions = map[string]string{
	"not_found":              "write %s before the command exits, or remove it from `outputs`.",
	"not_regular":            "write %s as one regular file (for example, archive a directory), or declare the files inside it.",
	"unsafe_path":            "declare %s as a relative path inside the job's working directory.",
	"wrong_type":             "make every parent of %s a real directory (no symlinked or file parents).",
	"permission_denied":      "make %s readable by the job's user.",
	"changed":                "finish writing %s (and stop background writers) before the command exits.",
	"baseline_mismatch":      "compare %s with the native baseline and fix the program or its declared tolerance.",
	"variation_exceeded":     "compare %s with the native baseline and fix the program or its declared variation.",
	"invalid_numeric_output": "write %s as matching, nonempty JSON numeric arrays.",
	"check_output_too_large": "reduce the size of %s.",
}

func outputFinding(d *FinalizationDetail) *JobFinding {
	output := "the declared output"
	if d.Output != "" {
		output = strconv.Quote(d.Output)
	}
	ran := "Your command finished"
	if d.CommandExitCode != nil {
		ran = "Your command exited 0"
	}
	problem := finalizationErrors[d.Error]
	if action, ok := outputFindingActions[d.Error]; ok {
		return &JobFinding{"output_" + d.Error, findingOwnerJob,
			fmt.Sprintf("%s, but %s broke the output contract at %s (%s), so Mycelium failed the job instead of accepting a partial result.", ran, output, strings.ReplaceAll(d.Step, "_", " "), problem),
			fmt.Sprintf(action, output)}
	}
	return &JobFinding{"output_publication", findingOwnerMycelium,
		fmt.Sprintf("%s, but Mycelium could not save %s at %s (%s).", ran, output, strings.ReplaceAll(d.Step, "_", " "), problem),
		"resubmit; if it repeats, report the job ID to the queue owner."}
}

func resourceStopFinding(j *Job, c *RuntimeCause) *JobFinding {
	switch c.Class {
	case rssLimitExceeded:
		what := "Mycelium stopped your command because its memory use passed the limit resolved from your declaration."
		action := "declare more memory (`needs.host_memory_gb`, or `needs.memory: auto`) or reduce the job's memory use."
		if e := c.RSSLimit; e != nil {
			what = fmt.Sprintf("Mycelium stopped your command because its sampled memory reached %s, above the %s limit resolved from your declaration.", gib(float64(e.ObservedBytes)), gib(float64(e.LimitBytes)))
			action = fmt.Sprintf("declare at least %s (`needs.host_memory_gb`, or `needs.memory: auto`) or reduce the job's memory use.", gib(float64(e.ObservedBytes)))
		}
		return &JobFinding{"memory_limit", findingOwnerJob, what, action}
	case wallTimeLimit:
		limit := "its declared"
		if j.Spec.WallTime != "" {
			limit = "its declared " + j.Spec.WallTime
		}
		return &JobFinding{"wall_time_limit", findingOwnerJob,
			fmt.Sprintf("Mycelium stopped your command when it reached %s wall_time.", limit),
			"raise `wall_time`, or checkpoint and mark the job resumable so a retry continues."}
	case diskLimitExceeded:
		return &JobFinding{"disk_limit", findingOwnerJob,
			"The host denied further writes because the attempt reached its declared disk bound.",
			"raise `disk` (input, output or checkpoint bytes) to what the job writes, or write less."}
	case "oom":
		return &JobFinding{"out_of_memory", findingOwnerJob,
			"The host's memory guard stopped your command for running out of memory.",
			"declare more memory (`needs.host_memory_gb`, or `needs.memory: auto`) or reduce the job's memory use."}
	}
	return nil
}

var ceilingReason = regexp.MustCompile(`^command failed: exceeded its ([0-9.]+) GiB memory ceiling$`)

func memoryExhaustedFinding(j *Job, raw string) *JobFinding {
	reduce := "reduce the job's memory use or split the work into smaller jobs."
	switch {
	case raw == "out of memory after one retry on a larger hypha", raw == "out of memory after one retry above the OOM envelope":
		return &JobFinding{"out_of_memory", findingOwnerJob,
			"Your command ran out of memory, and again after Mycelium retried it once with more memory.", reduce}
	case raw == "out of memory; previous capability unavailable":
		return &JobFinding{"out_of_memory", findingOwnerJob,
			"Your command ran out of memory, and no host with more memory is available to retry it.", reduce}
	case strings.HasPrefix(raw, "needs more memory than any eligible host allows"):
		return &JobFinding{"memory_exceeds_hosts", findingOwnerJob,
			"Your command ran out of memory, and the memory it needs " + strings.TrimPrefix(raw, "needs more memory than any eligible host allows ") + " exceeds every host it may run on.",
			"reduce the job's memory use, or allow it on hosts with more memory."}
	}
	if m := ceilingReason.FindStringSubmatch(raw); m != nil {
		return &JobFinding{"memory_limit", findingOwnerJob,
			fmt.Sprintf("The host's memory ceiling stopped your command at its %s GiB declaration.", m[1]),
			"declare more memory (`needs.host_memory_gb`, or `needs.memory: auto`) or reduce the job's memory use."}
	}
	return nil
}

func launchFinding(c *RuntimeCause) *JobFinding {
	switch {
	case c.Class == machoLibraryMissing:
		what := "Your binary needs a non-system library the host does not have, so it never started."
		if l := c.Linkage; l != nil {
			what = fmt.Sprintf("Your binary %q needs the non-system library %q, which the host does not have, so it never started.", l.Binary, l.Library)
		}
		return &JobFinding{"library_missing", findingOwnerJob, what, "bundle and relink the library, or build a self-contained binary."}
	case c.Class == "tools_missing" && len(c.MissingTools) > 0:
		return &JobFinding{"tools_missing", findingOwnerJob,
			fmt.Sprintf("The host lacked the tools %s, so your command never started.", strings.Join(c.MissingTools, ", ")),
			"declare the tools in `needs.tools` so placement picks a host that has them, or bundle them."}
	case c.Operation == "input_staging" && (c.Class == "input_unavailable" || c.Class == "invalid_input"):
		return &JobFinding{"input_unavailable", findingOwnerJob,
			fmt.Sprintf("Mycelium could not stage a declared input before running (%s).", c.String()),
			"check that every declared input exists, is readable and is unchanged, then resubmit."}
	case c.Predicate == "command_start" || c.Predicate == "environment_prepare" || c.Predicate == "native_workdir_containment":
		return &JobFinding{"launch_" + c.Predicate, findingOwnerJob,
			"Your command never started: " + c.Reason + ".",
			"fix the command or environment declaration, then resubmit."}
	}
	return nil
}

func dependencyFinding(j *Job) *JobFinding {
	switch j.DiagnosticCode {
	case "upstream_failed":
		return &JobFinding{"upstream_failed", findingOwnerJob,
			"An upstream job this one depends on produced no accepted output, so this job never ran.",
			"fix and resubmit the upstream job, then resubmit this one."}
	case "result_expired":
		return &JobFinding{"upstream_expired", findingOwnerJob,
			"The upstream outputs this job depends on expired before it could run.",
			"resubmit the upstream job, then this one."}
	}
	return nil
}

var signalName = regexp.MustCompile(`^SIG[A-Z0-9]{2,8}$`)

func signalFinding(j *Job) *JobFinding {
	if len(j.AttemptHistory) == 0 {
		return nil
	}
	last := j.AttemptHistory[len(j.AttemptHistory)-1]
	e := last.SignalEvidence
	if e == nil || last.Attempt != j.Attempt || !signalName.MatchString(e.Signal) {
		return nil
	}
	switch e.Signal {
	case "SIGSEGV", "SIGBUS", "SIGILL", "SIGFPE", "SIGABRT", "SIGTRAP", "SIGSYS":
		return &JobFinding{"signal_crash", findingOwnerJob,
			fmt.Sprintf("Your command crashed with %s; Mycelium did not stop it.", e.Signal),
			"reproduce the crash locally or read the job's log (`myce logs`) and fix the program."}
	}
	return &JobFinding{"signal_unknown_sender", findingOwnerUnknown,
		fmt.Sprintf("Your command ended on %s; Mycelium recorded no stop of its own, so the sender is unknown.", e.Signal),
		"check the job's log and memory use (`myce logs`), then resubmit."}
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}

func gib(bytes float64) string { return fmt.Sprintf("%.1f GiB", bytes/(1<<30)) }
