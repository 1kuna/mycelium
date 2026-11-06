package myce

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// Findings are a job-local explanation, never scheduling or release authority.
// Confidence describes the evidence; Cause stays unknown unless established.
// No raw error, argv, host identity, path, or another job enters this model.
type JobFinding struct {
	Code           string            `json:"code"`
	Classification string            `json:"classification"` // behaviour, infrastructure, waiting
	Attempt        int               `json:"attempt,omitempty"`
	Message        string            `json:"message"`
	Evidence       []FindingEvidence `json:"evidence"`
	Action         string            `json:"action"`
	NextStep       string            `json:"next_step"`
	Confidence     string            `json:"confidence"` // measured or unknown
	Cause          string            `json:"cause"`
}

type FindingEvidence struct {
	Metric     string   `json:"metric"`
	Value      *float64 `json:"value,omitempty"`
	Declared   *float64 `json:"declared,omitempty"`
	Learned    *float64 `json:"learned,omitempty"`
	Samples    int      `json:"samples,omitempty"`
	Unit       string   `json:"unit,omitempty"`
	Source     string   `json:"source"`
	Limitation string   `json:"limitation,omitempty"`
}

// Reuse accepted work identity, eligibility and retention. A range is observed
// variation, not a confidence interval or a universal workload multiplier.
type FindingRange struct {
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`
	Samples int     `json:"samples"`
}
type FindingBehavior struct {
	Project    string        `json:"project"`
	CPU        *FindingRange `json:"cpu,omitempty"`
	Processes  *FindingRange `json:"processes,omitempty"`
	RecordedAt time.Time     `json:"recorded_at"`
}

func (r *FindingRange) upper() float64 {
	if r == nil {
		return 0
	}
	return r.Max + (r.Max - r.Min)
}
func findingRange(r *FindingRange, value float64) *FindingRange {
	if !validMemoryValue(value) {
		return r
	}
	if r == nil {
		return &FindingRange{Min: value, Max: value, Samples: 1}
	}
	return &FindingRange{Min: min(r.Min, value), Max: max(r.Max, value), Samples: r.Samples + 1}
}
func (q *Queue) findingBehaviorBaseline(j *Job) *FindingBehavior {
	if j.Timing == nil || j.Timing.Key == nil {
		return nil
	}
	point := q.s.WorkObservations[identityHash(j.Timing.Key)]
	b := point.FindingBehavior
	if point.StaleReason != "" || point.Forbidden || b == nil || b.Project != j.Spec.Project || q.clock.Now().Before(b.RecordedAt) || q.clock.Now().Sub(b.RecordedAt) > workCurveMaxAge {
		return nil
	}
	return b // immutable; the accepted writer always replaces it.
}
func (q *Queue) recordFindingBehavior(j *Job, r Result, old WorkObservation, point *WorkObservation, eligible bool) {
	point.FindingBehavior = old.FindingBehavior
	// Reuse memory's clean accepted-attempt gate; interrupted/constrained runs
	// cannot normalize an incident. Incomplete CPU counters cannot teach a bound.
	if !eligible || !r.Success || r.Resources == nil {
		return
	}
	b := FindingBehavior{Project: j.Spec.Project}
	if old.FindingBehavior != nil && old.FindingBehavior.Project == j.Spec.Project && q.clock.Now().Sub(old.FindingBehavior.RecordedAt) <= workCurveMaxAge && old.StaleReason == "" {
		b = *old.FindingBehavior
	}
	v := r.Resources
	if v.CPU.UsageSeconds != nil && !v.CPU.LowerBound && v.CPU.Unavailable == "" && validRate(v.ElapsedSeconds) {
		b.CPU = findingRange(b.CPU, *v.CPU.UsageSeconds/v.ElapsedSeconds)
	}
	if v.ProcessPeak != nil {
		b.Processes = findingRange(b.Processes, float64(*v.ProcessPeak))
	}
	b.RecordedAt = q.clock.Now()
	point.FindingBehavior = &b
}

func findingValue(v float64) *float64 { return &v }
func newJobFinding(code, class string, attempt int, message, next string, evidence ...FindingEvidence) JobFinding {
	return JobFinding{Code: code, Classification: class, Attempt: attempt, Message: message, Evidence: evidence, Action: "retained the observation; execution policy is unchanged", NextStep: next, Confidence: "measured", Cause: "unknown"}
}

// Called only on an already detached observation generation, before payload
// redaction. Work is bounded by this job's retained facts. Status cache and
// targeted GET share this projection; there is no history scan or I/O.
func (q *Queue) projectJobFindings(j *Job) {
	j.Findings = nil
	j.AttemptHistory = slices.Clone(j.AttemptHistory)
	add := func(f JobFinding) { j.Findings = append(j.Findings, f) }
	for _, facts := range j.MemoryAttempts {
		p, e := facts.MemoryPeak, facts.MemoryEnvelope
		if p == nil || e == nil {
			continue
		}
		observed := p.HostGB
		source, limitation := "attempt resident memory samples", "sampled peak; unseen spikes are unknown"
		if p.Source == processGroupMemorySource {
			source, limitation = "process-group RSS sum", "shared pages can be counted repeatedly; compressed pages and unseen processes can be missed; not physical RAM use"
		} else if p.CgroupGB != nil {
			observed, source = p.CgroupGB, "attempt cgroup high-water mark"
		}
		limit := e.HostGB
		evidence := FindingEvidence{Metric: "host_memory", Value: observed, Unit: "GiB", Source: source, Limitation: limitation}
		if e.Basis == "declared" || e.Basis == "hint" {
			evidence.Declared = findingValue(e.HostGB)
		}
		if e.Learned != nil && e.Learned.Evidence > 0 {
			evidence.Learned, evidence.Samples = findingValue(e.Learned.HostGB), e.Learned.Evidence
			limit = max(limit, e.Learned.HostGB)
		}
		if observed != nil && validMemoryValue(*observed) && limit > 0 && *observed > limit && (evidence.Declared != nil || evidence.Learned != nil) {
			add(newJobFinding("memory_over_declaration", "behaviour", facts.Attempt, fmt.Sprintf("Measured %s peak %.3g GiB exceeds the %.3g GiB declaration/learned envelope.", source, *observed, limit), "Inspect your job's memory and child-process concurrency; use the measured scope when correcting needs or use memory: auto. The cause is unknown; a process profile would establish it.", evidence))
		}
	}
	for _, facts := range j.ResourceAttempts {
		r := facts.Resources
		if r == nil {
			continue
		}
		b := facts.Baseline
		if r.CPU.UsageSeconds != nil && validRate(r.ElapsedSeconds) && facts.CPUCores > 0 {
			cores := *r.CPU.UsageSeconds / r.ElapsedSeconds
			e := FindingEvidence{Metric: "average_cpu", Value: findingValue(cores), Declared: findingValue(float64(facts.CPUCores)), Unit: "cores", Source: "attempt CPU seconds / observed elapsed seconds", Limitation: "includes attempt wrappers; comparable accepted CPU history is unknown"}
			limit := float64(facts.CPUCores)
			if b != nil && b.CPU != nil {
				limit = max(limit, b.CPU.upper())
				e.Learned = findingValue(b.CPU.upper())
				e.Samples = b.CPU.Samples
				e.Limitation = "learned upper comparison is the accepted maximum plus observed range"
			}
			if r.CPU.LowerBound {
				e.Limitation += "; CPU counter is a sampled lower bound"
			}
			if validMemoryValue(cores) && cores > limit {
				add(newJobFinding("cpu_over_declaration", "behaviour", facts.Attempt, fmt.Sprintf("Measured CPU use averaged %.3g cores, exceeding the %.3g core declaration/learned comparison.", cores, limit), "Inspect thread and child-process parallelism; declare the cores the job uses or reduce concurrency. Cause unknown; a CPU profile would establish it.", e))
			}
		}
		if r.ProcessPeak != nil && r.ProcessInitial != nil && facts.CPUCores > 0 {
			// The first observed live tree plus declared runnable parallelism
			// provides a workload-relative fan-out comparison, never a process cap.
			limit := float64(*r.ProcessInitial) + float64(facts.CPUCores)
			e := FindingEvidence{Metric: "process_count", Value: findingValue(float64(*r.ProcessPeak)), Declared: findingValue(float64(facts.CPUCores)), Unit: "processes", Source: "attempt process-tree/cgroup samples", Limitation: "declared is CPU parallelism, not a process limit; first live process sample plus declared cores is the cold comparison; short-lived processes and fan-out before the initial sample can be missed"}
			if b != nil && b.Processes != nil {
				limit = max(limit, b.Processes.upper())
				e.Learned = findingValue(b.Processes.upper())
				e.Samples = b.Processes.Samples
			}
			if r.CPU.Source == "cgroup_v2" {
				e.Source = "attempt cgroup pids.current samples"
				e.Limitation += "; cgroup counts tasks, including threads, rather than processes alone"
			}
			if float64(*r.ProcessPeak) > limit {
				add(newJobFinding("process_fanout", "behaviour", facts.Attempt, fmt.Sprintf("Observed a peak of %d processes/tasks; initial live tree plus declared CPU parallelism/learned history accounts for %.3g.", *r.ProcessPeak, limit), "Inspect process creation and nested test/worker pools; bound parallelism to intended work. Cause unknown; a process creation trace would establish it.", e))
			}
		}
	}
	for i := range j.AttemptHistory {
		a := &j.AttemptHistory[i]
		a.Findings = nil
		for _, f := range j.Findings {
			if f.Attempt == a.Attempt {
				a.Findings = append(a.Findings, f)
			}
		}
		appendOutcome := func(f JobFinding) { a.Findings = append(a.Findings, f); add(f) }
		if c := safeRuntimeCause(a.ExecutionCause); c != nil {
			if guard := findingGuard(c, a.Attempt); guard != nil {
				appendOutcome(*guard)
			}
		}
		if findingInfrastructure(*a) {
			f := newJobFinding("infrastructure_interruption", "infrastructure", a.Attempt, "Infrastructure interrupted this attempt; this is not your job's fault. The underlying cause is unknown.", "Keep the same job ID while Mycelium retries. If terminal, give the queue owner the job ID and attempt so they can inspect worker contact and launch evidence.", FindingEvidence{Metric: "interrupted_attempt", Value: findingValue(float64(a.Attempt)), Source: "recorded attempt outcome"})
			f.Confidence = "unknown"
			if a.State == "queued" {
				f.Action = "requeued the same job through its existing retry policy"
			} else {
				f.Action = "ended the attempt; inspect the job state for the retry outcome"
			}
			appendOutcome(f)
		}
	}
	if j.EnvironmentBuildFailure != nil || strings.HasPrefix(j.Reason, "environment failed:") || j.Blocker != nil && j.Blocker.Code == "deterministically_failed" {
		f := newJobFinding("environment_build_failed", "behaviour", 0, "Environment preparation failed before your command could run. The package/build cause is unknown from safe structured evidence.", "Read this job's build log with myce logs; correct the locked recipe or missing dependency and submit changed inputs. With logs disabled, reproduce the build with logging enabled.", FindingEvidence{Metric: "build_outcome", Source: "recorded deterministic environment build failure"})
		f.Confidence = "unknown"
		f.Action = "withheld command launch; retained the failed environment identity"
		if b := j.EnvironmentBuildFailure; b != nil {
			f.Evidence = append(f.Evidence, FindingEvidence{Metric: "build_elapsed", Value: findingValue(b.BuildSeconds), Unit: "seconds", Source: "worker build report"})
			if cause := environmentReadCause(b.ReadCause); cause != "" {
				f.Cause, f.Confidence = cause, "measured"
				f.Message = "Environment preparation failed: " + cause + "."
				f.NextStep = "Correct the environment binary in the locked recipe and submit changed inputs; inspect this job's build log for the failed read."
			}
		}
		add(f)
	}
	if strings.HasPrefix(j.Reason, "environment retry pending:") {
		f := newJobFinding("environment_build_retry", "infrastructure", 0, "Environment preparation failed and is awaiting a retry. The underlying cause is unknown; this does not establish a problem in your command.", "Keep the same job ID while preparation retries; inspect this job's build log to establish the failure cause.", FindingEvidence{Metric: "build_retry", Source: "queue environment retry decision"})
		f.Confidence = "unknown"
		f.Action = "withheld command launch and retained the environment retry"
		add(f)
	}
	if j.State == "queued" && j.Pause == nil && j.Blocker != nil {
		seconds := max(0, q.clock.Now().Sub(j.Queued).Seconds())
		f := findingWait(j, seconds)
		add(f)
	}
}

// Share the queue's recorded outcome flags with operator-side insights. Shell
// status 137/139, stderr and stop intent never establish a crash or root cause.
func findingInfrastructure(a AttemptOutcome) bool {
	if !a.Interrupted || a.OutOfMemory || a.PoliteStop || a.OperatorStop || a.PriorityStop != nil || a.State == "cancelled" || strings.HasPrefix(a.Reason, "production start deadline exceeded") {
		return false
	}
	if c := a.ExecutionCause; c != nil {
		if c.Class == "invalid_input" || c.Class == "input_unavailable" || c.Class == "tools_missing" || c.Predicate == "native_workdir_containment" {
			return false
		}
	}
	return true
}

func findingGuard(c *RuntimeCause, attempt int) *JobFinding {
	var reason string
	e := FindingEvidence{Metric: "guard_stop", Source: "recorded exact-attempt execution cause"}
	switch c.Class {
	case rssLimitExceeded:
		p := safeRSSLimit(c.RSSLimit)
		if p == nil {
			return nil
		}
		reason = "the sampled RSS guard observed memory above the resolved limit"
		e.Metric, e.Unit, e.Value, e.Declared = "rss_guard", "bytes", findingValue(float64(p.ObservedBytes)), findingValue(float64(p.LimitBytes))
	case wallTimeLimit:
		reason = "the declared execution wall-time guard expired"
	case diskLimitExceeded:
		reason = "the storage limit guard refused further writes"
	default:
		return nil
	}
	f := newJobFinding("guard_stop", "behaviour", attempt, "Mycelium stopped this attempt because "+reason+".", "Inspect the recorded resource use and reduce demand or correct the declaration before retrying. The reason for the excess is unknown; profile the workload.", e)
	f.Action = "stopped this attempt through the existing guard and cleanup policy"
	f.Cause = reason
	return &f
}

func findingWait(j *Job, seconds float64) JobFinding {
	b := j.Blocker
	// Blocker text can name other projects' holders, hosts and build paths.
	// Translate only its finite code, never copy that text into owner findings.
	message, next, confidence := "Required capacity or preparation is unavailable.", "Keep this job queued; ask the queue owner to inspect its placement blocker.", "measured"
	switch b.Code {
	case "building":
		message, next = "The environment builder reports preparation in progress.", "Wait for preparation; inspect this job's build logs if progress stalls."
	case "unprepared":
		message, next = "A required environment or upstream result is not ready.", "Keep this job queued; complete its declared upstream result or let Mycelium prepare the environment."
	case "waiting_for_builder":
		message, next = "An eligible environment builder is not available yet.", "Keep this job queued; ask the queue owner to restore builder availability if the wait persists."
	case "unknown_observation":
		message, next, confidence = "The queue lacks fresh observations needed to place this job.", "Ask the queue owner to restore worker reports; availability and the underlying cause are unknown.", "unknown"
	case "waiting_for_worker":
		message, next = "The job is waiting for an eligible worker assignment.", "Keep this job queued; an eligible worker must check in."
	}
	if j.DiagnosticCode == "host_memory_headroom" || j.DiagnosticCode == memoryEvidenceRecovering {
		message = "The job's resolved memory need does not fit current admitted headroom."
		next = "Wait for capacity; compare declared and learned memory in status. Correct a stale declaration or allow additional compatible targets after checking whole-payload compatibility and required output parity."
	}
	if j.PlacementIntent != nil && len(j.PlacementIntent.Targets) > 0 {
		next += " Explicit targets restrict placement; allow other OS/architecture targets only after verifying the complete payload and required output parity."
	}
	f := newJobFinding("waiting", "waiting", 0, fmt.Sprintf("Waiting %.0f seconds: %s", seconds, message), next, FindingEvidence{Metric: "queued_elapsed", Value: findingValue(seconds), Unit: "seconds", Source: "queue clock and submission timestamp", Limitation: "elapsed is job age, including any earlier attempts; it does not prove an abnormal delay"})
	f.Action = "kept the same job queued without bypassing placement or release fences"
	f.Confidence = confidence
	return f
}

func JobFindingsText(findings []JobFinding) string {
	var lines []string
	for _, f := range findings {
		attempt := ""
		if f.Attempt > 0 {
			attempt = fmt.Sprintf(" attempt %d", f.Attempt)
		}
		text := fmt.Sprintf("finding%s [%s; confidence=%s]: %s Cause: %s. Mycelium: %s. Next: %s", attempt, f.Classification, f.Confidence, f.Message, f.Cause, f.Action, f.NextStep)
		var evidence []string
		for _, e := range f.Evidence {
			value := e.Metric
			if e.Value != nil {
				value += fmt.Sprintf("=%.3g %s", *e.Value, e.Unit)
			}
			if e.Declared != nil {
				value += fmt.Sprintf("; declared=%.3g", *e.Declared)
			}
			if e.Learned != nil {
				value += fmt.Sprintf("; learned=%.3g (%d samples)", *e.Learned, e.Samples)
			}
			value += "; source=" + e.Source
			if e.Limitation != "" {
				value += "; " + e.Limitation
			}
			evidence = append(evidence, value)
		}
		text += " Evidence: " + strings.Join(evidence, " | ")
		lines = append(lines, strings.Join(strings.Fields(text), " "))
	}
	return strings.Join(lines, "\n")
}

// Only finite, typed failed-read facts survive environment preparation. Raw
// installer errors/logs are available through the existing job log, not callbacks.
type EnvironmentBuildFailure struct {
	BuildSeconds float64 `json:"build_seconds"`
	ReadCause    string  `json:"read_cause,omitempty"`
}

func environmentBuildFailure(r EnvironmentReport) *EnvironmentBuildFailure {
	b := &EnvironmentBuildFailure{}
	if validMemoryValue(r.BuildSeconds) {
		b.BuildSeconds = r.BuildSeconds
	}
	if d := safeWorkerPathDiagnostic(r.PathDiagnostic); d != nil {
		b.ReadCause = d.Cause
	}
	return b
}
func environmentReadCause(c string) string {
	switch c {
	case "missing":
		return "the environment binary read found no file"
	case "unreadable":
		return "the environment binary read was denied permission"
	case "wrong_type":
		return "the environment binary read encountered the wrong file type"
	case "read_failed":
		return "the environment binary read failed; the underlying cause is unknown"
	}
	return ""
}
