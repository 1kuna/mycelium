package myce

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// These are runtime facts, not adapter conclusions. Intent is deliberately absent.
// Times are wall-clock evidence (not scheduling deadlines). A signal is added
// only after successful delivery and persistence; exit observation is immutable.
type deliveredSignal struct {
	Signal   string    `json:"signal"`
	At       time.Time `json:"at"`
	Shutdown bool      `json:"shutdown,omitempty"`
	// Native observation and delivery share a lock, proving this ordering even
	// when wall-clock timestamps tie. Older and daemon receipts use timestamps.
	BeforeExit bool `json:"before_exit,omitempty"`
}

// Separate from process.mu: an exit can be observed while control/persistence
// holds process.mu. Delivery and observation arbitrate here; neither waits for
// a running payload while holding this lock. Always release it before taking
// process.mu in the waiter. Senders take process.mu first.
type nativeExitObservation struct {
	mu   sync.Mutex
	exit *memberExit
}

func (o *nativeExitObservation) observe(cmd *exec.Cmd) *memberExit {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.exit == nil {
		o.exit = observedNativeExit(cmd, time.Now().UTC())
	}
	return o.exit
}

func (signal deliveredSignal) before(exit *memberExit) bool {
	return exit != nil && !signal.At.IsZero() && (signal.Signal == "TERM" || signal.Signal == "KILL") && (signal.BeforeExit || signal.At.Before(exit.ObservedAt))
}

type memberExit struct {
	CoreDumped bool            `json:"core_dumped,omitempty"`
	Signal     string          `json:"signal,omitempty"`
	Evidence   *SignalEvidence `json:"signal_evidence,omitempty"`
	Status     int             `json:"status"`
	ObservedAt time.Time       `json:"observed_at"`
}
type infrastructureFact struct {
	Kind             string           `json:"kind"`
	At               time.Time        `json:"at"`
	Member           string           `json:"member,omitempty"`
	Port             string           `json:"port,omitempty"`
	Group            int              `json:"group,omitempty"`
	Predicate        string           `json:"predicate,omitempty"`
	Admission        *AdmissionDetail `json:"admission,omitempty"`
	PrelaunchRefused bool             `json:"prelaunch_refused,omitempty"`
}

const (
	inputStagingFailed = "input_staging_failed"
	launchRefused      = "launch_refused"
	launchFailed       = "launch_failed"
	environmentFailed  = "environment_failed"
	foreignPort        = "foreign_port"
)

type memberOutcome struct {
	RSSLimit        *RSSLimitEvidence    `json:"rss_limit,omitempty"`
	Infrastructure  []infrastructureFact `json:"infrastructure_facts,omitempty"`
	Signals         []deliveredSignal    `json:"delivered_signals,omitempty"`
	Exit            *memberExit          `json:"exit,omitempty"`
	ObservationLost bool                 `json:"observation_lost,omitempty"`
}
type attemptCause struct {
	PriorityStop *PriorityPreemption
	memberOutcome
	OutOfMemory bool
	Cancelled   bool
	Stale       bool
}

func (facts memberOutcome) deliveredBeforeExit() bool {
	if facts.Exit == nil {
		return false
	}
	for _, signal := range facts.Signals {
		if signal.before(facts.Exit) {
			return true
		}
	}
	return false
}

func (facts memberOutcome) recordedInfrastructure() bool {
	for _, fact := range facts.Infrastructure {
		if fact.At.IsZero() {
			continue
		}
		switch fact.Kind {
		case launchRefused, launchFailed, environmentFailed, inputStagingFailed:
			return true
		case foreignPort:
			if fact.Port != "" && fact.Group > 0 {
				return true
			}
		}
	}
	return false
}

// The wrapper carries only facts already saved by Mycelium, preserving error
// identity for callers that retry admission or retain unconfirmed custody.
type recordedStartError struct {
	cause error
	facts memberOutcome
}

func (e *recordedStartError) Error() string { return e.cause.Error() }
func (e *recordedStartError) Unwrap() error { return e.cause }

func (s *workerStore) startFailure(j Job, kind string, cause error) error {
	return s.startFailureAt(j, kind, cause, time.Now().UTC())
}
func (s *workerStore) startFailureAt(j Job, kind string, cause error, at time.Time) error {
	// Identity alone is assigned before admission. Custody/launch intent, in
	// contrast, is written before any process can start and is never inferred
	// from an absent Started report. Persist this positive refusal proof with
	// the facts so release-only replay carries it too.
	prelaunch := kind == launchRefused && s.state.CustodyID == "" && s.state.PID == 0 && !s.state.LaunchPending && len(s.state.Groups) == 0 && s.state.Preparation == "" && s.state.Cgroup == ""
	facts := memberOutcome{Infrastructure: []infrastructureFact{{Kind: kind, At: at, Predicate: launchFailurePredicate(kind, cause), Admission: admissionDetail(cause, j), PrelaunchRefused: prelaunch}}}
	s.state.LaunchOutcome = fmt.Sprintf("launch-%s-%d.outcome.json", j.ID, j.Attempt)
	if err := saveWorkerJSON(s.dir, s.state.LaunchOutcome, facts); err != nil {
		return errors.Join(cause, err)
	}
	return &recordedStartError{cause: cause, facts: facts}
}
func startFailureOutcome(owner Result, err error) Result {
	var observed *executionEvidenceError
	if errors.As(err, &observed) {
		owner.ExecutionEvidence = safeExecutionEvidence(observed.evidence, owner.Attempt)
	}
	var recorded *recordedStartError
	facts := memberOutcome{}
	if errors.As(err, &recorded) {
		facts = recorded.facts
	}
	var staging *runtimeError
	if errors.As(err, &staging) && staging.diagnostic.Operation == "input_staging" {
		owner.ExecutionCause = runtimeDiagnostic(staging)
	}
	var rejection *rejectionError
	if errors.As(err, &rejection) && rejection.code == cpuIsolationUnavailable {
		owner.ExecutionCause = &RuntimeCause{Operation: "launch", Class: cpuIsolationUnavailable}
	}
	if errors.Is(err, errWorkerMemory) {
		owner.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit"}
	}
	if errors.Is(err, errWorkerStartAuthorization) {
		owner.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("start_authorization", err))
	}
	owner = classifyAttemptOutcome(owner, attemptCause{memberOutcome: facts})
	if errors.As(err, &staging) && (staging.diagnostic.Class == "tools_missing" || staging.diagnostic.Class == machoLibraryMissing) {
		owner.ExecutionCause = runtimeDiagnostic(staging)
	}
	if execFormatFailure(err) && !owner.OutOfMemory {
		// The payload cannot execute: this is an ordinary command failure, not
		// infrastructure loss. Preserve existing cancellation and stop authority.
		if !facts.deliveredBeforeExit() {
			owner.Interrupted = false
			owner.ExecutionCause = &RuntimeCause{Operation: "command", Class: "command_exit"}
			owner.Error = execFormatMessage
		}
	}
	return owner
}

func classifyAttemptOutcome(result Result, facts attemptCause) Result {
	if facts.Exit != nil {
		result.SignalEvidence = facts.Exit.Evidence
	}
	if e := safeRSSLimit(facts.RSSLimit); e != nil {
		result.Success, result.OutOfMemory = false, true
		result.ExecutionCause = &RuntimeCause{Operation: "command", Class: rssLimitExceeded, RSSLimit: e}
		result.Error = e.String()
	}
	result.OutOfMemory = result.OutOfMemory || facts.OutOfMemory
	diskFailure := attemptDiskLimitFailure(result)
	result.Interrupted = false
	result.PriorityStop = nil
	if facts.Stale || facts.Cancelled {
		result.Success, result.Forced = false, false
		return result
	}
	if result.OutOfMemory || facts.Exit != nil && facts.Exit.Status != 0 {
		result.Success = false
	}
	// A checkpoint handler can exit zero after TERM. Delivered priority/service
	// stops interrupt that partial attempt; intent or a signal after observed exit
	// cannot override an already completed payload.
	shutdownDelivered := false
	if facts.Exit != nil {
		for _, signal := range facts.Signals {
			if signal.Shutdown && signal.before(facts.Exit) {
				shutdownDelivered = true
			}
		}
	}
	if (facts.PriorityStop != nil && facts.deliveredBeforeExit() || shutdownDelivered) && !result.OutOfMemory && !diskFailure {
		result.Success = false
	}
	if !result.Success && !result.OutOfMemory && !diskFailure {
		result.Interrupted = facts.ObservationLost || facts.deliveredBeforeExit() || facts.recordedInfrastructure()
	}
	if !result.Interrupted {
		result.Forced = false
	}
	// Preserve pre-execution refusal/failure facts for supported diagnostics.
	// These are the same recorded facts that established interruption above.
	if result.Interrupted && result.ExecutionCause == nil {
		for _, fact := range facts.Infrastructure {
			if fact.At.IsZero() {
				continue
			}
			switch fact.Kind {
			case launchRefused, launchFailed, environmentFailed:
				class := fact.Kind
				if fact.Kind == launchRefused && fact.Predicate == "memory_capacity" {
					class = "resource_limit"
				}
				result.ExecutionCause = safeRuntimeCause(&RuntimeCause{Operation: "launch", Class: class, Predicate: fact.Predicate, Admission: fact.Admission, PrelaunchRefused: fact.PrelaunchRefused})
			}
			if result.ExecutionCause != nil {
				break
			}
		}
	}
	// A more specific runtime class (for example a transport timeout) retains
	// its classification while carrying the same recorded launch predicate.
	if result.Interrupted && result.ExecutionCause != nil {
		for _, fact := range facts.Infrastructure {
			if !fact.At.IsZero() && fact.Predicate != "" {
				cause := *result.ExecutionCause
				cause.Predicate = fact.Predicate
				cause.Admission = fact.Admission
				cause.PrelaunchRefused = fact.PrelaunchRefused
				result.ExecutionCause = safeRuntimeCause(&cause)
				break
			}
		}
	}
	if facts.Exit != nil && facts.Exit.Status == 137 && !result.Interrupted && !result.OutOfMemory {
		if result.Error != "" {
			result.Error += "; "
		}
		result.Error += "SIGKILL; interruption cause unconfirmed; treated as stage failure"
	}
	return withStopProvenance(result, facts.PriorityStop)
}

// Planned intent can annotate a proven interruption, never manufacture one.
func withStopProvenance(result Result, stop *PriorityPreemption) Result {
	result.PriorityStop = nil
	if result.Interrupted && !result.Success && !result.OutOfMemory {
		result.PriorityStop = stop
	}
	return result
}

// The wait status supplies only the exit status. It never establishes cause.
func observedNativeExit(cmd *exec.Cmd, at time.Time) *memberExit {
	if cmd.ProcessState != nil {
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
			return nativeWaitExit(status, at)
		}
		return &memberExit{Status: cmd.ProcessState.ExitCode(), ObservedAt: at}
	}
	return &memberExit{Status: 1, ObservedAt: at}
}
func nativeWaitExit(status syscall.WaitStatus, at time.Time) *memberExit {
	e := &memberExit{Status: status.ExitStatus(), ObservedAt: at}
	if status.Signaled() {
		e.Status = 128 + int(status.Signal())
		e.Signal = crashSignal(status.Signal())
		e.CoreDumped = status.CoreDump()
	}
	return e
}

// An operator marker is provenance, never evidence of signal delivery.
func operatorOutcome(result Result, stopped bool) Result {
	result.OperatorStop = result.OperatorStop || stopped
	return result
}

// Only these fixed predicates/messages cross the worker boundary. Never derive
// public text from Error(), paths, submitted commands or environment values.
var launchPredicateMessages = map[string]string{
	deviceFaultRecoveryTimeout:              "no eligible device recovery within the maximum production start window",
	"no_eligible_host_enabled":              "no eligible host is enabled",
	"native_workdir_containment":            rejectionMessages["native_workdir_containment"],
	"native_isolation":                      "native isolation setup refused; check managed workspace and isolation support",
	"device_probe_unavailable":              "accelerator inventory probe unavailable before launch",
	"device_identity_mismatch":              "device runtime mapping does not match saved identity",
	"device_budget_unavailable":             "accelerator memory budget unavailable",
	"device_fault_xe":                       "device quarantined after an xe kernel driver fault",
	"device_fault_i915":                     "device quarantined after an i915 kernel driver fault",
	"device_fault_nvidia_xid":               "device quarantined after an NVIDIA Xid",
	"no_matching_device":                    "no matching device is available",
	"cpu_accelerator_isolation_unavailable": "CPU accelerator select-none visibility is unqualified",
	"memory_capacity":                       "free memory unavailable or insufficient for declared needs",
	"environment_prepare":                   "execution environment preparation failed; check environment qualification",
	"command_start":                         "command could not start; check executable and managed environment",
	"launch_authorization":                  "queue launch authorization or assignment was refused; refresh job and host status",
}

func launchFailurePredicate(kind string, cause error) string {
	if kind != launchRefused && kind != launchFailed && kind != environmentFailed {
		return ""
	}
	var rejection *rejectionError
	if errors.As(cause, &rejection) && launchPredicateMessages[rejection.code] != "" {
		if rejection.code == "native_isolation" {
			if nested := launchFailurePredicate(kind, rejection.cause); nested != "launch_authorization" {
				return nested
			}
		}
		return rejection.code
	}
	if errors.Is(cause, errWorkerMemory) {
		return "memory_capacity"
	}
	switch kind {
	case environmentFailed:
		return "environment_prepare"
	case launchFailed:
		return "command_start"
	default:
		return "launch_authorization"
	}
}
