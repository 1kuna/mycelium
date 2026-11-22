package myce

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
)

type launchPhase string

const (
	launchDispatch       launchPhase = "assignment_dispatch"
	launchBinding        launchPhase = "worker_binding"
	launchCustodianSetup launchPhase = "custodian_setup"
	launchCustodianInit  launchPhase = "custodian_initialization"
	launchStaging        launchPhase = "input_staging"
	launchPrechecks      launchPhase = "post_staging_checks"
	launchCheckpoint     launchPhase = "checkpoint_preparation"
	launchEnvironment    launchPhase = "command_environment"
	launchCommandSetup   launchPhase = "command_setup"
	launchSandbox        launchPhase = "sandbox_setup"
	launchSupervisor     launchPhase = "supervisor_setup"
	launchAuthorization  launchPhase = "launch_authorization"
	launchAdmission      launchPhase = "local_launch_admission"
	launchReserve        launchPhase = "custody_reservation"
	launchReport         launchPhase = "custody_started"
	launchSidecarEnv     launchPhase = "sidecar_environment"
	launchSidecars       launchPhase = "sidecar_startup"
	launchStartChecks    launchPhase = "started_checks"
	launchStartedRPC     launchPhase = "started_rpc"
	launchPayload        launchPhase = "payload_handshake"
	launchRetry          launchPhase = "preparation_retry"
)

var launchPhases = [...]launchPhase{
	launchDispatch, launchCustodianSetup, launchCustodianInit, launchBinding, launchStaging, launchPrechecks,
	launchCheckpoint, launchEnvironment, launchCommandSetup, launchSandbox, launchSupervisor,
	launchAuthorization, launchAdmission, launchReserve, launchReport, launchSidecarEnv,
	launchSidecars, launchStartChecks, launchStartedRPC, launchPayload, launchRetry,
}

const (
	launchExecConfirmed = "exec_confirmed"
	launchGateReleased  = "payload_gate_released"
	launchTimingsHeader = "X-Myce-Launch-Timings"
	launchTimingsWire   = "launch-timings-v1"
)

type LaunchPhaseTiming struct {
	Phase   launchPhase `json:"phase"`
	Seconds float64     `json:"seconds"`
}

// LaunchTimings partitions worker-observed elapsed time into exclusive spans.
// The current phase is also the continuation cursor for the detached runner.
type LaunchTimings struct {
	AssignmentReceivedAt time.Time           `json:"assignment_received_at"`
	ObservedAt           time.Time           `json:"observed_at"`
	InputsReadyAt        *time.Time          `json:"inputs_ready_at,omitempty"`
	PreparationReadyAt   *time.Time          `json:"preparation_ready_at,omitempty"`
	CommandStartAt       *time.Time          `json:"command_start_at,omitempty"`
	CommandStartBoundary string              `json:"command_start_boundary,omitempty"`
	CurrentPhase         launchPhase         `json:"current_phase,omitempty"`
	Phases               []LaunchPhaseTiming `json:"phases"`
}

type launchRecorder struct {
	mu    sync.Mutex
	clock Clock
	value LaunchTimings
}

func newLaunchRecorder(clock Clock, value *LaunchTimings) *launchRecorder {
	if clock == nil || value == nil {
		return nil
	}
	v := *value
	v.Phases = slices.Clone(v.Phases)
	return &launchRecorder{clock: clock, value: v}
}

func (r *launchRecorder) advance(at time.Time) {
	elapsed := max(0, at.Sub(r.value.ObservedAt).Seconds())
	for i, span := range r.value.Phases {
		if span.Phase == r.value.CurrentPhase {
			r.value.Phases[i].Seconds += elapsed
			r.value.ObservedAt = at
			return
		}
	}
	r.value.Phases = append(r.value.Phases, LaunchPhaseTiming{Phase: r.value.CurrentPhase, Seconds: elapsed})
	r.value.ObservedAt = at
}

func (r *launchRecorder) enter(phase launchPhase) launchPhase {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.value.CurrentPhase
	if previous == "" || previous == phase {
		return previous
	}
	now := r.clock.Now()
	r.advance(now)
	r.value.CurrentPhase = phase
	switch {
	case phase == launchPrechecks && r.value.InputsReadyAt == nil:
		r.value.InputsReadyAt = &now
	case phase == launchCommandSetup && r.value.PreparationReadyAt == nil:
		r.value.PreparationReadyAt = &now
	}
	return previous
}

// Restore the surrounding phase so nested RPCs and sandbox work do not count
// twice. An optional phase on workers without a receipt costs no clock reads.
func (r *launchRecorder) within(phase launchPhase) func() {
	previous := r.enter(phase)
	return func() {
		if previous != "" {
			r.enter(previous)
		}
	}
}

func (r *launchRecorder) finish(boundary string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.value.CurrentPhase == "" {
		return
	}
	now := r.clock.Now()
	r.advance(now)
	r.value.CurrentPhase = ""
	if boundary != "" {
		r.value.CommandStartAt = &now
		r.value.CommandStartBoundary = boundary
	}
}

func (r *launchRecorder) snapshot() *LaunchTimings {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.value.snapshot(r.clock.Now())
}

func (t LaunchTimings) snapshot(now time.Time) *LaunchTimings {
	r := launchRecorder{value: t}
	r.value.Phases = slices.Clone(t.Phases)
	if t.CurrentPhase != "" {
		r.advance(now)
	}
	return &r.value
}

func (s *workerStore) launchRecorder(clock Clock) *launchRecorder {
	if s == nil || s.state.Job == nil {
		return nil
	}
	if s.launch == nil {
		s.launch = newLaunchRecorder(clock, s.state.Job.LaunchTimings)
	}
	return s.launch
}

// Call only after the existing exact attempt, machine and session fence.
func retainLaunchTimings(j *Job, t *LaunchTimings) {
	if t == nil || t.AssignmentReceivedAt.IsZero() || t.ObservedAt.Before(t.AssignmentReceivedAt) ||
		len(t.Phases) > len(launchPhases) || t.CurrentPhase != "" && !slices.Contains(launchPhases[:], t.CurrentPhase) {
		return
	}
	if t.CommandStartAt != nil {
		if t.CurrentPhase != "" || !t.CommandStartAt.Equal(t.ObservedAt) ||
			t.CommandStartBoundary != launchExecConfirmed && t.CommandStartBoundary != launchGateReleased {
			return
		}
	} else if t.CommandStartBoundary != "" {
		return
	}
	for _, at := range []*time.Time{t.InputsReadyAt, t.PreparationReadyAt} {
		if at != nil && (at.Before(t.AssignmentReceivedAt) || at.After(t.ObservedAt)) {
			return
		}
	}
	var seen [len(launchPhases)]bool
	for _, p := range t.Phases {
		i := slices.Index(launchPhases[:], p.Phase)
		if i < 0 || seen[i] || p.Seconds < 0 || math.IsNaN(p.Seconds) || math.IsInf(p.Seconds, 0) {
			return
		}
		seen[i] = true
	}
	if old := j.LaunchTimings; old != nil && (old.CurrentPhase == "" || t.CurrentPhase != "" && t.ObservedAt.Before(old.ObservedAt)) {
		return
	}
	j.LaunchTimings = t.snapshot(t.ObservedAt)
}

func (t *LaunchTimings) String() string {
	parts := []string{"launch received=" + t.AssignmentReceivedAt.UTC().Format(time.RFC3339Nano)}
	for _, span := range t.Phases {
		parts = append(parts, fmt.Sprintf("%s=%.3fs", span.Phase, span.Seconds))
	}
	if t.CommandStartAt != nil {
		parts = append(parts, t.CommandStartBoundary+"="+t.CommandStartAt.UTC().Format(time.RFC3339Nano))
	}
	return strings.Join(parts, " ")
}

// Adjacent older queues reject unknown worker fields. Keep local receipts and
// omit them on the old wire, independently of staging and fault telemetry.
func launchTimingWireInput(in any, supported bool) (any, bool) {
	switch c := in.(type) {
	case *Checkin:
		if c != nil {
			return launchTimingWireInput(*c, supported)
		}
	case *Result:
		if c != nil {
			return launchTimingWireInput(*c, supported)
		}
	case Checkin:
		for _, report := range c.Attempts {
			if report.LaunchTimings == nil {
				continue
			}
			if supported {
				return c, true
			}
			c.Attempts = slices.Clone(c.Attempts)
			for i := range c.Attempts {
				c.Attempts[i].LaunchTimings = nil
			}
			return c, false
		}
	case Result:
		if supported {
			return c, c.LaunchTimings != nil
		}
		c.LaunchTimings = nil
		return c, false
	}
	return in, false
}
