package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestAttemptOutcomeCause(t *testing.T) {
	before := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name                                     string
		signal                                   time.Time
		lost, oom, cancelled, stale, interrupted bool
	}{
		{name: "no-delivery"},
		{name: "delivered-before-exit", signal: before, interrupted: true},
		{name: "delivered-at-exit", signal: before.Add(time.Second)},
		{name: "delivered-after-exit", signal: before.Add(2 * time.Second)},
		{name: "observation-loss", lost: true, interrupted: true},
		{name: "oom-after-delivery", signal: before, oom: true},
		{name: "cancelled-after-delivery", signal: before, cancelled: true},
		{name: "stale-after-delivery", signal: before, stale: true},
	} {
		for _, code := range []int{7, 137, 143, 255} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, code), func(t *testing.T) {
				facts := attemptCause{memberOutcome: memberOutcome{Exit: &memberExit{Status: code, ObservedAt: before.Add(time.Second)}, ObservationLost: tc.lost}, OutOfMemory: tc.oom, Cancelled: tc.cancelled, Stale: tc.stale}
				if !tc.signal.IsZero() {
					facts.Signals = []deliveredSignal{{Signal: "TERM", At: tc.signal}}
				}
				r := classifyAttemptOutcome(Result{Forced: true}, facts)
				if r.Interrupted != tc.interrupted || r.OutOfMemory != tc.oom || r.Success {
					t.Fatal(r)
				}
				if !r.Interrupted && r.Forced {
					t.Fatal("stage/cancellation/stale outcome forced", r)
				}
			})
		}
	}
}

func TestDockerStandaloneKillDiagnostic(t *testing.T) {
	// A standalone container has no group observation to populate AmbiguousKill.
	for _, stopIntent := range []bool{false, true} {
		// #149: stop intent can exist even when the kill reply timed out.
		r := dockerOutcome(dockerExecution{ExitCode: 137, InfrastructureStop: stopIntent,
			CleanupCause: &RuntimeCause{Operation: "kill", Class: "timeout"}}, Result{}, nil)
		if r.Success || r.Interrupted || !strings.Contains(r.Error, "SIGKILL; interruption cause unconfirmed") {
			t.Fatal(r)
		}
	}
}

func TestDockerKnownMemoryFailureIsNotAmbiguous(t *testing.T) {
	const failure = "Docker memory ceiling not enforced"
	r := dockerOutcome(dockerExecution{ExitCode: 137, MemoryFailure: failure}, Result{}, nil)
	if r.Success || r.Interrupted || r.Error != failure {
		t.Fatal(r)
	}
}

func TestAttemptOutcomeRecordedInfrastructure(t *testing.T) {
	for _, kind := range []string{launchRefused, launchFailed, environmentFailed, foreignPort, "payload-stderr"} {
		for _, invalid := range []bool{false, true} {
			fact := infrastructureFact{Kind: kind, At: time.Now().UTC(), Port: "1234", Group: 10}
			if invalid {
				fact.At = time.Time{}
			}
			r := classifyAttemptOutcome(Result{}, attemptCause{memberOutcome: memberOutcome{Infrastructure: []infrastructureFact{fact}}})
			if r.Interrupted != (kind != "payload-stderr" && !invalid) {
				t.Fatal(kind, invalid, r)
			}
			for _, override := range []attemptCause{{OutOfMemory: true}, {Cancelled: true}, {Stale: true}} {
				override.Infrastructure = []infrastructureFact{fact}
				if r := classifyAttemptOutcome(Result{}, override); r.Interrupted {
					t.Fatal("infrastructure bypassed outcome fence", r)
				}
			}
		}
	}
}

func TestAttemptOutcomeMissingDeliveryTimestamp(t *testing.T) {
	r := classifyAttemptOutcome(Result{}, attemptCause{memberOutcome: memberOutcome{
		Signals: []deliveredSignal{{Signal: "TERM"}},
		Exit:    &memberExit{Status: 143, ObservedAt: time.Now().UTC()},
	}})
	if r.Interrupted {
		t.Fatal("untimestamped delivery became infrastructure evidence", r)
	}
}

func TestDockerRecoveryErrorIsNotObservationEvidence(t *testing.T) {
	r := dockerOutcome(dockerExecution{ExitCode: 143}, Result{}, errDockerRecovery)
	if r.Success || r.Interrupted {
		t.Fatal("recovery error substituted for a durable observation-loss fact", r)
	}
}
