package myce

import (
	"strings"
	"testing"
	"time"
)

func TestDockerStandaloneKillDiagnostic(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		// A standalone container has no group observation to populate AmbiguousKill.
		for _, stopIntent := range []bool{true} {
			// #149: stop intent can exist even when the kill reply timed out.
			r := dockerOutcome(dockerExecution{ExitCode: 137, InfrastructureStop: stopIntent,
				CleanupCause: &RuntimeCause{Operation: "kill", Class: "timeout"}}, Result{}, nil)
			if r.Success || r.Interrupted || !strings.Contains(r.Error, "SIGKILL; interruption cause unconfirmed") {
				t.Fatal(r)
			}
		}

	})

	t.Run("DockerKnownMemoryFailureIsNotAmbiguous", func(t *testing.T) {
		const failure = "Docker memory ceiling not enforced"
		r := dockerOutcome(dockerExecution{ExitCode: 137, MemoryFailure: failure}, Result{}, nil)
		if r.Success || r.Interrupted || r.Error != failure {
			t.Fatal(r)
		}

	})

	t.Run("AttemptOutcomeMissingDeliveryTimestamp", func(t *testing.T) {
		r := classifyAttemptOutcome(Result{}, attemptCause{memberOutcome: memberOutcome{
			Signals: []deliveredSignal{{Signal: "TERM"}},
			Exit:    &memberExit{Status: 143, ObservedAt: time.Now().UTC()},
		}})
		if r.Interrupted {
			t.Fatal("untimestamped delivery became infrastructure evidence", r)
		}

	})

	t.Run("DockerRecoveryErrorIsNotObservationEvidence", func(t *testing.T) {
		r := dockerOutcome(dockerExecution{ExitCode: 143}, Result{}, errDockerRecovery)
		if r.Success || r.Interrupted {
			t.Fatal("recovery error substituted for a durable observation-loss fact", r)
		}

	})
}
