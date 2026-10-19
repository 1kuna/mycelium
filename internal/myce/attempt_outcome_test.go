package myce

import (
	"strings"
	"testing"
)

func TestAttemptOutcomeCause(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cause       attemptCause
		interrupted bool
	}{
		{"stop", attemptCause{Infrastructure: true}, true},
		{"kill-during-stop", attemptCause{Infrastructure: true, AmbiguousKill: true}, true},
		{"observed-stage-failure-before-stop", attemptCause{Infrastructure: true, StageFailure: true}, false},
		{"oom-during-stop", attemptCause{Infrastructure: true, OutOfMemory: true}, false},
		{"self-exit", attemptCause{StageFailure: true}, false},
		{"unexplained-kill", attemptCause{AmbiguousKill: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := classifyAttemptOutcome(Result{Forced: true}, tc.cause)
			if r.Interrupted != tc.interrupted || r.OutOfMemory != tc.cause.OutOfMemory {
				t.Fatal(r)
			}
			if tc.cause.AmbiguousKill && !tc.interrupted && !strings.Contains(r.Error, "SIGKILL without an infrastructure stop") {
				t.Fatal("missing ambiguity diagnostic", r)
			}
			if (tc.cause.StageFailure || tc.cause.OutOfMemory) && r.Forced {
				t.Fatal("stage failure reported as forced infrastructure stop", r)
			}
		})
	}
}

func TestDockerStandaloneKillDiagnostic(t *testing.T) {
	// A standalone container has no group observation to populate AmbiguousKill.
	r := dockerOutcome(dockerExecution{ExitCode: 137}, Result{}, nil)
	if r.Success || r.Interrupted || !strings.Contains(r.Error, "SIGKILL without an infrastructure stop") {
		t.Fatal(r)
	}
}

func TestDockerKnownMemoryFailureIsNotAmbiguous(t *testing.T) {
	const failure = "Docker memory ceiling not enforced"
	r := dockerOutcome(dockerExecution{ExitCode: 137, MemoryFailure: failure}, Result{}, nil)
	if r.Success || r.Interrupted || r.Error != failure {
		t.Fatal(r)
	}
}
