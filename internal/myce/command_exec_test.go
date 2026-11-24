package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestCommandExecFormatIsOrdinaryFailure(t *testing.T) {
	for _, priority := range []string{"experiment", "production"} {
		t.Run(priority, func(t *testing.T) {
			q, clock := fixture(t)
			m := valueMachine("worker", "cpu", 16)
			check(t, q, m, nil)
			s := cpuSpec(1, 1)
			s.Priority = priority
			s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64"}}
			j := submit(t, q, s)
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != j.ID {
				t.Fatal("missing assignment", assigned)
			}
			err := &recordedStartError{cause: fmt.Errorf("exec program: %w", syscall.ENOEXEC), facts: memberOutcome{Infrastructure: []infrastructureFact{{Kind: launchFailed, At: clock.Now()}}}}
			r := startFailureOutcome(result(assigned, false), err)
			if r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Class != "command_exit" || r.Error != execFormatMessage {
				t.Fatalf("not an ordinary actionable failure: %+v", r)
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.State != "failed" || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("exec failure retried: %+v", got)
			}
			public := statusJobCopy(got)
			if !strings.Contains(public.Reason, execFormatMessage) {
				t.Fatal("public error lost guidance", public.Reason)
			}
		})
	}
	if !execFormatFailure(errors.New("command exec failed: exec format error")) {
		t.Fatal("exec receipt lost guidance")
	}
	if execFormatFailure(errors.New("exit status 1")) {
		t.Fatal("ordinary exit treated as exec failure")
	}
}

func TestCommandExecArgumentLimitIsTerminal(t *testing.T) {
	for _, priority := range []string{"experiment", "production"} {
		t.Run(priority, func(t *testing.T) {
			q, clock := fixture(t)
			m := valueMachine("worker", "cpu", 16)
			check(t, q, m, nil)
			s := cpuSpec(1, 1)
			s.Priority = priority
			j := submit(t, q, s)
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != j.ID {
				t.Fatal("missing assignment")
			}
			store := &workerStore{dir: t.TempDir()}
			err := store.startFailureAt(*assigned, launchFailed, fmt.Errorf("exec: %w", syscall.E2BIG), clock.Now())
			r := startFailureOutcome(result(assigned, false), err)
			if r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Predicate != execArgumentLimit {
				t.Fatalf("exec size failure is not terminal: %+v", r)
			}
			// Saved facts alone must preserve the same choice after recovery,
			// even when a grouped result initially has a generic runtime cause.
			var saved memberOutcome
			data, readErr := os.ReadFile(filepath.Join(store.dir, store.state.LaunchOutcome))
			if err := json.Unmarshal(data, &saved); readErr != nil || err != nil {
				t.Fatal(err)
			}
			replayed := classifyAttemptOutcome(Result{ExecutionCause: &RuntimeCause{Operation: "main", Class: "io_error"}}, attemptCause{memberOutcome: saved})
			if replayed.Interrupted || replayed.ExecutionCause.Predicate != execArgumentLimit {
				t.Fatal("saved exec size failure changed on replay")
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.State != "failed" || got.Losses != 0 || got.InfrastructureRetries != 0 || commandStarted(got) {
				t.Fatalf("deterministic launch failed to terminate without retries: %+v", got)
			}
			if !strings.Contains(statusJobCopy(got).Reason, "MYCE_BINDINGS_FILE") {
				t.Fatal("public reason lost actionable guidance")
			}
		})
	}
}
