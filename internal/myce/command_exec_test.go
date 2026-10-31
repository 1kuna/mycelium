package myce

import (
	"context"
	"errors"
	"fmt"
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

// A handled stderr error must not replace the cause of a later assertion failure.
func TestCommandHandledExecDiagnostic(t *testing.T) {
	w := recoveryWorker(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.Machine.HostMemoryMeasured = true
	w.Machine.HostAvailableMemoryGB, w.Machine.FreeHostMemoryGB = 64, 64
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = w.Machine.Session
	j := recoveryJob(t, []string{"sh", "-c", "echo 'sh: optional-probe: Exec format error' >&2; echo 'optional failure handled; continuing' >&2; echo 'AssertionError: actual regression' >&2; exit 1"})
	w = workerWithPreparedCheckpoint(t, w, store, j)
	p, err := startAfterFixtureInputs(t, w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stop(w.Clock.Now(), 0, true) })
	var r Result
	await(t, func() bool {
		select {
		case f := <-p.done:
			if f.err != nil {
				t.Fatal(f.err)
			}
			r = f.result
			return true
		default:
			return false
		}
	})
	if r.Success || r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Class != "command_exit" || r.ExecutionCause.ExitCode == nil || *r.ExecutionCause.ExitCode != 1 {
		t.Fatalf("handled diagnostic changed ordinary exit 1: %+v", r)
	}
}
