package myce

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestArchitectureMismatchMovesOnceAndLearns(t *testing.T) {
	root := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	database := filepath.Join(root, "queue.db")
	q, err := Open(database, filepath.Join(root, "checkpoints"), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	fakeCheckpointAvailability(t, q, true)
	small, other := valueMachine("amd64", "cpu", 16), valueMachine("arm64", "cpu", 32)
	other.CapabilityProfile.Arch = "arm64"
	check(t, q, small, nil)
	check(t, q, other, nil)
	s := cpuSpec(1, 1)
	s.Targets = []ExecutionTarget{{OS: "linux"}}
	s.Run.Command = []string{"tool", "test"}
	j := submit(t, q, s)
	first := check(t, q, small, nil).Job
	if first == nil || first.ID != j.ID {
		t.Fatal("initial attempt missing", first)
	}
	failure := result(first, false)
	failure.ExecutionCause = &RuntimeCause{Operation: "command", Class: architectureMismatch}
	clock.Advance(time.Second)
	if err := q.Complete(failure); err != nil {
		t.Fatal(err)
	}
	moved := job(t, q, j.ID)
	if moved.State != "queued" || moved.DiagnosticCode != architectureMismatch || moved.InfrastructureRetries != 0 || moved.Losses != 0 || !moved.Architecture.Moved {
		t.Fatalf("not an architecture move: %+v", moved)
	}
	if !moved.AttemptHistory[0].EndedAt.Equal(clock.Now()) || moved.AttemptHistory[0].ExecutionCause.Class != architectureMismatch {
		t.Fatal("lost clock-scoped mismatch evidence", moved.AttemptHistory)
	}
	if got := check(t, q, small, nil).Job; got != nil {
		t.Fatal("excluded arch assigned", got)
	}
	if err := q.Complete(failure); !errors.Is(err, ErrStale) {
		t.Fatal("stale result learned again", err)
	}
	second := check(t, q, other, nil).Job
	if second == nil || second.ID != j.ID || second.Attempt != 2 {
		t.Fatal("not moved to other arch", second)
	}
	// Same frozen program, even under another job name, inherits the learned
	// exclusion. Changed executable contracts do not inherit an unrelated pin.
	s.Name = "later-submission"
	later := submit(t, q, s)
	if later.Architecture == nil || executionJobTargetMatches(later, &small) {
		t.Fatal("later program forgot learned architecture")
	}
	changed := s
	changed.Run.Command = []string{"different-tool"}
	fresh := submit(t, q, changed)
	if !executionJobTargetMatches(fresh, &small) {
		t.Fatal("unrelated program inherited exclusion")
	}
	failure = result(second, false)
	failure.ExecutionCause = &RuntimeCause{Operation: "command", Class: architectureMismatch}
	if err := q.Complete(failure); err != nil {
		t.Fatal(err)
	}
	ended := job(t, q, j.ID)
	if ended.State != "failed" || ended.Attempt != 2 || ended.ExecutionCause.Class != architectureMismatch || !strings.Contains(ended.Reason, "no further architecture moves") {
		t.Fatalf("retried more than once or lost classification: %+v", ended)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(database, filepath.Join(root, "checkpoints"), clock)
	if err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, q, true)
	ended = job(t, q, j.ID)
	if ended.Architecture == nil || !ended.Architecture.Moved || len(ended.Architecture.Excluded) != 2 {
		t.Fatal("restart lost the one-move fence", ended.Architecture)
	}
	persisted := submit(t, q, s)
	if persisted.Architecture == nil || len(persisted.Architecture.Excluded) != 2 {
		t.Fatal("restart lost program learning", persisted.Architecture)
	}
}

func TestArchitectureMismatchOnlyClearSignal(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"sh: ./program: Exec format error\n", true},
		{"exec: /program: bad CPU type in executable", true},
		{"dyld: mach-o, but wrong architecture", true},
		{"test failed: expected exec format error, got success", false},
		{"permission denied", false},
		{"exit status 126", false},
	} {
		t.Run(tc.text, func(t *testing.T) {
			code := 126
			r := Result{ExecutionCause: &RuntimeCause{Operation: "command", Class: "command_exit", ExitCode: &code}}
			classifyArchitectureFailure(&r, architectureErrorText(tc.text))
			if (r.ExecutionCause.Class == architectureMismatch) != tc.want {
				t.Fatalf("classification: %+v", r)
			}
		})
	}
	r := startFailureOutcome(Result{}, syscall.ENOEXEC)
	if r.ExecutionCause == nil || r.ExecutionCause.Class != architectureMismatch || r.Interrupted {
		t.Fatal("typed exec error not classified", r)
	}
}

func TestArchitectureMismatchWorkerSignal(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, mode := range []string{"stderr", "no-logs", "stdout"} {
			t.Run(mode+"/"+map[bool]string{false: "command", true: "native-group"}[grouped], func(t *testing.T) {
				w := recoveryWorker(t)
				w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
				w.Machine.HostMemoryMeasured = true
				w.Machine.HostAvailableMemoryGB = 64
				w.Machine.FreeHostMemoryGB = 64
				store, err := w.openStore()
				if err != nil {
					t.Fatal(err)
				}
				defer store.close()
				store.state.Session = w.Machine.Session
				j := recoveryJob(t, []string{"sh", "-c", "echo 'sh: downloaded-program: Exec format error' >&2; exit 126"})
				if mode == "no-logs" {
					j.Spec.Logs = "none"
				}
				if mode == "stdout" {
					j.Spec.Run.Command = []string{"sh", "-c", "echo 'sh: downloaded-program: Exec format error'; exit 126"}
				}
				if grouped {
					j.Spec.Run.MemoryGB = 0.1
					j.Spec.Needs.HostMemoryGB = 1
				}
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
				if r.Success || r.Interrupted || r.ReleasePending || r.ExecutionCause == nil || (r.ExecutionCause.Class == architectureMismatch) != (mode != "stdout") {
					t.Fatalf("worker lost architecture diagnostic: %+v", r)
				}
			})
		}
	}

}

func TestArchitectureMismatchWaitsForExactRelease(t *testing.T) {
	q, _ := fixture(t)
	first, other := valueMachine("amd64", "cpu", 16), valueMachine("arm64", "cpu", 32)
	other.CapabilityProfile.Arch = "arm64"
	check(t, q, first, nil)
	check(t, q, other, nil)
	s := cpuSpec(1, 1)
	s.Targets = []ExecutionTarget{{OS: "linux"}}
	wanted := submit(t, q, s)
	assigned := check(t, q, first, nil).Job
	if assigned == nil || assigned.ID != wanted.ID {
		t.Fatal("missing attempt")
	}
	r := result(assigned, false)
	r.ReleasePending = true
	r.ExecutionCause = &RuntimeCause{Operation: "command", Class: architectureMismatch}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := check(t, q, other, nil).Job; got != nil {
		t.Fatal("moved before old attempt cleanup", got)
	}
	r.ReleasePending = false
	r.ReleaseOnly = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := check(t, q, other, nil).Job; got == nil || got.ID != wanted.ID {
		t.Fatal("exact release did not unblock compatible host", got)
	}
}

func TestArchitectureMismatchConcurrentLearning(t *testing.T) {
	q, _ := fixture(t)
	first, other := valueMachine("amd64", "cpu", 16), valueMachine("arm64", "cpu", 32)
	other.CapabilityProfile.Arch = "arm64"
	check(t, q, first, nil)
	check(t, q, other, nil)
	s := cpuSpec(4, 1)
	s.Targets = []ExecutionTarget{{OS: "linux"}}
	submit(t, q, s)
	s.Name = "concurrent"
	submit(t, q, s)
	a := check(t, q, first, nil).Job
	b := check(t, q, other, nil).Job
	if a == nil || b == nil || a.ID == b.ID {
		t.Fatal("missing concurrent attempts", a, b)
	}
	for _, attempt := range []*Job{a, b} {
		r := result(attempt, false)
		r.ExecutionCause = &RuntimeCause{Operation: "command", Class: architectureMismatch}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	s.Name = "later"
	learned := submit(t, q, s)
	if learned.Architecture == nil || len(learned.Architecture.Excluded) != 2 || executionJobTargetMatches(learned, &first) || executionJobTargetMatches(learned, &other) {
		t.Fatal("concurrent attempts overwrote learned constraints", learned.Architecture)
	}
}
