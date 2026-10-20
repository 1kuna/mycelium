package myce

import (
	"errors"
	"os/exec"
	"testing"
	"time"
)

func d5InterruptedOutcome(t *testing.T, runtime string, owner Result) Result {
	t.Helper()
	if runtime == "docker" {
		return dockerOutcome(dockerExecution{ExitCode: 137, MainFinished: true, InfrastructureStop: true}, owner, nil)
	}
	waitErr := exec.Command("sh", "-c", "kill -TERM $$").Run()
	return nativeOutcome(owner, waitErr, attemptCause{Infrastructure: true})
}

func TestNativeOutcomeTermStop(t *testing.T) {
	for _, group := range []bool{false, true} {
		for _, independentExit := range []bool{false, true} {
			t.Run(map[bool]string{false: "native", true: "native-group"}[group], func(t *testing.T) {
				dir := t.TempDir()
				command := `touch "$1"; exec sleep 600`
				if independentExit {
					command = `trap 'exit 7' TERM; touch "$1"; while :; do sleep .01; done`
				}
				w, _, p := d5NativeStart(t, group, nil, []string{"sh", "-c", command, "main", dir + "/ready"})
				nativeFile(t, dir, "ready")
				p.stop(w.Clock.Now(), time.Second, false)
				r := d5Finish(t, p)
				if r.Success || r.Interrupted == independentExit {
					t.Fatal("TERM stop became stage failure", r)
				}
			})
		}
	}
}

func TestOutcomeRequeueExhaustionAndNoDoubleCharge(t *testing.T) {
	for _, runtime := range []string{"native", "native-group", "docker"} {
		t.Run(runtime, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("worker", "fake")
			m.HostMemoryMeasured = true
			m.HostAvailableMemoryGB = 8
			m.FreeHostMemoryGB = 8
			s := spec("production")
			s.Needs.HostMemoryGB = 2
			s.SubmissionKey = "d5-requeue"
			// Member caps share the two-GiB parent; their sum is not a charge.
			s.Run.MemoryGB = 1.5
			s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "600"}, MemoryGB: 1.5}}
			j := submit(t, q, s)
			var previous Result
			for attempt := 1; attempt <= 3; attempt++ {
				running := check(t, q, m, nil).Job
				if running == nil || running.Attempt != attempt {
					t.Fatal("missing assignment", attempt)
				}
				if err := q.Started(result(running, false)); err != nil {
					t.Fatal(err)
				}
				q.refreshHostMemory()
				if got := q.s.Machines[m.ID].HostMemoryInUseGB; got != 2 {
					t.Fatal("member caps double charged", got)
				}
				if attempt > 1 {
					if err := q.Complete(previous); !errors.Is(err, ErrStale) {
						t.Fatal("stale outcome accepted", err)
					}
				}
				r := d5InterruptedOutcome(t, runtime, result(running, false))
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				if err := q.Complete(r); err != nil && !errors.Is(err, ErrStale) {
					t.Fatal(err)
				}
				got := job(t, q, j.ID)
				want := "queued"
				if attempt == 3 {
					want = "failed"
				}
				if got.State != want || got.InfrastructureRetries != min(attempt, 2) {
					t.Fatal("retry budget charged twice", got)
				}
				q.refreshHostMemory()
				if got := q.s.Machines[m.ID].HostMemoryInUseGB; got != 0 {
					t.Fatal("settled attempt retained charge", got)
				}
				previous = r
			}
		})
	}
}

func TestOutcomeCancellationPrecedence(t *testing.T) {
	for _, runtime := range []string{"native", "native-group", "docker"} {
		t.Run(runtime, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("worker", "fake")
			j := submit(t, q, spec("production"))
			running := check(t, q, m, nil).Job
			if err := q.Started(result(running, false)); err != nil {
				t.Fatal(err)
			}
			if err := q.Cancel(j.ID); err != nil {
				t.Fatal(err)
			}
			r := operatorOutcome(d5InterruptedOutcome(t, runtime, result(running, false)), true)
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.State != "cancelled" || got.InfrastructureRetries != 0 {
				t.Fatal("stop bypassed cancellation", got)
			}
		})
	}
}

func TestOutcomeRuntimeTable(t *testing.T) {
	for _, runtime := range []string{"native", "native-group", "docker"} {
		for _, tc := range []struct {
			name                 string
			code                 int
			stop, oom            bool
			recovery, operator   bool
			success, interrupted bool
		}{
			{name: "completion", success: true},
			{name: "completion-with-stop", stop: true, success: true},
			{name: "live-recovery-completion", stop: true, recovery: true, success: true},
			{name: "independent-exit-with-stop", code: 7, stop: true},
			{name: "saved-stage-failure-with-operator-stop", code: 7, stop: true, operator: true, recovery: true},
			{name: "oom-with-stop", code: 137, stop: true, oom: true},
			{name: "operator-stop", code: 143, stop: true, operator: true, interrupted: true},
			{name: "planned-stop", code: 143, stop: true, interrupted: true},
			{name: "unexplained-kill", code: 137},
		} {
			t.Run(runtime+"/"+tc.name, func(t *testing.T) {
				var r Result
				if runtime == "docker" {
					state := dockerExecution{Container: "main", MainFinished: true, ExitCode: tc.code, InfrastructureStop: tc.stop, OOMKilled: tc.oom}
					var recoveryErr error
					if tc.recovery {
						recoveryErr = dockerRecoveryError(state)
					}
					r = dockerOutcome(state, Result{}, recoveryErr)
				} else {
					command := "exit 0"
					switch tc.code {
					case 7:
						command = "exit 7"
					case 137:
						command = "kill -KILL $$"
					case 143:
						command = "kill -TERM $$"
					}
					waitErr := exec.Command("sh", "-c", command).Run()
					r = nativeOutcome(Result{Success: waitErr == nil}, waitErr, attemptCause{Infrastructure: tc.stop, OutOfMemory: tc.oom})
				}
				r = operatorOutcome(r, tc.operator)
				if r.Success != tc.success || r.Interrupted != tc.interrupted || r.OutOfMemory != tc.oom || r.OperatorStop != tc.operator {
					t.Fatalf("got %+v; want success=%v interrupted=%v oom=%v", r, tc.success, tc.interrupted, tc.oom)
				}
			})
		}
	}
}

func TestNativeOutcomeIndependentExitWithStop(t *testing.T) {
	for _, group := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "native-group"}[group], func(t *testing.T) {
			dir := t.TempDir()
			ready, finish := dir+"/ready", dir+"/finish"
			_, _, p := d5NativeStart(t, group, nil, []string{"sh", "-c", `touch "$1"; while [ ! -f "$2" ]; do sleep .01; done; exit 7`, "main", ready, finish})
			nativeFile(t, dir, "ready")
			// Freeze intent separately from delivery, then observe a payload exit.
			p.mu.Lock()
			p.stopped = true
			p.mu.Unlock()
			nativeTouch(t, dir, "finish")
			r := d5Finish(t, p)
			if r.Success || r.Interrupted {
				t.Fatal("independent exit masked", r)
			}
		})
	}
}
