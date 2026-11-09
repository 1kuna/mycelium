package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestRSSGuardInjectedClockAndSampler(t *testing.T) {
	for _, mode := range []string{"growth", "spike", "unknown", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			clock := &fakeClock{now: time.Unix(100, 0)}
			guard := rssGuard{limit: 1 << 30}
			roots := map[int]string{7: "launch-birth"}
			probe := processMemoryProbe{OwnedResident: func(map[int]string) (uint64, int, error) { return 2 << 30, 2, nil }}
			var evidence *RSSLimitEvidence
			for i := 0; i < 3; i++ {
				bytes, members, err := probe.OwnedResident(roots)
				if i == 1 {
					switch mode {
					case "spike":
						bytes = 1 << 29
					case "unknown":
						err = errors.New("inventory denied")
					case "replacement":
						roots = map[int]string{7: "reused-birth"}
					}
				}
				evidence = guard.observe(clock.Now(), roots, bytes, members, err)
				if i < 2 && evidence != nil {
					t.Fatal("stopped before grace")
				}
				clock.Advance(time.Second)
			}
			if (evidence != nil) != (mode == "growth") {
				t.Fatalf("%s: %+v", mode, evidence)
			}
			if evidence != nil {
				r := classifyAttemptOutcome(Result{Success: true, ReleasePending: true}, attemptCause{memberOutcome: memberOutcome{RSSLimit: evidence}})
				if r.Success || r.Interrupted || !r.OutOfMemory || !r.ReleasePending || safeRuntimeCause(r.ExecutionCause).Class != rssLimitExceeded {
					t.Fatalf("wrong resource outcome: %+v", r)
				}
			}
		})
	}
}

// Real command/cleanup path with synthetic resident readings and injected time.
func TestRSSGuardStopsExactAttempt(t *testing.T) {
	for _, native := range []bool{false, true} {
		name := "plain"
		if native {
			name = "native-group"
		}
		t.Run(name, func(t *testing.T) { testRSSGuardStopsAttempt(t, native) })
	}
}

func testRSSGuardStopsAttempt(t *testing.T, native bool) {
	w := recoveryWorker(t)
	w.Machine.GPU = "cpu"
	clock := &workerFakeClock{now: time.Unix(100, 0)}
	w.Clock = clock
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.processMemory = &processMemoryProbe{
		Pressure: func(context.Context) (*gpuHostState, error) { return &gpuHostState{available: 64, level: 1}, nil },
		OwnedResident: func(map[int]string) (uint64, int, error) {
			clock.Advance(time.Second)
			return 2 << 30, 2, nil
		},
	}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"sleep", "60"})
	j.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}
	if native {
		j.Spec.Run.MemoryGB = 0.5
	}
	p, err := startAfterFixtureInputs(t, w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stopDirective(clock.Now(), 0, true, true) })
	roots := p.rssRoots()
	r := d5Finish(t, p)
	if r.Success || r.Interrupted || !r.OutOfMemory || r.ReleasePending || r.ExecutionCause == nil || r.ExecutionCause.Class != rssLimitExceeded || r.MemoryPeak == nil || !r.MemoryPeak.Constrained {
		t.Fatalf("guard outcome: %+v", r)
	}
	for root := range roots {
		if members, err := workerGroupMembers(root); err != nil || len(members) != 0 {
			t.Fatalf("released live attempt: %v %v", members, err)
		}
	}
	// Lose the final result to exercise the receipt from before signal delivery.
	if err := os.Remove(store.dir + "/" + store.state.Identity + ".result.json"); err != nil {
		t.Fatal(err)
	}
	store.state.Result = nil
	if err := store.recoverRSSCause(w.Machine.ID); err != nil {
		t.Fatal(err)
	}
	if store.state.Result == nil || store.state.Result.ExecutionCause.Class != rssLimitExceeded || !store.state.Result.ReleasePending {
		t.Fatalf("restart lost cause/custody: %+v", store.state.Result)
	}
}

func TestRSSGuardRejectsLateAndReusedIdentity(t *testing.T) {
	e := &RSSLimitEvidence{ObservedBytes: 2 << 30, LimitBytes: 1 << 30, Quality: "owned_session_rss"}
	birth, err := processBirth(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	for _, exited := range []bool{false, true} {
		p := &process{cmd: &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}}, birth: "stale", saveOutcome: func(memberOutcome) error { t.Fatal("persisted stale stop"); return nil }}
		if exited {
			p.birth = birth
			p.outcome.Exit = &memberExit{}
		}
		if p.stopRSS(time.Unix(100, 0), p.rssRoots(), e) {
			t.Fatal("late sample stopped attempt")
		}
	}
}

func TestRSSGuardRejectsPendingNativeExit(t *testing.T) {
	birth, err := processBirth(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	p := &process{}
	pid := os.Getpid()
	g := &nativeGroup{p: p, supervisor: &exec.Cmd{Process: &os.Process{Pid: pid}}, custody: workerState{Groups: map[int]map[int]string{pid: {pid: birth}}}}
	g.main = &nativeMember{exitObservation: &nativeExitObservation{exit: &memberExit{Status: 0, ObservedAt: time.Unix(100, 0)}}}
	p.native = g
	if g.main.Exit != nil {
		t.Fatal("fixture must leave cached exit pending")
	}
	if p.stopRSS(time.Unix(101, 0), p.rssRoots(), &RSSLimitEvidence{ObservedBytes: 2 << 30, LimitBytes: 1 << 30, Quality: "owned_session_rss"}) || p.outcome.RSSLimit != nil {
		t.Fatal("pending successful native exit became OOM")
	}
}
