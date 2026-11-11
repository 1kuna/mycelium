package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
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

func TestRSSGuardLinuxKeepsCgroupAccounting(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux cgroup contract")
	}
	if localProcessMemory() != nil {
		t.Fatal("Linux acquired a process RSS guard")
	}
}

// Metal work is guarded on phys_footprint, which counts Metal buffers that RSS
// misses (#1044). The limit is the whole unified envelope (device + host +
// slack); the stop carries footprint evidence and the peak is labelled so
// learning can tell footprint evidence from RSS-era peaks.
func TestFootprintGuardStopsMetalAttempt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		footprint uint64
		stopped   bool
	}{
		{"within envelope", 4 << 30, false},
		{"past envelope", 6 << 30, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU, w.Machine.MemoryCeilingSlackGB = "metal", .125
			clock := &workerFakeClock{now: time.Unix(100, 0)}
			w.Clock = clock
			w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			w.processMemory = &processMemoryProbe{
				Pressure: func(context.Context) (*gpuHostState, error) { return &gpuHostState{available: 64, level: 1}, nil },
				// RSS never sees the buffers; a guard on it would never act.
				OwnedResident: func(map[int]string) (uint64, int, error) { return 64 << 20, 1, nil },
				OwnedFootprint: func(map[int]string) (uint64, uint64, int, error) {
					clock.Advance(time.Second)
					return tc.footprint, 64 << 20, 1, nil
				},
			}
			if !w.footprintGuarded(Job{Spec: Spec{Needs: ResourceNeeds{MemoryGB: 4, HostMemoryGB: 1, Platform: Platform{Required: []string{"metal"}}}}}) {
				t.Fatal("declared Metal job not footprint guarded")
			}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			duration := "0.3"
			if tc.stopped {
				duration = "60"
			}
			j := recoveryJob(t, []string{"sleep", duration})
			j.Spec.Resumable = false
			j.Spec.Needs = ResourceNeeds{MemoryGB: 4, HostMemoryGB: 1, Platform: Platform{Required: []string{"metal"}}}
			p, err := startAfterFixtureInputs(t, w, store, j)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.stopDirective(clock.Now(), 0, true, true) })
			r := d5Finish(t, p)
			// RSS is the host share; the footprint beyond it is the device share.
			if p := r.MemoryPeak; p == nil || p.HostGB == nil || *p.HostGB != 1.0/16 || p.DeviceGB == nil || *p.DeviceGB != float64(tc.footprint)/(1<<30)-1.0/16 || p.DeviceSource != deviceFootprintSource {
				t.Fatalf("peak: %+v", r.MemoryPeak)
			}
			if !tc.stopped {
				if !r.Success || r.ExecutionCause != nil {
					t.Fatalf("stopped within envelope: %+v", r)
				}
				return
			}
			e := r.ExecutionCause
			if r.Success || !r.OutOfMemory || e == nil || e.Class != rssLimitExceeded || e.RSSLimit == nil || e.RSSLimit.Quality != footprintGuardQuality || e.RSSLimit.LimitBytes != int64(5.125*(1<<30)) {
				t.Fatalf("footprint guard outcome: %+v %+v", r, e)
			}
		})
	}
}
