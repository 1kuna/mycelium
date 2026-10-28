package myce

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestGPUStopPreemptionWithinStartBudget(t *testing.T) {
	t.Run("native", func(t *testing.T) { testGPUStopNative(t, false, "") })
	t.Run("native-infrastructure", func(t *testing.T) { testGPUStopNative(t, false, "infrastructure") })
	t.Run("maintenance", func(t *testing.T) {
		q, _ := fixture(t)
		if err := q.SetPreemptionGrace(time.Minute); err != nil {
			t.Fatal(err)
		}
		s := spec("experiment")
		s.Resumable = false
		submit(t, q, s)
		m := machine("gpu", "fake", "host")
		victim := check(t, q, m, nil).Job
		maintenanceChange(t, q, "disable", true)
		d := check(t, q, m, victim)
		if !d.Stop || d.Kill || d.UserCancel || d.GraceSeconds != 15 {
			t.Fatal("maintenance did not cap GPU group grace", d)
		}
		testGPUStopNative(t, false, "", d)
	})
	t.Run("docker", func(t *testing.T) { testGPUStopDocker(t, false) })
	t.Run("docker-infrastructure", func(t *testing.T) { testGPUStopDocker(t, true) })
}

func TestGPUStopTermExitNeverKilled(t *testing.T) {
	t.Run("native", func(t *testing.T) { testGPUStopNative(t, true, "") })
	t.Run("native-shutdown", func(t *testing.T) { testGPUStopNative(t, true, "shutdown") })
	q, clock := fixture(t)
	s := spec("experiment")
	s.Resumable = false
	submit(t, q, s)
	m := machine("gpu", "fake")
	victim := check(t, q, m, nil).Job
	submit(t, q, spec("production"))
	directive := check(t, q, m, victim)
	if directive.Kill || directive.GraceSeconds <= 0 {
		t.Fatal("no TERM grace", directive)
	}
	d, dc, root := stopAttempt(t)
	if err := os.WriteFile(filepath.Join(root, "extra"), []byte("yes"), 0600); err != nil {
		t.Fatal(err)
	}
	d.stop(dc.Now(), time.Duration(directive.GraceSeconds)*time.Second, directive.Kill)
	if _, err := d.reconcile(); err != nil {
		t.Fatal(err)
	}
	// Simulate the daemon observing the main and helper's cooperative exits.
	if err := os.Remove(filepath.Join(root, "running")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "extra")); err != nil {
		t.Fatal(err)
	}
	dc.advance(time.Second)
	if done, err := d.reconcile(); err != nil || !done {
		t.Fatal("TERM exit not released", done, err)
	}
	dc.advance(20 * time.Second)
	if _, err := d.reconcile(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(root, "calls"))
	if strings.Contains(string(b), "kill --signal=KILL ") {
		t.Fatal("KILL after TERM exit", string(b))
	}
	clock.Advance(time.Second)
	finishInterrupted(t, q, victim)
}

func testGPUStopNative(t *testing.T, cooperative bool, stopPath string, directives ...Reply) {
	t.Helper()
	w := recoveryWorker(t)
	grace := 10 * time.Second
	if len(directives) > 0 {
		grace = time.Duration(directives[0].GraceSeconds * float64(time.Second))
		w.AcceleratorStopGrace = grace
	}
	clock := &manualWorkerClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	w.Clock = clock
	w.memoryCeilings = fakeCeilings(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = w.Machine.Session
	command := func(name string, exit bool) []string {
		trap := "trap '' TERM"
		if exit {
			trap = "trap 'exit 0' TERM"
		}
		return []string{"sh", "-c", trap + `; touch "$1"; while :; do sleep .01; done`, name, "{work}/" + name}
	}
	j := recoveryJob(t, command("main", cooperative))
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 2
	j.Spec.Run.MemoryGB = .5
	// The helper exits on TERM while main may still be draining. Its exit must
	// not trigger the unconditional cleanup KILL that bypassed group grace.
	j.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: command("helper", true), MemoryGB: 1}}
	p, err := w.start(store, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop(clock.Now(), 0, true)
	nativeFile(t, p.job.Spec.Run.Workdir, "main")
	nativeFile(t, p.job.Spec.Run.Workdir, "helper")
	control := processControl{clock: clock, processes: map[attemptKey]*process{jobKey(p.job): p}, pendingEvents: map[attemptKey]WorkerEvent{jobKey(p.job): {Execution: Execution{ID: p.job.ID, Attempt: p.job.Attempt}, Session: p.job.Session, UserCancel: false, PriorityStop: &PriorityPreemption{Cause: priorityPreemptionCause, Demand: "production", Victim: p.job.ID}, deadline: clock.Now().Add(10 * time.Second)}}}
	if len(directives) > 0 {
		d := directives[0]
		control.pendingEvents[jobKey(p.job)] = WorkerEvent{Execution: Execution{ID: p.job.ID, Attempt: p.job.Attempt}, Session: p.job.Session, UserCancel: d.UserCancel, Kill: d.Kill, deadline: clock.Now().Add(grace)}
	}
	if stopPath == "shutdown" {
		control.pendingEvents = nil
		control.leaving = true
	}
	if stopPath == "infrastructure" {
		p.stop(clock.Now(), 0, true)
	} else {
		control.apply()
	}
	p.mu.Lock()
	for _, m := range p.native.members {
		if len(m.Signals) != 1 || m.Signals[0].Signal != "TERM" {
			t.Errorf("member %s missed group TERM: %+v", m.Name, m.Signals)
		}
	}
	p.mu.Unlock()
	if cooperative {
		r := d5Finish(t, p)
		// Shutdown keeps the existing outcome rules: helper-first exits can
		// interrupt the attempt. Both orders must release without KILL.
		if r.Forced || r.ReleasePending || (stopPath != "shutdown" && !r.Interrupted) {
			t.Fatalf("TERM exit outcome: %+v", r)
		}
		clock.Advance(20 * time.Second)
		control.apply()
	} else {
		// Wait for the helper's exit observation to exercise early cleanup.
		await(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.native.members[0].Exit != nil })
		clock.Advance(grace - time.Nanosecond)
		if stopPath == "infrastructure" {
			p.enforceGrace(clock.Now())
		} else {
			control.apply()
		}
		select {
		case <-p.done:
			t.Fatal("native cleanup bypassed grace")
		default:
		}
		p.mu.Lock()
		killed := p.killed
		p.mu.Unlock()
		if killed {
			t.Fatal("native KILL before grace")
		}
		clock.Advance(time.Nanosecond)
		if stopPath == "infrastructure" {
			p.enforceGrace(clock.Now())
		} else {
			control.apply()
		}
		r := d5Finish(t, p)
		if !r.Forced || !r.Interrupted || r.ReleasePending {
			t.Fatal("native deadline escalation", r)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range p.native.members {
		for _, signal := range m.Signals {
			if cooperative && signal.Signal == "KILL" {
				t.Errorf("member %s killed after TERM exit", m.Name)
			}
		}
	}
}

func testGPUStopDocker(t *testing.T, infrastructure bool) {
	t.Helper()
	q, clock := fixture(t)
	s := spec("experiment")
	s.Resumable = false
	submit(t, q, s)
	m := machine("gpu", "fake")
	victim := check(t, q, m, nil).Job
	prod := submit(t, q, spec("production"))
	directive := check(t, q, m, victim)
	if !directive.Stop || directive.Kill || directive.UserCancel || directive.GraceSeconds != 10 {
		t.Fatalf("GPU preemption bypassed grace: %+v", directive)
	}
	d, dc, root := stopAttempt(t)
	if err := os.WriteFile(filepath.Join(root, "extra"), []byte("yes"), 0600); err != nil {
		t.Fatal(err)
	}
	d.state.Accelerator = true
	if infrastructure {
		d.stop(dc.Now(), 0, true)
	} else {
		d.stop(dc.Now(), time.Duration(directive.GraceSeconds)*time.Second, directive.Kill)
	}
	reconcile := func() {
		t.Helper()
		if _, err := d.reconcile(); err != nil {
			t.Fatal(err)
		}
	}
	signals := func() string { b, _ := os.ReadFile(filepath.Join(root, "calls")); return string(b) }
	reconcile()
	if got := signals(); strings.Count(got, "kill --signal=TERM ") != 2 || strings.Contains(got, "kill --signal=KILL ") {
		t.Fatal("group did not receive TERM first", got)
	}
	dc.advance(10*time.Second - time.Nanosecond)
	reconcile()
	if strings.Contains(signals(), "kill --signal=KILL ") {
		t.Fatal("early KILL")
	}
	dc.advance(time.Nanosecond)
	reconcile()
	if strings.Count(signals(), "kill --signal=KILL ") != 2 {
		t.Fatal("missing deadline escalation", signals())
	}
	reconcile()
	clock.Advance(10 * time.Second)
	finishInterrupted(t, q, victim)
	next := check(t, q, m, nil).Job
	if next == nil || next.ID != prod.ID {
		t.Fatal("production not assigned", next)
	}
	if err := q.Started(result(next, false)); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, prod.ID)
	if got.StartedAt == nil || !got.StartedAt.Before(*got.Spec.StartBy) || got.StartLatencySeconds > 15 {
		t.Fatal("production missed start budget", got)
	}
}

// Cancellation of GPU setup/benchmark commands and server unload must also
// preserve a surviving descendant's grace after its supervising main exits.
func TestGPUStopCommandAndEngineCleanup(t *testing.T) {
	for _, engine := range []bool{false, true} {
		t.Run(map[bool]string{false: "extension-command", true: "model-engine"}[engine], func(t *testing.T) {
			dir := t.TempDir()
			clock := &manualWorkerClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
			ready, term := filepath.Join(dir, "ready"), filepath.Join(dir, "term")
			argv := []string{"sh", "-c", `trap 'touch "$2"' TERM; touch "$1"; while :; do sleep .01; done`, "gpu", ready, term}
			done := make(chan error, 1)
			var stop func()
			if engine {
				cmd := exec.Command(argv[0], argv[1:]...)
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				l := &LlamaEngine{Dir: dir, Clock: clock, cmd: cmd, identity: ID(), stopTimeout: 10 * time.Second, exited: make(chan struct{}), custody: workerState{Accelerator: true}}
				go func() { l.waitErr = cmd.Wait(); close(l.exited) }()
				stop = func() { go func() { done <- l.Unload() }() }
				t.Cleanup(func() { _ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL) })
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				go func() {
					done <- runRecordedCommand(ctx, argv, dir, []string{"PATH=" + os.Getenv("PATH"), "MYCE_PLATFORM=xpu"}, io.Discard, commandScope{StateDir: dir, Clock: clock}, "attempt-command-test.json")
				}()
				stop = cancel
				t.Cleanup(func() {
					data, _ := os.ReadFile(filepath.Join(dir, "attempt-command-test.json"))
					var st workerState
					_ = json.Unmarshal(data, &st)
					if st.PID > 0 {
						_ = signalWorkerGroup(st.PID, syscall.SIGKILL)
					}
				})
			}
			nativeFile(t, dir, "ready")
			stop()
			nativeFile(t, dir, "term")
			clock.Advance(10*time.Second - time.Nanosecond)
			select {
			case err := <-done:
				t.Fatal("cleanup bypassed grace", err)
			default:
			}
			clock.Advance(time.Nanosecond)
			select {
			case err := <-done:
				if engine && err != nil {
					t.Fatal(err)
				}
			case <-time.After(testWaitTimeout):
				t.Fatal("cleanup did not escalate")
			}
		})
	}
}
