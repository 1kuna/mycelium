package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

func d5InterruptedOutcome(t *testing.T, runtime string, owner Result) Result {
	t.Helper()
	r := d5RuntimeOutcome(t, runtime, outcomeRow{name: "delivered-stop", code: 143, delivered: true})
	r.Machine, r.Session, r.Execution = owner.Machine, owner.Session, owner.Execution
	return r
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
				if r.Success || !r.Interrupted {
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

type outcomeRow struct {
	startFailureKind                                       string
	name                                                   string
	code                                                   int
	intent, delivered, late, lost, oom, recovery, operator bool
	priority                                               bool
	launchRefusal, portProbe                               bool
	success, interrupted                                   bool
}

func TestOutcomeRuntimeTable(t *testing.T) {
	for _, runtime := range []string{"native", "native-group", "docker"} {
		for _, tc := range []outcomeRow{
			{name: "completion", success: true},
			{name: "priority-cooperative-stop", priority: true, delivered: true, recovery: true, interrupted: true},
			{name: "priority-independent-exit", priority: true, code: 7},
			{name: "priority-oom", priority: true, code: 137, delivered: true, oom: true},
			{name: "mycelium-launch-refusal", launchRefusal: true, interrupted: true},
			{name: "mycelium-foreign-port-probe", code: 1, portProbe: true, interrupted: true},
			{name: "exit-143-without-signal", code: 143},
			{name: "observation-loss", code: 137, lost: true, interrupted: true},
		} {
			t.Run(runtime+"/"+tc.name, func(t *testing.T) {
				r := operatorOutcome(d5RuntimeOutcome(t, runtime, tc), tc.operator)
				if (r.PriorityStop != nil) != (tc.priority && tc.interrupted) {
					t.Fatalf("priority stop provenance: %+v", r)
				}
				if r.Success != tc.success || r.Interrupted != tc.interrupted || r.OutOfMemory != tc.oom || r.OperatorStop != tc.operator {
					t.Fatalf("got %+v; want success=%v interruption=%v oom=%v", r, tc.success, tc.interrupted, tc.oom)
				}
			})
		}
	}
}

// Only daemon/process-observation and memory accounting are faked. Each row
// starts the actual worker adapter, delivers real native signals and waits for
// its persisted result. In particular native and native-group never share a
// synthetic wait-status helper.
func d5RuntimeOutcome(t *testing.T, runtime string, tc outcomeRow) Result {
	t.Helper()
	if runtime == "docker" {
		return d5DockerRuntimeOutcome(t, tc)
	}
	if tc.launchRefusal || tc.startFailureKind != "" {
		return d5RuntimeStartFailure(t, runtime, tc.startFailureKind)
	}
	if tc.portProbe && runtime == "native-group" {
		return nativePortHandoff(t, 3)
	}
	if tc.lost {
		return d5NativeWorkerLoss(t, runtime == "native-group")
	}
	dir := t.TempDir()
	script := fmt.Sprintf(`trap 'exit %d' TERM; touch "$1"; while [ ! -f "$2" ]; do sleep .01; done; exit %d`, tc.code, tc.code)
	w, s, p := d5NativeStart(t, runtime == "native-group", nil, []string{"sh", "-c", script, "main", dir + "/ready", dir + "/finish"})
	nativeFile(t, dir, "ready")
	if tc.priority {
		if err := p.recordPriorityStop(s, &PriorityPreemption{Cause: priorityPreemptionCause}); err != nil {
			t.Fatal(err)
		}
	}
	if tc.portProbe {
		fact := d5ForeignPortProbe(t, p.cmd.Process.Pid)
		p.mu.Lock()
		p.outcome.Infrastructure = append(p.outcome.Infrastructure, fact)
		if err := p.saveOutcome(p.outcome); err != nil {
			t.Fatal(err)
		}
		p.mu.Unlock()
	}
	if tc.oom {
		leaf := filepath.Join(w.memoryCeilings.root, "attempt-"+s.state.Identity)
		p.mu.Lock()
		if p.native != nil {
			leaf = p.native.main.Cgroup
		}
		p.mu.Unlock()
		if err := os.WriteFile(filepath.Join(leaf, "memory.events"), []byte("oom 1\noom_kill 1\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	if tc.intent {
		p.mu.Lock()
		p.stopped = true
		p.mu.Unlock()
	}
	if tc.delivered {
		p.stop(w.Clock.Now(), time.Second, false)
	} else {
		nativeTouch(t, dir, "finish")
	}
	r := d5Finish(t, p)
	if tc.late {
		p.stop(w.Clock.Now(), time.Second, false)
	}
	if tc.recovery {
		s.state.Result = nil
		if err := s.recoverResult(w.Machine.ID); err != nil || s.state.Result == nil {
			t.Fatal("saved result recovery", err)
		}
		r = *s.state.Result
	}
	return r
}

func d5DockerRuntimeOutcome(t *testing.T, tc outcomeRow) Result {
	t.Helper()
	if tc.startFailureKind == environmentFailed {
		// Environment acquisition precedes runtime selection in Worker.start.
		return d5RuntimeStartFailure(t, "docker", environmentFailed)
	}
	if tc.launchRefusal || tc.startFailureKind == launchFailed {
		w, s, _, root := dockerFixture(t)
		if tc.launchRefusal {
			w.Client = d5DeniedLaunchClient(t)
		} else {
			if err := os.WriteFile(filepath.Join(root, "mode"), []byte("refuse-create"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		job := dockerGroupJob(t)
		if tc.startFailureKind == launchFailed {
			job = dockerJob()
			job.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
		}
		p, err := startDockerFixture(w, s, job)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.docker.close)
		return d5Finish(t, p)
	}
	_, s, d, c, root := restartResultFixture(t)
	if tc.priority {
		d.mu.Lock()
		d.state.PriorityStop = &PriorityPreemption{Cause: priorityPreemptionCause}
		err := d.saveLocked()
		d.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	if tc.portProbe {
		// Fake daemon boundary supplies a real, separate process group as the
		// synthetic container's PID namespace. The listener is outside that group.
		anchor := exec.Command("sleep", "600")
		anchor.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := anchor.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = anchor.Process.Kill(); _ = anchor.Wait() }()
		if err := d.recordInfrastructure(d5ForeignPortProbe(t, anchor.Process.Pid), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "mode"), []byte(fmt.Sprintf("main-exit-%d", tc.code)), 0600); err != nil {
		t.Fatal(err)
	}
	if tc.oom {
		// The fake daemon supplies OOMKilled through the real inspection adapter.
		if err := os.WriteFile(filepath.Join(root, "table-oom"), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if tc.lost {
		if err := os.WriteFile(filepath.Join(root, "mode"), []byte("down"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := d.reconcile(); err == nil {
			t.Fatal("lost daemon was observed")
		}
		if !restartRead(t, s).ObservationLost {
			t.Fatal("observation loss was not persisted")
		}
		if err := os.WriteFile(filepath.Join(root, "mode"), []byte(fmt.Sprintf("main-exit-%d", tc.code)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if tc.intent {
		d.stop(c.Now(), StopGrace, false)
	}
	if tc.delivered {
		d.stop(c.Now(), StopGrace, false)
		restartTick(t, d) // persist confirmed delivery while main is live
	}
	if err := os.Rename(filepath.Join(root, "running"), filepath.Join(root, "natural-exit")); err != nil {
		t.Fatal(err)
	}
	if tc.late {
		d.stop(c.Now(), StopGrace, false)
	}
	restartTick(t, d)
	c.advance(StopGrace)
	for i := 0; i < 5 && !d.state.Released; i++ {
		restartTick(t, d)
	}
	if !d.state.Released {
		t.Fatal("adapter did not release custody")
	}
	state := restartRead(t, s)
	var recoveryErr error
	if tc.recovery {
		recoveryErr = dockerRecoveryError(state)
	}
	return dockerOutcome(state, Result{}, recoveryErr)
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

// Kill only the worker process; the replacement uses actual orphan recovery.
func d5NativeWorkerLoss(t *testing.T, group bool) Result {
	t.Helper()
	q, _ := fixture(t)
	q.clock = RealClock{}
	dir := t.TempDir()
	marker := filepath.Join(dir, "ready")
	specification := spec("production")
	specification.Resumable = false
	specification.Needs.HostMemoryGB = 2
	specification.Run.Command = []string{"sh", "-c", `touch "$1"; exec sleep 600`, "main", marker}
	if group {
		specification.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "600"}, MemoryGB: 1}}
	}
	submit(t, q, specification)
	received := make(chan Result, 1)
	var once sync.Once
	api := API{Queue: q, Token: "review-synthetic"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var outcome Result
		completion := r.URL.Path == "/api/workers/complete"
		if completion {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			if err = json.Unmarshal(data, &outcome); err != nil {
				t.Error(err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		api.ServeHTTP(rw, r)
		if completion {
			once.Do(func() { received <- outcome })
		}
	}))
	defer server.Close()
	stateDir := filepath.Join(dir, "worker")
	child := exec.Command(os.Args[0], "-test.run=^TestNativeGroupWorkerCrashHelper$")
	child.Env = append(os.Environ(), "NATIVE_CRASH_WORKER=1", "NATIVE_CRASH_URL="+server.URL, "NATIVE_CRASH_STATE="+stateDir)
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
		store, err := (Worker{StateDir: stateDir}).openStore()
		if err == nil {
			defer store.close()
			if err = store.cleanupOrphan(RealClock{}); err != nil {
				t.Error(err)
			}
		}
	})
	nativeFile(t, dir, "ready")
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	if err := q.Leave("review-worker", true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := Worker{Clock: RealClock{}, StateDir: stateDir, Client: Client{URL: server.URL, Token: "review-synthetic"}, Machine: machine("review-worker", "fake"), Interval: 5 * time.Millisecond, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("replacement worker did not stop")
		}
	}()
	select {
	case r := <-received:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("worker loss was not reported")
		return Result{}
	}
}

func d5DeniedLaunchClient(t *testing.T) Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) { rw.WriteHeader(http.StatusConflict) }))
	t.Cleanup(server.Close)
	return Client{URL: server.URL}
}
func d5RuntimeStartFailure(t *testing.T, runtime string, kind string) Result {
	t.Helper()
	w := recoveryWorker(t)
	if kind == "" {
		w.Client = d5DeniedLaunchClient(t)
	}
	w.memoryCeilings = fakeCeilings(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.state.Session = w.Machine.Session
	j := recoveryJob(t, []string{"sleep", "600"})
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 2
	if runtime == "docker" {
		j.Spec.Run = dockerJob().Spec.Run
	}
	if kind == launchFailed {
		j.Spec.Run.Command = []string{"/does-not-exist/myce-table"}
	}
	if kind == environmentFailed {
		j.EnvironmentRecipes = map[string]EnvironmentRecipe{w.Machine.GPU: {}}
	}
	if runtime == "native-group" {
		j.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "600"}, MemoryGB: 1}}
	}
	p, err := w.start(s, j)
	if err != nil {
		return startFailureOutcome(Result{Error: err.Error()}, err)
	}
	defer p.stop(w.Clock.Now(), 0, true)
	return d5Finish(t, p)
}
func d5ForeignPortProbe(t *testing.T, group int) infrastructureFact {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if err := verifyNativeListener(port, group); !errors.Is(err, errNativeForeignListener) {
		t.Fatal("foreign listener was not proven", err)
	}
	return infrastructureFact{Kind: foreignPort, At: time.Now().UTC(), Port: port, Group: group}
}
