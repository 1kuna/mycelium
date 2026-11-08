package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestWorkerServiceShutdownCancelsInventoryAndSilentCheckin(t *testing.T) {
	entered, canceled, inventoryCanceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	var probes atomic.Int32
	w := Worker{EnvironmentLimit: 1 << 20, StateDir: t.TempDir(), Interval: time.Millisecond,
		Machine: machine("service", "fake"), ServiceConfig: &ServiceConfig{Role: "worker"}}
	w.Client = Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/workers/checkin" {
			once.Do(func() { close(entered) })
			<-r.Context().Done()
			close(canceled)
			return nil, r.Context().Err()
		}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}}
	probeEntered := make(chan struct{})
	w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) {
		if probes.Add(1) == 1 { // ordinary startup inventory
			return []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake-0", Selector: "0", Fingerprint: "fake"}}, nil
		}
		close(probeEntered)
		<-ctx.Done()
		close(inventoryCanceled)
		return nil, ctx.Err()
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer stop()
	for _, ready := range []<-chan struct{}{entered, probeEntered} {
		select {
		case <-ready:
		case err := <-done:
			t.Fatalf("worker exited before the blocked calls: %v", err)
		case <-time.After(3 * time.Second):
			t.Fatal("worker did not enter the blocked calls")
		}
	}
	stop() // signal.NotifyContext delivers installed SIGTERM by canceling this context.
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("idle installed worker requires a queue reply to exit after TERM")
	}
	for _, canceled := range []<-chan struct{}{canceled, inventoryCanceled} {
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatal("shutdown left checkin or GPU inventory alive")
		}
	}
	if probes.Load() != 2 {
		t.Fatal("inventory restarted after cancellation")
	}
}

func TestWorkerServiceShutdownDuringStagingRetainsInterruption(t *testing.T) {
	w := recoveryWorker(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.CPUIsolationProbe = func(context.Context) error { return nil }
	w.JobFileRoot = t.TempDir()
	if err := os.WriteFile(filepath.Join(w.JobFileRoot, "input"), []byte("value"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	j := recoveryJob(t, []string{"true"})
	j.Spec = cpuSpec(1, 1)
	j.Spec.Run.Command = []string{"true"}
	j.Spec.Inputs = map[string]string{"input": "input"}
	if err := normalizeResources(&j.Spec); err != nil {
		t.Fatal(err)
	}
	s, err := newAttemptStore(root, j)
	if err != nil {
		t.Fatal(err)
	}
	w.control = &processControl{clock: w.Clock}
	stop, term := context.WithCancel(context.Background())
	defer term()
	w.serviceStop = stop
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ioProgressKey{}, func() {
		once.Do(func() { close(entered); <-release })
	}))
	a := &attemptSupervisor{store: s, job: j, worker: w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go a.run(ctx)
	defer func() { unblock(); cancel(); <-a.done }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		data, _ := os.ReadFile(filepath.Join(s.dir, s.stateName))
		t.Fatalf("input staging did not enter: %s", data)
	}
	term()
	readResult := func() *Result {
		data, _ := os.ReadFile(filepath.Join(s.dir, s.stateName))
		var state workerState
		if json.Unmarshal(data, &state) != nil {
			return nil
		}
		return state.Result
	}
	await(t, func() bool { r := readResult(); return r != nil && r.ReleasePending })
	if r := readResult(); !r.Interrupted || r.Success {
		t.Fatalf("TERM during staging became a payload failure: %+v", r)
	}
	a.mu.Lock()
	released := a.released
	a.mu.Unlock()
	if released {
		t.Fatal("detached input writer was released before it stopped")
	}
	unblock()
	await(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.released
	})
	if r := readResult(); r == nil || !r.Interrupted || r.ReleasePending {
		t.Fatalf("cleanup lost the durable shutdown interruption: %+v", r)
	}
}

func TestWorkerInteractiveLeaveKeepsAvailabilitySampling(t *testing.T) {
	q, _ := fixture(t)
	clock := &workerFakeClock{now: q.clock.Now()}
	q.clock = clock
	release := filepath.Join(t.TempDir(), "release")
	spec := cpuSpec(1, 1)
	spec.Resumable = false
	spec.Run.Command = []string{"sh", "-c", `echo ready; while test ! -f "$RELEASE"; do sleep .01; done`}
	spec.Run.Env = map[string]string{"RELEASE": release}
	j := submit(t, q, spec)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	var samples atomic.Int32
	w := Worker{EnvironmentLimit: 1 << 20, StateDir: t.TempDir(), Clock: clock, Interval: time.Millisecond,
		Machine: machine("interactive", "fake"), DailyUse: true, Client: Client{URL: server.URL, Token: "test"},
		OwnerSignals: func(context.Context) OwnerSignals {
			// Give the synthetic OS observation a nonzero measured cost;
			// a zero-cost probe teaches a sub-tick freshness allowance.
			clock.Advance(100 * time.Millisecond)
			samples.Add(1)
			return idleSignals()
		}}
	ctx, leave := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { runErr = w.Run(ctx); close(done) }()
	defer func() {
		_ = os.WriteFile(release, nil, 0600)
		leave()
		_ = q.Cancel(j.ID)
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			select {
			case <-done:
				return true
			default:
				return false
			}
		})
		if runErr != nil {
			t.Error(runErr)
		}
	}()
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		logs, _ := q.Logs(j.ID, 0)
		for _, line := range logs {
			if strings.Contains(line.Text, "ready") {
				return true
			}
		}
		return false
	})
	before := samples.Load()
	leave()
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		return samples.Load() >= before+3
	})
	if got := job(t, q, j.ID); got.State != "running" && got.State != "stopping" {
		t.Fatalf("interactive drain stopped non-resumable CPU work: %s", got.State)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		return job(t, q, j.ID).State == "succeeded"
	})
}

// Model the service manager's TERM targets from the generated unit. With the
// old control-group unit the payload receives TERM without a Mycelium delivery
// receipt, reproducing #629's terminal command_exit 143 on CPU experiments.
func TestWorkerServiceShutdownRequeuesNative(t *testing.T) {
	for _, tc := range []struct {
		grouped bool
		exit    int
	}{{false, 143}, {true, 143}, {false, 0}, {true, 0}} {
		grouped := tc.grouped
		name := "command"
		if grouped {
			name = "native-group"
		}
		t.Run(fmt.Sprintf("%s/exit-%d", name, tc.exit), func(t *testing.T) {
			q, _ := fixture(t)
			clock := &workerFakeClock{now: q.clock.Now()}
			q.clock = clock
			s := cpuSpec(1, 1)
			s.Resumable = false
			s.Run.Command = []string{"sh", "-c", `trap 'exit "$EXIT_STATUS"' TERM; echo ready; while :; do sleep .01; done`}
			s.Run.Env = map[string]string{"EXIT_STATUS": fmt.Sprint(tc.exit)}
			if grouped {
				s.Run.MemoryGB = .5
			}
			j := submit(t, q, s)
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			w := Worker{EnvironmentLimit: 1 << 20, StateDir: t.TempDir(), Clock: clock, Interval: time.Millisecond,
				Machine: machine("service", "fake"), Client: Client{URL: server.URL, Token: "test"},
				ServiceConfig: &ServiceConfig{Role: "worker"}, memoryCeilings: fakeCeilings(t)}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			stopped := false
			waitStopped := func() bool {
				clock.Advance(time.Second)
				select {
				case err := <-done:
					stopped = true
					if err != nil {
						t.Error(err)
					}
					return true
				default:
					return false
				}
			}
			defer func() {
				cancel()
				_ = q.Cancel(j.ID)
				if !stopped {
					await(t, waitStopped)
				}
			}()
			await(t, func() bool {
				logs, _ := q.Logs(j.ID, 0)
				for _, l := range logs {
					if strings.Contains(l.Text, "ready") {
						return true
					}
				}
				return false
			})
			unit := (ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}}).ServiceText()
			if strings.Contains(unit, "KillMode=control-group\n") {
				data, err := readSingleAttemptState(filepath.Join(w.StateDir, "state.json"))
				if err != nil {
					t.Fatal(err)
				}
				var state workerState
				if err := json.Unmarshal(data, &state); err != nil || state.PID <= 0 {
					t.Fatalf("missing payload custody: %v", err)
				}
				if err := syscall.Kill(-state.PID, syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			cancel() // mixed: only the worker receives the manager's initial TERM.
			await(t, waitStopped)
			// Recovery must replay the same durable interruption after a lost ack.
			if files, err := filepath.Glob(filepath.Join(w.StateDir, "*.result.json")); err != nil || len(files) != 1 {
				t.Fatalf("missing durable interruption result: %v %v", files, err)
			} else if data, err := os.ReadFile(files[0]); err != nil || !strings.Contains(string(data), `"interrupted":true`) {
				t.Fatalf("restart would lose interruption: %s %v", data, err)
			} else {
				// Installed shutdown no longer waits for queue acknowledgement.
				// Deliver its exact durable receipt through recovery's API.
				var result Result
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatal(err)
				}
				if err := w.Client.Call(context.Background(), "POST", "/api/workers/complete", result, nil); err != nil {
					t.Fatal(err)
				}
			}
			got := job(t, q, j.ID)
			if got.State != "queued" || got.Losses != 1 || !got.AttemptHistory[0].Interrupted {
				t.Fatalf("service stop charged payload failure instead of requeue: %+v", got)
			}
		})
	}
}

func TestWorkerServiceShutdownPreservesPayloadFailure(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(map[bool]string{false: "command", true: "native-group"}[grouped], func(t *testing.T) {
			w := recoveryWorker(t)
			w.memoryCeilings = fakeCeilings(t)
			w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			j := recoveryJob(t, []string{"sh", "-c", "exit 143"})
			j.Spec = cpuSpec(1, 1)
			j.Spec.Run.Command = []string{"sh", "-c", "exit 143"}
			if grouped {
				j.Spec.Run.MemoryGB = .5
			}
			if err := normalizeResources(&j.Spec); err != nil {
				t.Fatal(err)
			}
			p, err := startAfterFixtureInputs(t, w, store, j)
			if err != nil {
				t.Fatal(err)
			}
			r := d5Finish(t, p)
			control := processControl{clock: w.Clock, service: true, leaving: true, processes: map[attemptKey]*process{jobKey(p.job): p}}
			control.apply()
			p.mu.Lock()
			defer p.mu.Unlock()
			if r.Success || r.Interrupted || p.outcome.deliveredBeforeExit() || r.ExecutionCause == nil || (r.ExecutionCause.ExitCode == nil || *r.ExecutionCause.ExitCode != 143) {
				t.Fatalf("shutdown rewrote an already observed payload failure: %+v", r)
			}
		})
	}
}

func TestWorkerServiceShutdownRequeuesDocker(t *testing.T) {
	w, store, clock, root := dockerFixture(t)
	j := dockerJob()
	j.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
	p, err := startDockerFixture(w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.docker.close()
	await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
	control := processControl{clock: clock, service: true, leaving: true, processes: map[attemptKey]*process{jobKey(p.job): p}}
	control.apply()
	await(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Contains(string(data), "kill --signal=TERM ")
	})
	clock.advance(StopGrace)
	control.apply()
	r := d5Finish(t, p)
	if r.Success || !r.Interrupted || r.ReleasePending {
		t.Fatalf("Docker shutdown lost interruption or custody: %+v", r)
	}
	q := restartQueue(t, store, w)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "queued" || got.Losses != 1 || !got.AttemptHistory[0].Interrupted {
		t.Fatalf("Docker service stop did not requeue: %+v", got)
	}
}
