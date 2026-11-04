package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

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
			w := Worker{StateDir: t.TempDir(), Clock: clock, Interval: time.Millisecond,
				Machine: machine("service", "fake"), Client: Client{URL: server.URL, Token: "test"},
				ServiceConfig: &ServiceConfig{Role: "worker"}, memoryCeilings: fakeCeilings(t)}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			defer func() {
				cancel()
				_ = q.Cancel(j.ID)
				await(t, func() bool {
					clock.Advance(time.Second)
					select {
					case err := <-done:
						if err != nil {
							t.Error(err)
						}
						return true
					default:
						return false
					}
				})
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
			await(t, func() bool { return len(job(t, q, j.ID).AttemptHistory) > 0 })
			got := job(t, q, j.ID)
			if got.State != "queued" || got.Losses != 1 || !got.AttemptHistory[0].Interrupted {
				t.Fatalf("service stop charged payload failure instead of requeue: %+v", got)
			}
			// Recovery must replay the same durable interruption after a lost ack.
			if files, err := filepath.Glob(filepath.Join(w.StateDir, "*.result.json")); err != nil || len(files) != 1 {
				t.Fatalf("missing durable interruption result: %v %v", files, err)
			} else if data, err := os.ReadFile(files[0]); err != nil || !strings.Contains(string(data), `"interrupted":true`) {
				t.Fatalf("restart would lose interruption: %s %v", data, err)
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
