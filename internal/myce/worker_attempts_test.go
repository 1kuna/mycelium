package myce

import (
	"context"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// #249: a transient cleanup failure must not require a worker restart to
// release custody, or leave/shutdown can wait forever on a finished command.
func TestAttemptRetriesNativeCleanupWithoutRestart(t *testing.T) {
	ps, err := exec.LookPath("ps")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	wrapper := `#!/bin/sh
if [ "$*" = "-ax -o pid=,pgid=,stat=" ] && [ -f "$MYCE_TEST_CLEANUP_PS_BLOCK" ]; then
 exit 1
fi
exec "$MYCE_TEST_CLEANUP_REAL_PS" "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "ps"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TEST_CLEANUP_REAL_PS", ps)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, grouped := range []bool{false, true} {
		name := "command"
		if grouped {
			name = "native_group"
		}
		t.Run(name, func(t *testing.T) {
			h := &machineHarness{controls: map[string]AttemptDirective{}}
			j := harnessJob(t, "cleanup-retry", "", "true")
			w := harnessWorker(t, h)
			dir := filepath.Join(w.StateDir, "job-files", j.ID, "1")
			marker, blocked := filepath.Join(dir, "executions"), filepath.Join(dir, "inventory-denied")
			t.Setenv("MYCE_TEST_CLEANUP_PS_BLOCK", blocked)
			j.Spec.Run.Command = []string{"sh", "-c", `printf x >> "$1"; : > "$2"`, "fixture", "{work}/executions", "{work}/inventory-denied"}
			if grouped {
				j.Spec.Run.MemoryGB = 1
			}
			h.jobs = []Job{j}
			w.memoryCeilings = fakeCeilings(t)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			defer func() {
				_ = os.Remove(blocked)
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("worker did not stop after native cleanup recovered")
				}
			}()
			await(t, func() bool { return h.completed(j.ID) })
			pending := h.result(j.ID)
			if !pending.ReleasePending || pending.Success || pending.CleanupCause == nil {
				t.Fatalf("cleanup failure lost pending custody or its outcome: %+v", pending)
			}
			h.mu.Lock()
			before := len(h.reports)
			h.mu.Unlock()
			await(t, func() bool {
				h.mu.Lock()
				defer h.mu.Unlock()
				return len(h.reports) > before
			})
			h.mu.Lock()
			for _, r := range h.results {
				if !r.ReleasePending {
					t.Error("released custody while cleanup was still denied")
				}
			}
			h.mu.Unlock()
			if err := os.Remove(blocked); err != nil {
				t.Fatal(err)
			}
			waitCtx, stopWait := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopWait()
			probeWait(t, waitCtx, func() bool {
				h.mu.Lock()
				defer h.mu.Unlock()
				for _, r := range h.results {
					if r.ID == j.ID && !r.ReleasePending {
						if r.Success || r.Error != pending.Error || r.CleanupCause == nil {
							t.Error("cleanup recovery changed the original failed outcome", r)
						}
						return len(h.reports[len(h.reports)-1].Attempts) == 0
					}
				}
				return false
			})
			if data, err := os.ReadFile(marker); err != nil || string(data) != "x" {
				t.Fatalf("cleanup recovery reran the payload: %q %v", data, err)
			}
		})
	}
}

func TestAttemptPendingReleaseRequiresCleanupCustody(t *testing.T) {
	s := &workerStore{dir: t.TempDir(), state: workerState{
		Identity: "incomplete-custody",
		Job:      &Job{},
		Result:   &Result{ReleasePending: true},
	}}
	// Older ordinary records did not retain group birth/cgroup custody, while
	// the actor had already cleared its live PID on receiving the result.
	if err := saveWorkerJSON(s.dir, "attempt-"+s.state.Identity+".json", workerState{Identity: s.state.Identity, PID: os.Getpid()}); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupOrphan(RealClock{}); err == nil || !s.state.Result.ReleasePending {
		t.Fatal("incomplete pending custody falsely confirmed cleanup", err)
	}
	// A live group without the exact attempt marker is not proof of release
	// and must never receive this attempt's signals.
	cmd := exec.Command("sleep", "600")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() }()
	s.state.PID = cmd.Process.Pid
	if err := saveWorkerJSON(s.dir, "attempt-"+s.state.Identity+".json", workerState{Identity: s.state.Identity, PID: s.state.PID}); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupOrphan(RealClock{}); err == nil {
		t.Fatal("unverified surviving group falsely confirmed cleanup")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("unverified group was signaled", err)
	}
}

func TestAttemptOrdinaryCleanupRetainsCgroupCustody(t *testing.T) {
	identity := ID()
	memory, err := fakeCeilings(t).create("attempt-"+identity, 1<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	s := &workerStore{dir: t.TempDir(), state: workerState{
		Identity: identity, PID: cmd.Process.Pid, Cgroup: memory.path,
		Job: &Job{Spec: Spec{Needs: ResourceNeeds{CPUCores: 1}}}, Result: &Result{ReleasePending: true},
	}}
	if err := saveWorkerJSON(s.dir, "attempt-"+identity+".json", workerState{Identity: identity, PID: s.state.PID}); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupOrphan(RealClock{}, sharedCleanupDeniedFS{}); err == nil {
		t.Fatal("ordinary cleanup ignored denied cgroup cleanup")
	}
	if _, err := os.Stat(memory.path); err != nil {
		t.Fatal("unconfirmed cgroup custody removed", err)
	}
	if err := s.cleanupOrphan(RealClock{}, fakeMemoryFS{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(memory.path); !os.IsNotExist(err) {
		t.Fatal("confirmed cgroup cleanup did not remove custody", err)
	}
}

// The queue may be unavailable after physical release. Reopening repeatedly
// must not resurrect the old pending result after its PID has been cleared.
func TestAttemptCleanupReleaseSurvivesRepeatedRestart(t *testing.T) {
	for _, live := range []bool{false, true} {
		name := "recovery"
		if live {
			name = "live_retry"
		}
		t.Run(name, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "executions")
			cmd := exec.Command("sh", "-c", `printf x >> "$1"; exit 7`, "fixture", marker)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := cmd.Run(); err == nil {
				t.Fatal("fixture payload did not fail")
			}
			w := recoveryWorker(t)
			w.control = &processControl{}
			s, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if s != nil {
					s.close()
				}
			}()
			j := harnessJob(t, "durable-cleanup", "", "true")
			j.Spec.Run.Command = cmd.Args
			j.Session = "original-session"
			r := Result{Machine: w.Machine.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Error: "payload failed; inventory unavailable", ReleasePending: true,
				ExecutionCause: &RuntimeCause{Operation: "command", Class: "command_exit"}, CleanupCause: &RuntimeCause{Operation: "process_group", Class: "command_exit"}}
			s.state = workerState{Session: j.Session, Identity: ID(), PID: cmd.Process.Pid, Job: &j, Result: &r}
			identity := s.state.Identity
			if err := saveWorkerJSON(s.dir, "attempt-"+identity+".json", workerState{Identity: identity, PID: s.state.PID}); err != nil {
				t.Fatal(err)
			}
			if err := saveWorkerJSON(s.dir, identity+".result.json", r); err != nil {
				t.Fatal(err)
			}
			if err := s.save(); err != nil {
				t.Fatal(err)
			}
			if live {
				actor := &attemptSupervisor{store: s, job: j, worker: w, done: make(chan struct{}), wake: make(chan struct{}, 1)}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go actor.run(ctx) // Disconnected: terminal queue acknowledgement is withheld.
				defer func() { cancel(); <-actor.done }()
				await(t, func() bool { actor.mu.Lock(); defer actor.mu.Unlock(); return actor.released })
				cancel()
				<-actor.done
			} else if p, err := w.recoverAttempt(s); err != nil || p != nil {
				t.Fatal("initial cleanup recovery failed", p, err)
			}
			want := r
			want.ReleasePending = false
			for range 2 {
				s.close()
				s, err = w.openStore()
				if err != nil {
					t.Fatal(err)
				}
				if p, err := w.recoverAttempt(s); err != nil || p != nil {
					t.Fatal("confirmed cleanup became pending after restart", p, err)
				}
				if s.state.PID != 0 || !reflect.DeepEqual(s.state.Result, &want) {
					t.Fatalf("restart changed the released failed outcome: %+v", s.state.Result)
				}
			}
			if data, err := os.ReadFile(marker); err != nil || string(data) != "x" {
				t.Fatalf("restart reran payload: %q %v", data, err)
			}
		})
	}
}

// Ordinary commands record delivered stop intent separately. Both their
// ordinary and shared-reader recovery paths must honor that same receipt.
func TestAttemptCleanupPreservesOrdinaryStopIntent(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		path := "ordinary"
		if grouped {
			path = "shared_group"
		}
		for _, userCancel := range []bool{false, true} {
			name := "elapsed_deadline"
			if userCancel {
				name = "user_cancel"
			}
			t.Run(path+"/"+name, func(t *testing.T) {
				dir, identity := t.TempDir(), ID()
				ready, term := filepath.Join(dir, "ready"), filepath.Join(dir, "term")
				cmd := exec.Command("sh", "-c", `trap 'touch "$2"' TERM; touch "$1"; while :; do sleep .01; done`, "fixture", ready, term)
				cmd.Env = append(os.Environ(), "MYCE_PROCESS_ID="+identity)
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() }()
				nativeFile(t, dir, "ready")
				clock := &manualWorkerClock{now: time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)}
				deadline := clock.Now()
				if userCancel {
					deadline = deadline.Add(15 * time.Second)
				}
				custody := workerState{Identity: identity, PID: cmd.Process.Pid, StopGrace: 15 * time.Second}
				if grouped {
					birth, err := processBirth(cmd.Process.Pid)
					if err != nil {
						t.Fatal(err)
					}
					custody.Groups = map[int]map[int]string{cmd.Process.Pid: {cmd.Process.Pid: birth}}
				}
				if err := saveWorkerJSON(dir, "attempt-"+identity+".json", custody); err != nil {
					t.Fatal(err)
				}
				if err := saveWorkerJSON(dir, identity+".stop.json", workerState{Identity: identity, StopAt: &deadline, UserCancel: userCancel}); err != nil {
					t.Fatal(err)
				}
				s := &workerStore{dir: dir, state: workerState{Identity: identity, PID: cmd.Process.Pid, Job: &Job{Spec: spec("experiment")}, StopGrace: custody.StopGrace}}
				done := make(chan struct{})
				var cleanupErr error
				go func() { cleanupErr = s.cleanupOrphan(clock); close(done) }()
				defer func() {
					clock.Advance(time.Hour)
					select {
					case <-done:
					case <-time.After(testWaitTimeout):
						t.Error("cleanup did not join after fixture clock advanced")
					}
				}()
				select {
				case <-done:
					if cleanupErr != nil {
						t.Fatal(cleanupErr)
					}
				case <-time.After(testWaitTimeout):
					t.Fatal("saved stop intent acquired a new accelerator grace")
				}
				if _, err := os.Stat(term); !os.IsNotExist(err) {
					t.Fatal("cleanup repeated an already delivered stop", err)
				}
				if s.state.StopAt == nil || !s.state.StopAt.Equal(deadline) || s.state.UserCancel != userCancel {
					t.Fatal("saved stop intent changed during cleanup")
				}
			})
		}
	}
}

// #152: an admission drain must not hold a running actor's reports or custody.
func TestAttemptReportsAndReleasesDuringAdmissionDrain(t *testing.T) {
	q, _ := fixture(t)
	clock := &workerFakeClock{now: q.clock.Now()}
	q.clock = clock
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", `mkfifo -m 600 "$1" || exit 1; exec 3<> "$1"; read line <&3; echo during-drain; read line <&3`, "fixture", "{work}/command"}
	j := submit(t, q, s)
	run := workerFixture(t, q)
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		return job(t, q, j.ID).StartedAt != nil
	})
	fifo := filepath.Join(nativeWorkerWork(t, run.state), "command")
	nativeFile(t, filepath.Dir(fifo), filepath.Base(fifo))
	command, err := os.OpenFile(fifo, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { command.Close() })
	const host = "fixture-worker-host"
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	// Observe two check-ins after draining: the first response must have reached
	// the actor before the machine loop can send the second request.
	for range 2 {
		clock.Advance(time.Second)
		seen := clock.Now()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			status, err := q.HostMaintenanceStatus(host)
			return err == nil && len(status.Workers) == 1 && !status.Workers[0].Machine.LastSeen.Before(seen)
		})
	}
	if _, err := command.WriteString("log\n"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probeWait(t, ctx, func() bool {
		clock.Advance(time.Millisecond)
		logs, err := q.Logs(j.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range logs {
			if strings.Contains(l.Text, "during-drain") {
				return true
			}
		}
		return false
	})
	if got := job(t, q, j.ID); got.State != "running" {
		t.Fatal("drain stopped the running attempt", got.State)
	}
	if _, err := command.WriteString("finish\n"); err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool {
		clock.Advance(time.Millisecond)
		got := job(t, q, j.ID)
		status, err := q.HostMaintenanceStatus(host)
		return err == nil && got.State == "succeeded" && got.ReleaseConfirmed && status.Quiescent && len(status.Attempts) == 0 && len(status.PendingReleases) == 0
	})
}

func TestAttemptReleasesWhenRAMDropsBeforeEntrypoint(t *testing.T) {
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	h.jobs = []Job{harnessJob(t, "ram-drop", "", "touch {work}/entrypoint")}
	h.jobs[0].Spec.Needs.HostMemoryGB = 22
	w := harnessWorker(t, h)
	marker := filepath.Join(w.StateDir, "job-files", "ram-drop", "1", "entrypoint")
	var free atomic.Uint64
	free.Store(math.Float64bits(64))
	w.HostMemorySource = func(context.Context) (float64, error) {
		return math.Float64frombits(free.Load()), nil
	}
	checkpoint := t.TempDir()
	w.checkpointPrepare = func(Job) (string, error) {
		// Admission succeeded; RAM changed while inputs/checkpoints were staged.
		free.Store(math.Float64bits(21.9))
		return checkpoint, nil
	}
	startHarness(t, w, h)
	deadline := time.After(3 * time.Second)
	for !h.completed("ram-drop") {
		select {
		case <-deadline:
			t.Fatal("RAM refusal retained assigned custody without starting or releasing")
		case <-time.After(5 * time.Millisecond):
		}
	}
	r := h.result("ram-drop")
	if r.Success || r.ReleasePending || r.ExecutionCause == nil || r.ExecutionCause.Operation != "launch" || r.ExecutionCause.Class != "resource_limit" || !strings.Contains(r.Error, "host RAM need 22 GiB, available 21.9 GiB") {
		t.Fatalf("RAM refusal must release with its admission diagnostic: %+v", r)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("entrypoint ran without its RAM reservation", err)
	}

	q, _ := fixture(t)
	m := machine("ram-worker", "fake", "ram-session")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	s := spec("experiment")
	s.Needs.HostMemoryGB = 22
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("fixture was not assigned")
	}
	r.Execution, r.Machine, r.Session = Execution{ID: j.ID, Attempt: assigned.Attempt}, m.ID, m.Session
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || !got.ReleaseConfirmed || got.StartedAt != nil || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("prelaunch RAM refusal must wait in the queue without a loss: %+v", got)
	}
}
