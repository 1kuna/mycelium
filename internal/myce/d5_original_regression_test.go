package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A worker restart is not proof a daemon-owned running command was lost.
func TestRQv2WorkerRecoversRunningContainer(t *testing.T) {
	w, store, clock, root := dockerFixture(t)
	q, _ := fixture(t)
	q.clock = clock
	w.CheckpointRoot = q.root
	m := w.Machine
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.FreeHostMemoryGB = true, 32, 32
	j := dockerGroupJob(t)
	j.Spec.Priority, j.Spec.SubmissionKey = "production", "running-recovery"
	submitted := submit(t, q, j.Spec)
	running := check(t, q, m, nil).Job
	if running == nil {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(running, false)); err != nil {
		t.Fatal(err)
	}
	p, err := startDockerFixture(w, store, *running)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	p.docker.close()
	if _, err := os.Stat(filepath.Join(root, "running")); err != nil {
		t.Fatal("container did not survive worker", err)
	}
	recovered, err := w.restoreDocker(store)
	if err != nil || recovered == nil {
		t.Fatal("restore", err)
	}
	t.Cleanup(recovered.docker.close)
	// Payload exits normally while replacement worker owns observation. Preserve
	// the fake daemon's marker as exit evidence rather than deleting it.
	if err := os.Rename(filepath.Join(root, "running"), filepath.Join(root, "natural-exit")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	await(t, func() bool {
		recovered.docker.mu.Lock()
		defer recovered.docker.mu.Unlock()
		return recovered.docker.state.Stopped
	})
	clock.advance(StopGrace)
	select {
	case f := <-recovered.done:
		if f.err != nil {
			t.Fatal(f.err)
		}
		if err := q.Complete(f.result); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, submitted.ID)
		calls, _ := os.ReadFile(filepath.Join(root, "calls"))
		t.Logf("recovered live container: interrupted=%v success=%v state=%s retries=%d main_forced=%v", f.result.Interrupted, f.result.Success, got.State, got.InfrastructureRetries, strings.Contains(string(calls), "kill --signal=KILL id1"))
		if got.State != "succeeded" || got.InfrastructureRetries != 0 || got.Attempt != 1 {
			t.Fatalf("live recovered attempt must complete without requeue: state=%s retries=%d attempt=%d error=%s", got.State, got.InfrastructureRetries, got.Attempt, f.result.Error)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not settle")
	}
}

// Exit precedes stop intent and every signal. Observation order cannot turn
// this independent exit into an infrastructure-caused termination.
func TestRQv2IndependentExitBeforeStopObservation(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, tc := range []struct {
			mode string
			code int
		}{{"main-killed", 137}} {
			t.Run(fmt.Sprint(tc.code), func(t *testing.T) {
				w, store, clock, root := dockerFixture(t)
				q, _ := fixture(t)
				q.clock = clock
				w.CheckpointRoot = q.root
				m := w.Machine
				m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.FreeHostMemoryGB = true, 32, 32
				j := dockerGroupJob(t)
				j.Spec.Priority = "production"
				submitted := submit(t, q, j.Spec)
				running := check(t, q, m, nil).Job
				if running == nil {
					t.Fatal("no assignment")
				}
				if err := q.Started(result(running, false)); err != nil {
					t.Fatal(err)
				}
				p, err := startDockerFixture(w, store, *running)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(p.docker.close)
				await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
				p.docker.close()
				if err := os.WriteFile(filepath.Join(root, "mode"), []byte(tc.mode), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(root, "running"), filepath.Join(root, "natural-exit")); err != nil {
					t.Fatal(err)
				}
				p.stop(clock.Now(), StopGrace, false)
				restartTick(t, p.docker)
				clock.advance(StopGrace)
				for i := 0; i < 4 && !p.docker.state.Released; i++ {
					restartTick(t, p.docker)
				}
				if !p.docker.state.Released {
					t.Fatal("custody not released")
				}

				calls, err := os.ReadFile(filepath.Join(root, "calls"))
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(calls), "kill --signal=KILL id1") || strings.Contains(string(calls), "kill --signal=TERM id1") {
					t.Fatal("fixture delivered a main stop signal")
				}
				if p.docker.state.ExitCode != tc.code {
					t.Fatal("lost independent exit status", p.docker.state.ExitCode)
				}
				r := dockerOutcome(p.docker.state, result(running, false), nil)
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, submitted.ID)
				t.Logf("independent exit before stop: exit=%d mainFinished=%v infrastructureStop=%v interrupted=%v state=%s retries=%d", p.docker.state.ExitCode, p.docker.state.MainFinished, p.docker.state.InfrastructureStop, r.Interrupted, got.State, got.InfrastructureRetries)
				if got.State != "failed" || got.InfrastructureRetries != 0 || r.Interrupted {
					t.Fatalf("independent exit must fail without retry: state=%s retries=%d error=%s", got.State, got.InfrastructureRetries, r.Error)
				}
			})
		}

	})

	t.Run("DockerIndependentHelper137BeforeStopObservation", func(t *testing.T) {
		_, s, d, c, root := restartResultFixture(t)
		if err := os.WriteFile(filepath.Join(root, "mode"), []byte("sidecar-killed"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(root, "running-sidecar-sortformer"), filepath.Join(root, "natural-sidecar-exit")); err != nil {
			t.Fatal(err)
		}
		d.stop(c.Now(), StopGrace, false)
		restartTick(t, d)
		c.advance(StopGrace)
		for i := 0; i < 5 && !d.state.Released; i++ {
			restartTick(t, d)
		}
		if !d.state.Released {
			t.Fatal("custody not released")
		}
		state := restartRead(t, s)
		helper := state.Containers["sidecar-sortformer"]
		if len(helper.Signals) != 0 || helper.Exit == nil || helper.Exit.Status != 137 {
			t.Fatal("independent helper evidence lost", helper)
		}
		r := dockerOutcome(state, Result{}, nil)
		if r.Success || r.Interrupted {
			t.Fatal("helper exit masked by later main delivery", r)
		}

	})
}

// The marker is stop provenance, not permission to erase a saved stage failure.
func TestRQv2OperatorMarkerPreservesSavedStageFailure(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		m := machine("operator", "fake", "d5-review-synthetic-host")
		j := submit(t, q, spec("production"))
		old := check(t, q, m, nil).Job
		if old == nil {
			t.Fatal("no assignment")
		}
		if err := q.Started(result(old, false)); err != nil {
			t.Fatal(err)
		}
		w := recoveryWorker(t)
		w.Machine = m
		w.CheckpointRoot = q.root
		server := workerTestServer(API{Queue: q, Token: "test"}.Handler())
		defer server.Close()
		w.Client = Client{URL: server.URL, Token: "test"}
		s, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		s.state = workerState{Session: m.Session, Identity: "operator-stage-failure", Job: old, Log: "operator-stage-failure.log", CheckpointRoot: q.root}
		if err := s.save(); err != nil {
			t.Fatal(err)
		}
		if err := saveWorkerJSON(s.dir, "operator-stop.json", s.state); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.dir, s.state.Log), nil, 0600); err != nil {
			t.Fatal(err)
		}
		r := result(old, false)
		r.Error = "exit status 7"
		if err := saveWorkerJSON(s.dir, s.state.Identity+".result.json", r); err != nil {
			t.Fatal(err)
		}
		s.close()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx) }()
		defer func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(8 * time.Second):
				t.Error("worker did not stop")
			}
		}()
		await(t, func() bool { got := job(t, q, j.ID); return got.InfrastructureRetries != 0 || terminal(got.State) })
		got := job(t, q, j.ID)
		t.Logf("saved nonzero + operator marker: state=%s retries=%d reason=%s", got.State, got.InfrastructureRetries, got.Reason)
		if got.State != "failed" || got.InfrastructureRetries != 0 {
			t.Fatalf("operator marker overwrote stage cause: state=%s retries=%d reason=%s", got.State, got.InfrastructureRetries, got.Reason)
		}

	})

	t.Run("NativeGroupCgroupRecoveryPreservesOOMWithOperatorStop", func(t *testing.T) {
		q, _ := fixture(t)
		m := machine("recovery-worker", "fake", "d5-synthetic-host")
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 64
		m.FreeHostMemoryGB = 64
		specification := spec("production")
		specification.Needs.HostMemoryGB = 2
		specification.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "600"}, MemoryGB: 1}}
		j := submit(t, q, specification)
		running := check(t, q, m, nil).Job
		if err := q.Started(result(running, false)); err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(API{Queue: q, Token: "d5-synthetic"}.Handler())
		defer server.Close()
		w := recoveryWorker(t)
		w.Machine = m
		w.Client = Client{URL: server.URL, Token: "d5-synthetic"}
		w.memoryCeilings = fakeCeilings(t)
		w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
		identity := ID()
		parent, err := w.memoryCeilings.create("attempt-"+identity, 2<<30, 0)
		if err != nil {
			t.Fatal(err)
		}
		children, err := parent.children()
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := children.create("sidecar-helper", 1<<30, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(leaf.path, "memory.events"), []byte("oom 1\noom_kill 1\n"), 0600); err != nil {
			t.Fatal(err)
		}
		store, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		store.state = workerState{Job: running, Session: m.Session, Identity: identity, Log: identity + ".log"}
		if err := store.save(); err != nil {
			t.Fatal(err)
		}
		if err := saveWorkerJSON(store.dir, "operator-stop.json", store.state); err != nil {
			t.Fatal(err)
		}
		if err := saveWorkerJSON(store.dir, "attempt-"+identity+".json", workerState{Identity: identity, Cgroup: parent.path, Groups: map[int]map[int]string{}}); err != nil {
			t.Fatal(err)
		}
		if err := saveWorkerJSON(store.dir, "native-"+identity+".json", struct {
			Identity string
			Members  []nativeMember
		}{identity, []nativeMember{{Name: "helper", Cgroup: leaf.path}}}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(store.dir, identity+".log"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		store.close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
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
				t.Error("recovered worker did not stop")
			}
		}()
		await(t, func() bool { got := job(t, q, j.ID); return got.State == "failed" && got.ReleaseConfirmed })
		got := job(t, q, j.ID)
		if got.InfrastructureRetries != 0 || got.ExecutionCause == nil || got.ExecutionCause.Class != "resource_limit" {
			t.Fatal("OOM reclassified as operator interruption", got)
		}
		if _, err := os.Stat(parent.path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("recovery retained empty child tree", err)
		}

	})
}
