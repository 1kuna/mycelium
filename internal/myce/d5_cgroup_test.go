package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNativeMainMemoryCapComposition(t *testing.T) {
	w := recoveryWorker(t)
	w.memoryCeilings = fakeCeilings(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	marker := filepath.Join(t.TempDir(), "main-cap")
	j := recoveryJob(t, []string{"true"})
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 2
	j.Spec.Run = RunSpec{}
	j.Spec.Execution = &ExecutionSpec{Main: ExecutionMember{MemoryGB: .5, Command: []string{"sh", "-c", `printf '%s' "$MYCE_MEMORY_LIMIT_BYTES" > "$1"`, "main", marker}}}
	p, err := w.start(store, j)
	if err != nil {
		t.Fatal(err)
	}
	if p.native == nil {
		t.Fatal("main cap bypassed child cgroup runner")
	}
	if r := d5Finish(t, p); !r.Success || r.ReleasePending {
		t.Fatal(r)
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "536870912" {
		t.Fatalf("main received wrong cap %s %v", data, err)
	}
	for _, gb := range []float64{-1, 3} {
		invalid := j.Spec
		invalid.Execution = &ExecutionSpec{Main: ExecutionMember{MemoryGB: gb, Command: []string{"true"}}}
		if err := lowerExecution(&invalid); err == nil {
			t.Fatal("invalid cap accepted", gb)
		}
	}
}

func TestNativeGroupCgroupRecoveryPreservesOOMWithOperatorStop(t *testing.T) {
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
}

func TestMemoryCgroupChildrenLinuxIntegration(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real child memory caps require Linux; macOS is admission-only")
	}
	root := filepath.Clean(os.Getenv("MYCE_TEST_MEMORY_CGROUP"))
	if !strings.HasPrefix(root, "/sys/fs/cgroup/") {
		t.Skip("requires MYCE_TEST_MEMORY_CGROUP delegated test parent")
	}
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(membership), "0::"+strings.TrimPrefix(filepath.Join(root, "worker"), "/sys/fs/cgroup")+"\n") {
		t.Skip("runner must be in the supplied parent's worker leaf")
	}
	c, err := delegateMemory(osMemoryFS{}, root, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	parent, err := c.create("attempt-children-"+ID(), 256<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(parent.path); err == nil {
			if err := parent.cleanup(RealClock{}); err != nil {
				t.Error(err)
			}
		}
	})
	children, err := parent.children()
	if err != nil {
		t.Fatal(err)
	}
	main, err := children.create("main", 32<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	side, err := children.create("sidecar-helper", 32<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	start := func(a *memoryAttempt, argv ...string) *exec.Cmd {
		t.Helper()
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		defer w.Close()
		cmd := exec.Command("sh", append([]string{"-c", `read gate || exit 125; exec "$@"`, "cap-test"}, argv...)...)
		cmd.Env = append(os.Environ(), "MYCE_TEST_ALLOCATE_MEMORY=1")
		cmd.Stdin = r
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		if err := a.attach(cmd.Process.Pid); err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteString("start\n"); err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	helper := start(side, "sleep", "600")
	allocation := start(main, os.Args[0], "-test.run=^TestMemoryCeilingAllocationHelper$")
	if err := allocation.Wait(); err == nil {
		t.Fatal("allocation escaped child cap")
	}
	if err := main.failure(); err == nil {
		t.Fatal("child OOM not recorded")
	}
	if err := side.failure(); err != nil {
		t.Fatal("sibling affected by main OOM", err)
	}
	if _, err := processBirth(helper.Process.Pid); err != nil {
		t.Fatal("sibling killed by main OOM", err)
	}
	if err := parent.cleanup(RealClock{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(parent.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("child hierarchy survived cleanup", err)
	}
}

type memberMemoryFS struct {
	fakeMemoryFS
	fault string
	trip  atomic.Bool
}

func (f *memberMemoryFS) WriteFile(p string, b []byte) error {
	if f.fault == "attach" && strings.Contains(p, "sidecar-helper/cgroup.procs") {
		return os.ErrPermission
	}
	return f.fakeMemoryFS.WriteFile(p, b)
}
func (f *memberMemoryFS) ReadFile(p string) ([]byte, error) {
	if f.trip.Load() && f.fault == "population" && filepath.Base(p) == "cgroup.events" {
		return nil, os.ErrPermission
	}
	return f.fakeMemoryFS.ReadFile(p)
}

func d5NativeStart(t *testing.T, group bool, fs *memberMemoryFS, command []string) (Worker, *workerStore, *process) {
	t.Helper()
	w := recoveryWorker(t)
	w.memoryCeilings = fakeCeilings(t)
	if fs != nil {
		w.memoryCeilings.fs = fs
	}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	s.state.Session = w.Machine.Session
	j := recoveryJob(t, command)
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 2
	if group {
		j.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"/bin/sleep", "600"}, MemoryGB: 1}}
		j.Spec.Run.MemoryGB = .5
	}
	p, err := w.start(s, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stop(w.Clock.Now(), 0, true) })
	return w, s, p
}
func d5Finish(t *testing.T, p *process) Result {
	t.Helper()
	select {
	case f := <-p.done:
		if f.err != nil {
			t.Fatal(f.err)
		}
		return f.result
	case <-time.After(10 * time.Second):
		t.Fatal("attempt did not finish")
		return Result{}
	}
}

func TestNativeGroupCgroupCapsAndOOM(t *testing.T) {
	for _, member := range []string{"main", "helper"} {
		t.Run(member, func(t *testing.T) {
			dir := t.TempDir()
			finish := filepath.Join(dir, "finish")
			w, s, p := d5NativeStart(t, true, nil, []string{"sh", "-c", `while [ ! -f "$1" ]; do sleep .01; done`, "main", finish})
			await(t, p.native.started)
			parent := p.native.memory.path
			for name, want := range map[string]int64{"control": 2 << 30, "main": 1 << 29, "sidecar-helper": 1 << 30} {
				data, err := os.ReadFile(filepath.Join(parent, name, "memory.max"))
				if err != nil || string(data) != strconv.FormatInt(want, 10) {
					t.Fatalf("%s cap=%s %v", name, data, err)
				}
				data, err = os.ReadFile(filepath.Join(parent, name, "cgroup.procs"))
				if err != nil || len(data) == 0 {
					t.Fatalf("%s not attached", name)
				}
			}
			data, _ := os.ReadFile(filepath.Join(parent, "cgroup.procs"))
			if len(data) != 0 {
				t.Fatal("parent contains internal processes")
			}
			data, _ = os.ReadFile(filepath.Join(parent, "memory.max"))
			if string(data) != "2147483648" {
				t.Fatal("parent charge changed")
			}
			var leaf string
			for _, m := range p.native.members {
				if m.Name == member {
					leaf = m.Cgroup
				}
			}
			if err := os.WriteFile(filepath.Join(leaf, "memory.events"), []byte("oom 1\noom_kill 1\n"), 0600); err != nil {
				t.Fatal(err)
			}
			p.stop(w.Clock.Now(), 0, true)
			r := d5Finish(t, p)
			if r.Success || r.Interrupted || !r.OutOfMemory || !strings.Contains(r.Error, "native member "+member) {
				t.Fatal(r)
			}
			if r.ReleasePending {
				t.Fatal("release unconfirmed", r)
			}
			if _, err := os.Stat(parent); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("cgroup tree survived", err)
			}
			if err := s.recoverResult(w.Machine.ID); err != nil || s.state.Result == nil || !s.state.Result.OutOfMemory {
				t.Fatal("durable outcome lost", err)
			}
		})
	}
}

func TestNativeGroupCgroupFaults(t *testing.T) {
	for _, fault := range []string{"attach", "population"} {
		t.Run(fault, func(t *testing.T) {
			fs := &memberMemoryFS{fault: fault}
			marker := filepath.Join(t.TempDir(), "main")
			command := []string{"touch", marker}
			if fault == "population" {
				command = []string{"sleep", "600"}
			}
			w, _, p := d5NativeStart(t, true, fs, command)
			if fault == "population" {
				await(t, p.native.started)
				fs.trip.Store(true)
				p.stop(w.Clock.Now(), 0, true)
			}
			r := d5Finish(t, p)
			if fault == "attach" {
				if r.Success || r.Interrupted || r.ReleasePending {
					t.Fatal(r)
				}
				if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("main ran after helper attach refusal")
				}
			} else {
				if !r.ReleasePending {
					t.Fatal("population read failure released custody", r)
				}
				if _, err := os.Stat(p.native.memory.path); err != nil {
					t.Fatal("pending cgroup removed", err)
				}
				fs.trip.Store(false)
				// Clearing the injected fault also lets the fixture join its supervisor.
				if err := p.native.cleanupProcesses(nil, false); err != nil {
					t.Fatal(err)
				}
				if err := p.native.memory.cleanup(w.Clock); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestNativeGroupSharedStopDeadline(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	w, s, p := d5NativeStart(t, true, nil, []string{"sh", "-c", `trap '' TERM; touch "$1"; exec sleep 600`, "main", ready})
	await(t, func() bool { _, err := os.Stat(ready); return err == nil })
	now := w.Clock.Now()
	p.stop(now, 100*time.Millisecond, false)
	p.mu.Lock()
	deadline := *p.stopAt
	p.mu.Unlock()
	p.stop(now.Add(50*time.Millisecond), time.Hour, false)
	p.mu.Lock()
	got := *p.stopAt
	p.mu.Unlock()
	if !got.Equal(deadline) {
		t.Fatal("second stop renewed group deadline")
	}
	var custody workerState
	data, _ := os.ReadFile(filepath.Join(s.dir, "attempt-"+s.state.Identity+".json"))
	if err := decodeWorkerJSON(data, &custody); err != nil {
		t.Fatal(err)
	}
	if custody.StopAt == nil || !custody.StopAt.Equal(deadline) {
		t.Fatal("group deadline not durable")
	}
	p.enforceGrace(deadline)
	if r := d5Finish(t, p); r.Success || !r.Interrupted || !r.Forced || r.ReleasePending {
		t.Fatal(r)
	}
}
