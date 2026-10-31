package myce

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Fake kernel files make delegation and launch ordering laptop-testable.
type fakeMemoryFS struct{ osMemoryFS }

func (f fakeMemoryFS) ReadFile(p string) ([]byte, error) {
	b, err := f.osMemoryFS.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) && filepath.Base(p) == "memory.events.local" {
		return f.osMemoryFS.ReadFile(filepath.Join(filepath.Dir(p), "memory.events"))
	}
	return b, err
}

func (f fakeMemoryFS) Mkdir(p string) error {
	if err := os.Mkdir(p, 0700); err != nil {
		return err
	}
	for name, value := range map[string]string{"cgroup.procs": "", "cgroup.events": "populated 0\n", "memory.max": "max", "memory.swap.max": "max", "memory.oom.group": "0", "memory.events": "oom 0\noom_kill 0\n", "cgroup.kill": ""} {
		if err := os.WriteFile(filepath.Join(p, name), []byte(value), 0600); err != nil {
			return err
		}
	}
	return nil
}
func (f fakeMemoryFS) WriteFile(p string, b []byte) error {
	if filepath.Base(filepath.Dir(p)) == "worker" && filepath.Base(p) == "cgroup.procs" {
		if err := os.WriteFile(filepath.Join(filepath.Dir(filepath.Dir(p)), "cgroup.procs"), nil, 0600); err != nil {
			return err
		}
	}
	return f.osMemoryFS.WriteFile(p, b)
}
func (f fakeMemoryFS) Remove(p string) error {
	entries, err := os.ReadDir(p)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			if err := os.Remove(filepath.Join(p, entry.Name())); err != nil {
				return err
			}
		}
	}
	return os.Remove(p)
}
func fakeCeilings(t *testing.T) *memoryCeilings {
	t.Helper()
	root := t.TempDir()
	for name, value := range map[string]string{"cgroup.controllers": "memory cpu", "cgroup.subtree_control": "", "cgroup.procs": "42\n"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	c, err := delegateMemory(fakeMemoryFS{}, root, 42)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestMemoryCeilingDelegationAndOOM(t *testing.T) {
	c := fakeCeilings(t)
	b, err := os.ReadFile(filepath.Join(c.root, "worker", "cgroup.procs"))
	if err != nil || string(b) != "42" {
		t.Fatalf("worker not in leaf: %s %v", b, err)
	}
	a, err := c.create("attempt-one", 5<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.attach(99); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"memory.max": "5368709120", "memory.swap.max": "0", "memory.oom.group": "1", "cgroup.procs": "99"} {
		b, err := os.ReadFile(filepath.Join(a.path, name))
		if err != nil || string(b) != want {
			t.Fatalf("%s = %s %v", name, b, err)
		}
	}
	if err = os.WriteFile(filepath.Join(a.path, "memory.events"), []byte("oom 2\noom_kill 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = a.failure(); err == nil || err.Error() != "exceeded its 5 GiB memory ceiling" {
		t.Fatalf("OOM: %v", err)
	}
}
func TestMemoryCeilingUnavailableIsExplicit(t *testing.T) {
	w := Worker{Machine: machine("laptop", "fake")}
	w.initMemoryCeilings()
	if !strings.Contains(w.Machine.MemoryEnforcement, "memory ceiling not enforced on this host (") {
		t.Fatal(w.Machine.MemoryEnforcement)
	}
}

func TestHostMemoryBudgetIncludesSlackAcrossCards(t *testing.T) {
	q, c := fixture(t)
	a, b := machine("card0", "fake"), machine("card1", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "shared"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 31
		m.HostMemoryBudgetGB = 5
		m.MemoryCeilingSlackGB = .25
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 2.5
	first := submit(t, q, s)
	c.Advance(1)
	second := submit(t, q, s)
	r := check(t, q, a, nil)
	if r.Job == nil || r.Job.ID != first.ID {
		t.Fatal("first refused")
	}
	if r := check(t, q, b, nil); r.Job != nil {
		t.Fatal("sum of ceilings exceeded 5 GiB")
	}
	finish(t, q, r.Job, true)
	if r := check(t, q, b, nil); r.Job == nil || r.Job.ID != second.ID {
		t.Fatal("released budget not reused")
	}
	if hostFits(&a, 0, 0) {
		t.Fatal("undeclared job bypassed capped host")
	}
}

func TestMemoryCeilingWorkerOOMIsCommandFailure(t *testing.T) {
	w := recoveryWorker(t)
	w.memoryCeilings = fakeCeilings(t)
	w.memoryCeilings.fs = envelopePeakFS{oom: true}
	w.HostMemorySource = func(context.Context) (float64, error) { return 31, nil }
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"sh", "-c", "exit 137"})
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 5
	w = workerWithPreparedCheckpoint(t, w, s, j)
	p, err := startAfterFixtureInputs(t, w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-p.done:
		if f.err != nil || f.result.Success || f.result.Interrupted || f.result.ReleasePending || f.result.Error != "exceeded its 5 GiB memory ceiling" {
			t.Fatalf("result %+v", f)
		}
		q, _ := fixture(t)
		m := machine("worker", "fake")
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 31
		check(t, q, m, nil)
		submitted := submit(t, q, j.Spec)
		running := check(t, q, m, nil).Job
		r := f.result
		r.Machine = m.ID
		r.Session = m.Session
		r.Execution = Execution{ID: running.ID, Attempt: running.Attempt}
		if err = q.Complete(r); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, submitted.ID)
		if got.State != "failed" || got.Losses != 0 {
			t.Fatalf("OOM treated as loss: %+v", got)
		}
		public, err := q.publicSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range public.Jobs {
			if got.ID == submitted.ID && !strings.Contains(got.Reason, "exceeded its 5 GiB memory ceiling") {
				t.Fatal(got.Reason)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not finish")
	}
}

func TestMemoryCeilingModelOOMAfterReady(t *testing.T) {
	c := fakeCeilings(t)
	m := serverTestModel(t, "ready")
	m.HostMemoryGB = 5
	e := &LlamaEngine{Dir: t.TempDir(), Clock: RealClock{}, memoryCeilings: c}
	if err := e.Load(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	defer e.Unload()
	b, err := os.ReadFile(filepath.Join(e.memoryAttempt.path, "cgroup.procs"))
	if err != nil || string(b) != fmt.Sprint(e.cmd.Process.Pid) {
		t.Fatalf("model not attached: %s %v", b, err)
	}
	if err = os.WriteFile(filepath.Join(e.memoryAttempt.path, "memory.events"), []byte("oom_kill 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	rt := &modelRuntime{models: map[string]*workerModel{m.Name: {model: m, engine: e, state: "ready"}}}
	rt.report()
	rt.wg.Wait()
	reports, used := rt.report()
	if len(reports) != 1 || reports[0].State != "failed" || reports[0].Error != "exceeded its 5 GiB memory ceiling" || used != 0 {
		t.Fatalf("model OOM: %+v used %v", reports, used)
	}
}

func TestMemoryCeilingDockerContract(t *testing.T) {
	for _, mode := range []string{"", "missing-memory-limit", "docker-oom"} {
		t.Run(mode, func(t *testing.T) {
			w, s, _, root := dockerFixture(t)
			w.HostMemorySource = func(context.Context) (float64, error) { return 31, nil }
			j := dockerJob()
			j.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
			j.Spec.Needs.HostMemoryGB = 5
			if err := os.WriteFile(filepath.Join(root, "mode"), []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
			p, err := startDockerFixture(w, s, j)
			if err != nil {
				t.Fatal(err)
			}
			defer p.docker.close()
			if mode == "" {
				await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
				p.stop(w.Clock.Now(), 0, true)
				expireDockerStop(t, p)
			}
			expireDockerStop(t, p)
			select {
			case f := <-p.done:
				calls, _ := os.ReadFile(filepath.Join(root, "calls"))
				if !strings.Contains(string(calls), "--memory 5368709120") || !strings.Contains(string(calls), "MYCE_MEMORY_LIMIT_BYTES=5368709120") {
					t.Fatalf("ceiling not supplied: %s", calls)
				}
				if mode == "missing-memory-limit" {
					if strings.Contains(string(calls), "start id1") || f.result.Success || f.result.Interrupted || !strings.Contains(f.result.Error, "Docker memory ceiling not enforced") {
						t.Fatalf("unlimited container started: %+v %s", f, calls)
					}
				}
				if mode == "docker-oom" {
					if f.result.Success || f.result.Interrupted || f.result.Error != "exceeded its 5 GiB memory ceiling" {
						t.Fatalf("Docker OOM: %+v", f)
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Docker did not finish")
			}
		})
	}
}

func TestMemoryCeilingInstallerDelegation(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}}
	if !strings.Contains(c.ServiceText(), "Delegate=memory cpu\n") {
		t.Fatal("worker has no delegated memory controller")
	}
	status := WorkerMemoryDelegationStatus(context.Background(), c, func(context.Context, string, ...string) ([]byte, error) { return []byte("Delegate=no\n"), nil })
	if !strings.Contains(status, "memory ceiling not enforced on this host (") {
		t.Fatal(status)
	}
}
func TestMemoryCeilingFinalLiveRAMGate(t *testing.T) {
	w := recoveryWorker(t)
	calls := 0
	w.HostMemorySource = func(context.Context) (float64, error) {
		calls++
		if calls == 1 {
			return 31, nil
		}
		return 0, nil
	}
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"true"})
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 5
	if p, err := startAfterFixtureInputs(t, w, s, j); err == nil || p != nil || !strings.Contains(err.Error(), "host RAM") {
		t.Fatalf("preparation stale RAM admitted: %v %v", p, err)
	}
}
func TestMemoryCeilingSharedBudgetRejectsUndeclaredSibling(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("capped", "fake"), machine("legacy", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "shared"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 31
	}
	a.HostMemoryBudgetGB = 5
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.AllowedMachines = []string{b.ID}
	submit(t, q, s)
	if r := check(t, q, b, nil); r.Job != nil {
		t.Fatal("uncapped sibling bypassed host budget")
	}
}

type deniedMemoryFS struct{ fakeMemoryFS }

func (f deniedMemoryFS) WriteFile(p string, b []byte) error {
	if filepath.Base(p) == "cgroup.procs" && strings.Contains(filepath.Base(filepath.Dir(p)), "attempt-") {
		return os.ErrPermission
	}
	return f.fakeMemoryFS.WriteFile(p, b)
}
func TestMemoryCeilingAttachFailureNeverRunsPayload(t *testing.T) {
	w := recoveryWorker(t)
	w.memoryCeilings = fakeCeilings(t)
	w.memoryCeilings.fs = deniedMemoryFS{}
	w.HostMemorySource = func(context.Context) (float64, error) { return 31, nil }
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"touch", "{work}/started"})
	marker := filepath.Join(s.dir, "job-files", j.ID, "1", "started")
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 5
	if p, err := startAfterFixtureInputs(t, w, s, j); err == nil || p != nil {
		t.Fatalf("attach refusal ignored: %v %v", p, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("payload escaped gate")
	}
}

func TestMemoryCeilingLinuxIntegration(t *testing.T) {
	root := os.Getenv("MYCE_TEST_MEMORY_CGROUP")
	if runtime.GOOS != "linux" || root == "" {
		t.Skip("requires Linux and explicit MYCE_TEST_MEMORY_CGROUP delegated parent; run tests from its worker leaf")
	}
	root = filepath.Clean(root)
	if !strings.HasPrefix(root, "/sys/fs/cgroup/") {
		t.Fatal("integration root must be a delegated cgroup v2 directory")
	}
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	want := "0::" + strings.TrimPrefix(filepath.Join(root, "worker"), "/sys/fs/cgroup")
	if !strings.Contains(string(membership), want+"\n") {
		t.Skip("test runner must already belong to the supplied parent's worker leaf")
	}
	c, err := delegateMemory(osMemoryFS{}, root, os.Getpid())
	if err != nil {
		t.Skipf("delegation unavailable: %v", err)
	}
	a, err := c.create("attempt-integration-"+ID(), 32<<20, 0)
	if err != nil {
		t.Skipf("memory controller unavailable: %v", err)
	}
	defer func() {
		if err := a.cleanup(RealClock{}); err != nil {
			t.Error(err)
		}
	}()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	cmd := exec.Command("sh", "-c", `read gate || exit 125; exec "$@"`, "memory-test", os.Args[0], "-test.run=^TestMemoryCeilingAllocationHelper$")
	cmd.Stdin = reader
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "MYCE_TOKEN=") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	cmd.Env = append(cmd.Env, "MYCE_TEST_ALLOCATE_MEMORY=1")
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err = a.attach(cmd.Process.Pid); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	if _, err = writer.Write([]byte("start\n")); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("allocation did not hit the ceiling")
	}
	if err = a.failure(); err == nil || err.Error() != "exceeded its 0.03125 GiB memory ceiling" {
		t.Fatalf("real kernel OOM evidence: %v", err)
	}
}
func TestMemoryCeilingAllocationHelper(t *testing.T) {
	if os.Getenv("MYCE_TEST_ALLOCATE_MEMORY") != "1" {
		return
	}
	b := make([]byte, 128<<20)
	for i := range b {
		b[i] = 1
	}
	runtime.KeepAlive(b)
	os.Exit(0)
}

func TestMemoryCeilingPrivacyAndValidation(t *testing.T) {
	for _, value := range []string{"exceeded its confidential GiB memory ceiling", "exceeded its NaN GiB memory ceiling", "exceeded its 5 GiB memory ceiling secret", "command failed: secret"} {
		if got := ceilingDiagnostic(value, "execution failed"); got != "execution failed" {
			t.Fatal(got)
		}
	}
	q, _ := fixture(t)
	m := machine("worker", "fake")
	m.MemoryEnforcement = "synthetic-secret"
	check(t, q, m, nil)
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, report := range s.Machines {
		if strings.Contains(report.MemoryEnforcement, "synthetic-secret") {
			t.Fatal("unsafe capability diagnostic")
		}
	}
	for _, v := range []struct {
		budget, slack float64
		swap          int64
	}{{-1, 0, 0}, {0, -1, 0}, {0, 0, -1}, {1e308, 0, 0}} {
		if _, err := MemoryCeilingConfig(v.budget, v.slack, v.swap); err == nil {
			t.Fatal("invalid config admitted")
		}
	}
}

func TestMemoryCeilingHostBudgetSurvivesWorkerLoss(t *testing.T) {
	for _, model := range []bool{false, true} {
		t.Run(fmt.Sprint(model), func(t *testing.T) {
			q, c := fixture(t)
			a, b := machine("lost", "fake"), machine("healthy", "fake")
			for _, m := range []*Machine{&a, &b} {
				m.HostID = "shared"
				m.HostMemoryMeasured = true
				m.HostAvailableMemoryGB = 31
				m.HostMemoryBudgetGB = 5
			}
			if model {
				if err := q.ConfigureModels(ModelConfig{Models: []Model{{Name: "server", Engine: "fake", Path: "weights", Platforms: []string{"fake"}, MemoryGB: 1, HostMemoryGB: 5}}, Idle: time.Hour, Wait: time.Minute}); err != nil {
					t.Fatal(err)
				}
				for _, m := range []*Machine{&a, &b} {
					m.ModelURL = "http://worker"
					m.ModelConfigHash = modelConfigHash(q.models)
				}
			}
			check(t, q, a, nil)
			check(t, q, b, nil)
			s := spec("experiment")
			s.Needs.HostMemoryGB = 5
			s.AllowedMachines = []string{a.ID}
			var running *Job
			if model {
				if err := q.Pin(ModelPin{Model: "server", Project: "test", Machine: a.ID}); err != nil {
					t.Fatal(err)
				}
			} else {
				submit(t, q, s)
				running = check(t, q, a, nil).Job
				if running == nil {
					t.Fatal("initial job refused")
				}
			}
			s.AllowedMachines = []string{b.ID}
			submit(t, q, s)
			for step := time.Duration(0); step < LossAfter; step += time.Second {
				c.Advance(time.Second)
				check(t, q, b, nil)
			}
			if r := check(t, q, b, nil); r.Job != nil {
				t.Fatal("worker loss freed unconfirmed RAM")
			}
			if !q.s.Machines[a.ID].Gone {
				t.Fatal("fixture did not lose worker")
			}
			if !model {
				if err := q.Complete(Result{Machine: a.ID, Session: a.Session, Execution: Execution{ID: running.ID, Attempt: running.Attempt}, Success: true}); !errors.Is(err, ErrStale) {
					t.Fatalf("obsolete result accepted while settling custody: %v", err)
				}
				if r := check(t, q, b, nil); r.Job == nil {
					t.Fatal("positive cleanup did not release RAM")
				}
			}
		})
	}
}

func TestMemoryCeilingServiceMembership(t *testing.T) {
	for _, line := range []string{"0::/user.slice/myce-worker@gpu1.service/worker\n", "1:name=systemd:/user.slice/myce-worker@gpu1.service\n"} {
		if got := workerUnitFromCgroup([]byte(line)); got != "myce-worker@gpu1.service" {
			t.Fatalf("service membership %q = %q", line, got)
		}
	}
}

func TestMemoryCeilingOOMDuringPreemptionIsFailure(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	submitted := submit(t, q, spec("experiment"))
	running := check(t, q, m, nil).Job
	q.stop(q.s.Jobs[submitted.ID], "production preemption")
	if err := q.Complete(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: running.ID, Attempt: running.Attempt}, Error: "exceeded its 5 GiB memory ceiling"}); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, submitted.ID)
	if got.State != "failed" || got.Losses != 0 {
		t.Fatalf("OOM requeued as interruption: %+v", got)
	}
}

func TestMemoryCeilingModelOOMDuringLoad(t *testing.T) {
	c := fakeCeilings(t)
	m := serverTestModel(t, "crash")
	m.HostMemoryGB = 5
	m.StartCommand = []string{"sh", "-c", fmt.Sprintf(`for file in %s/engine-*/memory.events; do printf 'oom_kill 1\n' > "$file"; done; exit 7`, c.root)}
	e := &LlamaEngine{Dir: t.TempDir(), Clock: RealClock{}, memoryCeilings: c}
	if err := e.Load(context.Background(), m); err == nil || err.Error() != "exceeded its 5 GiB memory ceiling" {
		t.Fatalf("model load OOM: %v", err)
	}
	if err := e.Unload(); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryCeilingDockerUndeclaredOOM(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
	if err := os.WriteFile(filepath.Join(root, "mode"), []byte("docker-oom"), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.docker.close()
	select {
	case f := <-p.done:
		if f.result.Success || f.result.Interrupted || !strings.Contains(f.result.Error, "OOM-killed without a declared memory ceiling") {
			t.Fatalf("undeclared OOM claimed a ceiling: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Docker did not finish")
	}
}

func TestRequireMemoryCeilingsStartupAndDeclarations(t *testing.T) {
	w := recoveryWorker(t)
	w.RequireMemoryCeilings = true
	if err := w.Run(context.Background()); !errors.Is(err, ErrMemoryCeilingsRequired) {
		t.Fatal("started without mandatory delegation", err)
	}
	c := fakeCeilings(t)
	w.memoryCeilings = c
	if err := w.requireMemoryCeilings(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.root, "myce-ceiling-preflight")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("preflight child remained", err)
	}
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"sh", "-c", "exit 0"})
	j.Spec.Needs.HostMemoryGB = 0
	if _, err := startAfterFixtureInputs(t, w, s, j); err == nil || !strings.Contains(err.Error(), "host_memory_gb") {
		t.Fatal("undeclared job started", err)
	}
	rt := modelRuntime{requireMemoryCeilings: true}
	if err := rt.checkLoadMemory(context.Background(), &workerModel{model: Model{MemoryGB: 1}}); err == nil || !strings.Contains(err.Error(), "host_memory_gb") {
		t.Fatal("undeclared model started", err)
	}
	w.memoryCeilings = &memoryCeilings{fs: pagedMemoryFS{pageSize: 4096, mismatch: "memory.max"}, root: c.root}
	if err := w.requireMemoryCeilings(); !errors.Is(err, ErrMemoryCeilingsRequired) {
		t.Fatal("unwritable ceiling accepted", err)
	}
}

// Model Linux's page truncation independently of the requested limit.
type pagedMemoryFS struct {
	fakeMemoryFS
	pageSize int64
	mismatch string
}

func (f pagedMemoryFS) WriteFile(p string, b []byte) error {
	name := filepath.Base(p)
	if name == "memory.max" || name == "memory.swap.max" {
		n, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return err
		}
		b = []byte(strconv.FormatInt(n/f.pageSize*f.pageSize, 10))
	}
	if name == f.mismatch {
		b = []byte("0")
	}
	return f.fakeMemoryFS.WriteFile(p, b)
}
func TestMemoryCeilingPageNormalization(t *testing.T) {
	for _, page := range []int64{4096, 65536} {
		for _, limits := range [][2]int64{{146028889, 1}, {403726926, 70001}, {1, 0}, {2 * page, page}} {
			t.Run(fmt.Sprintf("%d/%d/%d", page, limits[0], limits[1]), func(t *testing.T) {
				c := fakeCeilings(t)
				c.pageSize = int(page)
				c.fs = pagedMemoryFS{pageSize: page}
				a, err := c.create("attempt-rounded", limits[0], limits[1])
				if err != nil {
					t.Fatal(err)
				}
				wantMemory := max(page, limits[0]/page*page)
				wantSwap := int64(0)
				if limits[1] > 0 {
					wantSwap = max(page, limits[1]/page*page)
				}
				if a.bytes != wantMemory {
					t.Fatalf("diagnostic bytes = %d, want %d", a.bytes, wantMemory)
				}
				for name, want := range map[string]int64{"memory.max": wantMemory, "memory.swap.max": wantSwap, "memory.oom.group": 1} {
					b, err := c.fs.ReadFile(filepath.Join(a.path, name))
					if err != nil || string(b) != strconv.FormatInt(want, 10) {
						t.Fatalf("%s = %s, %v; want %d", name, b, err, want)
					}
				}
			})
		}
		for _, name := range []string{"memory.max", "memory.swap.max", "memory.oom.group"} {
			t.Run(fmt.Sprintf("%d/mismatch/%s", page, name), func(t *testing.T) {
				c := fakeCeilings(t)
				c.pageSize = int(page)
				c.fs = pagedMemoryFS{pageSize: page, mismatch: name}
				if a, err := c.create("attempt-mismatch", page+1, page+1); err == nil || a != nil || !strings.Contains(err.Error(), "write did not take effect") {
					t.Fatalf("mismatch accepted: %v %v", a, err)
				}
			})
		}
	}
}
func TestMemoryCeilingPageNormalizationInvalid(t *testing.T) {
	for _, limits := range [][2]int64{{-1, 0}, {4096, -1}} {
		c := fakeCeilings(t)
		c.pageSize = 4096
		if a, err := c.create("attempt-overflow", limits[0], limits[1]); err == nil || a != nil {
			t.Fatalf("invalid limits accepted: %v %v", a, err)
		}
		if _, err := os.Stat(filepath.Join(c.root, "attempt-overflow")); !os.IsNotExist(err) {
			t.Fatal("invalid limits created cgroup", err)
		}
	}
}

func TestMemoryCeilingPageNormalizationMaxInt(t *testing.T) {
	for _, page := range []int64{4096, 65536} {
		got, err := pageAlignedMemory(math.MaxInt64, int(page))
		if err != nil || got != math.MaxInt64/page*page {
			t.Fatalf("max limit = %d, %v", got, err)
		}
	}
}

// Exercise real queue admission across sibling cards, then independent kernel readback.
func TestMemoryCeilingAggregateBudget(t *testing.T) {
	for _, page := range []int64{4096, 65536} {
		t.Run(fmt.Sprint(page), func(t *testing.T) {
			q, _ := fixture(t)
			a, b := machine("card0", "fake"), machine("card1", "fake")
			for _, m := range []*Machine{&a, &b} {
				m.HostID = "shared"
				m.HostMemoryMeasured = true
				m.HostAvailableMemoryGB = 1
				m.HostMemoryBudgetGB = 1
				check(t, q, *m, nil)
			}
			var total int64
			for i, m := range []Machine{a, b} {
				s := spec("experiment")
				s.Needs.HostMemoryGB = []float64{0.3, 0.7}[i]
				s.AllowedMachines = []string{m.ID}
				submit(t, q, s)
				j := check(t, q, m, nil).Job
				if j == nil {
					t.Fatal("budget-fitting job refused")
				}
				w := Worker{Machine: m}
				bytes, err := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
				if err != nil {
					t.Fatal(err)
				}
				c := fakeCeilings(t)
				c.pageSize = int(page)
				c.fs = pagedMemoryFS{pageSize: page}
				attempt, err := c.create("attempt", bytes, 0)
				if err != nil {
					t.Fatal(err)
				}
				total += attempt.bytes
			}
			if total > 1<<30 {
				t.Fatalf("enforced %d > budget %d", total, 1<<30)
			}
		})
	}
}

func TestMemoryCeilingTinyRequestReservation(t *testing.T) {
	for _, page := range []int64{4096, 65536} {
		t.Run(fmt.Sprint(page), func(t *testing.T) {
			q, _ := fixture(t)
			a, b := machine("card0", "fake"), machine("card1", "fake")
			for _, m := range []*Machine{&a, &b} {
				m.HostID = "shared"
				m.HostMemoryMeasured = true
				m.HostAvailableMemoryGB = 1
				m.HostMemoryBudgetGB = 1
				check(t, q, *m, nil)
			}
			s := spec("experiment")
			s.Needs.HostMemoryGB = 1 - float64(page/2)/(1<<30)
			s.AllowedMachines = []string{a.ID}
			submit(t, q, s)
			if check(t, q, a, nil).Job == nil {
				t.Fatal("initial job refused")
			}
			s.Needs.HostMemoryGB = 1.0 / (1 << 30)
			s.AllowedMachines = []string{b.ID}
			submit(t, q, s)
			if check(t, q, b, nil).Job != nil {
				t.Fatal("sub-page request overcommitted near-full budget")
			}
			charge := hostNeed(&b, s.Needs.MemoryGB, s.Needs.HostMemoryGB)
			if charge*(1<<30) < float64(page) {
				t.Fatal("reservation smaller than one page")
			}
			w := Worker{Machine: b}
			w.Machine.HostMemoryBudgetGB = float64(page/2) / (1 << 30)

			if err := w.checkHostMemoryBudget(s.Needs.MemoryGB, s.Needs.HostMemoryGB); !errors.Is(err, errWorkerMemory) {
				t.Fatal("worker accepted budget below minimum reservation", err)
			}
			w.Machine.HostMemoryBudgetGB = charge

			if err := w.checkHostMemoryBudget(s.Needs.MemoryGB, s.Needs.HostMemoryGB); err != nil {
				t.Fatal("worker rejected minimum reservation", err)
			}
			b.FreeHostMemoryGB = charge
			if !hostFits(&b, s.Needs.MemoryGB, s.Needs.HostMemoryGB) {
				t.Fatal("minimum reservation does not fit")
			}
		})
	}
}

func TestMemoryCeilingFractionalByteReservation(t *testing.T) {
	for _, page := range []int64{4096, 65536} {
		for _, requestBytes := range []float64{131072 - 0.5, 65536 - 0.5, 0.5} {
			for _, allocations := range []int{1, 2} {
				t.Run(fmt.Sprintf("%d/%g/%d", page, requestBytes, allocations), func(t *testing.T) {
					requestGiB := requestBytes / (1 << 30)
					reservedBytes := max(requestBytes, 65536)
					q, _ := fixture(t)
					var enforced int64
					var reserved float64
					for i := 0; i < allocations; i++ {
						m := machine(fmt.Sprintf("card%d", i), "fake")
						m.HostID = "shared"
						m.HostMemoryMeasured = true
						m.HostAvailableMemoryGB = 1
						m.HostMemoryBudgetGB = float64(allocations) * reservedBytes / (1 << 30)
						check(t, q, m, nil)
						s := spec("experiment")
						s.Needs.HostMemoryGB = requestGiB
						s.AllowedMachines = []string{m.ID}
						submit(t, q, s)
						j := check(t, q, m, nil).Job
						if j == nil {
							t.Fatal("budget-fitting fractional request refused")
						}
						charge := hostNeed(&m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) * (1 << 30)
						if charge != reservedBytes {
							t.Fatalf("reserved %g bytes, want %g", charge, reservedBytes)
						}
						w := Worker{Machine: m}
						bytes, err := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
						if err != nil {
							t.Fatal(err)
						}
						c := fakeCeilings(t)
						c.pageSize = int(page)
						c.fs = pagedMemoryFS{pageSize: page}
						attempt, err := c.create("attempt", bytes, 0)
						if err != nil {
							t.Fatal(err)
						}
						if float64(attempt.bytes) > charge {
							t.Errorf("enforced %d > reserved %g", attempt.bytes, charge)
						}
						enforced += attempt.bytes
						reserved += charge
					}
					if float64(enforced) > reserved {
						t.Errorf("aggregate enforced %d > reserved %g", enforced, reserved)
					}
				})
			}
		}
	}
}
