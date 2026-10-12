package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Fake kernel files make delegation and launch ordering laptop-testable.
type fakeMemoryFS struct{ osMemoryFS }

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
func (f fakeMemoryFS) Remove(p string) error { return os.Rename(p, p+".finished") }
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
	w.HostMemorySource = func(context.Context) (float64, error) { return 31, nil }
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"sh", "-c", fmt.Sprintf("for file in %s/attempt-*/memory.events; do printf 'oom_kill 1\\n' > \"$file\"; done; exit 137", w.memoryCeilings.root)})
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 5
	p, err := w.start(s, j)
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
			}
			select {
			case f := <-p.done:
				calls, _ := os.ReadFile(filepath.Join(root, "calls"))
				if !strings.Contains(string(calls), "--memory 5368709120 --memory-swap 5368709120") || !strings.Contains(string(calls), "MYCE_MEMORY_LIMIT_BYTES=5368709120") {
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
	if !strings.Contains(c.ServiceText(), "Delegate=memory\n") {
		t.Fatal("worker has no delegated memory controller")
	}
	status := WorkerMemoryDelegationStatus(context.Background(), c, func(context.Context, string, ...string) ([]byte, error) { return []byte("Delegate=no\n"), nil })
	if !strings.Contains(status, "memory ceiling not enforced on this host (") {
		t.Fatal(status)
	}
}
func TestDisableSeesMemoryAttemptChildren(t *testing.T) {
	c := fakeCeilings(t)
	a, err := c.create("attempt-live", 5<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.attach(99); err != nil {
		t.Fatal(err)
	}
	populated := "1"
	s := &SystemdHostService{Unit: "myce-worker.service", group: "/user.slice/myce-worker.service", stopped: true,
		Command: func(context.Context, string, ...string) ([]byte, error) {
			return []byte("LoadState=loaded\nKillMode=control-group\nSendSIGKILL=yes\nActiveState=inactive\nControlGroup=/user.slice/myce-worker.service\n"), nil
		},
		ReadFile: func(p string) ([]byte, error) {
			if filepath.Base(p) == "cgroup.events" {
				return []byte("populated " + populated + "\n"), nil
			}
			return []byte(""), nil
		}}
	if empty, err := s.Empty(context.Background()); err != nil || empty {
		t.Fatalf("live child certified empty: %v %v", empty, err)
	}
	if err = a.cleanup(RealClock{}); err != nil {
		t.Fatal(err)
	}
	populated = "0"
	if empty, err := s.Empty(context.Background()); err != nil || !empty {
		t.Fatalf("released child not empty: %v %v", empty, err)
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
	if p, err := w.start(s, j); err == nil || p != nil || !strings.Contains(err.Error(), "host RAM") {
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
	marker := filepath.Join(t.TempDir(), "started")
	j := recoveryJob(t, []string{"touch", marker})
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 5
	if p, err := w.start(s, j); err == nil || p != nil {
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
				if err := q.Complete(Result{Machine: a.ID, Session: a.Session, Execution: Execution{ID: running.ID, Attempt: running.Attempt}, Success: true}); err != nil {
					t.Fatal(err)
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
	if _, err := w.start(s, j); err == nil || !strings.Contains(err.Error(), "host_memory_gb") {
		t.Fatal("undeclared job started", err)
	}
	rt := modelRuntime{requireMemoryCeilings: true}
	if err := rt.checkLoadMemory(context.Background(), &workerModel{model: Model{MemoryGB: 1}}); err == nil || !strings.Contains(err.Error(), "host_memory_gb") {
		t.Fatal("undeclared model started", err)
	}
	w.memoryCeilings = &memoryCeilings{fs: deniedMemoryFS{}, root: c.root}
	if err := w.requireMemoryCeilings(); !errors.Is(err, ErrMemoryCeilingsRequired) {
		t.Fatal("unwritable ceiling accepted", err)
	}
}
