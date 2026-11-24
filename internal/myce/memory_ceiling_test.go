package myce

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fake kernel files make delegation and launch ordering laptop-testable.
type fakeMemoryFS struct{ osMemoryFS }

func (fakeMemoryFS) setOOMScore(int, int) error { return nil }

// A user service may inherit a privileged minimum above zero. Model Linux's
// refusal without writing the laptop's real /proc, including on Darwin.
type inheritedOOMMemoryFS struct {
	fakeMemoryFS
	mu              sync.Mutex
	minimum         int
	refuseRaise     bool
	refusePromotion bool
	scores          map[string]int
	refused         int
	writes          []int
}

func (f *inheritedOOMMemoryFS) ReadFile(path string) ([]byte, error) {
	if strings.HasPrefix(path, "/proc/") && filepath.Base(path) == "oom_score_adj" {
		f.mu.Lock()
		defer f.mu.Unlock()
		pid := filepath.Base(filepath.Dir(path))
		score, ok := f.scores[pid]
		if !ok {
			score = f.minimum
		}
		return []byte(strconv.Itoa(score)), nil
	}
	return f.fakeMemoryFS.ReadFile(path)
}

func (f *inheritedOOMMemoryFS) WriteFile(path string, b []byte) error {
	if strings.HasPrefix(path, "/proc/") && filepath.Base(path) == "oom_score_adj" {
		f.mu.Lock()
		defer f.mu.Unlock()
		pid := filepath.Base(filepath.Dir(path))
		score, err := strconv.Atoi(string(b))
		if err != nil {
			return err
		}
		current, ok := f.scores[pid]
		if !ok {
			current = f.minimum
		}
		f.writes = append(f.writes, score)
		if score < f.minimum || score == current || f.refuseRaise && score > current || f.refusePromotion && score < current {
			f.refused++
			return os.ErrPermission
		}
		f.scores[pid] = score
		return nil
	}
	return f.fakeMemoryFS.WriteFile(path, b)
}

func (f *inheritedOOMMemoryFS) setOOMScore(pid, score int) error {
	return setAttemptOOMScore(f, pid, score)
}

func (f fakeMemoryFS) ReadFile(p string) ([]byte, error) {
	// Worker memory sensors stay synthetic on every laptop platform.
	switch p {
	case "/proc/meminfo":
		return []byte("MemAvailable: 67108864 kB\nSwapTotal: 8388608 kB\nSwapFree: 8388608 kB\n"), nil
	case "/proc/vmstat":
		return []byte("oom_kill 0\npswpout 0\n"), nil
	case "/proc/pressure/memory":
		return []byte("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"), nil
	}
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
	for name, value := range map[string]string{"cgroup.procs": "", "cgroup.events": "populated 0\n", "memory.max": "max", "memory.swap.max": "max", "memory.oom.group": "0", "memory.events": "oom 0\noom_kill 0\n", "memory.pressure": "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n", "cgroup.kill": ""} {
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

func TestMemoryCeilingJointAllowance(t *testing.T) {
	t.Run("unmigrated worker", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("production Linux membership regression")
		}
		c := fakeCeilings(t)
		// Use real local files, but never a real cgroup or service manager.
		// A legacy app.slice unit has no configured slice in its ancestry.
		c.fs = osMemoryFS{}
		w := Worker{Unit: "myce-worker.service", memoryCeilings: c}
		if err := w.syncJointMemoryCeiling(t.Context(), HostMemoryBudget{GiB: 5}); err != nil {
			t.Fatalf("unmigrated worker refused assignments: %v", err)
		}
	})
	for _, slice := range []string{"myce.slice", "mycetest0123456789abcdef0123456789abcdef.slice"} {
		t.Run(slice, func(t *testing.T) {
			c := fakeCeilings(t)
			c.pageSize = 4096
			root := filepath.Join(t.TempDir(), slice)
			if err := c.fs.Mkdir(root); err != nil {
				t.Fatal(err)
			}
			calls := 0
			command := func(_ context.Context, args ...string) error {
				calls++
				if len(args) != 6 || args[3] != slice || calls == 1 && args[4] != "MemoryMax=5368709120" {
					t.Fatal(args)
				}
				for _, arg := range args[4:] {
					key, value, _ := strings.Cut(arg, "=")
					name := map[string]string{"MemoryMax": "memory.max", "MemorySwapMax": "memory.swap.max"}[key]
					if value == "infinity" {
						value = "max"
					}
					if err := c.fs.WriteFile(filepath.Join(root, name), []byte(value)); err != nil {
						return err
					}
				}
				return nil
			}
			c.root = filepath.Join(root, "myce-worker.service")
			w := Worker{Unit: "myce-worker.service", ServiceConfig: &ServiceConfig{Slice: slice}, memoryCeilings: c, jointMemoryCommand: command}
			for _, gb := range []float64{5, 5, 3, 0} {
				if err := w.syncJointMemoryCeiling(t.Context(), HostMemoryBudget{GiB: gb}); err != nil {
					t.Fatal(err)
				}
				if st := w.Machine.MemoryContainment; st == nil || st.State != "available" {
					t.Fatalf("migrated worker status: %+v", st)
				}
			}
			if calls != 3 {
				t.Fatal("unchanged allowance rewrote systemd", calls)
			}
			w.jointMemoryCommand = func(context.Context, ...string) error { return os.ErrPermission }
			if err := w.syncJointMemoryCeiling(t.Context(), HostMemoryBudget{GiB: 7}); !errors.Is(err, os.ErrPermission) || w.Machine.MemoryContainment != nil {
				t.Fatalf("migrated manager failure bypassed: status=%+v err=%v", w.Machine.MemoryContainment, err)
			}
			w.ServiceConfig.Slice = "invalid/path.slice"
			if err := w.syncJointMemoryCeiling(t.Context(), HostMemoryBudget{GiB: 5}); err == nil {
				t.Fatal("invalid configuration degraded instead of refusing")
			}

			if _, err := jointMemoryRoot(filepath.Join(root, "jobs.slice", "attempt.scope"), slice); err != nil {
				t.Fatal(err)
			}
			if _, err := jointMemoryRoot("/user.slice/myce-worker.service", slice); err == nil {
				t.Fatal("old service silently bypassed common ceiling")
			}
			if err := c.fs.WriteFile(filepath.Join(root, "memory.oom.group"), []byte("1")); err != nil {
				t.Fatal(err)
			}
			if err := c.setJointMemoryLimit(context.Background(), root, HostMemoryBudget{}, 0, command); err == nil {
				t.Fatal("whole-host group kill accepted")
			}
			if err := c.setJointMemoryLimit(context.Background(), root, HostMemoryBudget{Measured: true}, 0, command); err == nil {
				t.Fatal("zero measured allowance disabled cap")
			}
			if slice != "myce.slice" {
				if _, err := jointMemoryRoot(filepath.Join(root, "worker.service"), "myce.slice"); err == nil {
					t.Fatal("fixture membership accepted as the live worker's slice")
				}
			}
		})
	}
}

func TestMemoryCeilingTierScores(t *testing.T) {
	for _, tc := range []struct {
		match PriorityMatch
		want  int
	}{
		{PriorityMatch{Tier: "production", Index: 0}, 0},
		{PriorityMatch{Tier: "deadline", Index: 1}, 1000},
		{PriorityMatch{Tier: "experiment", Index: 2}, 1000},
		{PriorityMatch{Tier: "owner-top", Index: 0}, 0},
		{PriorityMatch{Tier: "owner-lower", Index: 12}, 1000},
	} {
		j := Job{Spec: Spec{Priority: "production"}, PriorityPolicy: tc.match}
		if got := attemptOOMScore(j); got != tc.want {
			t.Fatal(tc.match, got)
		}
		if got := modelOOMScore(ModelSlot{PriorityPolicy: &tc.match}); got != tc.want {
			t.Fatal("model", tc.match, got)
		}
	}
	for _, kind := range []string{"batch", "model"} {
		for _, tier := range []string{"highest", "lower", "refused raise", "refused promotion"} {
			t.Run("inherited200/"+kind+"/"+tier, func(t *testing.T) {
				fs := &inheritedOOMMemoryFS{minimum: 200, refuseRaise: tier == "refused raise", refusePromotion: tier == "refused promotion", scores: map[string]int{}}
				c := fakeCeilings(t)
				c.fs = fs
				w := recoveryWorker(t)
				w.Unit, w.memoryCeilings = "myce-worker.service", c
				w.jointMemoryCommand = func(context.Context, ...string) error {
					t.Fatal("unmigrated worker changed a slice")
					return nil
				}
				w.initMemoryCeilings()
				if err := w.syncJointMemoryCeiling(t.Context(), HostMemoryBudget{GiB: 5}); err != nil {
					t.Fatal(err)
				}
				if st := w.Machine.MemoryContainment; st == nil || st.State != "unavailable" || st.Reason == "" || !strings.Contains(st.Next, "#1771") {
					t.Fatalf("unmigrated worker status: %+v", st)
				}
				if w.memoryCeilings != c || w.Machine.MemoryEnforcement != cgroupMemoryEnforcement {
					t.Fatal("per-attempt enforcement lost")
				}
				class, want := "test", "1000"
				if tier == "highest" || tier == "refused promotion" {
					class, want = "production", "200"
				}
				var pid int
				var err error
				var update func(int) error
				if kind == "batch" {
					w.HostMemorySource = func(context.Context) (float64, error) { return 31, nil }
					store, e := w.openStore()
					if e != nil {
						t.Fatal(e)
					}
					defer store.close()
					release := filepath.Join(t.TempDir(), "release")
					var p *process
					defer func() {
						if err := os.WriteFile(release, nil, 0600); err != nil {
							t.Error(err)
						}
						if p == nil {
							return
						}
						select {
						case f := <-p.done:
							if f.err != nil || !f.result.Success {
								t.Errorf("inherited-score batch failed: %+v", f)
							}
						case <-time.After(testWaitTimeout):
							t.Error("inherited-score batch did not complete")
						}
					}()
					j := recoveryJob(t, []string{"sh", "-c", `while [ ! -f "$1" ]; do sleep 0.01; done`, "sh", release})
					j.Spec.Class, j.Spec.Priority = class, ""
					j.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}
					limit := 1.0
					j.Spec.MemoryLimitGB = &limit
					w = workerWithPreparedCheckpoint(t, w, store, j)
					p, err = startAfterFixtureInputs(t, w, store, j)
					if err == nil {
						pid = p.cmd.Process.Pid
						update = p.memory.updateOOMScore
					}
				} else {
					m := serverTestModel(t, "ready")
					m.HostMemoryGB = 1
					e := &LlamaEngine{Dir: t.TempDir(), Clock: RealClock{}, memoryCeilings: c}
					e.memoryOOMScore.Store(int32(modelOOMScore(ModelSlot{Class: class})))
					t.Cleanup(func() {
						if err := e.Unload(); err != nil {
							t.Error(err)
						}
					})
					err = e.Load(t.Context(), m)
					if err == nil {
						pid = e.cmd.Process.Pid
						update = func(score int) error { return e.memoryAttempt.updateOOMScore(score) }
					}
				}
				if err == nil && (tier == "highest" || tier == "refused promotion") {
					if err := update(1000); err != nil {
						t.Fatal("demotion failed:", err)
					}
					err = update(0)
					fs.mu.Lock()
					writes := fmt.Sprint(fs.writes)
					fs.mu.Unlock()
					if writes != "[1000 200]" {
						t.Fatalf("demotion/promotion wrote %s, want [1000 200]", writes)
					}
				}
				if tier == "refused raise" || tier == "refused promotion" {
					cause := runtimeDiagnostic(runtimeErrorFor("launch", err))
					if err == nil || cause == nil || cause.Class != "oom_priority_unavailable" || !errors.Is(err, os.ErrPermission) {
						t.Fatalf("needed score write refusal lost typed diagnostic: %v %+v", err, cause)
					}
					return
				}
				if err != nil {
					t.Fatal("unprivileged inherited-score launch failed:", err)
				}
				entries, err := os.ReadDir(c.root)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, entry := range entries {
					if !entry.IsDir() || entry.Name() == "worker" {
						continue
					}
					found = true
					for name, want := range map[string]string{"memory.max": "1073741824", "memory.swap.max": "0", "memory.oom.group": "1", "cgroup.procs": strconv.Itoa(pid)} {
						b, err := fs.ReadFile(filepath.Join(c.root, entry.Name(), name))
						if err != nil || string(b) != want {
							t.Fatalf("retained %s=%s want=%s err=%v", name, b, want, err)
						}
					}
				}
				if !found {
					t.Fatal("per-attempt memory cgroup missing")
				}
				got, err := fs.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "oom_score_adj"))
				if err != nil || string(got) != want || fs.refused != 0 {
					t.Fatalf("score=%s want=%s refused=%d err=%v", got, want, fs.refused, err)
				}
			})
		}
	}
	q, _ := fixture(t)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.s.ModelSlots["fixture"] = &ModelSlot{Machine: m.ID, Model: "fixture", Project: "example", Class: "production"}
	if plan := q.modelPlan(m.ID); len(plan) != 1 || plan[0].PriorityPolicy != nil {
		t.Fatal("predecessor received new wire field", plan)
	}
	q.s.Machines[m.ID].Capabilities = append(q.s.Machines[m.ID].Capabilities, jointMemoryCapability)
	if plan := q.modelPlan(m.ID); len(plan) != 1 || plan[0].PriorityPolicy == nil || plan[0].PriorityPolicy.Index != 0 {
		t.Fatal("model lost owner tier", plan)
	}
}
func TestMemoryCeilingUnavailableIsExplicit(t *testing.T) {
	w := Worker{Machine: machine("laptop", "fake")}
	w.initMemoryCeilings()
	if !strings.Contains(w.Machine.MemoryEnforcement, "memory ceiling not enforced on this host (") {
		t.Fatal(w.Machine.MemoryEnforcement)
	}
	w.Unit, w.memoryCeilings = "myce-worker.service", fakeCeilings(t)
	w.jointMemoryCommand = func(context.Context, ...string) error {
		t.Fatal("legacy unit must not set slice properties")
		return nil
	}
	if err := w.syncJointMemoryCeiling(t.Context(), HostMemoryBudget{GiB: 5}); err != nil {
		t.Fatal(err)
	}
	q, _ := fixture(t)
	check(t, q, w.Machine, nil)
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Machines) != 1 || s.Machines[0].MemoryContainment == nil || s.Machines[0].MemoryContainment.State != "unavailable" {
		t.Fatalf("worker status lost containment condition: %+v", s.Machines)
	}
	compact := NewCompactStatus(s)
	data, err := json.Marshal(compact)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"memory_containment":{"state":"unavailable"`) {
		t.Fatal(string(data))
	}
	text := strings.Join(compact.Lines(false), "\n")
	for _, want := range []string{"memory_containment: unavailable", "outside the configured slice", "migrate the worker unit", "#1771"} {
		if !strings.Contains(text, want) {
			t.Fatalf("status missing %q: %s", want, text)
		}
	}
}

func TestHostMemoryBudgetAppliesAcrossCards(t *testing.T) {
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
	s.Needs.HostMemoryGB = 2.75
	first := submit(t, q, s)
	c.Advance(1)
	second := submit(t, q, s)
	r := check(t, q, a, nil)
	if r.Job == nil || r.Job.ID != first.ID {
		t.Fatal("first refused")
	}
	if r := check(t, q, b, nil); r.Job != nil {
		t.Fatal("sum of charges exceeded 5 GiB")
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
	limit := 5.0
	j.Spec.MemoryLimitGB = &limit
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
			if got.ID == submitted.ID && !strings.Contains(got.Reason, "explicit memory limit") {
				t.Fatal(got.Reason)
			}
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("worker did not finish")
	}
}

type launchMemoryEventsFS struct {
	fakeMemoryFS
	events string
}

func (f launchMemoryEventsFS) Mkdir(path string) error {
	if err := f.fakeMemoryFS.Mkdir(path); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(path, "memory.events"), []byte(f.events), 0600)
}

func TestMemoryCeilingWorkerOOMBeforeExecApproval(t *testing.T) {
	for _, tc := range []struct {
		name, events string
		oom          bool
	}{
		{"attempt ceiling", "max 19\noom 1\noom_kill 2\noom_group_kill 1\n", true},
		{"pressure only", "max 19\noom 0\noom_kill 0\n", false},
		{"no local trigger", "max 19\noom 0\noom_kill 1\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := recoveryWorker(t)
			w.memoryCeilings = fakeCeilings(t)
			w.memoryCeilings.fs = launchMemoryEventsFS{events: tc.events}
			w.HostMemorySource = func(context.Context) (float64, error) { return 31, nil }
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			j := recoveryJob(t, []string{"true"})
			j.Spec.Resumable = false
			j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
			j.Spec.Needs.MemoryGB = 0
			j.Spec.Needs.HostMemoryGB = minimumHostMemoryGB
			limit := minimumHostMemoryGB
			j.Spec.MemoryLimitGB = &limit
			j.Spec.Run.Env = map[string]string{"MYCE_TEST_EXIT_BEFORE_COMMAND_READY": "1"}
			deadline := w.Clock.Now().Add(testWaitTimeout)
			j.Spec.StartBy = &deadline
			p, err := startAfterFixtureInputs(t, w, store, j)
			if p != nil || err == nil || !strings.Contains(err.Error(), "command start gate unavailable: EOF") {
				t.Fatalf("expected a failed exec handshake, got %v", err)
			}
			r := startFailureOutcome(Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Error: err.Error()}, err)
			if r.Interrupted || r.OutOfMemory != tc.oom || r.ExecutionEvidence == nil || r.ExecutionEvidence.Main != executionNotStarted || r.ExecutionCause == nil || r.ExecutionCause.Operation != "launch" {
				t.Fatalf("unexpected launch outcome: %+v", r)
			}
			if tc.oom {
				f := resourceStopFinding(&j, r.ExecutionCause)
				if r.ExecutionCause.Class != "oom" || r.MemoryPeak == nil || r.MemoryPeak.Events == nil || r.MemoryPeak.Events.OOMKill != 2 ||
					f == nil || f.Code != "out_of_memory" || !strings.Contains(f.Action, "needs.memory_gb") {
					t.Fatalf("lost kernel evidence or resize guidance: %+v, %+v", r, f)
				}
			} else if r.ExecutionCause.Class != "io_error" {
				t.Fatalf("EOF without local OOM became %v", r.ExecutionCause)
			}
		})
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

func TestMemoryCeilingInstallerDelegation(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}}
	if c.workerSlice() != "myce.slice" || c.custodianSlice() != "myce-jobs.slice" {
		t.Fatal("production slice defaults changed")
	}
	if !strings.Contains(c.ServiceText(), "Delegate=memory cpu\n") || !strings.Contains(c.ServiceText(), "Slice=myce.slice\nMemoryAccounting=yes\n") {
		t.Fatal("worker has no delegated memory controller")
	}
	status := WorkerMemoryDelegationStatus(context.Background(), c, func(context.Context, string, ...string) ([]byte, error) { return []byte("Delegate=no\n"), nil })
	if !strings.Contains(status, "memory ceiling not enforced on this host (") {
		t.Fatal(status)
	}
	c.Slice = "mycetest0123456789abcdef0123456789abcdef.slice"
	if !strings.Contains(c.ServiceText(), "Slice="+c.Slice+"\n") || strings.Contains(c.ServiceText(), "Slice=myce.slice\n") {
		t.Fatal("custom worker slice was not rendered")
	}
	if c.custodianSlice() != "mycetest0123456789abcdef0123456789abcdef-jobs.slice" {
		t.Fatal("detached scopes would escape the fixture slice")
	}
	config := filepath.Join(t.TempDir(), "config.json")
	for _, slice := range []string{c.Slice, "", "../myce.slice", "myce.slice\nMemoryMax=infinity", "-.slice", "myce--test.slice", "myce.service"} {
		c.Slice = slice
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(config, data, 0600); err != nil {
			t.Fatal(err)
		}
		restored, err := ReadServiceConfig(config)
		valid := slice == "" || strings.HasPrefix(slice, "mycetest")
		if valid && (err != nil || restored.workerSlice() != c.workerSlice()) || !valid && err == nil {
			t.Fatalf("slice config round trip: %q %+v %v", slice, restored, err)
		}
		if slice == "" {
			if strings.Contains(string(data), `"slice"`) {
				t.Fatal("legacy config should omit slice")
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			fields["slice"] = json.RawMessage(`""`)
			data, err = json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(config, data, 0600); err != nil {
				t.Fatal(err)
			}
			restored, err = ReadServiceConfig(config)
			if err != nil || restored.workerSlice() != "myce.slice" {
				t.Fatalf("explicit empty legacy slice: %+v %v", restored, err)
			}
		}
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
	if p, err := startAfterFixtureInputs(t, w, s, j); err == nil || p != nil || !strings.Contains(err.Error(), memoryWaitReason) {
		t.Fatalf("preparation stale RAM admitted: %v %v", p, err)
	}
}
func TestMemoryCeilingSharedBudgetRejectsOverBudgetSibling(t *testing.T) {
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
	s.Needs.HostMemoryGB = 6
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
	t.Run("joint victim order", func(t *testing.T) {
		joint, err := c.create("joint-integration-"+ID(), 256<<20, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := joint.cleanup(RealClock{}); err != nil {
				t.Error(err)
			}
		}()
		children, err := joint.children()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		start := func(name string, score int) (*exec.Cmd, *os.File, *bufio.Reader, *memoryAttempt) {
			t.Helper()
			a, err := children.create(name, 224<<20, 0)
			if err != nil {
				t.Fatal(err)
			}
			a.oomScore = score
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { r.Close(); w.Close() })
			cmd := exec.CommandContext(ctx, "sh", "-c", `read gate || exit 125; exec "$@"`, "memory-test", os.Args[0], "-test.run=^TestMemoryCeilingAllocationHelper$")
			cmd.Stdin = r
			cmd.Env = append(withoutMyceToken(os.Environ()), "MYCE_TEST_ALLOCATE_MEMORY=1", "MYCE_TEST_MEMORY_HOLD=1")
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
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
			return cmd, w, bufio.NewReader(out), a
		}
		production, gate, ready, protected := start("production", attemptOOMScore(Job{PriorityPolicy: PriorityMatch{Tier: "experiment", Index: 2}}))
		if line, err := ready.ReadString('\n'); err != nil || line != "allocated\n" {
			t.Fatalf("production allocation: %q %v", line, err)
		}
		// A loaded model can be promoted without being restarted. Verify that
		// the unprivileged owner can lower its earlier score back to zero.
		if err := protected.updateOOMScore(attemptOOMScore(Job{PriorityPolicy: PriorityMatch{Tier: "production", Index: 0}})); err != nil {
			t.Fatal(err)
		}
		lower, _, _, victim := start("experiment", attemptOOMScore(Job{PriorityPolicy: PriorityMatch{Tier: "experiment", Index: 2}}))
		if err := lower.Wait(); err == nil {
			t.Fatal("joint overflow did not kill lower tier")
		}
		if err := victim.failure(); err == nil {
			t.Fatal("lower tier has no kernel OOM victim evidence")
		}
		local, err := victim.fs.ReadFile(filepath.Join(victim.path, "memory.events.local"))
		if err != nil || memoryCounters(local)["oom"] != 0 {
			t.Fatalf("victim hit its own limit: %s %v", local, err)
		}
		events, err := joint.fs.ReadFile(filepath.Join(joint.path, "memory.events.local"))
		if err != nil || memoryCounters(events)["oom"] == 0 {
			t.Fatalf("joint limit did not trigger: %s %v", events, err)
		}
		if err := protected.failure(); err != nil {
			t.Fatal("production killed", err)
		}
		if _, err := gate.WriteString("finish\n"); err != nil {
			t.Fatal(err)
		}
		if err := production.Wait(); err != nil {
			t.Fatal("production did not survive", err)
		}
	})
}
func TestMemoryCeilingAllocationHelper(t *testing.T) {
	if os.Getenv("MYCE_TEST_ALLOCATE_MEMORY") != "1" {
		return
	}
	size := 128 << 20
	if os.Getenv("MYCE_TEST_MEMORY_HOLD") == "1" {
		size = 160 << 20
	}
	b := make([]byte, size)
	for i := range b {
		b[i] = 1
	}
	if os.Getenv("MYCE_TEST_MEMORY_HOLD") == "1" {
		fmt.Println("allocated")
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
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
	m.MemoryContainment = &MemoryContainment{State: "unavailable", Reason: "synthetic-secret", Next: "synthetic-secret"}
	check(t, q, m, nil)
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, report := range s.Machines {
		if strings.Contains(report.MemoryEnforcement, "synthetic-secret") || report.MemoryContainment == nil || strings.Contains(report.MemoryContainment.String(), "synthetic-secret") {
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
	p, err := startAfterFixtureInputs(t, w, s, j)
	if err != nil {
		t.Fatal("undeclared batch could not use slice containment", err)
	}
	if result := d5Finish(t, p); !result.Success {
		t.Fatal(result)
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
			if page != 4096 {
				break // Write verification does not depend on page size (#1086).
			}
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
