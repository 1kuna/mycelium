package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The root tests vary storage readiness while both memory pools have room.
func localRootAdmission(w Worker, job Job) error {
	if w.HostMemorySource == nil {
		w.HostMemorySource = func(context.Context) (float64, error) { return 16, nil }
	}
	if !job.Spec.Needs.cpuOnly() && len(w.Machine.Devices) == 0 {
		w.Machine.Devices = []MachineDevice{{ID: firstDevice(job, w.Machine), Platform: w.Machine.GPU, MemoryMode: "fixed", BudgetGB: 8}}
	}
	return localAttemptAdmission(w, nil, job, nil)
}

func TestWorkerRootsMissingMountAndRecovery(t *testing.T) {
	nas := t.TempDir() // The mountpoint survives, so existence/statfs is not proof.
	mounted := false
	inventoryError := false
	w := Worker{ServiceConfig: &ServiceConfig{NAS: nas}, JobFileRoot: nas, ModelRoot: filepath.Join(nas, "models"), EnvironmentSourceRoot: filepath.Join(nas, "env"), EnvironmentRoot: t.TempDir(), MountRoots: func(context.Context) ([]string, error) {
		if inventoryError {
			return nil, errors.New("inventory failed")
		}
		if mounted {
			return []string{nas}, nil
		}
		return nil, nil
	}}
	checkpoint := filepath.Join(nas, "checkpoints")
	for _, step := range []struct{ mounted, bad bool }{{false, false}, {true, false}, {false, false}, {true, true}, {true, false}} {
		mounted, inventoryError = step.mounted, step.bad
		missing := w.sampleStorageRoots(context.Background(), checkpoint)
		if step.mounted && !step.bad {
			if len(missing) != 0 {
				t.Fatal("mount recovery not recognized", missing)
			}
			continue
		}
		for _, role := range []string{"job-files", "checkpoint", "model", "environment-source"} {
			if !slices.ContainsFunc(missing, func(r UnavailableRoot) bool { return r.Root == role && strings.Contains(r.Reason, "NAS mount") }) {
				t.Fatal("missing affected root", role, missing)
			}
		}
		if slices.ContainsFunc(missing, func(r UnavailableRoot) bool { return r.Root == "environment" }) {
			t.Fatal("local cache marked unavailable")
		}
	}
}

func TestWorkerRootsProductionAdmissionAndExactCustody(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker", "cpu", 4, 16)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	s.SharedInputs = map[string]SharedInput{"asset": {NAS: "assets/input.bin", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	m.Capabilities = append(m.Capabilities, "shared_inputs_v1")
	m.SharedInputCache = &SharedInputCapacity{CapacityBytes: 1 << 20, FreeBytes: 1 << 20}
	check(t, q, m, nil)
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("missing initial assignment")
	}
	clock.Advance(time.Second)
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
	check(t, q, m, assigned)
	q.mu.Lock()
	reason := q.startPlacementRejection(q.s.Jobs[j.ID])
	q.mu.Unlock()
	if reason == "" {
		t.Fatal("mount loss did not fence launch")
	}
	// This observation changes launch permission, never exact-attempt custody.
	if got := job(t, q, j.ID); got.Machine != assigned.Machine || got.Attempt != assigned.Attempt {
		t.Fatal("mount loss changed custody")
	}
	// Exercise the refusal rather than merely fabricating its failure report:
	// the queue now records this decision before the worker reports the result.
	if err := q.NativeCustody(NativeCustodyReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Machine: assigned.Machine, Session: assigned.Session, ProcessID: "process", CustodyID: "group", Phase: "reserved"}); !errors.Is(err, ErrStale) {
		t.Fatalf("mount loss did not refuse native custody: %v", err)
	}
	// A prelaunch refusal requeues with the same fence and does not spend a retry.
	r := result(assigned, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "admission_changed"}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if check(t, q, m, nil).Job != nil {
		t.Fatal("NAS production job bypassed root admission")
	}
	if got := job(t, q, j.ID); !strings.Contains(got.Reason, "checkpoint unavailable") || got.InfrastructureRetries != 0 {
		t.Fatal("missing waiting reason or charged retry", got.Reason)
	}
	local := s
	local.SharedInputs = nil
	local.Name = "local-only"
	localJob := submit(t, q, local)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != localJob.ID {
		t.Fatal("local-only production blocked")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	if r := check(t, q, m, run); r.Stop || r.Kill {
		t.Fatal("mount loss stopped local execution")
	}
	finish(t, q, run, true)
	m.UnavailableRoots = nil
	recovered := check(t, q, m, nil).Job
	if recovered == nil || recovered.ID != j.ID {
		t.Fatal("mount recovery did not restore admission")
	}
	if err := q.Started(result(recovered, false)); err != nil {
		t.Fatal(err)
	}
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
	if r := check(t, q, m, recovered); r.Stop || r.Kill {
		t.Fatal("mount loss revoked started custody")
	}
	finish(t, q, recovered, true)
}

func TestWorkerRootsDependencyChecks(t *testing.T) {
	q, _ := fixture(t)
	input := SharedInput{NAS: "input", SHA256: strings.Repeat("a", 64), SizeBytes: 1}
	base := Job{Spec: cpuSpec(1, 1)}
	base.Spec.Priority = "production"
	// Declared snapshots fall back to the model hub, so a missing model root
	// (the optional local hub cache) does not withhold them.
	snapshots := cloneJob(&base)
	snapshots.Spec.ModelSnapshots = map[string]ModelSnapshot{"base": {Repo: "org/model"}}
	noRoot := cpuMachine("worker", "worker", "cpu", 4, 16)
	noRoot.UnavailableRoots = []UnavailableRoot{{Root: "model", Reason: "configured NAS mount missing"}}
	if reason := q.storageRootReason(&snapshots, &noRoot); reason != "" {
		t.Fatal("model hub fallback withheld by the model root:", reason)
	}
	for _, tc := range []struct {
		root string
		edit func(*Job, *Machine)
	}{
		{"checkpoint", func(j *Job, m *Machine) { j.Spec.SharedInputs = map[string]SharedInput{"data": input} }},
		{"environment", func(j *Job, m *Machine) { j.Spec.Environment.Name = "runtime" }},
		{"extension", func(j *Job, m *Machine) { j.Spec.Extension = "fixture@1" }},
		{"file-cache", func(j *Job, m *Machine) {
			j.Spec.ModelSnapshots = map[string]ModelSnapshot{"base": {Repo: "org/model"}}
		}},
	} {
		t.Run(tc.root, func(t *testing.T) {
			m := cpuMachine("worker", "worker", "cpu", 4, 16)
			m.UnavailableRoots = []UnavailableRoot{{Root: tc.root, Reason: "configured NAS mount missing"}}
			j := cloneJob(&base)
			tc.edit(&j, &m)
			if q.storageRootReason(&j, &m) == "" {
				t.Fatal("NAS dependency bypassed admission")
			}
			if err := localRootAdmission(Worker{Machine: m}, j); err == nil {
				t.Fatal("local launch admitted missing root")
			}
			if tc.root == "checkpoint" {
				m.CachedSharedInputs = []string{input.key()}
				if reason := storageRootReason(&j, &m); reason != "" {
					t.Fatal("verified local input blocked", reason)
				}
			}
			m.UnavailableRoots = nil
			if tc.root == "file-cache" {
				// Snapshot bytes live in FileCacheRoot, independently of the
				// engine model cache configured by CacheRoot.
				m.UnavailableRoots = []UnavailableRoot{{Root: "model-cache", Reason: "configured NAS mount missing"}}
			}
			if reason := q.storageRootReason(&j, &m); reason != "" {
				t.Fatal("recovered dependency still blocked", reason)
			}
			if err := localRootAdmission(Worker{Machine: m}, j); err != nil {
				t.Fatal("recovered dependency refused locally", err)
			}
		})
	}
}

func TestWorkerRootsMissingExtensionAllowsPlainCommand(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("worker", "worker", "cpu", 4, 16)
	m.UnavailableRoots = []UnavailableRoot{{Root: "extension", Reason: "configured NAS mount missing"}}
	s := cpuSpec(1, 1)
	s.Priority = "production"
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("plain command blocked by unrelated extension storage")
	}
	w := Worker{Machine: m, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
	if err := localRootAdmission(w, *assigned); err != nil {
		t.Fatal("plain command refused locally", err)
	}
}

func TestWorkerRootsAutofsIsNotMountedNAS(t *testing.T) {
	for _, osname := range []string{"darwin", "linux"} {
		t.Run(osname, func(t *testing.T) {
			roots, err := workerMountedNASRoots(context.Background(), osname, func(context.Context, string, ...string) ([]byte, error) {
				if osname == "linux" {
					return []byte("map /nas autofs rw 0 0\nfixture /nas/online nfs rw 0 0\n"), nil
				}
				return []byte("map on /nas (autofs)\nfixture on /nas/online (smbfs)\n"), nil
			})
			if err != nil || len(roots) != 1 || roots[0] != "/nas/online" {
				t.Fatal("automount placeholder counted as NAS", roots, err)
			}
		})
	}
}

func TestWorkerRootsDiscoveredCachesReachActorAndModelLaunch(t *testing.T) {
	ctx := context.Background()
	cache, makeModel := cacheFixture(t)
	model := makeModel("cached", "data")
	model.Environment = "runtime"
	model.MemoryGB = 1
	model.Platforms = []string{"fake"}
	if _, err := cache.Prepare(ctx, model); err != nil {
		t.Fatal(err)
	}
	clock := cache.Clock
	shared := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
	input := sharedArchive(t, cache.NASRoot)
	if _, release, err := shared.Acquire(ctx, input, cache.NASRoot); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
	t.Cleanup(func() { _ = removeSharedEntry(shared.cache.Root) })
	recipe := binaryRecipe(t, "fake")
	envCache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
	if _, release, err := envCache.Acquire(ctx, recipe); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
	m := machine("worker", "fake")
	w := Worker{Machine: m, Clock: clock, ModelRoot: cache.NASRoot, CheckpointRoot: cache.NASRoot, EnvironmentSourceRoot: cache.NASRoot, ServiceConfig: &ServiceConfig{NAS: cache.NASRoot}, rootMonitor: &workerRootMonitor{expected: map[string]string{}}, ModelConfig: ModelConfig{Models: []Model{model}}, MountRoots: func(context.Context) ([]string, error) { return nil, nil }}
	actor, engineWorker := w, w // Both are captured before discovery, just like runtime.
	observed := m
	observed.CachedSharedInputs = shared.readyKeys()
	var err error
	observed.CachedModels, _, err = cache.Report([]Model{model})
	if err != nil || len(observed.CachedModels) != 1 || len(observed.CachedSharedInputs) != 1 {
		t.Fatal("missing real cache discovery", err)
	}
	observed.Environments = []string{recipe.Key}
	w.publishStorageReadiness(observed)
	// Remove NAS sources; later execution must use verified local bytes.
	for _, name := range []string{model.Path, input.NAS} {
		if err := os.Remove(filepath.Join(cache.NASRoot, name)); err != nil {
			t.Fatal(err)
		}
	}
	actor.Machine.UnavailableRoots = actor.sampleStorageRoots(ctx, actor.CheckpointRoot)
	next := Job{Spec: cpuSpec(1, 1)}
	next.Spec.Priority = "production"
	next.Spec.SharedInputs = map[string]SharedInput{"input": input}
	if err := localRootAdmission(actor, next); err != nil {
		t.Fatal("stale actor rejected discovered input", err)
	}
	rt := &modelRuntime{models: map[string]*workerModel{}, worker: &engineWorker, session: m.Session, machine: m.ID, platform: "fake", cache: cache, environmentCache: envCache, config: ModelConfig{Wait: time.Second, Models: []Model{model}}, memoryMachine: m, memorySource: func(context.Context) (float64, error) { return 8, nil }, engineFactory: func(Model) Engine { return &FakeEngine{} }, authorize: func(context.Context, HostLaunch) error { return nil }}
	rt.sync([]ModelSlot{{Model: model.Name, Session: m.Session, Attempt: "cache-after-mount-loss", State: "loading", EnvironmentRecipe: &recipe}})
	rt.wg.Wait()
	loaded := rt.models[model.Name]
	if loaded == nil || loaded.state != "ready" {
		t.Fatalf("cached model/prepared recipe failed after mount loss: %+v", loaded)
	}
	rt.sync(nil)
	rt.wg.Wait()
}

func TestWorkerRootsBatchModelUsesAssignedEnvironmentKey(t *testing.T) {
	q, _ := fixture(t)
	recipe := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"runtime": {"fake": recipe}}}); err != nil {
		t.Fatal(err)
	}
	model := Model{Name: "test", Path: "test", Engine: "fake", Environment: "runtime", MemoryGB: 1, Platforms: []string{"fake"}}
	config := ModelConfig{Wait: time.Second, Idle: time.Minute, Models: []Model{model}}
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.Resumable = false
	s.Needs.Model = model.Name
	submitted := submit(t, q, s)
	m := cpuMachine("worker", "worker", "fake", 4, 16)
	m.CachedModels = []string{model.Name}
	m.Environments = []string{recipe.Key}
	m.UnavailableRoots = []UnavailableRoot{{Root: "environment-source", Reason: "configured NAS mount missing"}}
	assignment := check(t, q, m, nil).Job
	if assignment == nil || assignment.ID != submitted.ID {
		t.Fatal("queue did not place cached/prepared batch")
	}
	if assignment.ModelEnvironmentKey != recipe.Key || !assignment.ModelEnvironmentNeedsSource {
		t.Fatal("assignment lost frozen model environment key")
	}
	w := Worker{Machine: m, ModelConfig: config}
	if err := localRootAdmission(w, *assignment); err != nil {
		t.Fatal("worker rejected queue-selected prepared model environment", err)
	}
	w.Machine.Environments = nil
	if err := localRootAdmission(w, *assignment); err == nil {
		t.Fatal("cold model environment accepted without source")
	}
	w.Machine.Environments = []string{recipe.Key}
	w.Machine.UnavailableRoots = []UnavailableRoot{{Root: "environment", Reason: "configured NAS mount missing"}}
	if err := localRootAdmission(w, *assignment); err == nil {
		t.Fatal("missing environment cache admitted")
	}
}

func TestWorkerRootsSidecarOnlyEnvironment(t *testing.T) {
	q, _ := fixture(t)
	recipe := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"helper-runtime": {"fake": recipe}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.Resumable = false
	s.Needs.HostMemoryGB = 2
	s.Run = RunSpec{}
	s.Execution = &ExecutionSpec{Main: ExecutionMember{Command: []string{"true"}}, Sidecars: []NativeSidecar{{Name: "helper", Command: []string{"true"}, Environment: "helper-runtime"}}}
	j := *submit(t, q, s)
	if len(j.EnvironmentRecipes) != 0 || j.SidecarEnvironmentRecipes["helper"]["fake"].Key != recipe.Key {
		t.Fatal("expected only a frozen helper environment")
	}
	m := machine("worker", "fake")
	w := Worker{Machine: m, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
	for _, root := range []string{"environment", "environment-source"} {
		m.UnavailableRoots = []UnavailableRoot{{Root: root, Reason: "configured NAS mount missing"}}
		w.Machine = m
		if reason := q.storageRootReason(&j, &m); !strings.Contains(reason, root+" unavailable") {
			t.Errorf("queue ignored helper %s dependency: %s", root, reason)
		}
		if err := localRootAdmission(w, j); err == nil || !strings.Contains(err.Error(), root+" unavailable") {
			t.Errorf("worker ignored helper %s dependency: %v", root, err)
		}
		m.Environments = []string{recipe.Key}
		w.Machine = m
		if reason := q.storageRootReason(&j, &m); (reason == "") != (root == "environment-source") {
			t.Errorf("prepared helper %s dependency: %s", root, reason)
		}
		if err := localRootAdmission(w, j); (err == nil) != (root == "environment-source") {
			t.Errorf("prepared helper worker %s dependency: %v", root, err)
		}
		m.Environments = nil
	}
	m.UnavailableRoots = nil
	w.Machine = m
	if reason := q.storageRootReason(&j, &m); reason != "" {
		t.Fatal("helper roots recovery ignored", reason)
	}
	if err := localRootAdmission(w, j); err != nil {
		t.Fatal("recovered helper refused locally", err)
	}
}

func TestWorkerRootsQueueBackedOutputs(t *testing.T) {
	q, _ := fixture(t)
	q.deliveryRoot = t.TempDir()
	s := asyncDeliverySpec()
	s.Priority = "production"
	s.Needs = cpuSpec(1, 1).Needs
	j := submit(t, q, s)
	m := cpuMachine("worker", "worker", "cpu", 4, 16)
	m.UnavailableRoots = []UnavailableRoot{{Root: "job-files", Reason: "configured NAS mount missing"}}
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("queue-backed outputs blocked by unused worker root")
	}
	w := Worker{Machine: m, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
	if err := localRootAdmission(w, *assigned); err != nil {
		t.Fatal("queue-backed outputs refused locally", err)
	}
	if err := w.publishExtensionFiles(*assigned, ""); err != nil {
		t.Fatal("queue-backed outputs used worker publication root", err)
	}
	pathInput := cloneJob(assigned)
	pathInput.Spec.Inputs = map[string]string{"input": "data/input"}
	directOutput := cloneJob(assigned)
	directOutput.Spec.OutputDelivery = ""
	directOutput.Spec.After = &PieceDependency{}
	for _, dependent := range []*Job{&pathInput, &directOutput} {
		if reason := q.storageRootReason(dependent, &m); !strings.Contains(reason, "job-files unavailable") {
			t.Fatal("NAS-dependent transport bypassed queue fence", reason)
		}
		if err := localRootAdmission(w, *dependent); err == nil || !strings.Contains(err.Error(), "job-files unavailable") {
			t.Fatal("NAS-dependent transport bypassed worker fence", err)
		}
		healthy := m
		healthy.UnavailableRoots = nil
		if reason := q.storageRootReason(dependent, &healthy); reason != "" {
			t.Fatal("worker root recovery ignored", reason)
		}
		healthyWorker := w
		healthyWorker.Machine = healthy
		if err := localRootAdmission(healthyWorker, *dependent); err != nil {
			t.Fatal("worker root recovery refused locally", err)
		}
	}
}
