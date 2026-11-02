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
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("missing initial assignment")
	}
	clock.Advance(time.Second)
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
	check(t, q, m, assigned)
	if reason := q.startPlacementRejection(q.s.Jobs[j.ID]); reason == "" {
		t.Fatal("mount loss did not fence launch")
	}
	// This observation changes launch permission, never exact-attempt custody.
	if got := job(t, q, j.ID); got.Machine != assigned.Machine || got.Attempt != assigned.Attempt {
		t.Fatal("mount loss changed custody")
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
	for _, tc := range []struct {
		root string
		edit func(*Job, *Machine)
	}{
		{"checkpoint", func(j *Job, m *Machine) { j.Spec.SharedInputs = map[string]SharedInput{"data": input} }},
		{"model", func(j *Job, m *Machine) {
			j.Spec.ModelSnapshots = map[string]ModelSnapshot{"base": {Repo: "org/model"}}
		}},
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
			if err := localAttemptAdmission(Worker{Machine: m}, nil, j, nil); err == nil {
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
			if err := localAttemptAdmission(Worker{Machine: m}, nil, j, nil); err != nil {
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
	if err := localAttemptAdmission(w, nil, *assigned, nil); err != nil {
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

func TestWorkerRootsCachedModelAndDirectNAS(t *testing.T) {
	m := Machine{UnavailableRoots: []UnavailableRoot{{Root: "model", Reason: "configured NAS mount missing"}}, CachedModels: []string{"model"}}
	j := Job{Spec: cpuSpec(1, 1)}
	j.Spec.Priority = "production"
	j.Spec.Needs.Model = "model"
	model := Model{Name: "model"}
	if storageRootReasonForModel(&j, &m, model, nil) != "" {
		t.Fatal("local cached model blocked")
	}
	model.DirectNAS = true
	if storageRootReasonForModel(&j, &m, model, nil) == "" {
		t.Fatal("direct NAS model bypassed missing mount")
	}
}

func TestWorkerRootsCachedModelKeepsSnapshotSource(t *testing.T) {
	q, _ := fixture(t)
	model := Model{Name: "test", Path: "test", Engine: "fake", MemoryGB: 1, Platforms: []string{"fake"}}
	config := ModelConfig{Wait: time.Second, Idle: time.Minute, Models: []Model{model}}
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	m := machine("worker", "fake")
	m.CachedModels = []string{model.Name}
	m.UnavailableRoots = []UnavailableRoot{{Root: "model", Reason: "configured NAS mount missing"}}
	j := Job{Spec: spec("production")}
	j.Spec.Resumable = false
	j.Spec.Needs.Model = model.Name
	w := Worker{Machine: m, ModelConfig: config}
	if reason := q.storageRootReason(&j, &m); reason != "" {
		t.Fatal("cached engine model blocked", reason)
	}
	if err := localAttemptAdmission(w, nil, j, nil); err != nil {
		t.Fatal("cached engine model refused locally", err)
	}
	j.Spec.ModelSnapshots = map[string]ModelSnapshot{"base": {Repo: "org/model", Revision: strings.Repeat("a", 40)}}
	if reason := q.storageRootReason(&j, &m); !strings.Contains(reason, "model unavailable") {
		t.Fatal("cached engine model hid snapshot source dependency", reason)
	}
	if err := localAttemptAdmission(w, nil, j, nil); err == nil || !strings.Contains(err.Error(), "model unavailable") {
		t.Fatal("snapshot admitted locally without source", err)
	}
	m.UnavailableRoots = nil
	w.Machine = m
	if reason := q.storageRootReason(&j, &m); reason != "" {
		t.Fatal("snapshot source recovery ignored", reason)
	}
	if err := localAttemptAdmission(w, nil, j, nil); err != nil {
		t.Fatal("recovered snapshot refused locally", err)
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
	observed.CachedModels, _, err = cache.report([]Model{model}, nil)
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
	if err := localAttemptAdmission(actor, nil, next, nil); err != nil {
		t.Fatal("stale actor rejected discovered input", err)
	}
	rt := &modelRuntime{models: map[string]*workerModel{}, worker: &engineWorker, session: m.Session, machine: m.ID, platform: "fake", cache: cache, environmentCache: envCache, config: ModelConfig{Wait: time.Second, Models: []Model{model}}, memoryMachine: m, memorySource: func(context.Context) (float64, error) { return 8, nil }, engineFactory: func(Model) Engine { return &FakeEngine{} }, authorize: func(HostLaunch) error { return nil }}
	rt.sync([]ModelSlot{{Model: model.Name, Session: m.Session, Attempt: "cache-after-mount-loss", State: "loading", EnvironmentRecipe: &recipe}})
	rt.wg.Wait()
	loaded := rt.models[model.Name]
	if loaded == nil || loaded.state != "ready" {
		t.Fatalf("cached model/prepared recipe failed after mount loss: %+v", loaded)
	}
	rt.sync(nil)
	rt.wg.Wait()
}

func TestWorkerRootsModelEnvironmentPlacement(t *testing.T) {
	q, _ := fixture(t)
	recipe := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"runtime": {"fake": recipe}}}); err != nil {
		t.Fatal(err)
	}
	model := Model{Name: "test", Path: "test", Engine: "fake", Environment: "runtime", MemoryGB: 1, Platforms: []string{"fake"}}
	if err := q.ConfigureModels(ModelConfig{Wait: time.Second, Idle: time.Minute, Models: []Model{model}}); err != nil {
		t.Fatal(err)
	}
	m := machine("missing-environment", "fake")
	m.CachedModels = []string{model.Name}
	m.Environments = []string{recipe.Key}
	m.UnavailableRoots = []UnavailableRoot{{Root: "environment", Reason: "configured NAS mount missing"}}
	demand := callJob(&ModelCall{Model: model.Name, Priority: "production"})
	if q.placementAllowed(demand, &m) {
		t.Fatal("model placement ignored required environment root")
	}
	healthy := m
	healthy.ID = "healthy"
	healthy.UnavailableRoots = nil
	if !q.placementAllowed(demand, &healthy) {
		t.Fatal("healthy model target excluded")
	}
	m.UnavailableRoots = []UnavailableRoot{{Root: "environment-source", Reason: "configured NAS mount missing"}}
	if !q.placementAllowed(demand, &m) {
		t.Fatal("prepared model environment treated as missing")
	}
	m.Environments = nil
	if q.placementAllowed(demand, &m) {
		t.Fatal("cold model environment bypassed source mount loss")
	}
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
	if assignment.ModelEnvironmentKey != recipe.Key {
		t.Fatal("assignment lost frozen model environment key")
	}
	w := Worker{Machine: m, ModelConfig: config}
	if err := localAttemptAdmission(w, nil, *assignment, nil); err != nil {
		t.Fatal("worker rejected queue-selected prepared model environment", err)
	}
	w.Machine.Environments = nil
	if err := localAttemptAdmission(w, nil, *assignment, nil); err == nil {
		t.Fatal("cold model environment accepted without source")
	}
	w.Machine.Environments = []string{recipe.Key}
	w.Machine.UnavailableRoots = []UnavailableRoot{{Root: "environment", Reason: "configured NAS mount missing"}}
	if err := localAttemptAdmission(w, nil, *assignment, nil); err == nil {
		t.Fatal("missing environment cache admitted")
	}
}
