package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
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
	w := Worker{ServiceConfig: &ServiceConfig{NAS: nas, CheckpointMount: nas}, JobFileRoot: nas, ModelRoot: filepath.Join(nas, "models"), EnvironmentSourceRoot: filepath.Join(nas, "env"), EnvironmentRoot: t.TempDir(), MountRoots: func(context.Context) ([]string, error) {
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

func TestWorkerRootsNestedCheckpointMountSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	nas, state := t.TempDir(), t.TempDir()
	nested := filepath.Join(nas, "nested")
	root := filepath.Join(nested, "checkpoints")
	j := Job{Checkpoint: filepath.Join(root, "job"), Attempt: 2, Spec: Spec{Resumable: true}}
	first := filepath.Join(j.Checkpoint, "attempt-1")
	if err := os.MkdirAll(first, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(first, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":7}}`), 0600); err != nil {
		t.Fatal(err)
	}
	mounts := []string{nas, nested}
	newWorker := func() Worker {
		return Worker{StateDir: state, CheckpointRoot: root, ServiceConfig: &ServiceConfig{NAS: nas, CheckpointMount: nested}, MountRoots: func(context.Context) ([]string, error) { return mounts, nil }}
	}
	w := newWorker()
	if missing := w.sampleStorageRoots(ctx, root); len(missing) != 0 {
		t.Fatal("healthy nested mount", missing)
	}
	// A writable directory on the parent share is exposed after the distinct
	// checkpoint mount disappears. The durable generation is on the other share.
	mountedCopy := filepath.Join(t.TempDir(), "offline")
	if err := os.Rename(nested, mountedCopy); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	mounts = []string{nas}
	w.Machine.UnavailableRoots = w.sampleStorageRoots(ctx, root)
	if reason := w.storageRootReason(&j); !strings.Contains(reason, "checkpoint unavailable") {
		t.Fatal("running worker accepted the parent share", reason)
	}
	w = newWorker()
	w.Machine.UnavailableRoots = w.sampleStorageRoots(ctx, root)
	if reason := w.storageRootReason(&j); !strings.Contains(reason, "checkpoint unavailable") {
		if err := workerCheckpointAvailable(root, func() {}); err != nil {
			t.Fatal("fallback fixture is not writable", err)
		}
		dest, err := prepareCheckpoint(j)
		if err != nil {
			t.Fatal("fallback checkpoint preparation failed unexpectedly", err)
		}
		progress, err := checkpointProgress(dest)
		t.Fatalf("restarted worker prepared a fallback attempt with progress %d: %v", progress, err)
	}
	if _, err := os.Lstat(j.Checkpoint); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("held checkpoint was materialized in the fallback", err)
	}
	if err := os.Rename(nested, filepath.Join(t.TempDir(), "fallback")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(mountedCopy, nested); err != nil {
		t.Fatal(err)
	}
	mounts = []string{nas, nested}
	w.Machine.UnavailableRoots = w.sampleStorageRoots(ctx, root)
	if reason := w.storageRootReason(&j); reason != "" {
		t.Fatal("restored checkpoint share remained held", reason)
	}
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatal(err)
	}
	if progress, err := checkpointProgress(dest); err != nil || progress != 7 {
		t.Fatalf("recovered attempt lost the durable generation: %d, %v", progress, err)
	}
}

func TestWorkerRootsColdStartDoesNotAcceptFallbackCheckpoint(t *testing.T) {
	for _, legacyReceipt := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_receipt=%t", legacyReceipt), func(t *testing.T) {
			q, _ := fixture(t)
			fakeCheckpointAvailability(t, q, true)
			nas, state, offline := t.TempDir(), t.TempDir(), t.TempDir()
			root := filepath.Join(nas, "nested", "checkpoints")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			if legacyReceipt {
				receipt := map[string]string{"path": root, "mount": nas}
				if err := saveWorkerJSON(state, "checkpoint-mount.json", receipt); err != nil {
					t.Fatal(err)
				}
			}
			s := cpuSpec(1, 1)
			s.Class, s.Priority, s.Resumable = "experiment", "", true
			m := cpuMachine("worker", "worker", "cpu", 4, 16)
			check(t, q, m, nil)
			j := submit(t, q, s)
			prior := check(t, q, m, nil).Job
			if prior == nil || prior.ID != j.ID || prior.Attempt != 1 {
				t.Fatal("missing first attempt", prior)
			}
			// The queue has no positive generation evidence. Generation 7 is
			// on the nested share, which is offline when this worker starts.
			first := filepath.Join(offline, j.ID, "attempt-1")
			if err := os.MkdirAll(first, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(first, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":7}}`), 0600); err != nil {
				t.Fatal(err)
			}
			interrupted := result(prior, false)
			interrupted.Interrupted = true
			if err := q.Complete(interrupted); err != nil {
				t.Fatal(err)
			}
			w := Worker{StateDir: state, CheckpointRoot: root, ServiceConfig: &ServiceConfig{NAS: nas}, MountRoots: func(context.Context) ([]string, error) { return []string{nas}, nil }}
			m.UnavailableRoots = w.sampleStorageRoots(context.Background(), root)
			if assigned := check(t, q, m, nil).Job; assigned != nil {
				assigned.Checkpoint = filepath.Join(root, j.ID)
				dest, err := prepareCheckpoint(*assigned)
				if err != nil {
					t.Fatal("fallback preparation failed", err)
				}
				generation, err := checkpointProgress(dest)
				t.Fatalf("prepared fallback attempt %d at generation %d while generation 7 was offline: %v", assigned.Attempt, generation, err)
			}
			got, err := q.jobSnapshot(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Hold == nil || got.Hold.Kind != "storage_root" || !strings.Contains(got.Hold.Detail, "checkpoint unavailable") {
				t.Fatalf("missing typed checkpoint hold: %+v", got.Hold)
			}
			if _, err := os.Lstat(filepath.Join(root, j.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("held checkpoint materialized in the fallback", err)
			}
		})
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

func TestWorkerRootsReadmissionAndCheckpointPlacement(t *testing.T) {
	for _, mounted := range []bool{true, false} {
		for _, class := range []string{"production", "deadline", "experiment", "test", "local_first", "default_checkpoint"} {
			name := fmt.Sprintf("%s/mounted=%t", class, mounted)
			t.Run(name, func(t *testing.T) {
				q, clock := fixture(t)
				nas := t.TempDir()
				root := filepath.Join(nas, "checkpoints")
				w := Worker{ServiceConfig: &ServiceConfig{NAS: nas}, MountRoots: func(context.Context) ([]string, error) { return []string{nas}, nil }}
				if mounted {
					w.ServiceConfig.CheckpointMount = nas
				}
				m := cpuMachine("worker", "worker", "cpu", 4, 16)
				m.UnavailableRoots = w.sampleStorageRoots(context.Background(), root)
				m.CheckpointUnavailable = checkpointRootUnavailable(&m)
				if m.CheckpointUnavailable == mounted {
					t.Fatal("unexpected checkpoint root status", m.UnavailableRoots)
				}
				m.Capabilities = append(m.Capabilities, localFirstProtocol)
				m = reportDisk(t, m, clock, "data", []string{"data", "file-cache"}, 100<<30, 50<<30)
				if mounted {
					nas := reportDisk(t, m, clock, "checkpoint", []string{"checkpoint"}, 100<<30, 50<<30)
					m.DiskVolumes = append(m.DiskVolumes, nas.DiskVolumes...)
				} else {
					m.DiskVolumes = append(m.DiskVolumes, DiskVolume{Roots: []string{"checkpoint"}, Error: "disk reading unavailable"})
				}
				check(t, q, m, nil)
				drained, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain"})
				if err != nil || !drained.Quiescent {
					t.Fatalf("fixture drain: %+v %v", drained, err)
				}
				resumed, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &drained.Policy.UpdatedAt})
				if err != nil || resumed.Policy.Mode != "enabled" || !mounted && resumed.State != "degraded" {
					t.Fatalf("readmission: %+v %v", resumed, err)
				}
				s := cpuSpec(1, 1)
				s.Class, s.Priority = class, ""
				switch class {
				case "deadline":
					deadline := clock.Now().Add(time.Hour)
					s.Deadline = &deadline
				case "local_first", "default_checkpoint":
					s.Class, s.Resumable, s.Disk = "experiment", true, &DiskNeeds{CheckpointBytes: 1 << 20}
					if class == "local_first" {
						s.Advanced = &Advanced{CheckpointPolicy: "local_first"}
						fakeCheckpointAvailability(t, q, mounted)
					}
				}
				queued := submit(t, q, s)
				assigned := check(t, q, m, nil).Job
				if !mounted && class == "default_checkpoint" {
					got, err := q.jobSnapshot(queued.ID)
					if assigned != nil || err != nil || got.Hold == nil || got.Hold.Kind != "storage_root" ||
						!strings.Contains(got.Hold.Detail, "checkpoint unavailable") {
						t.Fatalf("expected a typed checkpoint hold: %+v %v", got.Hold, err)
					}
					return
				}
				if assigned == nil || assigned.ID != queued.ID {
					t.Fatal("compatible job was not placed", job(t, q, queued.ID).Reason)
				}
				if err := q.Started(result(assigned, false)); err != nil {
					t.Fatal(err)
				}
				finish(t, q, assigned, true)
			})
		}
	}
}

func TestWorkerRootsRunCommandWithoutNAS(t *testing.T) {
	q, clock := fixture(t)
	fakeCheckpointAvailability(t, q, true)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	nas := t.TempDir()
	w := Worker{
		Clock: clock, Interval: 10 * time.Millisecond,
		Client: Client{URL: server.URL, Token: "test"}, Machine: machine("worker", "cpu", "fixture-worker"),
		StateDir: newTestStateRoot(t), EnvironmentLimit: 1 << 30,
		CheckpointRoot: filepath.Join(nas, "checkpoints"), ServiceConfig: &ServiceConfig{NAS: nas},
		MountRoots:        func(context.Context) ([]string, error) { return nil, nil },
		CPUIsolationProbe: func(context.Context) error { return nil },
		sharedCPUDev:      true, devEnvironmentSizing: true, processMemory: workerDevProcessMemory(),
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
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
	s := cpuSpec(1, 1)
	s.Class, s.Priority = "test", ""
	s.Run.Command = []string{"sh", "-c", `printf 'mountless command ran\n' | tee "$MYCE_CHECKPOINT_DIR/proof"`}
	local := submit(t, q, s)
	s.Name, s.Class, s.Resumable = "needs-checkpoint-mount", "experiment", true
	dependent := submit(t, q, s)
	deadline, stopWaiting := context.WithTimeout(ctx, testWaitTimeout)
	defer stopWaiting()
	var observed Job
	defer func() {
		if t.Failed() {
			t.Logf("local command state=%s reason=%q release=%t", observed.State, observed.Reason, observed.ReleaseConfirmed)
		}
	}()
	probeWait(t, deadline, func() bool {
		clock.Advance(time.Millisecond)
		v, err := q.jobSnapshot(local.ID)
		if err != nil {
			t.Fatal(err)
		}
		observed = *v
		if terminal(v.State) && v.State != "succeeded" {
			t.Fatalf("mountless command failed: %s", v.Reason)
		}
		return v.State == "succeeded" && v.ReleaseConfirmed
	})
	logs, err := q.Logs(local.ID, 0)
	if err != nil || !slices.ContainsFunc(logs, func(l Log) bool { return strings.Contains(l.Text, "mountless command ran") }) {
		t.Fatal("worker never published command output", logs, err)
	}
	v, err := q.jobSnapshot(dependent.ID)
	if err != nil || v.State != "queued" || v.Hold == nil || v.Hold.Kind != "storage_root" || !strings.Contains(v.Hold.Detail, "checkpoint unavailable") {
		t.Fatalf("NAS checkpoint work escaped the mount hold: state=%s hold=%+v err=%v", v.State, v.Hold, err)
	}
	if _, err := os.Lstat(w.CheckpointRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("real worker created the unavailable NAS checkpoint root", err)
	}
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
			mounts, err := mountedNASInventory(context.Background(), osname, func(context.Context, string, ...string) ([]byte, error) {
				if osname == "linux" {
					return []byte("map /nas autofs rw 0 0\nfixture /nas/online nfs rw 0 0\n"), nil
				}
				return []byte("map on /nas (autofs)\nfixture on /nas/online (smbfs)\n"), nil
			})
			if err != nil || checkpointMountConfirmed(mounts, "/nas/checkpoints", "/nas") || !checkpointMountConfirmed(mounts, "/nas/online/checkpoints", "/nas/online") {
				t.Fatal("automount placeholder counted as NAS or an active nested mount was missed", err)
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
