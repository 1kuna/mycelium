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

func TestWorkerNASFreePlacement(t *testing.T) {
	for _, mounted := range []bool{true, false} {
		for _, class := range []string{"production", "deadline", "experiment", "test"} {
			for _, storage := range []string{"plain", "local_first", "nas_resume", "nas_bytes", "nas_shared_input"} {
				t.Run(fmt.Sprintf("%s/%s/mounted=%t", class, storage, mounted), func(t *testing.T) {
					q, clock := fixture(t)
					fakeCheckpointAvailability(t, q, true) // Isolate worker storage from queue storage.
					m := cpuMachine("worker", "worker", "cpu", 4, 16)
					m.Capabilities = append(m.Capabilities, localFirstProtocol, "shared_inputs_v1")
					m.SharedInputCache = &SharedInputCapacity{CapacityBytes: 1 << 20, FreeBytes: 1 << 20}
					m = reportDisk(t, m, clock, "data", []string{"data", "file-cache"}, 100<<30, 50<<30)
					if !mounted {
						m.CheckpointUnavailable = true
						m.UnavailableRoots = []UnavailableRoot{
							{Root: "checkpoint", Reason: "configured NAS mount missing"},
							{Root: "job-files", Reason: "configured NAS mount missing"},
						}
						m.DiskVolumes = append(m.DiskVolumes, DiskVolume{Roots: []string{"checkpoint"}, Error: "disk reading unavailable"})
					}
					check(t, q, m, nil)
					s := cpuSpec(1, 1)
					s.Class, s.Priority = class, ""
					if class == "deadline" {
						deadline := clock.Now().Add(time.Hour)
						s.Deadline = &deadline
					}
					switch storage {
					case "local_first":
						s.Resumable = true
						s.Advanced = &Advanced{CheckpointPolicy: "local_first"}
						s.Disk = &DiskNeeds{CheckpointBytes: 1 << 20}
						fakeCheckpointAvailability(t, q, mounted)
					case "nas_resume":
						s.Resumable = true
					case "nas_bytes":
						s.Disk = &DiskNeeds{CheckpointBytes: 1 << 20}
					case "nas_shared_input":
						s.SharedInputs = map[string]SharedInput{"data": {NAS: "input", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
					}
					queued := submit(t, q, s)
					assigned := check(t, q, m, nil).Job
					if !mounted && strings.HasPrefix(storage, "nas_") {
						expectWorkerStorageHold(t, q, queued.ID, "checkpoint")
						if assigned != nil {
							t.Fatal("NAS-dependent job was assigned")
						}
						return
					}
					if assigned == nil || assigned.ID != queued.ID {
						t.Fatal("compatible job was held", job(t, q, queued.ID).Hold)
					}
				})
			}
		}
	}
}

func expectWorkerStorageHold(t *testing.T, q *Queue, id, role string) {
	t.Helper()
	got, err := q.jobSnapshot(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "queued" || got.Hold == nil || got.Hold.Kind != "storage_root" || !strings.Contains(got.Hold.Detail, role+" unavailable") {
		t.Fatalf("expected %s storage hold, got state=%s hold=%+v", role, got.State, got.Hold)
	}
}

func TestWorkerNASFreeEphemeralCacheReceipt(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	s := ephemeralSpec()
	input := SharedInput{NAS: "input", SHA256: strings.Repeat("a", 64), SizeBytes: 1}
	s.SharedInputs = map[string]SharedInput{"data": input}
	m := ephemeralMachine()
	m.Capabilities = append(m.Capabilities, "shared_inputs_v1")
	m.CachedSharedInputs = []string{input.key()}
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
	check(t, q, m, nil)
	queued := submit(t, q, s)
	if assigned := check(t, q, m, nil).Job; assigned != nil {
		t.Fatal("private staging borrowed an ordinary cache receipt")
	}
	expectWorkerStorageHold(t, q, queued.ID, "checkpoint")
	if err := localRootAdmission(Worker{Machine: m}, *queued); err == nil || !strings.Contains(err.Error(), "checkpoint unavailable") {
		t.Fatalf("worker launch did not enforce the private source dependency: %v", err)
	}
	m.UnavailableRoots = nil
	if assigned := check(t, q, m, nil).Job; assigned == nil || assigned.ID != queued.ID {
		t.Fatal("recovered private source still held")
	}
}

func TestWorkerNASFreeDirectModelNeedsRoot(t *testing.T) {
	q, _, m := modelFixture(t)
	catalog := q.models
	catalog.Models = slices.Clone(catalog.Models)
	catalog.Models[0].DirectNAS = true
	if err := q.ConfigureModels(catalog); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	m.CachedModels = []string{"test"}
	m.UnavailableRoots = []UnavailableRoot{{Root: "model", Reason: "configured NAS mount missing"}}
	check(t, q, m, nil)
	call := addModelCall(t, q, "experiment")
	expectWorkerStorageHold(t, q, call.ID, "model")
	s := spec("production")
	s.Resumable = false
	s.Needs.Model = "test"
	queued := submit(t, q, s)
	if assigned := check(t, q, m, nil).Job; assigned != nil {
		t.Fatal("cached receipt admitted a direct-NAS model job")
	}
	expectWorkerStorageHold(t, q, queued.ID, "model")
}

func TestWorkerNASFreeRunsShellCommand(t *testing.T) {
	q, clock := fixture(t)
	fakeCheckpointAvailability(t, q, true)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	nas := t.TempDir()
	checkpoint := filepath.Join(nas, "checkpoints")
	if err := os.Mkdir(checkpoint, 0700); err != nil {
		t.Fatal(err)
	}
	w := Worker{
		Clock: clock, Interval: 10 * time.Millisecond,
		Client: Client{URL: server.URL, Token: "test"}, Machine: machine("worker", "cpu", "fixture-worker"),
		StateDir: newTestStateRoot(t), EnvironmentLimit: 1 << 30,
		CheckpointRoot: checkpoint, ServiceConfig: &ServiceConfig{NAS: nas},
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
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
				return true
			default:
				return false
			}
		})
		entries, err := os.ReadDir(checkpoint)
		if err != nil || len(entries) != 0 {
			t.Fatalf("worker wrote to the unmounted checkpoint directory: %v %v", entries, err)
		}
	}()
	s := cpuSpec(1, 1)
	s.Class, s.Priority = "test", ""
	s.Run.Command = []string{"sh", "-c", `printf 'command ran\n' | tee "$MYCE_CHECKPOINT_DIR/proof"`}
	local := submit(t, q, s)
	s.Name, s.Class, s.Resumable = "nas-checkpoint", "experiment", true
	dependent := submit(t, q, s)
	wait, stopWaiting := context.WithTimeout(ctx, testWaitTimeout)
	defer stopWaiting()
	probeWait(t, wait, func() bool {
		clock.Advance(time.Millisecond)
		got, err := q.jobSnapshot(local.ID)
		if err != nil {
			t.Fatal(err)
		}
		if terminal(got.State) && got.State != "succeeded" {
			t.Fatalf("local command failed: %s", got.Reason)
		}
		return got.State == "succeeded" && got.ReleaseConfirmed
	})
	logs, err := q.Logs(local.ID, 0)
	if err != nil || !slices.ContainsFunc(logs, func(log Log) bool { return strings.Contains(log.Text, "command ran") }) {
		t.Fatalf("worker output missing: %v %v", logs, err)
	}
	expectWorkerStorageHold(t, q, dependent.ID, "checkpoint")
}

func TestWorkerNASFreeFleetReadmission(t *testing.T) {
	q, clock, workers := fleetFixture(t, "queue", "worker")
	m := workers[0]
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
	m.CheckpointUnavailable = true
	check(t, q, m, nil)
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	fleetActivate(t, q, clock)
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.Mode != "enabled" || status.State != "degraded" {
		t.Fatalf("replacement did not readmit with its root still unavailable: %+v %v", status, err)
	}
	h := fleetHostStatus(t, q, m.HostID)
	if h.Phase != "complete" || !strings.Contains(h.Wait, "assignment start verified") {
		t.Fatalf("degraded replacement did not pass ordinary start verification: %+v", h)
	}
}

func TestWorkerNASFreeRestoresAcceptedLocalFirst(t *testing.T) {
	q, clock := fixture(t)
	initial := localCheckpointAssignment(t, q)
	writer := localWorker(t, q, initial)
	accepted := localGeneration(t, initial, 7)
	if _, err := writer.publishFileCheckpoint(initial, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if err := q.update(func() error {
		q.requeue(q.touchJob(initial.ID), "planned stop", false)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, q, false)
	m := machine("next-worker", "fake")
	m.Capabilities = append(m.Capabilities, localFirstProtocol)
	m.CheckpointUnavailable = true
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
	next := check(t, q, m, nil).Job
	if next == nil || next.ID != initial.ID || next.ResumeFiles == nil || next.ResumeFiles.Generation != accepted.Generation {
		t.Fatal("accepted generation was not assigned while NAS was absent")
	}
	next.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
	worker := Worker{fileCache: &FileCache{Root: t.TempDir(), Limit: 1 << 20, Client: writer.Client, Clock: clock}}
	dest, err := worker.prepareFileCheckpoint(*next, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(filepath.Join(dest, "generation-7.json"))
	if err != nil || ref != accepted.Files["generation-7.json"] {
		t.Fatal("restored bytes did not match the queue-accepted generation", err)
	}
}
