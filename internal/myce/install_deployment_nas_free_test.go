package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInstalledWorkerNASFreePreflight(t *testing.T) {
	for _, layout := range []string{"missing", "mountpoint directory", "automount placeholder", "local model", "invalid mountpoint", "invalid checkpoint", "invalid local model", "new selection"} {
		t.Run(layout, func(t *testing.T) {
			c, _ := onboardingNAS(t, "darwin")
			c.RequireMountedNAS = true
			previous := c
			wantCode := ""
			switch layout {
			case "missing":
				if err := os.Remove(c.NAS); err != nil {
					t.Fatal(err)
				}
			case "local model", "invalid local model":
				c.ModelRoot = filepath.Join(t.TempDir(), "models")
				previous.ModelRoot = c.ModelRoot
				if layout == "invalid local model" {
					wantCode = "model_root_not_local"
					if err := os.WriteFile(c.ModelRoot, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "invalid mountpoint":
				wantCode = "nas_root_invalid"
				if err := os.Remove(c.NAS); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(c.NAS, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "invalid checkpoint":
				wantCode = "nas_root_invalid"
				if err := os.MkdirAll(filepath.Dir(c.Checkpoints), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(c.Checkpoints, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "new selection":
				c.Checkpoints = filepath.Join(c.NAS, "changed")
				wantCode = "nas_not_mounted"
			}
			inventory := func(_ context.Context, _ string, _ ...string) ([]byte, error) {
				if layout == "automount placeholder" {
					return []byte("map on " + c.NAS + " (autofs)\n"), nil
				}
				return nil, nil
			}
			fs := &onboardingStorageFS{
				lstat: os.Lstat,
				mkdir: func(string, os.FileMode) error { return errors.New("unexpected directory creation") },
				probe: func(string) error { return errors.New("unexpected write probe") },
			}
			for _, mode := range []installStorageMode{installStoragePreview, installStorageProbe, installStorageWrite} {
				if c.PreservesWorkerStorage(previous) {
					mode |= installStoragePreserveNAS
				}
				warning, err := prepareInstallStorage(context.Background(), c, inventory, mode, fs)
				if wantCode != "" {
					requirePreflightCode(t, err, wantCode)
					if warning != nil {
						t.Fatalf("invalid configuration reported advisory: %+v", warning)
					}
				} else if err != nil || warning == nil || warning.Root != "nas" || warning.Operation != "inventory" || warning.TimedOut {
					t.Fatalf("missing NAS was not reported as degraded: %+v %v", warning, err)
				}
			}
		})
	}
}

func TestFleetInstallNASFreeWorkerActivates(t *testing.T) {
	for _, osname := range []string{"darwin", "linux"} {
		t.Run(osname, func(t *testing.T) {
			f := newFleetInstallFixture(t, osname)
			f.c.RequireMountedNAS = true
			f.c.NAS = t.TempDir()
			f.c.Checkpoints = filepath.Join(f.c.NAS, "mycelium", "checkpoints")
			f.c.ModelRoot = filepath.Join(f.c.NAS, "mycelium", "models")
			f.c.EnvironmentRoot = filepath.Join(f.c.NAS, "mycelium", "environments")
			data, err := json.Marshal(f.c)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.c.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			f.i.StorageWarnings = true
			manager := f.deps.command
			f.deps.command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name == cachedMountInventoryCommand || (name == "cat" && len(args) == 1 && args[0] == "/proc/mounts") {
					return nil, nil
				}
				return manager(ctx, name, args...)
			}
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			if err := f.apply(); err != nil {
				t.Fatal(err)
			}
			f.assertBinary(f.next)
			receipt, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
			if err != nil || receipt.Phase != "restarting" || f.restarts != 1 ||
				receipt.StorageWarning == nil || receipt.StorageWarning.Root != "nas" || receipt.StorageFailure != nil {
				t.Fatalf("missing degraded activation: phase=%s warning=%+v failure=%+v err=%v", receipt.Phase, receipt.StorageWarning, receipt.StorageFailure, err)
			}
			if entries, err := os.ReadDir(f.c.NAS); err != nil || len(entries) != 0 {
				t.Fatalf("installer wrote onto the fallback directory: %v %v", entries, err)
			}
		})
	}
}

func TestInstalledWorkerLegacyLinuxNASCheckpoint(t *testing.T) {
	c, inventory := onboardingNAS(t, "linux")
	c.RequireMountedNAS = true
	c.Checkpoints = filepath.Join(c.NAS, "checkpoints")
	if err := os.Mkdir(c.Checkpoints, 0700); err != nil {
		t.Fatal(err)
	}
	if warning, err := preflightFleetStorageResult(context.Background(), c, inventory, nil); err != nil || warning != nil {
		t.Fatalf("healthy legacy layout failed preflight: %+v %v", warning, err)
	}
	if _, err := os.Stat(storageProbePath(c.Checkpoints, ".myce-install-probe-", "install")); err != nil {
		t.Fatal(err)
	}
	q, _ := fixture(t)
	fakeCheckpointAvailability(t, q, true)
	w := Worker{
		ServiceConfig: &c,
		MountRoots: func(ctx context.Context) ([]string, error) {
			return workerMountedNASRoots(ctx, "linux", inventory)
		},
	}
	m := cpuMachine("worker", "worker", "cpu", 4, 16)
	m.UnavailableRoots = w.sampleStorageRoots(context.Background(), c.Checkpoints)
	if len(m.UnavailableRoots) != 0 {
		t.Fatal("mounted parent share did not qualify its checkpoint directory", m.UnavailableRoots)
	}
	check(t, q, m, nil)
	spec := cpuSpec(1, 1)
	spec.Resumable = true
	queued := submit(t, q, spec)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID {
		t.Fatal("legacy worker lost checkpoint placement")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	finish(t, q, run, true)
}
