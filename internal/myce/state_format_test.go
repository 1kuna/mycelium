package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Root admission depends only on the marker, never on payload formats.
func refusedStateFiles() map[string][]byte {
	return map[string][]byte{"arbitrary": []byte("surviving data"), FormatMarker: []byte("v2\n")}
}

func TestStateRootFormats(t *testing.T) {
	files := refusedStateFiles()
	// Default and explicitly configured roots, including independent caches and
	// queue data locations. All use exactly the same gate, regardless of contents.
	for _, relative := range []string{
		"queue/state", "worker/state", "worker-gpu0/state", "worker-gpu1/state",
		"host-control", ".local/share/myce", "environment-cache", "model-cache",
		"file-cache", "upload-spool", "file-store", "joining-identity", "benchmark-state",
	} {
		t.Run(relative, func(t *testing.T) {
			for _, mode := range []string{"absent", "empty", "marked"} {
				t.Run(mode, func(t *testing.T) {
					root := filepath.Join(t.TempDir(), relative)
					if mode != "absent" {
						if err := os.MkdirAll(root, 0700); err != nil {
							t.Fatal(err)
						}
					}
					if mode == "marked" {
						if err := os.WriteFile(filepath.Join(root, FormatMarker), []byte(stateFormat), 0600); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(root, "arbitrary"), []byte("current data"), 0600); err != nil {
							t.Fatal(err)
						}
					}
					if err := PrepareStateRoots(root); err != nil {
						t.Fatal(err)
					}
					data, err := os.ReadFile(filepath.Join(root, FormatMarker))
					if err != nil || string(data) != stateFormat {
						t.Fatal(string(data), err)
					}
					if mode != "marked" {
						entries, err := os.ReadDir(root)
						if err != nil || len(entries) != 1 || entries[0].Name() != FormatMarker {
							t.Fatal("marker was not first and only entry", entries, err)
						}
					}
				})
			}
			for name, data := range files {
				t.Run("unmarked/"+name, func(t *testing.T) {
					parent := t.TempDir()
					root := filepath.Join(parent, relative)
					if err := os.MkdirAll(root, 0700); err != nil {
						t.Fatal(err)
					}
					// Refuse before inspecting the payload.
					if err := os.WriteFile(filepath.Join(root, filepath.Base(name)), data, 0600); err != nil {
						t.Fatal(err)
					}
					before := stateTree(t, parent)
					err := PrepareStateRoots(filepath.Join(parent, "fresh-sibling"), root)
					var legacy *LegacyStateError
					if !errors.As(err, &legacy) || !errors.Is(err, ErrLegacyState) || !reflect.DeepEqual(legacy.Paths, []string{root}) {
						t.Fatalf("accepted %s: %v", name, err)
					}
					if !reflect.DeepEqual(before, stateTree(t, parent)) {
						t.Fatal("refusal changed tree")
					}
				})
			}
		})
	}
}

func TestStateRootInvalidMarker(t *testing.T) {
	for _, data := range []string{"", "v2\n", "v4\n", "v3\nextra"} {
		t.Run(data, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, FormatMarker), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			before := stateTree(t, root)
			if err := PrepareStateRoots(root); !errors.Is(err, ErrLegacyState) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, stateTree(t, root)) {
				t.Fatal("invalid marker changed")
			}
		})
	}
}

func TestStateStartupRefusalNoWrites(t *testing.T) {
	for _, entry := range []string{"queue", "worker", "host-init", "host-control", "install", "benchmark", "docker-reconcile"} {
		for name, data := range refusedStateFiles() {
			t.Run(entry+"/"+name, func(t *testing.T) {
				parent := t.TempDir()
				root := filepath.Join(parent, "old-state")
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, filepath.Base(name)), data, 0600); err != nil {
					t.Fatal(err)
				}
				before := stateTree(t, parent)
				var err error
				switch entry {
				case "queue":
					q, e := Open(filepath.Join(root, "queue.db"), parent, RealClock{})
					err = e
					if q != nil {
						q.Close()
					}
				case "worker":
					s, e := (Worker{StateDir: root, HostStateDir: filepath.Join(parent, "fresh-host")}).openStore()
					err = e
					if s != nil {
						s.close()
					}
				case "docker-reconcile":
					err = ReconcileDockerAttempt(context.Background(), filepath.Join(parent, "fresh-host"), root, "fixture-attempt")
				case "benchmark":
					err = RunOnboardingBenchmark(context.Background(), BenchmarkConfig{StateDir: root}, &CapabilityProfile{})
				case "host-init":
					_, err = InitializeHostIntent(root, "fixture-host", RealClock{})
				case "host-control":
					_, err = (HostControl{Dir: root}).Disable(context.Background(), "test", 0)
				case "install":
					c, e := PlanInstall("worker", parent, "", parent, "http://fixture.invalid", "fixture", Detection{OS: "linux", Platform: "fake"})
					if e != nil {
						t.Fatal(e)
					}
					c.HostStateDir = root
					err = Install(context.Background(), c, "unused", "", func(context.Context, string, ...string) ([]byte, error) {
						t.Fatal("refusal called manager")
						return nil, nil
					})
				}
				if !errors.Is(err, ErrLegacyState) || !strings.Contains(err.Error(), root) {
					t.Fatalf("missing root refusal: %v", err)
				}
				if !reflect.DeepEqual(before, stateTree(t, parent)) {
					t.Fatal("startup refusal wrote filesystem entries")
				}
			})
		}
	}
}

func newTestStateRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := PrepareStateRoots(root); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestDevStartupRefusalNoWrites(t *testing.T) {
	for _, cache := range []string{"environment-cache", "model-cache", "file-cache"} {
		t.Run(cache, func(t *testing.T) {
			parent := newTestStateRoot(t)
			worker := filepath.Join(parent, "workers", "fake-1")
			if err := PrepareStateRoots(worker); err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(worker, cache)
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "arbitrary"), []byte("surviving data"), 0600); err != nil {
				t.Fatal(err)
			}
			before := stateTree(t, parent)
			d, err := StartDev(DevConfig{Database: filepath.Join(parent, "queue.db"), Checkpoints: filepath.Join(parent, "checkpoints"), Workers: 1, MemoryGB: 8, Token: "synthetic-fixture"})
			if d != nil {
				d.Close()
			}
			if !errors.Is(err, ErrLegacyState) || !strings.Contains(err.Error(), root) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, stateTree(t, parent)) {
				t.Fatal("dev refusal changed filesystem")
			}
		})
	}
}

func TestHostRefusalChecksRootsBesideCorruptRegistry(t *testing.T) {
	for _, corruptFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "corrupt-first", false: "corrupt-last"}[corruptFirst], func(t *testing.T) {
			parent := t.TempDir()
			dir := readyTestHost(t, filepath.Join(parent, "host"), "fixture-host")
			oldRoot := filepath.Join(parent, "unmarked-worker")
			if err := os.Mkdir(oldRoot, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(oldRoot, "orphan.result.json"), []byte("surviving data"), 0600); err != nil {
				t.Fatal(err)
			}
			corrupt, current := "worker-a.json", "worker-z.json"
			if !corruptFirst {
				corrupt, current = current, corrupt
			}
			if err := os.WriteFile(filepath.Join(dir, corrupt), []byte("{"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := saveWorkerJSON(dir, current, HostWorker{State: oldRoot, Host: "fixture-host", Name: "fixture-worker"}); err != nil {
				t.Fatal(err)
			}
			before := stateTree(t, parent)
			service := &fakeHostService{}
			_, err := (HostControl{Dir: dir, Service: service}).Disable(context.Background(), "test", 0)
			if !errors.Is(err, ErrLegacyState) || !strings.Contains(err.Error(), oldRoot) {
				t.Fatal("corruption hid an unmarked root", err)
			}
			if service.now.Load() || !reflect.DeepEqual(before, stateTree(t, parent)) {
				t.Fatal("refusal performed cleanup or changed the filesystem")
			}
		})
	}
}

func TestHostMutationChecksUnregisteredConfiguredRoots(t *testing.T) {
	for _, selected := range []string{"state", "model-cache", "environment-cache"} {
		t.Run(selected, func(t *testing.T) {
			parent := t.TempDir()
			dir := readyTestHost(t, filepath.Join(parent, "host"), "fixture-host")
			config := ServiceConfig{Role: "worker", State: filepath.Join(parent, "worker"), ModelCache: filepath.Join(parent, "model-cache"), EnvironmentCache: filepath.Join(parent, "environment-cache")}
			root := map[string]string{"state": config.State, "model-cache": config.ModelCache, "environment-cache": config.EnvironmentCache}[selected]
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "arbitrary"), []byte("surviving state"), 0600); err != nil {
				t.Fatal(err)
			}
			before := stateTree(t, parent)
			executor := HostMutationExecutor{Probe: MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) {
				return nil, errors.New("unexpected service-manager call")
			}}}
			_, err := executor.Execute(context.Background(), HostInvocation{StateDir: dir, Config: config}, HostMutation{Operation: "close"})
			if !errors.Is(err, ErrLegacyState) || !strings.Contains(err.Error(), root) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, stateTree(t, parent)) {
				t.Fatal("unregistered root refusal changed tree")
			}
		})
	}
}
