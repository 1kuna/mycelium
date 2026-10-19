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
		"environment-cache", "model-cache",
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
	for _, entry := range []string{"queue", "worker", "install", "benchmark", "docker-reconcile"} {
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
					s, e := (Worker{StateDir: root}).openStore()
					err = e
					if s != nil {
						s.close()
					}
				case "docker-reconcile":
					err = ReconcileDockerAttempt(context.Background(), root, "fixture-attempt")
				case "benchmark":
					err = RunOnboardingBenchmark(context.Background(), BenchmarkConfig{StateDir: root}, &CapabilityProfile{})
				case "install":
					c, e := PlanInstall("worker", parent, "", parent, "http://fixture.invalid", "fixture", Detection{OS: "linux", Platform: "fake"})
					if e != nil {
						t.Fatal(e)
					}
					c.State = root
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
