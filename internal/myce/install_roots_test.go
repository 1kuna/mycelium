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

func installRuntimeRoots(t *testing.T, c ServiceConfig) []string {
	t.Helper()
	if c.Role == "queue" {
		return QueueStateRoots(c.DB, c.HostStateDir, c.UploadSpoolDir, "", "", true)
	}
	roots, err := (Worker{StateDir: c.State, HostStateDir: c.HostStateDir, CacheRoot: c.ModelCache, EnvironmentRoot: c.EnvironmentCache}).StateRoots()
	if err != nil {
		t.Fatal(err)
	}
	return roots
}

func assertInstallRootRefusal(t *testing.T, c ServiceConfig, source, root string) {
	t.Helper()
	home := filepath.Dir(source)
	before := stateTree(t, home)
	calls := 0
	err := Install(context.Background(), c, source, "synthetic", func(context.Context, string, ...string) ([]byte, error) {
		calls++
		return nil, nil
	})
	var legacy *LegacyStateError
	if !errors.Is(err, ErrLegacyState) || !errors.As(err, &legacy) || !reflect.DeepEqual(legacy.Paths, []string{root}) {
		t.Errorf("expected legacy refusal for %s, got %v", root, err)
	}
	if calls != 0 {
		t.Errorf("refusal called service manager %d times", calls)
	}
	if !reflect.DeepEqual(before, stateTree(t, home)) {
		t.Error("refusal changed filesystem entries or contents")
	}
}

// Enumerate the startup registry, not a test-maintained list of cache names.
// Mark ancestors only: unrelated fresh roots must stay absent on refusal.
func TestInstallPreflightEveryRoot(t *testing.T) {
	for _, layout := range []string{"worker/default", "worker/external-caches", "worker/default-caches", "queue/default", "queue/external-roots"} {
		t.Run(layout, func(t *testing.T) {
			configure := func(c *ServiceConfig) {
				if strings.HasPrefix(layout, "queue/") {
					next, err := PlanInstall("queue", c.WorkerHome, "", c.NAS, c.Queue, c.Name, c.Detection)
					if err != nil {
						t.Fatal(err)
					}
					*c = next
				}
				if layout == "queue/external-roots" {
					c.DB = filepath.Join(c.NAS, "database", "queue.db")
					c.UploadSpoolDir = filepath.Join(c.NAS, "uploads")
				} else if layout == "worker/external-caches" {
					c.ModelCache = filepath.Join(c.WorkerHome, "models")
					c.EnvironmentCache = filepath.Join(c.WorkerHome, "environments")
				} else if layout == "worker/default-caches" {
					c.ModelCache, c.EnvironmentCache = "", ""
				}
			}
			sample, _ := preserveWorkerFixture(t)
			configure(&sample)
			for index, sampleRoot := range installRuntimeRoots(t, sample) {
				t.Run(filepath.Base(sampleRoot), func(t *testing.T) {
					c, source := preserveWorkerFixture(t)
					configure(&c)
					roots := installRuntimeRoots(t, c)
					root := roots[index]
					for _, ancestor := range roots {
						rel, err := filepath.Rel(ancestor, root)
						if err != nil {
							t.Fatal(err)
						}
						if rel != "." && filepath.IsLocal(rel) {
							if err := PrepareStateRoots(ancestor); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := os.MkdirAll(root, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(root, "old-blob"), []byte("surviving data"), 0600); err != nil {
						t.Fatal(err)
					}
					assertInstallRootRefusal(t, c, source, root)
				})
			}
		})
	}
}

func TestInstallFileCachePreflight(t *testing.T) {
	for _, mode := range []string{"existing-host", "missing-host"} {
		t.Run(mode, func(t *testing.T) {
			c, source := preserveWorkerFixture(t)
			if mode == "existing-host" {
				if err := Install(context.Background(), c, source, "synthetic", fakeInstallManager); err != nil {
					t.Fatal(err)
				}
			} else if err := PrepareStateRoots(c.State); err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(c.State, "file-cache")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, FormatMarker)); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "old-blob"), []byte("RC file-cache survivor"), 0600); err != nil {
				t.Fatal(err)
			}
			c.Heartbeat *= 2
			assertInstallRootRefusal(t, c, source, root)
		})
	}
}

func TestFreshInstallMarkedRootCustodyRefused(t *testing.T) {
	sample, _ := preserveWorkerFixture(t)
	for index, sampleRoot := range installRuntimeRoots(t, sample) {
		t.Run(filepath.Base(sampleRoot), func(t *testing.T) {
			c, source := preserveWorkerFixture(t)
			roots := installRuntimeRoots(t, c)
			if err := PrepareStateRoots(roots...); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(roots[index], "surviving-custody"), []byte("current data"), 0600); err != nil {
				t.Fatal(err)
			}
			before := stateTree(t, filepath.Dir(source))
			if err := prepareClosedWorkerInstall(c); !errors.Is(err, ErrHostState) {
				t.Fatalf("surviving marked root was not refused: %v", err)
			}
			if !reflect.DeepEqual(before, stateTree(t, filepath.Dir(source))) {
				t.Fatal("surviving custody was changed or host state was initialized")
			}
		})
	}
}
