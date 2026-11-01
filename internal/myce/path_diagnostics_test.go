package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPathDiagnosticQueueLibrary(t *testing.T) {
	root := t.TempDir()
	catalog := filepath.Join(root, "catalog.yaml")
	if err := os.WriteFile(catalog, []byte("environments:\n  tools:\n    cpu: {backend: pixi, lock_file: pixi.lock}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "pixi.lock"), []byte("locked"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := ReadEnvironments(catalog)
	d := pathDiagnostic(err)
	if d == nil || d.Side != "queue" || d.Field != "environment.manifest" || d.Companion != "pixi.toml" || d.Cause != "companion_absent" || d.ResolutionBase != "environment-library" || d.Path != "environment:tools/cpu" {
		t.Fatalf("diagnostic: %+v, %v", d, err)
	}
	if strings.Contains(err.Error(), root) || !errors.Is(err, os.ErrNotExist) {
		t.Fatal("private root or lost local cause", err)
	}
	// ENOTDIR is a real wrong-root read, without guessing whether some other
	// host might have the desired path.
	_, err = snapshotEnvironment(EnvironmentRecipe{LockFile: filepath.Join(root, "pixi.lock", "requirements.txt"), Python: "3.12"}, "cpu")
	if d = pathDiagnostic(err); d == nil || d.Cause != "wrong_type" {
		t.Fatal(err)
	}
}

func TestPathDiagnosticUnreadable(t *testing.T) {
	// Deterministic even when tests run with privileges that bypass mode bits.
	err := PathReadError("environment.lock_file", "queue", "requirements.txt", "queue-working-directory", "", &os.PathError{Op: "open", Path: "/synthetic/hidden/requirements.txt", Err: os.ErrPermission})
	if d := pathDiagnostic(err); d.Cause != "unreadable" || strings.Contains(err.Error(), "/synthetic/hidden") || !errors.Is(err, os.ErrPermission) {
		t.Fatal(err)
	}
}

func TestPathDiagnosticWorkerReadAndFreshness(t *testing.T) {
	q, clock := fixture(t)
	recipe := binaryRecipe(t, "cpu")
	// The queue's binary snapshot exists, but the worker mapping is wrong.
	recipe.Binary = filepath.Join(t.TempDir(), "missing", "tool")
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
	_, release, err := cache.Acquire(context.Background(), recipe)
	if release != nil {
		release()
	}
	d := pathDiagnostic(err)
	if d == nil || d.Side != "worker" || d.Path != "binary" || d.ResolutionBase != "worker-environment-library" || d.Cause != "missing" || strings.Contains(err.Error(), filepath.Dir(recipe.Binary)) {
		t.Fatalf("%+v, %v", d, err)
	}
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": recipe}}}); err != nil {
		t.Fatal(err)
	}
	m := machine("synthetic-worker", "cpu")
	m.LastSeen = clock.Now()
	m.EnvironmentReports = []EnvironmentReport{{Key: recipe.Key, State: "failed", PathDiagnostic: d}}
	q.s.Machines[m.ID] = &m
	observation := q.EnvironmentEntries()[0].Platforms[0].WorkerReads[0]
	if observation.State != "failed" || observation.Diagnostic == nil {
		t.Fatal(observation)
	}
	clock.Advance(q.machineLossTimeout(m.ID) + time.Second)
	observation = q.EnvironmentEntries()[0].Platforms[0].WorkerReads[0]
	if observation.State != "unknown" || observation.Diagnostic != nil {
		t.Fatal("stale read became current visibility", observation)
	}
	injected := *d
	injected.Path = "/synthetic/worker/private"
	if safeWorkerPathDiagnostic(&injected) != nil {
		t.Fatal("accepted arbitrary worker path")
	}
}

func TestEnvironmentLibraryDifferentRoots(t *testing.T) {
	var key string
	for range 2 {
		root := t.TempDir()
		for name, contents := range map[string]string{"catalog.yaml": "environments:\n  tools:\n    cpu: {python: '3.12', lock_file: requirements.txt}\n", "requirements.txt": "# empty locked CPU fixture\n"} {
			if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
		catalog, err := ReadEnvironments(filepath.Join(root, "catalog.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		r := catalog.Environments["tools"]["cpu"]
		if key != "" && key != r.Key {
			t.Fatal("root changed recipe identity")
		}
		key = r.Key
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
		cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}, Run: func(_ context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
			data, err := os.ReadFile(filepath.Join(dir, "requirements.txt"))
			if err != nil || string(data) != "# empty locked CPU fixture\n" {
				t.Fatalf("snapshot at worker root: %q, %v", data, err)
			}
			return nil
		}}
		_, release, err := cache.Acquire(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
}
