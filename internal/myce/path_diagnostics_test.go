package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPathDiagnosticQueueLibrary(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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
	})

	t.Run("ResolveInlineEnvironmentValidationError", func(t *testing.T) {
		q, _ := fixture(t)
		s := cpuResolveSpec()
		root := t.TempDir()
		s.Environment = Environment{LockFile: filepath.Join(root, "x-requirements.txt"), Python: "3.12"}
		if err := os.WriteFile(s.Environment.LockFile, []byte("# locked fixture\n"), 0600); err != nil {
			t.Fatal(err)
		}
		r, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil {
			t.Fatal(err)
		}
		f := resolveFinding(r, "environment")
		if f == nil || f.Problem != "lock_file must be uv.lock, pyproject.toml, or requirements.txt" || f.PathDiagnostic != nil || !strings.Contains(f.Fix, "Correct the environment declaration") {
			t.Fatalf("inline validation error hidden: %+v", r.Findings)
		}
		// A missing correctly named file still reports the queue-side path diagnostic.
		s.Environment.LockFile = filepath.Join(root, "requirements.txt")
		r, err = q.Resolve(ResolveRequest{Spec: s})
		if err != nil {
			t.Fatal(err)
		}
		f = resolveFinding(r, "environment.lock_file")
		if f == nil || f.PathDiagnostic == nil || f.Side != "queue" || f.PathDiagnostic.Path != s.Environment.LockFile || f.PathDiagnostic.ResolutionBase != "queue-working-directory" || !strings.Contains(f.Problem, "not the submitter") {
			t.Fatalf("queue path diagnostic lost: %+v", r.Findings)
		}

	})
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
	check(t, q, m, nil)
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
