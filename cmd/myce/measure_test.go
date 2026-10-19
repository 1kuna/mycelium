package main

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"

	"mycelium/internal/myce"
	"strings"
	"testing"
)

func TestRunMeasureRequiresExplicitTargetAndBudget(t *testing.T) {
	var out, errout bytes.Buffer
	e := runMeasure(context.Background(), nil, &out, &errout)
	if e == nil || !strings.Contains(e.Error(), "--worker") {
		t.Fatalf("expected explicit target validation: %v", e)
	}
}
func TestRunMeasureRejectsUnknownOutputBeforeReadingCatalog(t *testing.T) {
	var out, errout bytes.Buffer
	e := runMeasure(context.Background(), []string{"--model-config", "missing", "--models", "one", "--worker", "local", "--device", "0", "--platform", "cuda", "--memory-gb", "10", "--output", "csv"}, &out, &errout)
	if e == nil || !strings.Contains(e.Error(), "--output") {
		t.Fatalf("expected output validation: %v", e)
	}
}

func TestRunMeasureRejectsInvalidConcurrencyAndTimeout(t *testing.T) {
	for _, args := range [][]string{{"--warmup-parallel", "0"}, {"--load-timeout", "0s"}, {"--warmup-parallel", "-1"}, {"--load-timeout", "-1s"}} {
		var out, errout bytes.Buffer
		if err := runMeasure(context.Background(), args, &out, &errout); err == nil || !strings.Contains(err.Error(), "must be positive") {
			t.Fatalf("%v: %v", args, err)
		}
	}
}

func TestMeasureStartupRefusalNoWrites(t *testing.T) {
	for _, refused := range []string{"state", "model-cache", "environment-cache", "file-cache"} {
		t.Run(refused, func(t *testing.T) {
			parent := t.TempDir()
			roots := map[string]string{}
			for _, name := range []string{"state", "model-cache", "environment-cache"} {
				roots[name] = filepath.Join(parent, name)
			}
			roots["file-cache"] = filepath.Join(roots["state"], "file-cache")
			if refused == "file-cache" {
				if err := myce.PrepareStateRoots(roots["state"]); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.MkdirAll(roots[refused], 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(roots[refused], "arbitrary"), []byte("surviving state"), 0600); err != nil {
				t.Fatal(err)
			}
			catalog := filepath.Join(parent, "models.yaml")
			if err := os.WriteFile(catalog, []byte("models:\n  - name: fixture-model\n    engine: fake\n    path: fake\n    memory_gb: 1\n    platforms: [fake]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			snapshot := func() map[string]string {
				t.Helper()
				out := map[string]string{}
				if err := filepath.WalkDir(parent, func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if d.IsDir() {
						out[path] = "directory"
						return nil
					}
					data, err := os.ReadFile(path)
					out[path] = string(data)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				return out
			}
			before := snapshot()
			var out, errout bytes.Buffer
			err := runMeasure(context.Background(), []string{"--model-config", catalog, "--models", "fixture-model", "--worker", "local", "--device", "0", "--platform", "fake", "--memory-gb", "10", "--state-dir", roots["state"], "--model-cache", roots["model-cache"], "--environment-cache", roots["environment-cache"], "--report-prefix", filepath.Join(parent, "report")}, &out, &errout)
			if !reflect.DeepEqual(before, snapshot()) {
				t.Fatal("measurement refusal created state or reports")
			}
			if !errors.Is(err, myce.ErrLegacyState) || !strings.Contains(err.Error(), roots[refused]) {
				t.Fatal(err)
			}
		})
	}
}
