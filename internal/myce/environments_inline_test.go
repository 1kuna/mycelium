package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnvironmentInlineBackends(t *testing.T) {
	for _, backend := range []string{"", "uv", "pixi", "unsupported"} {
		t.Run("backend="+backend, func(t *testing.T) {
			q, _ := fixture(t)
			dir := t.TempDir()
			files := map[string]string{"uv.lock": "locked-uv", "pyproject.toml": "[project]\nname='example'\n"}
			lock, python := "uv.lock", "3.12"
			if backend == "pixi" {
				lock, python = "pixi.lock", ""
				files = map[string]string{"pixi.lock": "version: 6\nenvironments: {}\n", "pixi.toml": "[workspace]\nname='example'\n"}
			}
			for name, data := range files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			s := cpuResolveSpec()
			s.Environment = Environment{Backend: backend, LockFile: filepath.Join(dir, lock), Python: python, Platforms: []string{"cpu"}}
			c := Client{URL: testServerForEnvironments(t, q), Token: "test"}
			var report ResolveReport
			err := c.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &report)
			if backend == "unsupported" {
				if err == nil {
					t.Fatal("resolve accepted unsupported backend")
				}
				if _, err := c.Submit(context.Background(), s); err == nil {
					t.Fatal("submission accepted unsupported backend")
				}
				return
			}
			if err != nil || resolveFinding(report, "environment") != nil {
				t.Fatalf("resolve: %+v, %v", report, err)
			}
			j, err := c.Submit(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			// Inspect committed queue bytes; public job replies redact recipe inputs.
			recipe := q.s.Jobs[j.ID].EnvironmentRecipes["cpu"]
			if err := q.files.hydrateRecipe(&recipe); err != nil {
				t.Fatal(err)
			}
			if recipe.Key == "" || recipe.Backend != backend || recipe.Python != python || len(recipe.Files) != len(files) {
				t.Fatalf("recipe: %+v", recipe)
			}
			for name, data := range files {
				if string(recipe.Files[name]) != data {
					t.Fatalf("lost %s snapshot", name)
				}
				if err := os.Remove(filepath.Join(dir, name)); err != nil {
					t.Fatal(err)
				}
				frozen := q.s.Jobs[j.ID].EnvironmentRecipes["cpu"]
				if err := q.files.hydrateRecipe(&frozen); err != nil {
					t.Fatal(err)
				}
				if string(frozen.Files[name]) != data {
					t.Fatalf("snapshot still depends on %s", name)
				}
			}
		})
	}
}

func TestEnvironmentInlinePixiMissingCompanion(t *testing.T) {
	q, _ := fixture(t)
	lock := filepath.Join(t.TempDir(), "pixi.lock")
	if err := os.WriteFile(lock, []byte("locked"), 0600); err != nil {
		t.Fatal(err)
	}
	s := cpuResolveSpec()
	s.Environment = Environment{Backend: "pixi", LockFile: lock, Platforms: []string{"cpu"}}
	c := Client{URL: testServerForEnvironments(t, q), Token: "test"}
	var report ResolveReport
	if err := c.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &report); err != nil {
		t.Fatal(err)
	}
	f := resolveFinding(report, "environment.manifest")
	if f == nil || f.PathDiagnostic == nil || f.PathDiagnostic.Cause != "companion_absent" || f.PathDiagnostic.Companion != "pixi.toml" {
		t.Fatalf("resolve: %+v", report.Findings)
	}
	_, err := c.Submit(context.Background(), s)
	var apiErr *HTTPError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API refusal: %v", err)
	}
	d := apiErr.PathDiagnostic
	if d == nil || d.Cause != "companion_absent" || d.Companion != "pixi.toml" || d.Side != "queue" {
		t.Fatalf("submit diagnostic: %+v, %v", d, err)
	}
}

func TestEnvironmentInlineOversizedInput(t *testing.T) {
	for _, input := range []struct{ backend, lock, oversized, field, companion string }{
		{"pixi", "pixi.lock", "pixi.lock", "environment.lock_file", ""},
		{"pixi", "pixi.lock", "pixi.toml", "environment.manifest", "pixi.toml"},
		{"uv", "uv.lock", "uv.lock", "environment.lock_file", ""},
		{"uv", "uv.lock", "pyproject.toml", "environment.lock_file", "pyproject.toml"},
		{"", "requirements.txt", "requirements.txt", "environment.lock_file", ""},
	} {
		t.Run(input.oversized, func(t *testing.T) {
			q, _ := fixture(t)
			dir := t.TempDir()
			for _, name := range []string{"pixi.lock", "pixi.toml", "uv.lock", "pyproject.toml", "requirements.txt"} {
				data := "fixture"
				if name == input.oversized {
					data = strings.Repeat("x", (1<<20)+1)
				}
				if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			s := cpuResolveSpec()
			s.Environment = Environment{Backend: input.backend, LockFile: filepath.Join(dir, input.lock), Python: "3.12", Platforms: []string{"cpu"}}
			c := Client{URL: testServerForEnvironments(t, q), Token: "test"}
			want := PathDiagnostic{Field: input.field, Side: "queue", Path: s.Environment.LockFile, ResolutionBase: "queue-working-directory", Cause: "too_large", Companion: input.companion}
			var report ResolveReport
			if err := c.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &report); err != nil {
				t.Fatal(err)
			}
			f := resolveFinding(report, input.field)
			if f == nil || f.PathDiagnostic == nil || *f.PathDiagnostic != want {
				t.Fatalf("resolve: %+v", report.Findings)
			}
			_, err := c.Submit(context.Background(), s)
			var apiErr *HTTPError
			if !errors.As(err, &apiErr) || apiErr.Code != "job_environment_invalid" || apiErr.PathDiagnostic == nil || *apiErr.PathDiagnostic != want {
				t.Fatalf("submit: %+v, %v", apiErr, err)
			}
			if len(q.s.Jobs) != 0 {
				t.Fatal("oversized input accepted")
			}
		})
	}
}

// Submitter-uploaded lock bytes (relative lock_file from `myce run --profile`)
// freeze to the same recipe identity as a queue-side read, without the queue
// reading the path; names and the 1 MiB limit match the queue-side read.
func TestEnvironmentInlineUploadedFiles(t *testing.T) {
	q, _ := fixture(t)
	c := Client{URL: testServerForEnvironments(t, q), Token: "test"}
	files := map[string][]byte{"pixi.lock": []byte("version: 6\nenvironments: {}\n"), "pixi.toml": []byte("[workspace]\nname='example'\n")}
	dir := t.TempDir()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	read := cpuResolveSpec()
	read.Environment = Environment{Backend: "pixi", LockFile: filepath.Join(dir, "pixi.lock"), Platforms: []string{"cpu"}}
	readJob, err := c.Submit(context.Background(), read)
	if err != nil {
		t.Fatal(err)
	}
	uploaded := cpuResolveSpec()
	uploaded.Environment = Environment{Backend: "pixi", LockFile: "absent-on-queue/pixi.lock", Platforms: []string{"cpu"}, Files: files}
	var report ResolveReport
	if err := c.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: uploaded}, &report); err != nil || resolveFinding(report, "environment") != nil || resolveFinding(report, "environment.lock_file") != nil {
		t.Fatalf("resolve: %+v, %v", report.Findings, err)
	}
	j, err := c.Submit(context.Background(), uploaded)
	if err != nil {
		t.Fatal(err)
	}
	stored := q.s.Jobs[j.ID]
	recipe := stored.EnvironmentRecipes["cpu"]
	if err := q.files.hydrateRecipe(&recipe); err != nil {
		t.Fatal(err)
	}
	if recipe.Key == "" || recipe.Key != q.s.Jobs[readJob.ID].EnvironmentRecipes["cpu"].Key || string(recipe.Files["pixi.toml"]) != string(files["pixi.toml"]) || stored.Spec.Environment.Files != nil {
		t.Fatalf("uploaded recipe: %+v spec files=%d", recipe, len(stored.Spec.Environment.Files))
	}
	for name, bad := range map[string]map[string][]byte{
		"missing companion": {"pixi.lock": files["pixi.lock"]},
		"extra file":        {"pixi.lock": files["pixi.lock"], "pixi.toml": files["pixi.toml"], "other": nil},
		"oversized":         {"pixi.lock": files["pixi.lock"], "pixi.toml": []byte(strings.Repeat("x", (1<<20)+1))},
	} {
		s := uploaded
		s.Environment.Files = bad
		_, err := c.Submit(context.Background(), s)
		var apiErr *HTTPError
		if !errors.As(err, &apiErr) || apiErr.Code != "job_environment_invalid" {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
}
