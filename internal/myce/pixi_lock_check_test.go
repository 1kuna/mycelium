package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// fakeWorkerPixi installs a pinned-version pixi on PATH whose shell-hook
// reports a lock containing "stale" as out of date, like `pixi -v` does, and
// counts invocations. HOME is isolated so a host's real managed Pixi is unused.
func fakeWorkerPixi(t *testing.T, version string) string {
	t.Helper()
	bin := t.TempDir()
	calls := filepath.Join(t.TempDir(), "calls")
	script := `#!/bin/sh
if [ "$1" = --version ]; then echo "pixi ` + version + `"; exit 0; fi
echo "$@" >> '` + calls + `'
while [ $# -gt 0 ]; do [ "$1" = --manifest-path ] && manifest=$2; shift; done
if grep -q stale "$(dirname "$manifest")/pixi.lock"; then
  echo " INFO pixi_core::lock_file::outdated: the dependencies of environment 'default' for platform linux-64 are out of date because the requirement '__unix=0' could not be satisfied (required by '<environment>')" >&2
  echo "Error:   x lock file not up-to-date with the workspace" >&2
  exit 1
fi
echo '{"environment_variables":{"PATH":"/bin"}}'
`
	if err := os.WriteFile(filepath.Join(bin, "pixi"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HOME", t.TempDir())
	pixiLockResults.Lock()
	pixiLockResults.m = map[string]error{}
	pixiLockResults.Unlock()
	return calls
}

func TestPixiLockCheckRefusesOutdatedLockAtSubmission(t *testing.T) {
	calls := fakeWorkerPixi(t, workerPixiVersion())
	q, _ := fixture(t)
	c := Client{URL: testServerForEnvironments(t, q), Token: "test"}
	s := cpuResolveSpec()
	s.Environment = Environment{Backend: "pixi", LockFile: "pixi.lock", Platforms: []string{"cpu"}, Files: map[string][]byte{"pixi.lock": []byte("version: 6 # stale\n"), "pixi.toml": []byte("[workspace]\n")}}
	var report ResolveReport
	if err := c.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &report); err != nil {
		t.Fatal(err)
	}
	f := resolveFinding(report, "environment")
	if f == nil || !strings.Contains(f.Problem, "requirement '__unix=0' could not be satisfied") || !strings.Contains(f.Fix, "pixi lock") || !strings.Contains(f.Fix, workerPixiVersion()) {
		t.Fatalf("resolve: %+v", report.Findings)
	}
	_, err := c.Submit(context.Background(), s)
	var apiErr *HTTPError
	if !errors.As(err, &apiErr) || apiErr.Code != "job_environment_invalid" || !strings.Contains(apiErr.Message, "__unix=0") || !strings.Contains(apiErr.Message, "run `pixi lock` with Pixi >= "+workerPixiVersion()) {
		t.Fatalf("submit: %v", err)
	}
	// One subprocess for the same frozen inputs; resolve and submit share it.
	if b, _ := os.ReadFile(calls); strings.Count(string(b), "\n") != 1 || !strings.Contains(string(b), "shell-hook --json --locked --no-install --environment default --no-config") {
		t.Fatalf("calls: %q", b)
	}
	s.Environment.Files["pixi.lock"] = []byte("version: 6 # current\n")
	if _, err := c.Submit(context.Background(), s); err != nil {
		t.Fatal(err)
	}
}

// A wrong or absent Pixi cannot judge the workers' lock check: skip, never refuse.
func TestPixiLockCheckSkipsWithoutWorkerVersion(t *testing.T) {
	calls := fakeWorkerPixi(t, "0.1.0")
	r := EnvironmentRecipe{Backend: "pixi", Files: map[string][]byte{"pixi.lock": []byte("stale"), "pixi.toml": []byte("[workspace]\n")}}
	if err := checkInlinePixiLock(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(calls); err == nil {
		t.Fatal("unpinned pixi ran the check")
	}
}

func TestPixiLockCheckRefusesOutdatedExtensionRegistration(t *testing.T) {
	fakeWorkerPixi(t, workerPixiVersion())
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "pixi", LockFile: "env/pixi.lock"}}
	v.Execution.Main.Environment = "main"
	e.Files["env/pixi.lock"] = []byte("stale")
	e.Files["env/pixi.toml"] = []byte("[workspace]")
	e = freezeComposition(t, e)
	q, _ := fixture(t)
	if err := q.AddExtension(e); err == nil || !isPixiLockError(err) || !strings.Contains(err.Error(), "environment main") {
		t.Fatal(err)
	}
}

// Laptop proof against the real pinned Pixi when this host has it: a lock
// that no longer satisfies its manifest gives Pixi's exact reason.
func TestPixiLockCheckRealPinnedPixi(t *testing.T) {
	// Test harnesses isolate HOME; the managed tool lives in the account's.
	account, err := user.Current()
	if err != nil {
		t.Skip(err)
	}
	home := account.HomeDir
	tool := filepath.Join(home, ".local", "share", "mycelium", "tools", "pixi-"+workerPixiVersion()+"-"+runtime.GOOS+"-"+runtime.GOARCH)
	if _, err := exec.LookPath(filepath.Join(tool, "pixi")); err != nil {
		t.Skip("pinned Pixi not installed on this host")
	}
	t.Setenv("HOME", home)
	pixiLockResults.Lock()
	pixiLockResults.m = map[string]error{}
	pixiLockResults.Unlock()
	lock := "version: 6\nenvironments:\n  default:\n    channels:\n    - url: https://conda.anaconda.org/conda-forge/\n    packages: {}\npackages: []\n"
	manifest := "[workspace]\nname = \"example\"\nchannels = [\"conda-forge\"]\nplatforms = [\"linux-64\"]\n\n[dependencies]\nzlib = \"*\"\n"
	err = checkInlinePixiLock(context.Background(), EnvironmentRecipe{Backend: "pixi", Files: map[string][]byte{"pixi.lock": []byte(lock), "pixi.toml": []byte(manifest)}})
	t.Log(err)
	if !isPixiLockError(err) || !strings.Contains(err.Error(), "zlib") {
		t.Fatal(err)
	}
}
