package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func fakePixi(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
set -eu
# Version probes run before a build and must not require its activation env.
if [ "${1:-}" = --version ]; then printf 'pixi 0.55.0\n'; exit 0; fi
test "$PIXI_NO_PROGRESS" = true
case "$1" in
install)
 test "$2" = --locked
 manifest=
 for arg in "$@"; do manifest="$arg"; done
 test "$(cat "$(dirname "$manifest")/pixi.lock")" != broken || { echo 'lock inconsistent with manifest' >&2; exit 9; }
 printf 'install\n' >> "$MYCE_TEST_PIXI_BUILDS"
 prefix="$PWD/.pixi/envs/default"
 mkdir -p "$prefix/bin"
 printf '#!/bin/sh\nprintf "activated:%%s\\n" "$PIXI_ACTIVE"\n' > "$prefix/bin/tool"
 chmod +x "$prefix/bin/tool"
 printf '%s' "$prefix" > "$prefix/embedded-prefix"
 ;;
shell-hook)
 echo 'WARN deprecated manifest header' >&2
 test "${MYCE_TEST_PIXI_HOOK:-}" != invalid || { echo 'not JSON'; exit 0; }
 prefix="$PWD/.pixi/envs/default"
 printf '{"environment_variables":{"PATH":"%s/bin:/usr/bin:/bin","CONDA_PREFIX":"%s","PIXI_ACTIVE":"yes","MYCE_TOKEN":"forbidden","CUDA_VISIBLE_DEVICES":"wrong"},"activation_scripts":[]}' "$prefix" "$prefix"
 test "${MYCE_TEST_PIXI_HOOK:-}" != failure || exit 7
 ;;
*) exit 88 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "pixi"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	builds := filepath.Join(dir, "builds")
	t.Setenv("MYCE_TEST_PIXI_BUILDS", builds)
	return builds
}
func pixiRecipe(t *testing.T, lock string) EnvironmentRecipe {
	t.Helper()
	dir := t.TempDir()
	for name, b := range map[string]string{"pixi.toml": "[workspace]\nname='synthetic'\nchannels=[]\nplatforms=['osx-arm64','linux-64']\n", "pixi.lock": lock} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(b), 0600); err != nil {
			t.Fatal(err)
		}
	}
	r, err := snapshotEnvironment(EnvironmentRecipe{Backend: "pixi", LockFile: filepath.Join(dir, "pixi.lock")}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestPixiEnvironmentSingleFlightStableActivationAndFingerprint(t *testing.T) {
	builds := fakePixi(t)
	r := pixiRecipe(t, "lock-one")
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, err := c.Acquire(context.Background(), r)
			if err != nil {
				t.Error(err)
				return
			}
			release()
		}()
	}
	wg.Wait()
	b, err := os.ReadFile(builds)
	if err != nil || string(b) != "install\n" {
		t.Fatalf("builds %q %v", b, err)
	}
	root := filepath.Join(c.Root, r.Key)
	var env = map[string]string{"PATH": os.Getenv("PATH"), "MYCE_TOKEN": "worker-only", "CUDA_VISIBLE_DEVICES": "selected"}
	bindEnvironment(env, root)
	if env["MYCE_TOKEN"] != "worker-only" || env["CUDA_VISIBLE_DEVICES"] != "selected" || env["PIXI_ACTIVE"] != "yes" {
		t.Fatal("activation overwrote worker ownership", env)
	}
	cmd := exec.Command(filepath.Join(environmentBins(root)[0], "tool"))
	cmd.Env = cleanExtensionEnv(env)
	output, err := cmd.Output()
	if err != nil || string(output) != "activated:yes\n" {
		t.Fatal(string(output), err)
	}
	prefix := filepath.Join(root, ".pixi", "envs", "default")
	b, _ = os.ReadFile(filepath.Join(prefix, "embedded-prefix"))
	if string(b) != prefix {
		t.Fatal("relocated embedded prefix")
	}
	changed := pixiRecipe(t, "lock-two")
	if changed.Key == r.Key {
		t.Fatal("stale lock key")
	}
	_, release, err := c.Acquire(context.Background(), changed)
	if err != nil {
		t.Fatal(err)
	}
	release()
	b, _ = os.ReadFile(builds)
	if strings.Count(string(b), "install\n") != 2 {
		t.Fatal("changed fingerprint did not rebuild")
	}
}
func TestPixiEnvironmentBrokenLockAndPartialRecovery(t *testing.T) {
	fakePixi(t)
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}
	r := pixiRecipe(t, "broken")
	_, _, err := c.Acquire(context.Background(), r)
	if err == nil || !strings.Contains(err.Error(), "build failed") || c.ready(r.Key) {
		t.Fatal("broken lock accepted", err)
	}
	if _, err = os.Stat(filepath.Join(c.Root, r.Key)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed prefix retained as usable", err)
	}
	key := strings.Repeat("e", 64)
	dir := filepath.Join(c.Root, key)
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "half-installed"), []byte("incomplete"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("restart retained partial", err)
	}
	trash, _ := os.ReadDir(filepath.Join(c.Root, ".Trash"))
	if len(trash) != 2 {
		t.Fatal("lost recoverable builds", len(trash))
	}
}

func TestPixiEnvironmentActivationDiagnostics(t *testing.T) {
	for _, mode := range []string{"warning", "invalid", "failure"} {
		t.Run(mode, func(t *testing.T) {
			fakePixi(t)
			t.Setenv("MYCE_TEST_PIXI_HOOK", mode)
			r := pixiRecipe(t, "locked")
			c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}
			_, release, err := c.Acquire(context.Background(), r)
			if release != nil {
				defer release()
			}
			log, readErr := os.ReadFile(filepath.Join(c.Root, r.Key+".build.log"))
			if readErr != nil || !strings.Contains(string(log), "WARN deprecated manifest header") {
				t.Fatalf("missing stderr diagnostics: %v %s", readErr, log)
			}
			if mode == "warning" {
				if err != nil || !c.ready(r.Key) {
					t.Fatal("stderr warning failed activation", err)
				}
			} else {
				if err == nil || c.ready(r.Key) {
					t.Fatal("invalid activation published readiness")
				}
				if mode == "invalid" && !strings.Contains(err.Error(), "pixi activation:") {
					t.Fatal("invalid stdout was not rejected as JSON", err)
				}
				if mode == "failure" && !strings.Contains(err.Error(), "exit status 7") {
					t.Fatal("nonzero exit with valid stdout was not rejected", err)
				}
			}
		})
	}
}

// Optional dependency, automatic integration coverage: use an explicit binary
// when supplied, otherwise PATH. A broken explicit prerequisite must fail.
func realPixi(t *testing.T) {
	t.Helper()
	bin := os.Getenv("MYCE_PIXI_BIN")
	if bin == "" {
		var err error
		bin, err = exec.LookPath("pixi")
		if err != nil {
			t.Skip("pixi not installed; set MYCE_PIXI_BIN or add pixi to PATH")
		}
	}
	bin, err := filepath.Abs(bin)
	if err != nil {
		t.Fatal(err)
	}
	// Workers invoke the ordinary pixi command, including prerequisite checks.
	dir := t.TempDir()
	if err := os.Symlink(bin, filepath.Join(dir, "pixi")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := exec.CommandContext(ctx, "pixi", "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("pixi version: %v %s", err, b)
	}
	t.Log(strings.TrimSpace(string(b)))
}

func TestPixiRealEnvironment(t *testing.T) {
	realPixi(t)
	// Empty locked native environment needs neither downloads nor GPU access.
	platform := map[string]string{"darwin/arm64": "osx-arm64", "darwin/amd64": "osx-64", "linux/amd64": "linux-64", "linux/arm64": "linux-aarch64", "windows/amd64": "win-64"}[runtime.GOOS+"/"+runtime.GOARCH]
	if platform == "" {
		t.Skip("unsupported real pixi test target")
	}
	dir := t.TempDir()
	manifest := fmt.Sprintf("[workspace]\nname='myce-test'\nchannels=[]\nplatforms=['%s']\n", platform)
	if err := os.WriteFile(filepath.Join(dir, "pixi.toml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("pixi", "lock", "--no-config", "--manifest-path", filepath.Join(dir, "pixi.toml"))
	cmd.Env = cleanExtensionEnv(nil)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pixi lock: %v %s", err, b)
	}
	r, err := snapshotEnvironment(EnvironmentRecipe{Backend: "pixi", LockFile: filepath.Join(dir, "pixi.lock")}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 64 << 20}
	root, release, err := c.Acquire(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if environmentActivation(root)["CONDA_PREFIX"] == "" {
		t.Fatal("real activation absent")
	}
}

func TestPixiRealExtensionActivationJob(t *testing.T) {
	realPixi(t)
	platform := map[string]string{"darwin/arm64": "osx-arm64", "darwin/amd64": "osx-64", "linux/amd64": "linux-64", "linux/arm64": "linux-aarch64"}[runtime.GOOS+"/"+runtime.GOARCH]
	if platform == "" {
		t.Skip("unsupported real pixi test target")
	}
	// Empty channels/dependencies keep this test rootless, offline and GPU-free.
	// [project] intentionally exercises pixi's supported deprecation warning.
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "pixi", LockFile: "env/pixi.lock"}}
	v.Execution.Main.Environment = "main"
	v.Execution.Main.Command = []string{"sh", "-c", `test "$E2_ACTIVATION_EXPECTED" = present && test "$E2_SHARED_ACTIVATION" = shared && printf 'activation:%s/%s\n' "$E2_ACTIVATION_EXPECTED" "$E2_SHARED_ACTIVATION"`}
	e.Files["env/pixi.toml"] = []byte(fmt.Sprintf("[project]\nname='myce-activation-test'\nchannels=[]\nplatforms=['%s']\n[activation]\nscripts=['scripts/activate.sh', '../shared/activate.sh']\n", platform))
	e.Files["env/scripts/activate.sh"] = []byte("export E2_ACTIVATION_EXPECTED=present\n")
	e.Files["shared/activate.sh"] = []byte("export E2_SHARED_ACTIVATION=shared\n")
	dir := t.TempDir()
	for name, b := range e.Files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "pixi", "lock", "--no-config", "--manifest-path", filepath.Join(dir, "env", "pixi.toml"))
	cmd.Env = cleanExtensionEnv(nil)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pixi lock: %v %s", err, b)
	}
	lock, err := os.ReadFile(filepath.Join(dir, "env", "pixi.lock"))
	if err != nil {
		t.Fatal(err)
	}
	e.Files["env/pixi.lock"] = lock
	e = freezeComposition(t, e)
	w, logs := runPreparedExtensionJob(t, e)
	root := filepath.Join(w.environmentCache.Root, e.Variants[0].Environments["main"].Key)
	for name, want := range e.Files {
		got, err := os.ReadFile(filepath.Join(root, "extension", name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("prepared folder changed digest-verified file %s: %v", name, err)
		}
	}
	buildLog, err := os.ReadFile(root + ".build.log")
	if err != nil || !strings.Contains(strings.ToLower(string(buildLog)), "deprecat") {
		t.Fatalf("real pixi warning was not retained: %v %s", err, buildLog)
	}
	for _, log := range logs {
		if strings.Contains(log.Text, "activation:present/shared") {
			return
		}
	}
	t.Fatal("real pixi job did not receive bundled activation", logs)
}
func TestEnvironmentCancelledBuildRecoversStablePrefix(t *testing.T) {
	r, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"blocked"}}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Run: func(ctx context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := c.Acquire(ctx, r); done <- err }()
	<-started
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancel succeeded")
	}
	if c.ready(r.Key) {
		t.Fatal("cancel published ready")
	}
	if _, err := os.Stat(filepath.Join(c.Root, r.Key)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancel prefix not cleaned", err)
	}
}
func TestPixiDeclarationSnapshotsCompleteFolder(t *testing.T) {
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "pixi", Manifest: "env/pixi.toml", LockFile: "env/pixi.lock"}}
	v.Execution.Main.Environment = "main"
	e.Files["env/pixi.toml"] = []byte("[workspace]\n")
	e.Files["env/pixi.lock"] = []byte("locked")
	e.Files["env/scripts/activate.sh"] = []byte("export E2_ACTIVATION_EXPECTED=present\n")
	e.Files["shared/config.txt"] = []byte("configuration")
	e = freezeComposition(t, e)
	r := e.Variants[0].Environments["main"]
	for name, want := range e.Files {
		if !bytes.Equal(r.Files["extension/"+name], want) {
			t.Fatalf("missing bundled file %s", name)
		}
	}
	var out Extension
	b, _ := json.Marshal(e)
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Variants[0].Environments["main"].Key != r.Key {
		t.Fatal("snapshot transport changed")
	}
	e.Files["shared/config.txt"] = []byte("changed configuration")
	changed := freezeComposition(t, e)
	if changed.Digest == e.Digest || changed.Variants[0].Environments["main"].Key == r.Key {
		t.Fatal("bundled content change did not invalidate digest and environment")
	}
}
