package myce

import (
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
)

func fakePixi(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
set -eu
case "$1" in
install)
 test "$2" = --locked
 test "$(cat pixi.lock)" != broken || { echo 'lock inconsistent with manifest' >&2; exit 9; }
 printf 'install\n' >> "$MYCE_TEST_PIXI_BUILDS"
 prefix="$PWD/.pixi/envs/default"
 mkdir -p "$prefix/bin"
 printf '#!/bin/sh\nprintf "activated:%%s\\n" "$PIXI_ACTIVE"\n' > "$prefix/bin/tool"
 chmod +x "$prefix/bin/tool"
 printf '%s' "$prefix" > "$prefix/embedded-prefix"
 ;;
shell-hook)
 prefix="$PWD/.pixi/envs/default"
 printf '{"environment_variables":{"PATH":"%s/bin:/usr/bin:/bin","CONDA_PREFIX":"%s","PIXI_ACTIVE":"yes","MYCE_TOKEN":"forbidden","CUDA_VISIBLE_DEVICES":"wrong"},"activation_scripts":[]}' "$prefix" "$prefix"
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
func TestPixiRealEnvironment(t *testing.T) {
	if _, err := exec.LookPath("pixi"); err != nil {
		t.Skip("pixi not installed")
	}
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
	cmd := exec.Command("pixi", "lock", "--manifest-path", filepath.Join(dir, "pixi.toml"))
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
func TestEnvironmentCancelledBuildRecoversStablePrefix(t *testing.T) {
	r, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"blocked"}}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Run: func(ctx context.Context, _ []string, dir string, _ []string, _ io.Writer) error {
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
func TestPixiDeclarationSnapshotsBothInputs(t *testing.T) {
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "pixi", Manifest: "env/pixi.toml", LockFile: "env/pixi.lock"}}
	v.Execution.Main.Environment = "main"
	e.Files["env/pixi.toml"] = []byte("[workspace]\n")
	e.Files["env/pixi.lock"] = []byte("locked")
	e = freezeComposition(t, e)
	r := e.Variants[0].Environments["main"]
	if string(r.Files["pixi.lock"]) != "locked" || string(r.Files["pixi.toml"]) != "[workspace]\n" {
		t.Fatal("missing input snapshot")
	}
	var out Extension
	b, _ := json.Marshal(e)
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Variants[0].Environments["main"].Key != r.Key {
		t.Fatal("snapshot transport changed")
	}
}
