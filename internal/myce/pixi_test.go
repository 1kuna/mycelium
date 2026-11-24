package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func fakePixi(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
set -eu
# Version probes run before a build and must not require its activation env.
if [ "${1:-}" = --version ]; then printf 'pixi 0.81.0\n'; exit 0; fi
test "$PIXI_NO_PROGRESS" = true
case "$1" in
install)
 test "$2" = --locked
 manifest=
 for arg in "$@"; do manifest="$arg"; done
 test "$(cat "$(dirname "$manifest")/pixi.lock")" != broken || { echo 'lock inconsistent with manifest' >&2; exit 9; }
 printf 'install\n' >> "__DIR__/builds"
 prefix="$PWD/.pixi/envs/default"
 mkdir -p "$prefix/bin"
 printf '#!/bin/sh\nprintf "activated:%%s\\n" "$PIXI_ACTIVE"\n' > "$prefix/bin/tool"
 chmod +x "$prefix/bin/tool"
 printf '%s' "$prefix" > "$prefix/embedded-prefix"
 ;;
shell-hook)
 echo 'WARN deprecated manifest header' >&2
 hook=$(cat "__DIR__/hook" 2>/dev/null || true)
 test "$hook" != invalid || { echo 'not JSON'; exit 0; }
 prefix="$PWD/.pixi/envs/default"
 printf '{"environment_variables":{"PATH":"%s/bin:/usr/bin:/bin","CONDA_PREFIX":"%s","PIXI_ACTIVE":"yes","MYCE_TOKEN":"forbidden","CUDA_VISIBLE_DEVICES":"wrong"},"activation_scripts":[]}' "$prefix" "$prefix"
 test "$hook" != failure || exit 7
 ;;
*) exit 88 ;;
esac
`
	// Jobs and builds do not inherit test variables (#219); markers are files.
	script = strings.ReplaceAll(script, "__DIR__", dir)
	if err := os.WriteFile(filepath.Join(dir, "pixi"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return filepath.Join(dir, "builds")
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
	if len(trash) != 0 {
		t.Fatal("retained retired builds", len(trash))
	}
}

func TestPixiEnvironmentActivationDiagnostics(t *testing.T) {
	for _, mode := range []string{"warning", "invalid", "failure"} {
		t.Run(mode, func(t *testing.T) {
			builds := fakePixi(t)
			if err := os.WriteFile(filepath.Join(filepath.Dir(builds), "hook"), []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
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
