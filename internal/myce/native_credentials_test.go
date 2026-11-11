//go:build linux || darwin

package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #1309: a plain (MY-25 default) native job keeps ordinary home access but
// can read neither this worker's nor a co-installed queue's credential,
// configuration and environment files, nor the user's SSH keys.
func TestPlainNativeJobWithholdsCredentials(t *testing.T) {
	setWorkerSession(t)
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	write := func(rel string, mode os.FileMode) string {
		path := filepath.Join(home, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	install := filepath.Join(home, ".local/share/mycelium")
	secrets := []string{
		write(".ssh/id_test", 0600),
		write(".local/share/mycelium/worker/credentials.json", 0600),
		write(".local/share/mycelium/worker/joining.json", 0600),
		write(".local/share/mycelium/worker/config.json.rollback.json", 0600),
		write(".config/myce/fleet.env", 0600),
		write(".config/myce/queue.env", 0600),
		write(".local/share/mycelium/queue/state/myce.db", 0600),
	}
	config := filepath.Join(install, "worker/config.json")
	queueConfig := filepath.Join(install, "queue/config.json")
	if err := os.WriteFile(queueConfig, []byte(`{"environment_file":"`+filepath.Join(home, ".config/myce/queue.env")+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	secrets = append(secrets, write(".local/share/mycelium/worker/config.json", 0600), queueConfig)
	readable := []string{
		write("notes.txt", 0600),
		write(".cache/tool/data", 0600),
		write(".config/tool/settings", 0600),
		write(".local/share/mycelium/worker/myce.bin", 0700),
		write(".local/share/mycelium/tools/pixi/pixi", 0700),
	}
	w := recoveryWorker(t)
	w.ServiceConfig = &ServiceConfig{Config: config, EnvironmentFile: filepath.Join(home, ".config/myce/fleet.env")}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	script := `fail() { echo "failed: $1"; failures=$((failures+1)); }
failures=0
for f in $SECRETS; do head -c1 "$f" >/dev/null 2>&1 && fail "read $f"; done
for f in $READABLE; do head -c1 "$f" >/dev/null 2>&1 || fail "unreadable $f"; done
ls "$HOME" >/dev/null 2>&1 || fail home-list
mkdir -p "$HOME/.cache/tool/new" && printf c > "$HOME/.cache/tool/new/c" && test "$(cat "$HOME/.cache/tool/new/c")" = c || fail cache
mkdir -p a b && printf r > a/f && mv a/f b/f && test "$(cat b/f)" = r || fail rename
`
	if _, err := exec.LookPath("cc"); err == nil {
		script += `printf 'int main(void){return 0;}\n' > t.c && cc t.c -o t.bin && ./t.bin || fail build
`
	}
	if _, err := exec.LookPath("git"); err == nil {
		script += `git init -q repo && cd repo && echo x > f && git add f && git -c user.email=p@example.invalid -c user.name=p commit -qm init && cd .. || fail git
`
	}
	job := recoveryJob(t, []string{"/bin/sh", "-c", script + `test "$failures" = 0 && echo plain-passed`})
	job.Spec.Resumable, job.Spec.Logs = false, "keep"
	job.Spec.Run.Env = map[string]string{"SECRETS": strings.Join(secrets, " "), "READABLE": strings.Join(readable, " "), "GIT_CONFIG_NOSYSTEM": "1"}
	if !nativeUnsandboxed(&job) {
		t.Fatal("fixture must be a plain unsandboxed job")
	}
	store, err := newAttemptStore(root, job)
	if err != nil {
		t.Fatal(err)
	}
	p, err := startAfterFixtureInputs(t, w, store, job)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stop(w.Clock.Now(), 0, true) })
	select {
	case finish := <-p.done:
		out, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
		if finish.err != nil || !finish.result.Success || !strings.Contains(string(out), "plain-passed") {
			t.Fatalf("plain job: %+v\n%s", finish, out)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("plain job timed out")
	}
}

// A grant enclosing a denied path would re-expose it; nested denials collapse.
func TestNativeCredentialPathsLayout(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	install := filepath.Join(home, ".local/share/mycelium")
	state := filepath.Join(install, "worker/state")
	for _, dir := range []string{filepath.Join(state, "spool"), filepath.Join(home, ".ssh")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(install, "worker/config.json")
	if err := os.WriteFile(config, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	w := Worker{StateDir: state, JobFileRoot: home, ServiceConfig: &ServiceConfig{Config: config, State: state}}
	deny, allow, err := w.nativeCredentialPaths(filepath.Join(state, "spool"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(deny, ",") != filepath.Join(home, ".local/share/mycelium")+","+filepath.Join(home, ".ssh") {
		t.Fatalf("deny %q", deny)
	}
	// The home-wide job-file root encloses denied paths and is dropped.
	if len(allow) != 2 || allow[0] != state || allow[1] != filepath.Join(state, "spool") {
		t.Fatalf("allow %q", allow)
	}
}
