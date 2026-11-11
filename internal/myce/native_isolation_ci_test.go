//go:build linux || darwin

package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// #1298: a ci job sees neither the NAS root, the worker user's home nor
// another job's files, and an ordinary build still works from attempt scratch.
func TestNativeIsolationCIJob(t *testing.T) {
	setWorkerSession(t)
	w := recoveryWorker(t)
	if !slices.Contains(w.nativeIsolationCapabilities(), nativeCICapability) {
		t.Skip("host confinement unavailable")
	}
	nas := t.TempDir()
	if err := os.WriteFile(filepath.Join(nas, "secret"), []byte("nas"), 0600); err != nil {
		t.Fatal(err)
	}
	w.ServiceConfig = &ServiceConfig{NAS: nas}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	sibling := filepath.Join(root.dir, "job-files", "other-job", "1")
	if err := os.MkdirAll(sibling, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sibling, "secret"), []byte("sibling"), 0600); err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	build := ""
	if _, err := exec.LookPath("cc"); err == nil {
		build += `printf 'int main(void){return 0;}\n' > t.c && cc t.c -o t.bin && ./t.bin || fail build
`
	}
	if _, err := exec.LookPath("git"); err == nil {
		build += `git config --global user.email ci@example.invalid && git config --global user.name ci || fail git-config
git init -q repo && cd repo && echo x > f && git add f && git commit -qm init && cd .. || fail git
`
	}
	job := recoveryJob(t, []string{"/bin/sh", "-c", `
fail() { echo "failed: $1"; failures=$((failures+1)); }
failures=0
ls "$NAS" >/dev/null 2>&1 && fail nas-list
cat "$NAS/secret" >/dev/null 2>&1 && fail nas-read
ls "$SIBLING" >/dev/null 2>&1 && fail sibling-list
cat "$SIBLING/secret" >/dev/null 2>&1 && fail sibling-read
ls "$REAL_HOME" >/dev/null 2>&1 && fail home-list
test "$(cd "$HOME" && pwd -P)" = "$(pwd -P)/.home" || fail home-not-private
mkdir -p "$HOME/.cache/tool" && printf c > "$HOME/.cache/tool/c" || fail cache
` + build + `test "$failures" = 0 && echo ci-passed`})
	job.Spec.Isolation = nativeIsolationCI
	job.Spec.Resumable, job.Spec.Logs = false, "keep"
	// An authored HOME cannot point the job back at the worker user's home.
	job.Spec.Run.Env = map[string]string{"NAS": nas, "SIBLING": sibling, "REAL_HOME": home, "HOME": home, "GIT_CONFIG_NOSYSTEM": "1"}
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
		if finish.err != nil || !finish.result.Success || !strings.Contains(string(out), "ci-passed") {
			t.Fatalf("ci job: %+v\n%s", finish, out)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("ci job timed out")
	}
}

func TestNativeCIPlacementRequiresCapability(t *testing.T) {
	job := Job{Spec: Spec{Isolation: nativeIsolationCI, Run: RunSpec{Command: []string{"true"}}}}
	old := &Machine{Capabilities: []string{"native_isolation_seatbelt"}}
	current := &Machine{Capabilities: []string{"native_isolation_seatbelt", nativeCICapability}}
	if nativeNetworkFits(&job, old) || !nativeNetworkFits(&job, current) {
		t.Fatal("ci jobs must place only on workers advertising native_isolation_ci")
	}
	if reason := targetFitReason("native network isolation", &job, old); !strings.Contains(reason, nativeCICapability) {
		t.Fatalf("reason %q", reason)
	}
	if err := ValidateNativeIsolation(nativeIsolationCI, ""); err != nil {
		t.Fatal(err)
	}
}
