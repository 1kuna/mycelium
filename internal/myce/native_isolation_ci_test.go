//go:build linux || darwin

package myce

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	// Jobs inherit this process's PATH. Under a fleet profile it starts with the
	// declared Pixi/conda environment, which lives in the outer worker's cache
	// under the hidden home. In production the job's own environment is in its
	// worker's granted environment cache, so grant exactly that prefix the same
	// way; the rest of the home stays hidden (home-list, env-parent-list).
	envPrefix := ciPathEnvironmentPrefix()
	if envPrefix != "" {
		w.environmentCache = &EnvironmentCache{Root: envPrefix}
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
	for i, git := range ciGitCandidates(t, home, envPrefix) {
		repo, g := quoteShell(fmt.Sprintf("repo-%d", i)), quoteShell(git)
		build += fmt.Sprintf(`%s init -q %s && echo x > %s/f && %s -C %s add f && %s -C %s commit -qm init || fail %s
`, g, repo, repo, g, repo, g, repo, quoteShell("git:"+git))
	}
	if envPrefix != "" && nativePathContains(home, envPrefix) {
		build += `ls "$ENV_PARENT" >/dev/null 2>&1 && fail env-parent-list
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
	job.Spec.Run.Env = map[string]string{"NAS": nas, "SIBLING": sibling, "REAL_HOME": home, "HOME": home, "GIT_CONFIG_NOSYSTEM": "1", "ENV_PARENT": filepath.Dir(envPrefix)}
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

// The first PATH directory that is a Pixi/conda environment's bin, as the
// worker prepends a declared environment; empty when PATH has none.
func ciPathEnvironmentPrefix() string {
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		prefix, err := filepath.EvalSymlinks(filepath.Dir(dir))
		if err != nil || filepath.Base(dir) != "bin" {
			continue
		}
		if info, err := os.Stat(filepath.Join(prefix, "conda-meta")); err == nil && info.IsDir() {
			return prefix
		}
	}
	return ""
}

// Every standard git installation present on this host, by real path: the
// system shim (Command Line Tools or Xcode on macOS), each toolchain directly,
// Homebrew, and a granted declared environment. A tool installed elsewhere in
// the worker user's home is hidden by design and not a ci toolchain.
func ciGitCandidates(t *testing.T, home, envPrefix string) []string {
	t.Helper()
	paths := []string{"/usr/bin/git", "/usr/local/bin/git", "/opt/homebrew/bin/git"}
	if runtime.GOOS == "darwin" {
		// Without a developer directory the /usr/bin shim offers an installer.
		if exec.Command("/usr/bin/xcode-select", "-p").Run() != nil {
			paths = paths[1:]
		}
		paths = append(paths, "/Library/Developer/CommandLineTools/usr/bin/git", "/Applications/Xcode.app/Contents/Developer/usr/bin/git")
	}
	if envPrefix != "" {
		paths = append(paths, filepath.Join(envPrefix, "bin", "git"))
	}
	var found []string
	seen := map[string]bool{}
	for _, path := range paths {
		real, err := filepath.EvalSymlinks(path)
		if err != nil || seen[real] {
			continue
		}
		if info, err := os.Stat(real); err != nil || info.Mode()&0111 == 0 {
			continue
		}
		seen[real] = true
		if nativePathContains(home, real) && (envPrefix == "" || !nativePathContains(envPrefix, real)) {
			t.Logf("skipping hidden home git %s", real)
			continue
		}
		found = append(found, path)
	}
	return found
}
