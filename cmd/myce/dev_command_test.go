package main

import (
	"context"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDevCommandQuickstart(t *testing.T) {
	// Full gates receive source archives without Git history. Build a tiny local
	// origin from these exact production bytes, then run the published five lines.
	// Redirect only Git transport: the offline regression never contacts GitHub.
	source, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	origin := filepath.Join(root, "origin")
	if err := os.MkdirAll(origin, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go.mod", "go.sum", "cmd", "internal", "examples", ".mycelium/profiles/quickstart.yaml"} {
		err := filepath.WalkDir(filepath.Join(source, name), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(source, path)
			if err != nil {
				return err
			}
			dest := filepath.Join(origin, rel)
			if entry.IsDir() {
				return os.MkdirAll(dest, 0700)
			}
			if strings.HasSuffix(path, "_test.go") {
				return nil
			}
			if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(dest, data, 0600)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // Includes one cached production build plus two tiny real jobs.
	run := func(dir string, env []string, command string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, command, args...)
		cmd.Dir, cmd.Env = dir, env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", command, err, sanitizeSmokeEvidence(string(out), root, "ambient-fixture-secret"))
		}
		return string(out)
	}
	env := append(os.Environ(), "GOMAXPROCS=2")
	run(origin, env, "git", "init", "-q")
	run(origin, env, "git", "add", ".")
	run(origin, env, "git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
	env = append(env, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=url."+origin+".insteadOf", "GIT_CONFIG_VALUE_0=https://github.com/1kuna/mycelium.git",
		"MYCE_TOKEN=ambient-fixture-secret", "MYCE_QUEUE=http://127.0.0.1:1", "MYCE_JOINING_FILE=/nonexistent-fixture-authority", "TMPDIR="+root)
	commands, err := os.ReadFile(filepath.Join(source, "examples/quickstart-commands.sh"))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	out := run(root, env, "sh", "-ec", string(commands))
	for _, want := range []string{"hello from CPU", "Submitted fake-GPU job ", "succeeded", "Verified quickstart-result/result.txt", "hello from Mycelium"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "ambient-fixture-secret") {
		t.Fatal("ambient authority leaked")
	}
	result, err := os.ReadFile(filepath.Join(root, "mycelium", "quickstart-result", "result.txt"))
	if err != nil || string(result) != "hello from Mycelium\n" {
		t.Fatal("wrong accepted bytes", err)
	}
	assertClean := func() {
		t.Helper()
		left, err := filepath.Glob(filepath.Join(root, "myce-dev-*"))
		if err != nil || len(left) != 0 {
			t.Fatalf("dev state survived: %v (%v)", left, err)
		}
	}
	assertClean()
	t.Logf("five exact quick-start commands: %s; CPU log and verified fake-GPU result accepted", time.Since(started))
	// A failing helper still closes its queue and preserves its exit status.
	cmd := exec.CommandContext(ctx, "./myce", "dev", "--", "sh", "-c", "exit 7")
	cmd.Dir, cmd.Env = filepath.Join(root, "mycelium"), env
	if err := cmd.Run(); err == nil || cmd.ProcessState.ExitCode() != 7 {
		t.Fatalf("helper failure: %v", err)
	}
	assertClean()
	// Interrupt a real command tree, then prove its child and listener are gone.
	script := `sleep 60 & echo $! > child.pid; printf '%s' "$MYCE_QUEUE" > endpoint; wait`
	cmd = exec.CommandContext(ctx, "./myce", "dev", "--", "sh", "-c", script)
	cmd.Dir, cmd.Env = filepath.Join(root, "mycelium"), env
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	var endpoint []byte
	for len(endpoint) == 0 {
		endpoint, _ = os.ReadFile(filepath.Join(cmd.Dir, "endpoint"))
		if err := waitPause(ctx, 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil || cmd.ProcessState.ExitCode() != 130 {
		t.Fatalf("interrupt exit: %v", err)
	}
	pidBytes, err := os.ReadFile(filepath.Join(cmd.Dir, "child.pid"))
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	if _, err := fmt.Sscan(string(pidBytes), &pid); err != nil {
		t.Fatal(err)
	}
	// A killed child may remain a zombie until the OS reaps it; it must not run.
	stateBytes, psErr := exec.Command("ps", "-o", "stat=", "-p", fmt.Sprint(pid)).Output()
	if psErr != nil && len(stateBytes) != 0 {
		t.Fatal(psErr)
	}
	state := string(stateBytes)
	if state != "" && !strings.HasPrefix(strings.TrimSpace(state), "Z") {
		t.Fatalf("helper survived: %s", state)
	}
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(string(endpoint), "http://"), time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("dev listener survived")
	}
	assertClean()
}
