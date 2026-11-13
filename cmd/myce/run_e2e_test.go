package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestRunDevEndToEnd(t *testing.T) {
	root := t.TempDir()
	binary := devTestBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	const token = "run-dev-fixture-token"
	environment := []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TMPDIR=" + root, "MYCE_TOKEN=" + token}
	dev := exec.Command(binary, "dev", "--environment-cache-bytes", "1073741824", "--listen", "127.0.0.1:0", "--db", filepath.Join(root, "queue.db"), "--checkpoints", filepath.Join(root, "checkpoints"), "--file-store", filepath.Join(root, "files"), "--workers", "1", "--gpu", "cpu")
	dev.Dir, dev.Env = root, environment
	stdout, err := dev.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostics enrollmentLogBuffer
	dev.Stderr = &diagnostics
	if err = dev.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- dev.Wait() }()
	var client myce.Client
	defer func() {
		_ = dev.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("dev exit: %v %s", err, diagnostics.String())
			}
		case <-ctx.Done():
			_ = dev.Process.Kill()
			<-done
			t.Error("dev teardown deadline")
		}
		if client.URL != "" {
			conn, err := net.DialTimeout("tcp", strings.TrimPrefix(client.URL, "http://"), time.Second)
			if err == nil {
				conn.Close()
				t.Error("dev listener survived teardown")
			}
		}
	}()
	endpoint := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) >= 3 {
				endpoint <- fields[2]
				return
			}
		}
		endpoint <- ""
	}()
	select {
	case address := <-endpoint:
		if address == "" {
			t.Fatal("no dev address")
		}
		client = myce.Client{URL: address, Token: token}
	case <-ctx.Done():
		t.Fatal("dev startup deadline")
	}
	command := func(args ...string) *exec.Cmd {
		c := exec.CommandContext(ctx, binary, append([]string{"run", "--queue", client.URL}, args...)...)
		c.Dir, c.Env = root, environment
		return c
	}
	output, err := command("--project", "demo", "--cpu", "1", "--mem", "1G", "--", "sh", "-c", `test -z "$MYCE_TOKEN" && printf 'hello cluster\n' && printf done > effect && printf 'effect=%s\n' "$(cat effect)"`).CombinedOutput()
	if err != nil || !strings.Contains(string(output), "hello cluster\n") || strings.Contains(string(output), token) {
		t.Fatalf("run: %v %s", err, output)
	}
	// Accepted attempts may reclaim undeclared scratch immediately. Read the
	// actual file in the payload and verify its durable log instead of racing
	// worker collection after the run returns.
	if !strings.Contains(string(output), "effect=done\n") {
		t.Fatal("real command effect missing", string(output))
	}

	t.Run("tagged-synthetic-resource-probes", func(t *testing.T) {
		assertSyntheticResourceTelemetry(t, ctx, client, strings.Fields(string(output))[0])
	})
	// Profiles upload a frozen source and explicit inputs even with NAS checkpoints.
	profileDir := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(profileDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source", "run.sh"), []byte("test -z \"$MYCE_TOKEN\" && cat payload > result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "payload"), []byte("profile on NAS"), 0600); err != nil {
		t.Fatal(err)
	}
	profile := `name: nas-profile
project: demo
class: experiment
source: {directory: source}
environment: {}
run: {command: [sh, run.sh]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: ` + runtime.GOOS + `, arch: ` + runtime.GOARCH + `, platform: cpu}]
inputs: {payload: payload}
outputs: {result: results}
check: [test, -s, result]
`
	profilePath := filepath.Join(profileDir, "nas.yaml")
	if err := os.WriteFile(profilePath, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	profileCommand := command("--profile", profilePath, "--key", "nas-profile")
	var profileDiagnostics enrollmentLogBuffer
	profileCommand.Stderr = &profileDiagnostics
	output, err = profileCommand.Output()
	if err != nil {
		t.Fatalf("profile submission: %v %s %s", err, output, profileDiagnostics.String())
	}
	var receipt profileReceipt
	if err := json.Unmarshal(output, &receipt); err != nil || receipt.JobID == "" {
		t.Fatalf("profile receipt: %s %v", output, err)
	}
	finished, err := client.Wait(ctx, receipt.JobID)
	if err != nil || finished.State != "succeeded" || finished.FileTransport || len(receipt.Spec.Files) != 2 {
		t.Fatalf("NAS profile execution: %+v %v", finished, err)
	}
	dest := filepath.Join(root, "profile-result")
	if err := client.DownloadResult(ctx, receipt.JobID, "result", dest); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(dest); err != nil || string(data) != "profile on NAS" {
		t.Fatalf("profile result: %s %v", data, err)
	}
	receiptPath := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"demo", receipt.Key}), "receipt.json")
	wait(t, ctx, func() bool {
		var observed profileReceipt
		return readProfileJSON(receiptPath, &observed) == nil && observed.Status == "succeeded" && observed.Observation == "not_armed"
	})
	output, err = command("--", "sh", "-c", "echo failure >&2; exit 7").CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 || !strings.Contains(string(output), "failure") {
		t.Fatalf("exit propagation: %v %s", err, output)
	}
	output, err = command("--detach", "--", "echo", "detached").Output()
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(output))
	if j, err := client.Wait(ctx, id); err != nil || j.State != "succeeded" {
		t.Fatalf("detached job: %+v %v", j, err)
	}
	running := command("--", "sh", "-c", "echo cancel-ready; sleep 30")
	var logs enrollmentLogBuffer
	running.Stdout, running.Stderr = &logs, &logs
	if err = running.Start(); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- running.Wait() }()
	defer func() {
		if running.ProcessState == nil {
			_ = running.Process.Kill()
			<-runDone
		}
	}()
	wait(t, ctx, func() bool { return strings.Contains(logs.String(), "cancel-ready") })
	_ = running.Process.Signal(os.Interrupt)
	select {
	case err = <-runDone:
		if !errors.As(err, &exit) || exit.ExitCode() != runDetachedExit {
			t.Fatalf("interrupt: %v %s", err, logs.String())
		}
	case <-ctx.Done():
		t.Fatal("run cancellation deadline")
	}
	// Interrupt only detached the waiter. Explicit cancellation still stops and
	// releases the running command.
	// State lines start with the exact job ID; do not search status projections
	// for command text, which they intentionally omit.
	interruptedID := strings.Fields(logs.String())[0]
	j, err := client.GetJob(ctx, interruptedID)
	if err != nil || j.State != "running" || j.CancelledAt != nil {
		t.Fatalf("interrupt cancelled: %+v %v", j, err)
	}
	c := exec.CommandContext(ctx, binary, "cancel", "--wait", "--queue", client.URL, interruptedID)
	c.Env = environment
	if output, err := c.CombinedOutput(); err != nil {
		t.Fatalf("explicit cancel: %v %s", err, output)
	}
	stopped, err := client.GetJob(ctx, interruptedID)
	if err != nil || stopped.State != "cancelled" || !stopped.ReleaseConfirmed {
		t.Fatalf("explicit release: %+v %v", stopped, err)
	}
	t.Run("wait-out", func(t *testing.T) {
		exitOf := func(err error) int {
			var exit *exec.ExitError
			if err == nil {
				return 0
			}
			if !errors.As(err, &exit) {
				t.Fatal(err)
			}
			return exit.ExitCode()
		}
		// Inline --out returns declared files, verified against output_refs.
		inlineOut := filepath.Join(root, "inline-out")
		output, err := command("--out", inlineOut, "--output", "nested/inline.txt", "--", "sh", "-c", "mkdir -p nested && printf inline-ok > nested/inline.txt").CombinedOutput()
		if b, e := os.ReadFile(filepath.Join(inlineOut, "nested", "inline.txt")); err != nil || e != nil || string(b) != "inline-ok" {
			t.Fatalf("inline --out: %v %s %q %v", err, output, b, e)
		}
		if err := os.WriteFile(filepath.Join(root, "source", "entry.sh"), []byte(`test -z "${MYCE_TOKEN:-}"
case "$1" in
ok) echo adapter-log; printf adapter-ok > result ;;
fail) echo adapter-failing >&2; exit 9 ;;
sleep) echo adapter-ready; sleep 60 ;;
tree) mkdir -p tree/sub logs && printf a > tree/a && printf b > tree/sub/b && printf x > logs/x.log && printf y > logs/y.log && printf skip > logs/z.txt ;;
esac
`), 0600); err != nil {
			t.Fatal(err)
		}
		adapter := `schema: 1
project: demo
default: work
entries:
  work:
    source: {directory: source}
    run: {command: [sh, entry.sh]}
    needs: {cpu_cores: 1, memory_gb: 0.25, platform: cpu}
    targets: [{os: ` + runtime.GOOS + `, arch: ` + runtime.GOARCH + `, platform: cpu}]
    outputs: {result: results}
    checks: {result: [test, -s, result]}
  tree:
    source: {directory: source}
    run: {command: [sh, entry.sh, tree]}
    needs: {cpu_cores: 1, memory_gb: 0.25, platform: cpu}
    targets: [{os: ` + runtime.GOOS + `, arch: ` + runtime.GOARCH + `, platform: cpu}]
    outputs: {"tree/": results, "logs/*.log": results}
`
		if err := os.WriteFile(filepath.Join(root, ".mycelium", "adapter.yaml"), []byte(adapter), 0600); err != nil {
			t.Fatal(err)
		}
		entry := func(args ...string) (string, string, int) {
			c := command(append([]string{"--entry", "work"}, args...)...)
			var stdout, stderr enrollmentLogBuffer
			c.Stdout, c.Stderr = &stdout, &stderr
			code := exitOf(c.Run())
			if strings.Contains(stdout.String()+stderr.String(), token) {
				t.Fatal("credential exposed")
			}
			return stdout.String(), stderr.String(), code
		}
		entryOut := filepath.Join(root, "entry-out")
		stdout, stderr, code := entry("--wait", "--out", entryOut, "--", "ok")
		if b, e := os.ReadFile(filepath.Join(entryOut, "result")); code != 0 || e != nil || string(b) != "adapter-ok" || stdout != "adapter-log\n" || !strings.Contains(stderr, "outputs verified") {
			t.Fatalf("entry success: code=%d stdout=%q stderr=%s result=%q %v", code, stdout, stderr, b, e)
		}
		// Folder and glob outputs resolve on adapter entries and download every file (#1341).
		treeOut := filepath.Join(root, "tree-out")
		if output, err := command("--entry", "tree", "--wait", "--out", treeOut).CombinedOutput(); err != nil {
			t.Fatalf("pattern entry: %v %s", err, output)
		}
		for name, want := range map[string]string{"tree/a": "a", "tree/sub/b": "b", "logs/x.log": "x", "logs/y.log": "y"} {
			if b, e := os.ReadFile(filepath.Join(treeOut, filepath.FromSlash(name))); e != nil || string(b) != want {
				t.Fatalf("pattern output %s: %q %v", name, b, e)
			}
		}
		if _, e := os.Stat(filepath.Join(treeOut, "logs", "z.txt")); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("glob downloaded an unmatched file", e)
		}
		if stdout, stderr, code = entry("--wait", "--out", entryOut, "--", "fail"); code != 9 || !strings.Contains(stdout+stderr, "adapter-failing") {
			t.Fatalf("entry command exit: code=%d %s %s", code, stdout, stderr)
		}
		if output, err := command("--entry", "absent", "--wait").CombinedOutput(); exitOf(err) != 64 || !strings.Contains(string(output), "adapter_invalid") {
			t.Fatalf("typed refusal exit: %v %s", err, output)
		}
		// SIGTERM detaches the entry waiter; explicit cancel stops the job.
		running := command("--entry", "work", "--wait", "--", "sleep")
		var logs enrollmentLogBuffer
		running.Stdout, running.Stderr = &logs, &logs
		if err := running.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- running.Wait() }()
		wait(t, ctx, func() bool { return strings.Contains(logs.String(), "adapter-ready") })
		_ = running.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if exitOf(err) != runDetachedExit || !strings.Contains(logs.String(), "continues") {
				t.Fatalf("entry interrupt: %v %s", err, logs.String())
			}
		case <-ctx.Done():
			_ = running.Process.Kill()
			<-done
			t.Fatal("entry cancellation deadline")
		}
		var interruptedID string
		for _, line := range strings.Split(logs.String(), "\n") {
			fields := strings.Fields(line)
			if len(fields) > 1 && fields[1] == "submitted:" {
				interruptedID = fields[0]
				break
			}
		}
		if interruptedID == "" {
			t.Fatal("entry job ID missing", logs.String())
		}
		j, err := client.GetJob(ctx, interruptedID)
		if err != nil || j.State != "running" || j.CancelledAt != nil {
			t.Fatalf("entry interrupt cancelled: %+v %v", j, err)
		}
		c := exec.CommandContext(ctx, binary, "cancel", "--wait", "--queue", client.URL, interruptedID)
		c.Env = environment
		if output, err := c.CombinedOutput(); err != nil {
			t.Fatalf("explicit entry cancel: %v %s", err, output)
		}

	})
	t.Log("real dev: CPU command effect/logs, NAS-mode profile source/input upload and verified output, no inherited token, exit 7, detach, Ctrl-C detach and explicit cancellation/release, --wait/--out entry and inline outputs, exit 9 and typed refusal 64, SIGTERM detach; owned dev joined and listener closed")
}
