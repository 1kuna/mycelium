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
	dev := exec.Command(binary, "dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(root, "queue.db"), "--checkpoints", filepath.Join(root, "checkpoints"), "--file-store", filepath.Join(root, "files"), "--workers", "1", "--gpu", "cpu")
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
	output, err := command("--project", "demo", "--cpu", "1", "--mem", "1G", "--", "sh", "-c", `test -z "$MYCE_TOKEN" && printf 'hello cluster\n' && printf done > effect`).CombinedOutput()
	if err != nil || !strings.Contains(string(output), "hello cluster\n") || strings.Contains(string(output), token) {
		t.Fatalf("run: %v %s", err, output)
	}
	marker := filepath.Join(root, "workers", "fake-1", "job-files", strings.Fields(string(output))[0], "1", "effect")
	if data, err := os.ReadFile(marker); err != nil || string(data) != "done" {
		t.Fatal("real command effect missing")
	}
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
		if !errors.As(err, &exit) || exit.ExitCode() != 130 {
			t.Fatalf("interrupt: %v %s", err, logs.String())
		}
	case <-ctx.Done():
		t.Fatal("run cancellation deadline")
	}
	wait(t, ctx, func() bool {
		var s myce.Snapshot
		if err := client.Call(ctx, "GET", "/api/status", nil, &s); err != nil {
			t.Fatal(err)
		}
		for _, j := range s.Jobs {
			if j.Spec.Name == "sh" && j.CancelledAt != nil {
				return j.State == "cancelled" && j.ReleaseConfirmed
			}
		}
		return false
	})
	t.Log("real dev: CPU command effect/logs, NAS-mode profile source/input upload and verified output, no inherited token, exit 7, detach, Ctrl-C cancellation and exact release; owned dev joined and listener closed")
}
