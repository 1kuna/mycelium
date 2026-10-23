package main

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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
	dev := exec.Command(binary, "dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(root, "queue.db"), "--checkpoints", filepath.Join(root, "checkpoints"), "--workers", "1", "--gpu", "cpu")
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
	marker := filepath.Join(root, "effect")
	output, err := command("--project", "demo", "--cpu", "1", "--mem", "1G", "--", "sh", "-c", `test -z "$MYCE_TOKEN" && printf 'hello cluster\n' && printf done > "$1"`, "sh", marker).CombinedOutput()
	if err != nil || !strings.Contains(string(output), "hello cluster\n") || strings.Contains(string(output), token) {
		t.Fatalf("run: %v %s", err, output)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "done" {
		t.Fatal("real command effect missing")
	}
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
	t.Log("real dev: CPU command effect/logs, no inherited token, exit 7, detach, Ctrl-C cancellation and exact release; owned dev joined and listener closed")
}
