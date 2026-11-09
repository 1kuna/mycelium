package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"mycelium/internal/myce"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	output, err := command("--project", "demo", "--cpu", "1", "--mem", "1G", "--", "sh", "-c", `test -z "$MYCE_TOKEN" && printf 'hello cluster\n' && printf done > effect`).CombinedOutput()
	if err != nil || !strings.Contains(string(output), "hello cluster\n") || strings.Contains(string(output), token) {
		t.Fatalf("run: %v %s", err, output)
	}
	job, err := client.GetJob(ctx, strings.Fields(string(output))[0])
	if err != nil {
		t.Fatal(err)
	}
	submitted := job.Spec
	if submitted.AuthoringOrigin != myce.OriginCommand {
		t.Fatalf("command origin missing: %+v", submitted)
	}
	marker := filepath.Join(root, "workers", "fake-1", "job-files", strings.Fields(string(output))[0], "1", "effect")
	if data, err := os.ReadFile(marker); err != nil || string(data) != "done" {
		t.Fatal("real command effect missing")
	}
	output, err = command("--", "sh", "-c", "echo failure >&2; exit 7").CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 || !strings.Contains(string(output), "failure") {
		t.Fatalf("exit propagation: %v %s", err, output)
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
	t.Log("real dev: CPU command effect/logs, no inherited token, exit 7, Ctrl-C cancellation and exact release; owned dev joined and listener closed")

	{
		c := client
		c.HTTP = &http.Client{Timeout: 10 * time.Second}
		cancelKey := func(key string) myce.Cancellation {
			t.Helper()
			ack, err := c.CancelSubmission(ctx, "test", key)
			if err != nil {
				t.Fatal(err)
			}
			if ack.Project != "test" || ack.SubmissionKey == nil || *ack.SubmissionKey != key || !ack.WillNotRun {
				t.Fatalf("bad ack: %+v", ack)
			}
			return ack
		}
		spec := myce.Spec{Name: "clock-test", Project: "test", Priority: "experiment"}
		spec.Run.Command = []string{"echo", "done"}
		spec.Needs.MemoryGB = 8
		// No worker fits, so cancellation and expiry need no process timing assumptions.
		spec.Needs.Platform = myce.Platform{Required: []string{"xpu"}}
		submit := func(key string) (myce.Job, error) {
			s := spec
			s.SubmissionKey = key
			var j myce.Job
			err := c.Call(ctx, "POST", "/api/jobs", s, &j)
			return j, err
		}
		if ack := cancelKey("expires"); ack.State != "absent" || ack.JobID != nil {
			t.Fatal(ack)
		}
		if ack := cancelKey("binds"); ack.State != "absent" {
			t.Fatal(ack)
		}
		bound, err := submit("binds")
		if err != nil || bound.ID == "" || bound.State != "cancelled" || bound.Attempt != 0 {
			t.Fatal(bound, err)
		}
		for _, duration := range []string{"30d", "1s"} {
			command := exec.CommandContext(ctx, binary, "dev", "clock", "advance", duration, "--queue", client.URL, "--token", token)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("advance: %v %s", err, output)
			}
			var reply struct {
				Now time.Time `json:"now"`
			}
			if err := json.Unmarshal(output, &reply); err != nil || reply.Now.IsZero() {
				t.Fatalf("reply: %s %v", output, err)
			}
		}
		retained, err := submit("expires")
		if err != nil || retained.State != "cancelled" || retained.Attempt != 0 {
			t.Fatalf("aged cancellation lost: %+v %v", retained, err)
		}
		if ack := cancelKey("expires"); ack.State != "cancelled" || ack.JobID == nil || *ack.JobID != retained.ID {
			t.Fatal(ack)
		}
		for key, id := range map[string]string{"binds": bound.ID} {
			replay, err := submit(key)
			if err != nil || replay.ID != id || replay.State != "cancelled" || replay.Attempt != 0 {
				t.Fatal(replay, err)
			}
		}
	}
}
