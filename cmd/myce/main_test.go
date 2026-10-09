package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// This test binary also serves as a real checkpointing command in the dev test.
func TestCheckpointCommand(t *testing.T) {
	if os.Getenv("MYCE_TEST_CHILD") != "1" {
		return
	}
	folder := os.Getenv("MYCE_CHECKPOINT_DIR")
	attempt := os.Getenv("MYCE_ATTEMPT")
	path := filepath.Join(folder, "progress")
	data, _ := os.ReadFile(path)
	n, _ := strconv.Atoi(string(data))
	fmt.Printf("start attempt=%s progress=%d\n", attempt, n)
	if os.Getenv("MYCE_TOKEN") != "" {
		t.Fatal("fleet token inherited by command")
	}
	save := func() {
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, []byte(strconv.Itoa(n)), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
	}
	if attempt != "1" {
		if n == 0 {
			t.Fatal("resumed from zero")
		}
		fmt.Printf("resumed progress=%d\n", n)
		n++
		save()
		fmt.Println("checkpoint job finished")
		return
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	defer signal.Stop(sig)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			n++
			save()
			if n == 1 {
				fmt.Println("checkpoint ready")
			}
		case <-sig:
			save()
			fmt.Printf("saved on stop progress=%d\n", n)
			return
		}
	}
}

func TestDevCheckpointEndToEnd(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "myce")
	build := exec.Command("go", "build", "-race", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	cmd := exec.Command(binary, "dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(dir, "dev.db"), "--checkpoints", filepath.Join(dir, "checkpoints"), "--workers", "1", "--memory-gb", "8")
	cmd.Env = append(os.Environ(), "MYCE_TOKEN=e2e-token")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrPath := filepath.Join(dir, "dev.stderr")
	stderr, err := os.Create(stderrPath)
	if err != nil {
		t.Fatal(err)
	}
	defer stderr.Close()
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var c myce.Client
	t.Cleanup(func() {
		if c.URL != "" {
			var s myce.Snapshot
			if c.Call(context.Background(), "GET", "/api/status", nil, &s) == nil {
				for _, j := range s.Jobs {
					_ = c.Call(context.Background(), "POST", "/api/jobs/"+j.ID+"/cancel", nil, nil)
				}
			}
		}
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil {
				data, _ := os.ReadFile(stderrPath)
				t.Errorf("dev exited: %v\n%s", err, data)
			}
		case <-time.After(8 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("dev did not shut down gracefully")
		}
	})
	line := make(chan string, 1)
	go func() {
		s := bufio.NewScanner(stdout)
		if s.Scan() {
			line <- s.Text()
		} else {
			line <- ""
		}
	}()
	select {
	case v := <-line:
		parts := strings.Fields(v)
		if len(parts) < 3 {
			t.Fatalf("dev startup: %q", v)
		}
		c = myce.Client{URL: parts[2], Token: "e2e-token"}
	case <-time.After(10 * time.Second):
		t.Fatal("dev did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	s := myce.Spec{Name: "checkpoint", Project: "test", Priority: "experiment", Resumable: true}
	s.Run.Command = []string{helper, "-test.run=^TestCheckpointCommand$"}
	s.Run.Env = map[string]string{"MYCE_TEST_CHILD": "1"}
	s.Needs.MemoryGB = 8
	s.Needs.Platform.Any = true
	var experiment myce.Job
	if err := c.Call(ctx, "POST", "/api/jobs", s, &experiment); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		logs := readLogs(t, ctx, c, experiment.ID)
		return strings.Contains(logs, "checkpoint ready")
	})
	production := s
	production.Name = "production"
	production.Priority = "production"
	production.Resumable = false
	production.Run.Command = []string{"sh", "-c", "printf 'production finished\\n'"}
	production.Run.Env = nil
	var prod myce.Job
	if err := c.Call(ctx, "POST", "/api/jobs", production, &prod); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var snap myce.Snapshot
		if err := c.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
			t.Fatal(err)
		}
		both := 0
		for _, j := range snap.Jobs {
			if j.ID == experiment.ID || j.ID == prod.ID {
				if j.State == "failed" {
					t.Fatalf("job failed: %+v", j)
				}
				if j.State == "succeeded" {
					both++
				}
				if j.ID == experiment.ID && j.State == "succeeded" && j.Attempt != 2 {
					t.Fatalf("attempt=%d", j.Attempt)
				}
			}
		}
		return both == 2
	})
	logs := readLogs(t, ctx, c, experiment.ID)
	for _, want := range []string{"saved on stop progress=", "start attempt=2 progress=", "resumed progress=", "checkpoint job finished"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("missing %q in logs:\n%s", want, logs)
		}
	}
	if !strings.Contains(readLogs(t, ctx, c, prod.ID), "production finished") {
		t.Fatal("production did not run")
	}
	data, err := os.ReadFile(filepath.Join(experiment.Checkpoint, "attempt-2", "progress"))
	if err != nil {
		t.Fatal(err)
	}
	progress, _ := strconv.Atoi(string(data))
	if progress < 2 {
		t.Fatalf("checkpoint=%s", data)
	}
	t.Logf("real myce dev: production completed; experiment stopped, saved, resumed on attempt 2 from checkpoint %d", progress)
}
func wait(t *testing.T, ctx context.Context, f func() bool) {
	t.Helper()
	for {
		if f() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for dev job")
		case <-time.After(30 * time.Millisecond):
		}
	}
}
func readLogs(t *testing.T, ctx context.Context, c myce.Client, id string) string {
	t.Helper()
	var ls []myce.Log
	if err := c.Call(ctx, "GET", "/api/jobs/"+id+"/logs", nil, &ls); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, l := range ls {
		b.WriteString(l.Text)
	}
	return b.String()
}

func TestDevModelEndToEnd(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "myce")
	if out, e := exec.Command("go", "build", "-race", "-o", binary, ".").CombinedOutput(); e != nil {
		t.Fatalf("build: %v %s", e, out)
	}
	cmd := exec.Command(binary, "dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(dir, "db"), "--checkpoints", filepath.Join(dir, "checkpoints"), "--workers", "1", "--model-idle", "1s")
	cmd.Env = append(os.Environ(), "MYCE_TOKEN=model-test-token")
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case e := <-done:
			if e != nil {
				t.Errorf("dev: %v %s", e, stderr.String())
			}
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("dev shutdown timeout")
		}
	})
	lines := make(chan string, 1)
	go func() { s := bufio.NewScanner(stdout); s.Scan(); lines <- s.Text() }()
	var url string
	select {
	case line := <-lines:
		parts := strings.Fields(line)
		if len(parts) < 3 {
			t.Fatalf("startup %q", line)
		}
		url = parts[2]
	case <-time.After(10 * time.Second):
		t.Fatal("dev startup timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := myce.Client{URL: url, Token: "model-test-token"}
	call := func() {
		req, _ := http.NewRequestWithContext(ctx, "POST", url+"/v1/chat/completions", strings.NewReader(`{"model":"dev-model","messages":[{"role":"user","content":"hello"}]}`))
		req.Header.Set("Authorization", "Bearer model-test-token")
		req.Header.Set("X-Myce-Project", "test")
		req.Header.Set("X-Myce-Priority", "production")
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		data, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 || !strings.Contains(string(data), "fake answer") {
			t.Fatalf("call: %d %s", res.StatusCode, data)
		}
	}
	var s myce.Snapshot
	if e = c.Call(ctx, "GET", "/api/status", nil, &s); e != nil {
		t.Fatal(e)
	}
	if len(s.LoadedModels) != 0 {
		t.Fatal("model was already loaded")
	}
	call()
	if e = c.Call(ctx, "GET", "/api/status", nil, &s); e != nil {
		t.Fatal(e)
	}
	if len(s.LoadedModels) != 1 || s.LoadedModels[0].State != "ready" {
		t.Fatalf("not loaded: %+v", s.LoadedModels)
	}
	wait(t, ctx, func() bool {
		if e = c.Call(ctx, "GET", "/api/status", nil, &s); e != nil {
			t.Fatal(e)
		}
		return len(s.LoadedModels) == 0
	})
	call()
	if e = c.Call(ctx, "GET", "/api/status", nil, &s); e != nil {
		t.Fatal(e)
	}
	if len(s.LoadedModels) != 1 || s.LoadedModels[0].State != "ready" {
		t.Fatal("second call did not reload model")
	}
	t.Log("real myce dev: unloaded model loaded on first OpenAI call, answered, unloaded after idle, and loaded again for second call")
}
