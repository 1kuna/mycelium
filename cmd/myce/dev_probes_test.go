package main

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestDevStandaloneSmoke(t *testing.T) {
	for _, backend := range []string{"fake", "cpu"} {
		t.Run(backend, func(t *testing.T) { devStandaloneSmoke(t, backend) })
	}
}
func devStandaloneSmoke(t *testing.T, backend string) {
	root := t.TempDir()
	binary := os.Getenv("MYCE_DEV_SMOKE_BINARY")
	if binary == "" {
		binary = devTestBinary(t)
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const token = "standalone-smoke-fixture"
	// Neither the child nor its jobs can see ambient MYCE_* values or user config.
	// Fresh services and integration clients need not have /usr/sbin in PATH.
	env := []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TMPDIR=" + root, "MYCE_TOKEN=" + token}
	t.Setenv("MYCE_TOKEN", token)
	cmd := exec.CommandContext(ctx, binary, "dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(root, "state", "queue.db"), "--checkpoints", filepath.Join(root, "checkpoints"), "--host-id", "smoke-host", "--file-store", filepath.Join(root, "files"), "--workers", "1", "--gpu", backend, "--memory-gb", "8")
	cmd.Dir, cmd.Env = root, env
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(root, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var client myce.Client
	// Registered before any assertion: even a failing readiness/effect check must
	// fence and join this exact child. No host service manager is invoked.
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("owned dev exit: %v", err)
			}
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			<-done
			t.Error("smoke teardown exceeded total 30s budget")
		}
		if ctx.Err() != nil {
			t.Error("smoke total deadline expired")
		}
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			t.Error("owned child not joined")
		}
		if client.URL != "" {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(client.URL, "http://"))
			if err == nil {
				conn.Close()
				t.Error("dev listener survived child exit")
			}
		}
	}()
	endpoint := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			parts := strings.Fields(scanner.Text())
			if len(parts) >= 3 && parts[0] == "Mycelium" && parts[1] == "dev:" {
				select {
				case endpoint <- parts[2]:
				default:
				}
			}
		}
	}()
	select {
	case url := <-endpoint:
		client = myce.Client{URL: url, Token: token, HTTP: &http.Client{Timeout: time.Second}}
	case <-ctx.Done():
		t.Fatal("startup deadline")
	}
	waitFor := func(condition func() bool) {
		t.Helper()
		for !condition() {
			if err := waitPause(ctx, 10*time.Millisecond); err != nil {
				t.Fatal("smoke effect deadline", err)
			}
		}
	}
	waitFor(func() bool {
		var s myce.Snapshot
		if client.Call(ctx, "GET", "/api/status", nil, &s) != nil {
			return false
		}
		for _, m := range s.Machines {
			if m.ID == "fake-1" && !m.Gone && !m.Leaving {
				return true
			}
		}
		return false
	})
	unauthorized := client
	unauthorized.Token = "wrong-fixture-token"
	var apiError *myce.HTTPError
	if err := unauthorized.Call(ctx, "GET", "/api/status", nil, nil); !errors.As(err, &apiError) || apiError.Status != 401 {
		t.Fatal("readiness was not authenticated", err)
	}
	spec := myce.Spec{Name: "smoke-short", Project: "probe", Priority: "experiment"}
	spec.Needs.MemoryGB = 1
	if backend == "cpu" {
		spec.Needs.CPUCores = 1
	} else {
		spec.Needs.Platform.Any = true
	}
	spec.Run.Command = []string{"sh", "-c", `test -z "$MYCE_TOKEN" || exit 99; printf done > effect`}
	var job myce.Job
	if err := client.Call(ctx, "POST", "/api/jobs", spec, &job); err != nil {
		t.Fatal(err)
	}
	waitFor(func() bool {
		var s myce.Snapshot
		if err := client.Call(ctx, "GET", "/api/status", nil, &s); err != nil {
			t.Fatal(err)
		}
		for _, j := range s.Jobs {
			if j.ID == job.ID {
				if j.State == "failed" {
					t.Fatal("smoke command failed")
				}
				return j.State == "succeeded" && j.ReleaseConfirmed
			}
		}
		return false
	})
	marker := filepath.Join(root, "state", "workers", "fake-1", "job-files", job.ID, "1", "effect")
	if effect, err := os.ReadFile(marker); err != nil || string(effect) != "done" {
		t.Fatal("short command effect missing", err)
	}
	var maintenance myce.HostMaintenanceStatus
	if err := client.Call(ctx, "POST", "/api/hosts/smoke-host/maintenance", myce.HostMaintenanceRequest{Action: "drain"}, &maintenance); err != nil {
		t.Fatal(err)
	}
	waitFor(func() bool {
		return client.Call(ctx, "GET", "/api/hosts/smoke-host/maintenance", nil, &maintenance) == nil && maintenance.Quiescent
	})
	t.Log("PASS: standalone binary, authenticated API, command effect/exact release, queue maintenance; child and listener joined")
}
