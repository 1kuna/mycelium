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

func TestDevProbeInstallUpgrade(t *testing.T) {
	// The composed fixtures include synchronous install calls and subprocesses
	// without contexts. Isolate them so one deadline bounds the entire probe,
	// including fixture cleanup, without changing the production installer.
	const childEnv = "MYCE_TEST_INSTALL_PROBE_CHILD"
	if os.Getenv(childEnv) != "1" {
		const bound = 60 * time.Second
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDevProbeInstallUpgrade$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		cmd.WaitDelay = time.Second
		out, err := cmd.CombinedOutput()
		elapsed := time.Since(started)
		t.Logf("install probe child (%s):\n%s", elapsed, out)
		if ctx.Err() != nil || elapsed >= bound {
			t.Fatalf("install probe exceeded %s bound: elapsed=%s, deadline=%v", bound, elapsed, ctx.Err())
		}
		if err != nil {
			t.Fatal("install probe child:", err)
		}
		return
	}
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"configuration-failure", TestInstallCLIDevicelessNeedsExplicitFlagAndBudget},
		{"preserve-config", TestInstallPreservesEveryConfigField},
		{"archive-refuse-fresh-closed", TestLegacyInstallArchiveFresh},
	} {
		if !t.Run(tc.name, tc.run) {
			return
		}
	}
	t.Log("PASS: invalid config refused, upgrade preserves config, legacy refusal read-only, archived fresh install CLOSED")
}

func TestDevProbeRestVerdicts(t *testing.T) {
	if !t.Run("real-dev-ready-active-closed-fenced", TestHostRestDevExpectations) {
		return
	}
	t.Log("PASS: real dev API/CLI READY with active work, CLOSED with fenced queue, reopen and final CLOSED")
}

func TestDevStandaloneSmoke(t *testing.T) {
	root := t.TempDir()
	binary := os.Getenv("MYCE_DEV_SMOKE_BINARY")
	if binary == "" {
		binary = filepath.Join(root, "myce")
		if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
			t.Fatalf("build smoke binary: %v %s", err, out)
		}
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	host := filepath.Join(root, "host")
	const token = "standalone-smoke-fixture"
	// Neither the child nor its jobs can see ambient MYCE_* values or user config.
	// Fresh services and integration clients need not have /usr/sbin in PATH.
	env := []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TMPDIR=" + root, "MYCE_TOKEN=" + token}
	t.Setenv("MYCE_TOKEN", token) // HostControl's local queue reconciliation only.
	cmd := exec.CommandContext(ctx, binary, "dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(root, "state", "queue.db"), "--checkpoints", filepath.Join(root, "checkpoints"), "--host-state-dir", host, "--host-id", "smoke-host", "--file-store", filepath.Join(root, "files"), "--workers", "1", "--memory-gb", "8")
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
		if client.URL != "" {
			report, err := (myce.HostControl{Dir: host, Clock: myce.RealClock{}, Service: envelopeService{}, Observe: envelopeHardware}).Disable(ctx, "smoke finally", 0)
			if err != nil || !report.Confirmed {
				t.Errorf("finally close: %v, confirmed=%v", err, report.Confirmed)
			}
		}
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
	marker := filepath.Join(root, "effect")
	spec := myce.Spec{Name: "smoke-short", Project: "probe", Priority: "experiment"}
	spec.Needs.MemoryGB, spec.Needs.Platform.Any = 1, true
	spec.Run.Command = []string{"sh", "-c", `test -z "$MYCE_TOKEN" || exit 99; printf done > "$1"`, "fixture", marker}
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
	if effect, err := os.ReadFile(marker); err != nil || string(effect) != "done" {
		t.Fatal("short command effect missing", err)
	}
	report, err := (myce.HostControl{Dir: host, Clock: myce.RealClock{}, Service: envelopeService{}, Observe: envelopeHardware}).Disable(ctx, "smoke close", 0)
	if err != nil || !report.Confirmed {
		t.Fatal("close", report, err)
	}
	waitFor(func() bool {
		state, err := myce.ReadHostIntent(host)
		return err == nil && state.Desired == "closed" && state.HostSettlement() == "confirmed"
	})
	t.Log("PASS: exact standalone binary, isolated HOME/state/env, authenticated API, short command effect/release, CLOSED/settled; owned child/listener joined by teardown")
}
