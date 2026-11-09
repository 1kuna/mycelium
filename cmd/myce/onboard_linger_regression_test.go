package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEnrollmentPreflightRefusalLeavesHostUnchanged(t *testing.T) {
	previousVersion := myce.BuildVersion
	myce.BuildVersion = "v1.0.1"
	defer func() { myce.BuildVersion = previousVersion }()
	for _, failure := range []string{"state-root", "service-environment", "session", "manager"} {
		t.Run(failure, func(t *testing.T) {
			home := t.TempDir()
			var operations []string
			lingerEnabled := false
			var mu sync.Mutex
			record := func(value string) { mu.Lock(); defer mu.Unlock(); operations = append(operations, value) }
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record(r.Method + " queue")
				http.Error(w, "stop after preflights", http.StatusServiceUnavailable)
			}))
			defer server.Close()
			c, err := myce.PlanInstall("worker", home, "", home, server.URL, "hypha", myce.Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "fixed", MemoryGB: 8, ReserveGB: 1})
			if err != nil {
				t.Fatal(err)
			}
			c.EnvironmentFile, c.JoiningFile, c.Enrollment = "", filepath.Join(home, "joining.json"), true
			c.EnvironmentCacheBytes = 1 << 20
			c.HostID = "hypha"
			if err = myce.WriteInvitation(c.JoiningFile, myce.Invitation{Version: 1, Address: server.URL, Fingerprint: fmt.Sprintf("%x", sha256.Sum256(server.Certificate().Raw)), Token: "test"}); err != nil {
				t.Fatal(err)
			}
			if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(c.Binary, []byte("#!/bin/sh\necho '{\"version\":\"v1.0.0\"}'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(c)
			if err = os.WriteFile(c.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			if failure == "state-root" {
				if err = os.MkdirAll(c.State, 0700); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(c.State, "legacy-data"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Glob: func(pattern string) ([]string, error) {
				if strings.HasSuffix(pattern, "/class") {
					return []string{"/fixture/0000:01:00.0/class"}, nil
				}
				return nil, nil
			}, ReadFile: func(path string) ([]byte, error) {
				switch filepath.Base(path) {
				case "class":
					return []byte("0x030000"), nil
				case "vendor":
					return []byte("0x10de"), nil
				case "current_link_width":
					return []byte("16"), nil
				}
				return nil, os.ErrNotExist
			}}
			probe.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
				record(name + " " + strings.Join(args, " "))
				switch name {
				case "nvidia-smi":
					if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
						return []byte("GPU-test, fixture-driver, 0000:01:00.0"), nil
					}
					return []byte("GPU, 8192"), nil
				case "cat":
					return []byte("fixture " + home + " nfs rw 0 0\n"), nil
				case "loginctl":
					if failure == "session" {
						return nil, errors.New("missing manager")
					}
					if len(args) > 0 && args[0] == "show-user" {
						if !lingerEnabled {
							return []byte("no"), nil
						}
						return []byte("yes"), nil
					}
					if len(args) > 0 && args[0] == "enable-linger" {
						lingerEnabled = true
					}
					return nil, nil
				case "systemctl":
					if failure == "manager" && strings.Contains(strings.Join(args, " "), "show-environment") {
						return nil, errors.New("missing user manager")
					}
					if strings.Contains(strings.Join(args, " "), "DropInPaths") || strings.Contains(strings.Join(args, " "), "show-environment") {
						return nil, nil
					}
					t.Fatalf("preflight changed service: %v", args)
				case "systemd-run":
					if failure == "service-environment" {
						return nil, errors.New("runtime inaccessible in manager")
					}
					return json.Marshal(myce.HardwarePreflight{Detection: c.Detection, Devices: []myce.DeviceInstall{{Selector: "GPU-test", Detection: c.Detection}}})
				}
				return nil, exec.ErrNotFound
			}
			err = enrollmentCommand([]string{"install", "worker", "--enroll", "--config", c.Config, "--allow-downgrade"}, probe, home, nil)
			if err == nil {
				t.Fatal("failure unexpectedly succeeded")
			}
			wantCode := map[string]string{"state-root": "legacy_state_present", "service-environment": "service_environment_invalid", "session": "lingering_unavailable", "manager": "service_session_unavailable"}[failure]
			if !strings.Contains(err.Error(), wantCode) {
				t.Fatalf("did not reach intended refusal %s: %v", wantCode, err)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, operation := range operations {
				if strings.Contains(operation, "enable-linger") {
					t.Fatalf("persistent service-session mutation before state preflight: %v; result: %v", operations, err)
				}
			}
			queueReached := false
			for _, operation := range operations {
				if strings.HasPrefix(operation, "POST") {
					t.Fatal("preflight failure drained host", operations, err)
				}
				queueReached = queueReached || operation == "GET queue"
			}
			if queueReached {
				t.Fatalf("preflight ordering: operations=%v err=%v", operations, err)
			}
			if _, e := os.Stat(filepath.Join(home, "mycelium")); !os.IsNotExist(e) {
				t.Fatal("preflight left newly created NAS namespace behind", e)
			}
			after, e := os.ReadFile(c.Config)
			if e != nil || string(after) != string(data) {
				t.Fatal("preflight changed installed config", e)
			}
		})
	}

	for _, failure := range []string{"hardware"} {
		t.Run(failure, func(t *testing.T) {
			home := t.TempDir()
			var operations []string
			var mu sync.Mutex
			record := func(value string) { mu.Lock(); defer mu.Unlock(); operations = append(operations, value) }
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record(r.Method + " queue")
				http.Error(w, "stop after preflights", http.StatusServiceUnavailable)
			}))
			defer server.Close()
			c, err := myce.PlanInstall("worker", home, "", home, server.URL, "hypha", myce.Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "fixed", MemoryGB: 8, ReserveGB: 1})
			if err != nil {
				t.Fatal(err)
			}
			c.EnvironmentFile, c.JoiningFile, c.Enrollment = "", filepath.Join(home, "joining.json"), true
			c.EnvironmentCacheBytes = 1 << 20
			c.HostID = "hypha"
			if err = myce.WriteInvitation(c.JoiningFile, myce.Invitation{Version: 1, Address: server.URL, Fingerprint: fmt.Sprintf("%x", sha256.Sum256(server.Certificate().Raw)), Token: "test"}); err != nil {
				t.Fatal(err)
			}
			if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(c.Binary, []byte("#!/bin/sh\necho '{\"version\":\"v1.0.0\"}'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(c)
			if err = os.WriteFile(c.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			if failure == "state-root" {
				if err = os.MkdirAll(c.State, 0700); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(c.State, "legacy-data"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Glob: func(pattern string) ([]string, error) {
				if strings.HasSuffix(pattern, "/class") {
					return []string{"/fixture/0000:01:00.0/class"}, nil
				}
				return nil, nil
			}, ReadFile: func(path string) ([]byte, error) {
				switch filepath.Base(path) {
				case "class":
					return []byte("0x030000"), nil
				case "vendor":
					return []byte("0x10de"), nil
				case "current_link_width":
					return []byte("16"), nil
				}
				return nil, os.ErrNotExist
			}}
			probe.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
				record(name)
				switch name {
				case "nvidia-smi":
					if failure == "hardware" {
						return nil, errors.New("broken runtime")
					}
					if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
						return []byte("GPU-test, fixture-driver, 0000:01:00.0"), nil
					}
					return []byte("GPU, 8192"), nil
				case "cat":
					if failure == "storage" {
						return nil, errors.New("missing mount")
					}
					return []byte("fixture " + home + " nfs rw 0 0\n"), nil
				case "loginctl":
					if failure == "session" {
						return nil, errors.New("missing manager")
					}
					return []byte("yes"), nil
				case "systemctl":
					if strings.Contains(strings.Join(args, " "), "DropInPaths") || strings.Contains(strings.Join(args, " "), "show-environment") {
						return nil, nil
					}
					t.Fatalf("preflight changed service: %v", args)
				case "systemd-run":
					if failure == "service-environment" {
						return nil, errors.New("runtime inaccessible in manager")
					}
					return json.Marshal(myce.HardwarePreflight{Detection: c.Detection, Devices: []myce.DeviceInstall{{Selector: "GPU-test", Detection: c.Detection}}})
				}
				return nil, exec.ErrNotFound
			}
			err = enrollmentCommand([]string{"install", "worker", "--enroll", "--config", c.Config, "--allow-downgrade"}, probe, home, nil)
			if err == nil {
				t.Fatal("failure unexpectedly succeeded")
			}
			mu.Lock()
			defer mu.Unlock()
			queueReached := false
			for _, operation := range operations {
				if strings.HasPrefix(operation, "POST") {
					t.Fatal("preflight failure drained host", operations, err)
				}
				queueReached = queueReached || operation == "GET queue"
			}
			if queueReached != (failure == "none") {
				t.Fatalf("preflight ordering: operations=%v err=%v", operations, err)
			}
			if failure == "none" && !strings.Contains(strings.Join(operations, ","), "systemd-run,GET queue") {
				t.Fatalf("service hardware not checked before drain preparation: %v", operations)
			}
			after, e := os.ReadFile(c.Config)
			if e != nil || string(after) != string(data) {
				t.Fatal("preflight changed installed config", e)
			}
		})
	}

	for _, mode := range []string{"install-failure"} {
		t.Run(mode, func(t *testing.T) {
			home := t.TempDir()
			args := []string{"install", "worker", "--gpu", "fake", "--memory-mode", "fixed", "--memory-gb", "2", "--allow-deviceless", "--nas", home}
			if mode == "storage-refusal" {
				args = []string{"install", "worker", "--gpu", "cpu", "--allow-deviceless", "--nas", home}
				if err := os.Mkdir(filepath.Join(home, "mycelium"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(home, "mycelium", "checkpoints"), []byte("not a directory"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			args = append(args, "--environment-cache-bytes", "1048576")
			if mode == "dry-run" {
				args = append(args, "--dry-run")
			}
			called := false
			var installErr error
			output := captureJoiningOutput(t, func() error {
				installErr = setupCommandWithInstaller(args, myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) {
					return []byte("fixture " + home + " nfs rw 0 0\n"), nil
				}}, home, func(string) (myce.WorkerAccount, error) { return myce.WorkerAccount{}, myce.ErrHostIdentity },
					func(context.Context, myce.ServiceConfig, string, string, func(context.Context, string, ...string) ([]byte, error)) error {
						called = true
						// A marker in the same output stream makes the ordering observable.
						_, _ = os.Stdout.WriteString("INSTALL CALLBACK\n")
						if mode == "install-failure" {
							return errors.New("injected install refusal")
						}
						return nil
					})
				return nil // Capture a deliberate failure too.
			})
			if (installErr != nil) != (mode == "install-failure" || mode == "storage-refusal") {
				t.Fatalf("unexpected result: %v", installErr)
			}
			if mode == "storage-refusal" && (installErr == nil || !strings.Contains(installErr.Error(), "nas_root_invalid")) {
				t.Fatalf("did not reach storage refusal: %v", installErr)
			}
			if called != (mode != "dry-run" && mode != "storage-refusal") {
				t.Fatalf("installer invoked=%v", called)
			}
			success := strings.Index(output, "Worker installed;")
			if mode != "success" && (success >= 0 || strings.Contains(output, "Installed. Run")) {
				t.Fatalf("reported success without install: %s", output)
			}
			if mode == "success" && (success < strings.Index(output, "INSTALL CALLBACK") || success < 0) {
				t.Fatalf("success preceded installation: %s", output)
			}
		})
	}

	t.Run("drain-has-separate-install-budget", func(t *testing.T) {
		previousVersion := myce.BuildVersion
		myce.BuildVersion = "v1.0.1"
		t.Cleanup(func() { myce.BuildVersion = previousVersion })
		clock := &waitClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
		started := clock.Now()
		type phaseDeadline struct {
			at     time.Time
			cancel context.CancelFunc
		}
		var deadlines []phaseDeadline
		setupTimeout := func(parent context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(parent)
			deadlines = append(deadlines, phaseDeadline{clock.Now().Add(15 * time.Second), cancel})
			return ctx, cancel
		}
		wait := func(ctx context.Context, d time.Duration) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			clock.advance(d)
			for _, deadline := range deadlines {
				if !clock.Now().Before(deadline.at) {
					deadline.cancel()
				}
			}
			return nil
		}
		home := t.TempDir()
		cpuBudget := 0
		machine := myce.Machine{ID: "hypha", HostID: "hypha", Session: "session", HostMemoryMeasured: true, HostAvailableMemoryGB: 16,
			CapabilityProfile: &myce.CapabilityProfile{OS: "linux", Arch: "amd64", LogicalCPUs: 8, SystemMemoryBytes: 32 << 30},
			Enrollment:        &myce.EnrollmentProof{Version: myce.BuildVersion, StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol, RunnerSmoke: true}}
		status := myce.HostMaintenanceStatus{Host: "hypha", Healthy: true, Policy: myce.HostMaintenance{Mode: "enabled", UpdatedAt: started, CPUBudgetCores: &cpuBudget, MemoryBudget: &myce.HostMemoryBudget{}}, Workers: []myce.MaintenanceWorker{{Machine: machine, Fresh: true}}}
		var mu sync.Mutex
		var drainedAt time.Time
		var drainedFor time.Duration
		var actions []string
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			if strings.Contains(r.URL.Path, "/enrollment") {
				_ = json.NewEncoder(w).Encode(myce.EnrollmentObservation{Fresh: true, Machine: machine, StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol, Policy: status.Policy})
				return
			}
			if r.Method == "POST" {
				var req myce.HostMaintenanceRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				actions = append(actions, req.Action)
				switch req.Action {
				case "drain":
					status.Policy.Mode, status.Policy.DrainOwner = "drained", req.DrainOwner
					drainedAt = clock.Now()
				case "resume":
					status.Policy.Mode, status.Policy.DrainOwner = "enabled", ""
				}
				status.Policy.UpdatedAt = clock.Now()
			}
			status.Quiescent = status.Policy.Mode == "drained" && clock.Now().Sub(drainedAt) >= 16*time.Second
			if status.Quiescent && drainedFor == 0 {
				drainedFor = clock.Now().Sub(drainedAt)
			}
			_ = json.NewEncoder(w).Encode(status)
		}))
		t.Cleanup(server.Close)
		c, err := myce.PlanInstall("worker", home, "", home, server.URL, "hypha", myce.Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 8})
		if err != nil {
			t.Fatal(err)
		}
		// The old non-enrollment worker has no prior receipt. The injected manager
		// publishes an exact replacement receipt when it activates the new install.
		c.EnvironmentFile, c.JoiningFile, c.HostID, c.AllowDeviceless = "", filepath.Join(home, "joining.json"), "hypha", true
		c.EnvironmentCacheBytes = 1 << 20
		roots, err := c.StateRoots()
		if err != nil {
			t.Fatal(err)
		}
		if err = myce.PrepareStateRoots(roots...); err != nil {
			t.Fatal(err)
		}
		if err = myce.WriteInvitation(c.JoiningFile, myce.Invitation{Version: 1, Address: server.URL, Fingerprint: fmt.Sprintf("%x", sha256.Sum256(server.Certificate().Raw)), Token: "fixture"}); err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(c.Binary, []byte("#!/bin/sh\necho '{\"version\":\"v1.0.0\"}'\n"), 0700); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(c.Config, data, 0600); err != nil {
			t.Fatal(err)
		}
		var installedAt time.Time
		probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if name != "systemctl" {
				return nil, fmt.Errorf("unexpected command %s", name)
			}
			text := strings.Join(args, " ")
			switch {
			case strings.Contains(text, "--property=UnitFileState"):
				return []byte("enabled"), nil
			case strings.Contains(text, "--property=ActiveState,"):
				return []byte("ActiveState=active\nSubState=running\nMainPID=123\nNRestarts=0\nResult=success\n"), nil
			case strings.Contains(text, " enable --now "):
				installedAt = clock.Now()
				receipt := myce.EnrollmentReceipt{ProcessID: 123, Session: machine.Session, Version: myce.BuildVersion, LogicalCPUs: 8, SystemMemoryBytes: 32 << 30}
				data, err := json.Marshal(receipt)
				if err != nil {
					return nil, err
				}
				return nil, os.WriteFile(filepath.Join(c.State, "enrollment.json"), data, 0600)
			default:
				return nil, nil
			}
		}}
		args := []string{"install", "worker", "--enroll", "--config", c.Config, "--allow-downgrade"}
		if err := enrollmentCommandWithTiming(args, probe, home, nil, clock, wait, setupTimeout); err != nil {
			t.Fatalf("upgrade after a 16-second drain must succeed: %v", err)
		}
		if installedAt.Sub(started) != 16*time.Second || len(deadlines) != 3 {
			t.Fatalf("installation did not start with its own budget after drain: at=%s phases=%d", installedAt.Sub(started), len(deadlines))
		}
		mu.Lock()
		defer mu.Unlock()
		if drainedFor != 16*time.Second || strings.Join(actions, ",") != "drain,resume" || status.Policy.Mode != "enabled" {
			t.Fatalf("drain/resume incomplete: waited=%s actions=%v mode=%s", drainedFor, actions, status.Policy.Mode)
		}
		if _, err := os.Stat(filepath.Join(c.State, "enrollment-resume.json")); !os.IsNotExist(err) {
			t.Fatal("completed upgrade retained resume authority", err)
		}
	})
}
