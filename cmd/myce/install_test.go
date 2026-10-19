package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestInstallCLIDryRunFakeDetection(t *testing.T) {
	for _, platform := range []string{"cuda", "xpu", "metal"} {
		t.Run(platform, func(t *testing.T) {
			home := t.TempDir()
			p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, n string, args ...string) ([]byte, error) {
				switch n {
				case "nvidia-smi":
					if platform == "cuda" {
						return []byte("RTX 4090, 24576"), nil
					}
				case "sycl-ls":
					if platform == "xpu" {
						return []byte("[level_zero:gpu] Intel UHD Integrated GPU"), nil
					}
				case "/usr/sbin/system_profiler":
					if platform == "metal" {
						return []byte("Metal Support: Metal 3"), nil
					}
				}
				if n == "systemctl" || n == "launchctl" {
					t.Fatal("dry run touched service")
				}
				return nil, errors.New("absent")
			}}
			if platform == "metal" {
				p.OS = "darwin"
				p.Arch = "arm64"
			}
			if e := setupCommandWithProbe([]string{"install", "worker", "--nas", filepath.Join(home, "nas"), "--dry-run"}, p, home); e != nil {
				t.Fatal(e)
			}
			entries, _ := os.ReadDir(home)
			if len(entries) != 0 {
				t.Fatal("dry run wrote files")
			}
		})
	}
	home := t.TempDir()
	if e := setupCommandWithProbe([]string{"install", "queue", "--nas", filepath.Join(home, "nas"), "--dry-run"}, myce.MachineProbe{OS: "linux", Arch: "arm64"}, home); e != nil {
		t.Fatal(e)
	}
}

func TestInstallCLIStoresAbsoluteCatalogPaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("MYCE_TOKEN", "test-only-token")
	fleet := filepath.Join(home, ".config", "myce", "fleet.env")
	os.MkdirAll(filepath.Dir(fleet), 0700)
	os.WriteFile(fleet, []byte("MYCE_TOKEN=test-only-token\nMYCE_QUEUE=http://queue:7331\n"), 0600)
	p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) { return nil, nil }}
	if err := setupCommandWithProbe([]string{"install", "queue", "--nas", home, "--require-mounted-nas=false", "--model-config", "models.yaml", "--environment-config", "environments/catalog.yaml"}, p, home); err != nil {
		t.Fatal(err)
	}
	c, err := myce.ReadServiceConfig(filepath.Join(home, ".local", "share", "mycelium", "queue", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	models, _ := filepath.Abs("models.yaml")
	environments, _ := filepath.Abs("environments/catalog.yaml")
	if c.ModelCatalog != models || c.EnvironmentCatalog != environments || c.EnvironmentRoot != filepath.Dir(environments) {
		t.Fatalf("relative service paths: %+v", c)
	}
}

func TestDoctorCLIUsesInjectedRunner(t *testing.T) {
	home := t.TempDir()
	t.Setenv("MYCE_TOKEN", "test-only-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/status" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := myce.ServiceConfig{Role: "queue", Detection: myce.Detection{OS: "linux"}, Queue: server.URL, NAS: home, State: home}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(home, "config.json")
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	lingering := "yes\n"
	calls := map[string]int{}
	p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		calls[name]++
		switch name {
		case "mount":
			return []byte("server:/nas on " + home + " type nfs (rw)\n"), nil
		case "uv":
			return []byte("uv test-version"), nil
		case "loginctl":
			return []byte(lingering), nil
		default:
			t.Fatalf("unexpected live command %s %v", name, args)
			return nil, errors.New("unexpected command")
		}
	}}
	noAccount := func(string) (myce.WorkerAccount, error) {
		t.Fatal("doctor looked up worker account")
		return myce.WorkerAccount{}, myce.ErrHostIdentity
	}
	if err = setupCommandWithAccountLookup([]string{"doctor", "--config", path}, p, home, noAccount); err != nil {
		t.Fatal(err)
	}
	lingering = "no\n"
	if err = setupCommandWithAccountLookup([]string{"doctor", "--config", path}, p, home, noAccount); err == nil || !strings.Contains(err.Error(), "doctor found problems") {
		t.Fatal(err)
	}
	for _, name := range []string{"mount", "uv", "loginctl"} {
		if calls[name] != 2 {
			t.Fatalf("%s calls=%d", name, calls[name])
		}
	}
}

func TestSetupCLIRejectsMissingOrUnknownAction(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}} {
		if err := setupCommandWithProbe(args, myce.MachineProbe{}, t.TempDir()); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestInstallCLIDevicelessNeedsExplicitFlagAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		ok   bool
	}{
		{name: "no opt out"},
		{name: "no budget", args: []string{"--allow-deviceless"}},
		{name: "CPU test budget", args: []string{"--allow-deviceless", "--memory-gb", "2"}, ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("no GPU") }}
			args := append([]string{"install", "worker", "--nas", home, "--dry-run"}, tc.args...)
			err := setupCommandWithProbe(args, probe, home)
			if (err == nil) != tc.ok {
				t.Fatal(err)
			}
			entries, _ := os.ReadDir(home)
			if len(entries) != 0 {
				t.Fatal("dry run wrote files")
			}
		})
	}
}

func TestInstallQueueDeploymentFlags(t *testing.T) {
	home := t.TempDir()
	out, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = out
	defer func() { os.Stdout = saved; out.Close() }()
	args := []string{"install", "queue", "--nas", home, "--dry-run", "--listen", "192.0.2.1:7331", "--db", home + "/db.sqlite", "--project-config", home + "/projects.yaml", "--host-config", home + "/hosts.yaml", "--log-retention", "24h", "--upload-spool-dir", home + "/uploads", "--upload-spool-bytes", "12345", "--restart-grace", "2m", "--production-infrastructure-retries", "4", "--production-loss-timeout", "6s", "--deadline-loss-timeout", "21s", "--experiment-loss-timeout", "61s", "--checkpoints", home + "/checkpoints", "--environment-file", home + "/fleet.env", "--private-devices=false", "--no-new-privileges=false", "--nice", "12", "--cpu-quota", "30%", "--memory-max", "600M", "--tasks-max", "70"}
	if err := setupCommandWithProbe(args, myce.MachineProbe{OS: "linux", Arch: "amd64"}, home); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out.Name())
	for _, want := range []string{"--listen", "192.0.2.1:7331", "--db", home + "/db.sqlite", "--project-config", home + "/projects.yaml", "--host-config", home + "/hosts.yaml", "--log-retention", "24h0m0s", "--upload-spool-dir", home + "/uploads", "--upload-spool-bytes", "12345", "--restart-grace", "2m0s", "--production-infrastructure-retries", "4", "--production-loss-timeout", "6s", "--deadline-loss-timeout", "21s", "--experiment-loss-timeout", "1m1s", "--checkpoints", home + "/checkpoints", "EnvironmentFile=", home + "/fleet.env", "PrivateDevices=no", "NoNewPrivileges=no", "Nice=12", "CPUQuota=30%", "MemoryMax=600M", "TasksMax=70"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %q in dry-run unit", want)
		}
	}
	entries, _ := os.ReadDir(home)
	if len(entries) != 0 {
		t.Fatal("dry run wrote files")
	}
}

func TestInstallWorkerDeploymentFlags(t *testing.T) {
	home := t.TempDir()
	out, _ := os.CreateTemp(t.TempDir(), "stdout")
	saved := os.Stdout
	os.Stdout = out
	defer func() { os.Stdout = saved; out.Close() }()
	args := []string{"install", "worker", "--nas", home, "--dry-run", "--name", "b70-gpu1", "--host-id", "b70", "--instance", "gpu1", "--device", "1", "--gpu", "xpu", "--memory-mode", "fixed", "--memory-gb", "16", "--reserve-memory-gb", "2", "--reserve-host-memory-gb", "4", "--host-memory-budget-gb", "5", "--memory-ceiling-slack-gb", "0", "--memory-swap-max-bytes", "0", "--state-dir", home + "/spool", "--host-state-dir", home + "/control", "--checkpoint-root", home + "/cp", "--model-root", home + "/models", "--environment-root", home + "/envs", "--model-cache", home + "/mc", "--model-cache-bytes", "1111", "--environment-cache", home + "/ec", "--environment-cache-bytes", "2222", "--heartbeat", "2s", "--log-retention", "24h", "--start-disabled", "--require-memory-ceilings", "--require-mounted-nas=false"}
	if err := setupCommandWithProbe(args, myce.MachineProbe{OS: "linux", Arch: "amd64"}, home); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out.Name())
	for _, want := range []string{"b70-gpu1", "--host-id", `"b70"`, "myce-worker@gpu1.service", "--device", `"1"`, "--gpu", `"xpu"`, "--memory-mode", `"fixed"`, "--memory-gb", `"16"`, "--reserve-memory-gb", `"2"`, "--reserve-host-memory-gb", `"4"`, "--host-memory-budget-gb", `"5"`, "--memory-ceiling-slack-gb", `"0"`, "--memory-swap-max-bytes", "--state-dir", home + "/spool", "--host-state-dir", home + "/control", "--checkpoint-root", home + "/cp", "--model-root", home + "/models", "--environment-root", home + "/envs", "--model-cache", home + "/mc", "--model-cache-bytes", "1111", "--environment-cache", home + "/ec", "--environment-cache-bytes", "2222", "--heartbeat", "2s", "--log-retention", "24h0m0s", "--require-memory-ceilings", "RestartPreventExitStatus=78", "closed"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestRequireMemoryCeilingsExitPreventsRestart(t *testing.T) {
	if got := exitCode(fmt.Errorf("startup refused: %w", myce.ErrMemoryCeilingsRequired)); got != 78 {
		t.Fatal("wrong startup exit", got)
	}
}

// A subprocess exercises the installed service entry point without an OS service manager.
func TestInstalledQueueServiceProcess(t *testing.T) {
	if path := os.Getenv("MYCE_TEST_INSTALLED_SERVICE"); path != "" {
		if err := serviceCommand([]string{"--config", path}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInstallQueueStartsAndReportsLocally(t *testing.T) {
	home := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	ln.Close()
	c, err := myce.PlanInstall("queue", home, "", home, "http://"+address, "queue", myce.Detection{OS: "linux", Arch: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	c.Listen = address
	c.EnvironmentFile = filepath.Join(home, "fleet.env")
	os.WriteFile(c.EnvironmentFile, []byte("MYCE_TOKEN=private-test-only\nMYCE_QUEUE=http://"+address+"\n"), 0600)
	var child *exec.Cmd
	runner := func(_ context.Context, n string, a ...string) ([]byte, error) {
		if strings.Join(a, " ") == "--user enable --now myce-queue.service" {
			child = exec.Command(c.Binary, "-test.run=^TestInstalledQueueServiceProcess$")
			child.Env = append(os.Environ(), "MYCE_TEST_INSTALLED_SERVICE="+c.Config, "INVOCATION_ID=")
			return nil, child.Start()
		}
		return nil, nil
	}
	if err = myce.Install(context.Background(), c, os.Args[0], "", runner); err != nil {
		t.Fatal(err)
	}
	defer func() {
		child.Process.Signal(syscall.SIGTERM)
		if err := child.Wait(); err != nil {
			t.Error("queue shutdown", err)
		}
	}()
	client := myce.Client{URL: "http://" + address, Token: "private-test-only"}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var snapshot myce.Snapshot
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err = client.Call(ctx, "GET", "/api/status", nil, &snapshot)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("installed queue did not report", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestInstalledDisabledServiceDoesNotReadCredentialsOrJoin(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "control")
	c, _ := myce.PlanInstall("worker", home, "", home, "http://unused:7331", "card", myce.Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
	c.HostStateDir = dir
	c.EnvironmentFile = filepath.Join(home, "missing-fleet.env")
	scoped := dir
	os.MkdirAll(scoped, 0700)
	if _, e := myce.InitializeHostIntent(scoped, c.HostID, myce.RealClock{}); e != nil {
		t.Fatal(e)
	}
	os.MkdirAll(filepath.Dir(c.Config), 0700)
	b, _ := json.Marshal(c)
	os.WriteFile(c.Config, b, 0600)
	t.Setenv("INVOCATION_ID", "")
	if err := serviceCommand([]string{"--config", c.Config}); err != nil {
		t.Fatal("disabled service tried to start", err)
	}
}

func TestInstallWorkerOverridesDetectedReserve(t *testing.T) {
	home := t.TempDir()
	out, _ := os.CreateTemp(t.TempDir(), "stdout")
	saved := os.Stdout
	os.Stdout = out
	defer func() { os.Stdout = saved; out.Close() }()
	probe := myce.MachineProbe{OS: "linux", Arch: "arm64", Command: func(context.Context, string, ...string) ([]byte, error) { return []byte("NVIDIA GB10, [N/A]"), nil }}
	if err := setupCommandWithProbe([]string{"install", "worker", "--nas", home, "--reserve-memory-gb", "24", "--dry-run"}, probe, home); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out.Name())
	if !strings.Contains(string(b), `"--reserve-memory-gb" "24"`) {
		t.Fatal("explicit reserve ignored after detection")
	}
}

func TestInstallCLILingerMessage(t *testing.T) {
	for _, state := range []string{"yes\n", "no\n", "unknown\n"} {
		t.Run(strings.TrimSpace(state), func(t *testing.T) {
			out, err := os.CreateTemp(t.TempDir(), "stdout")
			if err != nil {
				t.Fatal(err)
			}
			saved := os.Stdout
			os.Stdout = out
			defer func() { os.Stdout = saved; out.Close() }()
			calls := 0
			p := myce.MachineProbe{OS: "linux", Arch: "arm64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != "loginctl" || strings.Join(args, " ") != fmt.Sprintf("show-user %d --property=Linger --value", os.Getuid()) {
					t.Fatalf("unexpected command %s %v", name, args)
				}
				calls++
				return []byte(state), nil
			}}
			if err := setupCommandWithProbe([]string{"install", "queue", "--nas", "/tmp/nas", "--dry-run"}, p, t.TempDir()); err != nil {
				t.Fatal(err)
			}
			b, _ := os.ReadFile(out.Name())
			if calls != 1 || strings.Contains(string(b), "must run loginctl enable-linger") != (state == "no\n") {
				t.Fatalf("incorrect linger advice: calls=%d %s", calls, b)
			}
			if state == "unknown\n" && !strings.Contains(string(b), "cannot inspect user lingering") {
				t.Fatal("unknown state hidden")
			}
		})
	}
}

func TestInstallQueueLocalDisableReceiptsFlags(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	for _, tc := range []struct {
		name    string
		flags   []string
		wantErr bool
	}{
		{"default", nil, false},
		{"missing-host", []string{"--local-disable-receipts", "--host-state-dir", "host-state"}, true},
		{"missing-directory", []string{"--local-disable-receipts", "--host-id", "local-host"}, true},
		{"explicit", []string{"--local-disable-receipts", "--host-id", "local-host", "--host-state-dir", "host-state"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			args := append([]string{"install", "queue", "--nas", home, "--dry-run"}, tc.flags...)
			err := setupCommandWithProbe(args, myce.MachineProbe{OS: "linux", Arch: "arm64"}, home)
			if (err != nil) != tc.wantErr {
				t.Fatalf("flags: %v", err)
			}
			entries, err := os.ReadDir(home)
			if err != nil || len(entries) != 0 {
				t.Fatal("dry run wrote files", err)
			}
		})
	}
}

func TestQueueLocalDisableReceiptsOptionRemoved(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	for _, flags := range [][]string{
		{"--local-disable-receipts"},
		{"--local-disable-receipts", "--host-id", "local-host"},
		{"--local-disable-receipts", "--host-state-dir", "host-state"},
	} {
		err := runWithProbe(append([]string{"queue"}, flags...), myce.MachineProbe{OS: "linux", Arch: "arm64"})
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined: -local-disable-receipts") {
			t.Fatal("removed delivery-mode flag accepted", err)
		}
	}
}

func TestLegacyReceiptInstalledServiceRejected(t *testing.T) {
	for _, configFlag := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "config.json")
		data, err := json.Marshal(myce.ServiceConfig{Role: "queue", LocalDisableReceipts: configFlag})
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		args := []string{"--config", path}
		if !configFlag {
			args = append(args, "--", "queue", "--local-disable-receipts")
		}
		if err = serviceCommand(args); err == nil || !strings.Contains(err.Error(), "reinstall") {
			t.Fatal("installed legacy flag did not explain recovery", err)
		}
	}
}

func TestInstallWithoutPasswdEntry(t *testing.T) {
	for _, role := range []string{"queue", "worker"} {
		t.Run(role, func(t *testing.T) {
			home := t.TempDir()
			calls := 0
			lookup := func(string) (myce.WorkerAccount, error) {
				calls++
				if role != "worker" {
					t.Fatal("queue looked up worker account")
				}
				return myce.WorkerAccount{Name: "discard-on-error", Home: home}, myce.ErrHostIdentity
			}
			args := []string{"install", role, "--nas", home, "--dry-run"}
			if role == "worker" {
				args = append(args, "--gpu", "fake", "--memory-mode", "fixed", "--memory-gb", "2")
			}
			captureJoiningOutput(t, func() error {
				return setupCommandWithAccountLookup(args, myce.MachineProbe{OS: "linux", Arch: "amd64"}, home, lookup)
			})
			if (role == "worker" && calls != 1) || (role == "queue" && calls != 0) {
				t.Fatal(calls)
			}
		})
	}
}

func TestUninstallWithoutPasswdEntry(t *testing.T) {
	home := t.TempDir()
	config := filepath.Join(home, "config.json")
	b, err := json.Marshal(myce.ServiceConfig{Role: "queue", Config: config, Service: filepath.Join(home, "myce-queue.service"), Detection: myce.Detection{OS: "linux"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(config, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = setupCommandWithAccountLookup([]string{"uninstall", "--config", config, "--dry-run"}, myce.MachineProbe{}, home, func(string) (myce.WorkerAccount, error) {
		t.Fatal("uninstall looked up worker account")
		return myce.WorkerAccount{}, myce.ErrHostIdentity
	}); err != nil {
		t.Fatal(err)
	}
}
