package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mycelium/internal/myce"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The fake manager exposes the same effective lifecycle properties as systemd.
func fakeSetupManager(_ context.Context, name string, args ...string) ([]byte, error) {
	if name == "systemctl" && strings.Contains(strings.Join(args, " "), "--property=TimeoutStopUSec") {
		return []byte("TimeoutStopUSec=45s\nTimeoutStartUSec=1min 30s\n"), nil
	}
	return nil, nil
}

func TestInstallCLICPUWithoutGPU(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		ok   bool
	}{
		{name: "CPU auto detection", ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Glob: func(string) ([]string, error) { return nil, nil }, Command: func(context.Context, string, ...string) ([]byte, error) { return nil, exec.ErrNotFound }}
			args := append([]string{"install", "worker", "--nas", home, "--dry-run", "--environment-cache-bytes", "1048576"}, tc.args...)
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
	client := myce.Client{URL: "http://" + address, Token: "private-test-only"}
	var joiningBind string
	for start := 0; start < 2; start++ {
		if err = myce.Install(context.Background(), c, os.Args[0], "", runner); err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				child.Process.Signal(syscall.SIGTERM)
				if err := child.Wait(); err != nil {
					t.Error("queue shutdown", err)
				}
			}()
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
			c, err = myce.ReadServiceConfig(c.Config)
			if err != nil || c.DiscoveryListen == "" {
				t.Fatal("queue did not save its joining bind", err)
			}
			if start == 0 {
				joiningBind = c.DiscoveryListen
			} else if c.DiscoveryListen != joiningBind {
				t.Fatal("queue reinstall/restart changed its joining port")
			}
			identity, err := myce.LoadJoiningIdentity(c.DB + ".joining")
			if err != nil {
				t.Fatal(err)
			}
			invitation := myce.Invitation{Version: 1, Token: "private-test-only", Fingerprint: identity.Fingerprint()}
			joined, err := myce.ResolveJoining(context.Background(), invitation, "https://"+joiningBind, nil)
			if err != nil {
				t.Fatal("saved HTTPS joining address failed after startup", err)
			}
			joined.HTTP.CloseIdleConnections()
			if start == 0 {
				t.Setenv("MYCE_TOKEN", "private-test-only")
				lingering := "yes\n"
				p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
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
				if err = setupCommandWithAccountLookup([]string{"doctor", "--config", c.Config}, p, home, noAccount); err != nil {
					t.Fatal(err)
				}
				lingering = "no\n"
				if err = setupCommandWithAccountLookup([]string{"doctor", "--config", c.Config}, p, home, noAccount); err == nil || !strings.Contains(err.Error(), "doctor found problems") {
					t.Fatal(err)
				}
			}

		}()
	}

	{
		if got := exitCode(fmt.Errorf("startup refused: %w", myce.ErrMemoryCeilingsRequired)); got != 78 {
			t.Fatal("wrong startup exit", got)
		}
	}

	for _, configFlag := range []bool{true} {
		path := filepath.Join(home, "legacy-config.json")
		data, err := json.Marshal(myce.ServiceConfig{Role: "queue", LocalDisableReceipts: configFlag})
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		args := []string{"--config", path}

		if err = serviceCommand(args); err == nil || !strings.Contains(err.Error(), "reinstall") {
			t.Fatal("installed legacy flag did not explain recovery", err)
		}
	}

}

func TestInstallCLILingerMessage(t *testing.T) {
	for _, state := range []string{"unknown\n"} {
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
