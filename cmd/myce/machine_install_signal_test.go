package main

import (
	"context"
	"encoding/json"
	"errors"
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

type m4SignalExit int

func (e m4SignalExit) Error() string { return "synthetic service inactive" }
func (e m4SignalExit) ExitCode() int { return int(e) }

func TestM4InstallSignalJournal(t *testing.T) {
	if root := os.Getenv("MYCE_R2_SIGNAL_CHILD"); root != "" {
		probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			call := strings.Join(args, " ")
			if strings.Contains(call, "disable --now") {
				if err := os.WriteFile(filepath.Join(root, "blocked"), nil, 0600); err != nil {
					return nil, err
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			if len(args) > 1 && args[1] == "is-active" {
				return nil, m4SignalExit(3)
			}
			if strings.Contains(call, "--property=UnitFileState") {
				if _, err := os.Stat(filepath.Join(root, "units", args[2])); err == nil {
					return []byte("disabled"), nil
				}
				return nil, nil
			}
			return nil, nil
		}}
		err := setupCommandWithProbe([]string{"install", "worker", "--config", filepath.Join(root, "machine", "config.json"), "--from-worker-config", filepath.Join(root, "gpu0", "config.json")}, probe, root)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("signal did not cancel setup: %v", err)
		}
		if err := os.WriteFile(filepath.Join(root, "cancelled"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, sig := range []syscall.Signal{syscall.SIGHUP, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			root := t.TempDir()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/inspection" {
					json.NewEncoder(w).Encode(myce.Snapshot{Machines: []*myce.Machine{{ID: "host-gpu0", HostID: "host", Gone: true, Replacement: &myce.MachineReplacement{Machine: "host", Device: "0"}}}})
					return
				}
				json.NewEncoder(w).Encode(myce.HostMaintenanceStatus{Policy: myce.HostMaintenance{Mode: "disabled"}, Healthy: true, Quiescent: true})
			}))
			defer server.Close()
			c, err := myce.PlanInstall("worker", root, root, root, server.URL, "host-gpu0", myce.Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 8})
			if err != nil {
				t.Fatal(err)
			}
			c.Config = filepath.Join(root, "gpu0", "config.json")
			c.Service = filepath.Join(root, "units", "myce-worker@gpu0.service")
			c.Device = "0"
			c.HostID = "host"
			c.EnvironmentFile = filepath.Join(root, "synthetic-secret-reference")
			for _, p := range []string{filepath.Dir(c.Config), filepath.Dir(c.Service)} {
				if err = os.MkdirAll(p, 0700); err != nil {
					t.Fatal(err)
				}
			}
			data, _ := json.Marshal(c)
			if err = os.WriteFile(c.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(c.Service, []byte("synthetic unit"), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestM4InstallSignalJournal$", "-test.count=1")
			cmd.Env = append(os.Environ(), "MYCE_R2_SIGNAL_CHILD="+root, "MYCE_TOKEN=synthetic-local-test")
			var output strings.Builder
			cmd.Stdout = &output
			cmd.Stderr = &output
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				if _, err = os.Stat(filepath.Join(root, "blocked")); err == nil {
					break
				}
				select {
				case err = <-done:
					t.Fatalf("child before prepared journal: %v %s", err, output.String())
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-ticker.C:
				}
			}
			if err = cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			if err = <-done; err != nil {
				t.Fatalf("setup was killed instead of cancelling: %v %s", err, output.String())
			}
			journal, err := os.ReadFile(filepath.Join(root, "machine", "config.json.m4-rollback.json"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(journal), `"Phase": "rolled-back"`) {
				t.Fatal("journal not recoverable")
			}
			if _, err = os.Stat(filepath.Join(root, "cancelled")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
