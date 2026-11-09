package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mycelium/internal/myce"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveNativeCLIRequiresExactCustody(t *testing.T) {
	if os.Getenv("MYCE_R2_SIGNAL_CHILD") != "" {
		testM4InstallSignalJournal(t)
		return
	}
	for _, args := range [][]string{nil, {"--state-dir", t.TempDir()}, {"--state-dir", t.TempDir(), "--attempt-id", "exact", "--timeout", "0s"}, {"--state-dir", t.TempDir(), "--attempt-id", "../escape"}} {
		if err := resolveNativeCommand(args); err == nil {
			t.Fatal("accepted missing or invalid custody", args)
		}
	}
	if err := run([]string{"resolve-native", "--confirm-gone"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatal("operator assertion bypass accepted", err)
	}

	t.Run("install-sigint-journal", testM4InstallSignalJournal)

	t.Run("verified-empty-evidence", func(t *testing.T) {
		root := t.TempDir()
		c, err := myce.PlanInstall("queue", root, root, root, "http://queue.invalid", "queue", myce.Detection{OS: "linux", Arch: "amd64"})
		if err != nil {
			t.Fatal(err)
		}
		q, err := myce.Open(c.DB, c.Checkpoints, myce.RealClock{})
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(c)
		if err := os.WriteFile(c.Config, data, 0600); err != nil {
			t.Fatal(err)
		}
		mapping := filepath.Join(root, "map.json")
		if err := os.WriteFile(mapping, []byte(`[{"from":"legacy","to":"host","device":"0"}]`), 0600); err != nil {
			t.Fatal(err)
		}
		err = setupCommandWithProbe([]string{"install", "queue", "--config", c.Config, "--machine-map", mapping, "--drain-evidence=verified-empty", "--dry-run"}, myce.MachineProbe{OS: "linux", Arch: "amd64"}, root)
		var typed *myce.HTTPError
		if !errors.As(err, &typed) || typed.Code != "machine_drain_evidence_missing" {
			t.Fatal("CLI did not enforce explicit source evidence", err)
		}
	})

	t.Run("conversion-dry-run-custody", func(t *testing.T) {
		home := t.TempDir()
		base, err := myce.PlanInstall("worker", home, "", home, "http://queue.invalid", "host", myce.Detection{OS: "linux", Arch: "amd64", Platform: "xpu", MemoryMode: "fixed", MemoryGB: 24, ReserveGB: 2})
		if err != nil {
			t.Fatal(err)
		}
		base.HostID = "host"
		base.EnvironmentCacheBytes = 1 << 20
		args := []string{"install", "worker", "--config", base.Config, "--dry-run"}
		for _, device := range []string{"0", "1"} {
			c := base
			if err := c.WorkerInstance("gpu" + device); err != nil {
				t.Fatal(err)
			}
			c.Device = device
			c.Name = "host-gpu" + device
			if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Dir(c.Service), 0700); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(c)
			if err := os.WriteFile(c.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(c.Service, []byte("legacy"), 0600); err != nil {
				t.Fatal(err)
			}
			args = append(args, "--from-worker-config", c.Config)
		}
		probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) {
			t.Fatal("dry run invoked service/probe")
			return nil, nil
		}}
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		original := os.Stdout
		os.Stdout = w
		defer func() { os.Stdout = original }()
		captured := make(chan []byte, 1)
		go func() { output, _ := io.ReadAll(r); r.Close(); captured <- output }()
		err = setupCommandWithProbe(args, probe, home)
		os.Stdout = original
		w.Close()
		<-captured
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(base.Config); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("dry run wrote destination", err)
		}
		if err := setupCommandWithProbe([]string{"install", "worker", "--config", args[6], "--dry-run"}, probe, home); err == nil || !strings.Contains(err.Error(), "machine_worker_conflict") {
			t.Fatal("deploy gate", err)
		}
	})
}
