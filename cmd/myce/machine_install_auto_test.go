package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"mycelium/internal/myce"
)

func TestMachineInstallAutomaticSparkPreview(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("MYCE_TOKEN", "")
	c, err := myce.PlanInstall("worker", home, "", filepath.Join(home, "nas"), "http://queue.invalid", "spark", myce.Detection{
		OS: "linux", Arch: "arm64", Platform: "cuda", MemoryMode: "unified", ReserveGB: 16, GPU: "NVIDIA GB10",
	})
	if err != nil {
		t.Fatal(err)
	}
	c.HostID = "spark"
	c.HostMemoryBudgetGB = 105
	c.EnvironmentFile = filepath.Join(home, "unread-secret-reference")
	if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	original, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(c.Config, original, 0600); err != nil {
		t.Fatal(err)
	}
	probes := 0
	probe := myce.MachineProbe{OS: "linux", Arch: "arm64", Files: fstest.MapFS{
		"sys/bus/pci/devices/0000:01:00.0/class":              {Data: []byte("0x030000")},
		"sys/bus/pci/devices/0000:01:00.0/vendor":             {Data: []byte("0x10de")},
		"sys/bus/pci/devices/0000:01:00.0/current_link_width": {Data: []byte("0")},
	}, Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "nvidia-smi" || strings.Join(args, " ") != "--query-gpu=uuid,driver_version,pci.bus_id --format=csv,noheader,nounits" {
			t.Fatalf("preview invoked unexpected command %s %q", name, args)
		}
		probes++
		return []byte("GPU-spark-gb10-fixture, 580.95.05, 0000:01:00.0\n"), nil
	}}
	destination := filepath.Join(home, "machine", "config.json")
	output := captureJoiningOutput(t, func() error {
		return setupCommandWithProbe([]string{"install", "worker", "--config", destination, "--from-worker-config", c.Config, "--dry-run"}, probe, home)
	})
	for _, want := range []string{"fresh spool", "GPU-spark-gb10-fixture", `"selector":"GPU-spark-gb10-fixture"`, `"memory_mode":"unified"`, `"reserve_memory_gb":16`} {
		if !strings.Contains(output, want) {
			t.Fatalf("preview missing %s: %s", want, output)
		}
	}
	if probes != 1 {
		t.Fatalf("expected one inventory observation, got %d", probes)
	}
	if got, err := os.ReadFile(c.Config); err != nil || string(got) != string(original) {
		t.Fatal("preview changed source config", err)
	}
	if _, err = os.Stat(filepath.Dir(destination)); !os.IsNotExist(err) {
		t.Fatal("preview wrote destination", err)
	}
}
