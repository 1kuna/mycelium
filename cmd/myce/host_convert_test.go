package main

import (
	"bytes"
	"encoding/json"
	"mycelium/internal/myce"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostConvertCLI(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "DISABLED"), []byte(`{"generation":7,"who":"1000","when":"2026-10-17T00:00:00Z","reason":"operator","grace_seconds":0}`), 0640); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"--dry-run", "--apply", "--apply"} {
		var out bytes.Buffer
		if err := hostConvertCommand([]string{"--host-state-dir", dir, "--host-id", "test-host", mode}, &out); err != nil {
			t.Fatal(err)
		}
		var m myce.HostConversionManifest
		if err := json.Unmarshal(out.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		if m.Intent.Revision != 8 || m.Intent.Desired != "closed" || m.Intent.Observed != nil {
			t.Fatal(m)
		}
	}
	if err := runWithProbe([]string{"_host-start-check", "--host-state-dir", dir}, myce.MachineProbe{}); err == nil {
		t.Fatal("closed startup admitted")
	}
	var out bytes.Buffer
	if err := hostConvertCommand([]string{"--host-state-dir", dir, "--host-id", "test-host", "--apply", "--dry-run"}, &out); err == nil {
		t.Fatal("ambiguous conversion mode")
	}
	if err := hostCommand([]string{"enable", "--unit", "myce-worker@card-a.service", "--host-state-dir", dir}, myce.MachineProbe{OS: "linux"}, &out); err == nil {
		t.Fatal("per-card enable accepted")
	}
}

func TestLegacyEnableHasMigrationPointer(t *testing.T) {
	var out bytes.Buffer
	err := hostCommand([]string{"enable"}, myce.MachineProbe{}, &out)
	if err == nil || !strings.Contains(err.Error(), "myce host open --expect-revision") {
		t.Fatal(err)
	}
}
func TestBenchmarkCLIClosedHostRefused(t *testing.T) {
	dir := t.TempDir()
	if _, err := myce.InitializeHostIntent(dir, "test-host", myce.RealClock{}); err != nil {
		t.Fatal(err)
	}
	if err := benchmarkCommand([]string{"--state-dir", t.TempDir(), "--gpu", "fake", "--host-state-dir", dir}); err == nil || !strings.Contains(err.Error(), "host disabled") {
		t.Fatal("closed benchmark admitted", err)
	}
}
