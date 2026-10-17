package main

import (
	"bytes"

	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestSafeResumeCommandNeverRecordsToken(t *testing.T) {
	got := strings.Join(safeJoinCommand([]string{"join", "--token=secret", "--name", "worker", "--token", "another-secret", "--gpu", "fake"}), " ")
	if got != "myce join --name worker --gpu fake" {
		t.Fatalf("unsafe resume command %q", got)
	}
}

func TestDisableWritesFlagBeforeBrokenInstallConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("MYCE_HOST_STATE_DIR", filepath.Join(home, "host"))
	dir := filepath.Join(home, "host")
	if _, e := myce.InitializeHostIntent(dir, "test-host", myce.RealClock{}); e != nil {
		t.Fatal(e)
	}
	config := filepath.Join(home, "broken-config.json")
	if err := os.WriteFile(config, []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hostCommand([]string{"disable", "--now", "--config", config}, myce.MachineProbe{OS: "linux"}, &out); err == nil {
		t.Fatal("broken config succeeded")
	}
	if reason, err := myce.HostDisabled(""); err != nil || reason == "" {
		t.Fatalf("config failure prevented local fence: %q %v", reason, err)
	}
}
