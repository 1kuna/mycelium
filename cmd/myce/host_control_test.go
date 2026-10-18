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

func TestRetiredHostFormsNeverMutate(t *testing.T) {
	dir := t.TempDir()
	if _, err := myce.InitializeHostIntent(dir, "fixture-host", myce.RealClock{}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "host-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"disable", "enable"} {
		var out bytes.Buffer
		if err := hostCommand([]string{op, "--unit", "myce-worker.service"}, myce.MachineProbe{}, &out); err == nil {
			t.Fatal("retired command accepted")
		}
		if err := runWithProbe([]string{op, "--state-dir", dir}, myce.MachineProbe{}); err == nil {
			t.Fatal("retired dispatch accepted")
		}
	}
	after, err := os.ReadFile(filepath.Join(dir, "host-state.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("retired command mutated state", err)
	}
}
