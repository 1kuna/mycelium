package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestJoinExplicitAutoRestoresUnifiedProbe(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	dir := t.TempDir()
	marker := filepath.Join(dir, "probe")
	t.Setenv("MYCE_UNIFIED_TEST_MARKER", marker)
	// Only a fixture executable is available; this never queries hardware.
	t.Setenv("PATH", dir)
	script := `#!/bin/sh
printf '%s\n' "$@" > "$MYCE_UNIFIED_TEST_MARKER"
printf 'N/A\n'
`
	if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	probe := myce.MachineProbe{Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name != "nvidia-smi" || !strings.Contains(strings.Join(args, " "), "--query-gpu=memory.free") {
			t.Fatal("explicit GPU ran setup detection")
		}
		return exec.CommandContext(ctx, filepath.Join(dir, name), args...).Output()
	}}
	err := runWithProbe([]string{"join", "--gpu", "cuda", "--device", "0,1", "--memory-mode", "auto", "--heartbeat", "0"}, probe)
	if err == nil || err.Error() != "heartbeat must be positive" {
		t.Fatalf("startup setup: %v", err)
	}
	data, err := os.ReadFile(marker)
	if err != nil || !strings.Contains(string(data), "--query-gpu=memory.free\n") || !strings.Contains(string(data), "--id=0\n") || strings.Contains(string(data), "0,1") {
		t.Fatalf("auto mode skipped the single-device unified probe: %q %v", data, err)
	}
}
