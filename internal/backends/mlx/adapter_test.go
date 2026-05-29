package mlx

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/test/fixtures"
)

func TestNewAdapterNamesMLX(t *testing.T) {
	adapter := NewAdapter("mlx-lm")
	if adapter.Name() != "mlx" {
		t.Fatalf("name = %s", adapter.Name())
	}
	configured := NewAdapterWithConfig(Config{BinaryPath: "mlx_lm.server"})
	if configured.Name() != "mlx" {
		t.Fatalf("configured name = %s", configured.Name())
	}
}

func TestAdapterUsesMLXServerExecutableDirectly(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args.txt")
	binaryPath := filepath.Join(dir, "mlx_lm.server")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" + strings.ReplaceAll(argsPath, "'", "'\\''") + "'\nsleep 30\n"
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mlx server: %v", err)
	}

	adapter := NewAdapter(binaryPath)
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(fixtures.WithModelRef("mlx-model")), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Stop(context.Background(), handle) })

	raw := readRecordedArgs(t, argsPath)
	got := strings.Fields(string(raw))
	want := []string{"--model", "mlx-model", "--host", "127.0.0.1", "--port", "1"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %q, want %q", got, want)
	}
}

func readRecordedArgs(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			return raw
		}
		select {
		case <-deadline:
			t.Fatalf("read recorded args: %v", err)
		case <-tick.C:
		}
	}
}
