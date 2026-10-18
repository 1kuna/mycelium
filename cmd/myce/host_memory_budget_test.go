package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestHostMemoryBudgetCLIWorksOffline(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "control")
	t.Setenv("HOME", home)
	t.Setenv("MYCE_HOST_STATE_DIR", dir)
	t.Setenv("MYCE_TOKEN", "")
	if _, err := myce.InitializeHostIntent(dir, "fixture-host", myce.RealClock{}); err != nil {
		t.Fatal(err)
	}
	out, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = out
	defer func() { os.Stdout = saved; out.Close() }()
	if err := run([]string{"host-memory-budget", "set", "3"}); err == nil {
		t.Fatal("retired per-run command accepted")
	}
	if err := run([]string{"host-memory-budget", "configure", "3"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"host-memory-budget", "show"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"3221225472 bytes", "3 GiB", "UNQUALIFIED", "host-state.json"} {
		if !strings.Contains(string(b), want) {
			t.Fatal("missing effective allowance", want)
		}
	}
	if strings.Contains(string(b), "generation") {
		t.Fatal("retired generation protocol output")
	}
}
