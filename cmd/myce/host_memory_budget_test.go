package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostMemoryBudgetCLIWorksOffline(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("MYCE_HOST_STATE_DIR", filepath.Join(home, "control"))
	t.Setenv("MYCE_TOKEN", "")
	out, _ := os.CreateTemp(t.TempDir(), "stdout")
	saved := os.Stdout
	os.Stdout = out
	defer func() { os.Stdout = saved; out.Close() }()
	if err := run([]string{"host-memory-budget", "set", "3"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"host-memory-budget", "show"}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out.Name())
	if !strings.Contains(string(b), "3 GiB") || !strings.Contains(string(b), "generation 1") || !strings.Contains(string(b), "host-memory-budget.json") {
		t.Fatal("missing effective value/source")
	}
}
