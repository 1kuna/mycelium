package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestInstallCLIPolicyDryRun(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "hosts.yaml")
	if err := os.WriteFile(path, []byte("hosts:\n  fixture:\n    production_idle: 10s\n"), 0600); err != nil {
		t.Fatal(err)
	}
	err := setupCommandWithProbe([]string{"install", "queue", "--dry-run", "--host-config", path}, myce.MachineProbe{OS: "linux", Arch: "amd64"}, home)
	if err == nil {
		t.Fatal("dry run accepted retired policy")
	}
	for _, want := range []string{"hosts.yaml", "production_idle", "line 3", "retired"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s: %v", want, err)
		}
	}
}
