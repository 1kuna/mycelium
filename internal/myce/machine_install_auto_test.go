package myce

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func automaticSparkSource(t *testing.T) (ServiceConfig, ServiceConfig) {
	t.Helper()
	sources, dest := m4Sources(t)
	source := sources[0]
	source.HostID, source.Name, source.Device = "spark", "spark", ""
	source.Detection = Detection{OS: "linux", Arch: "arm64", Platform: "cuda", MemoryMode: "unified", ReserveGB: 16, GPU: "NVIDIA GB10"}
	source.Service = dest.Service
	data, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(source.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	return source, dest
}

func TestMachineInstallAutomaticSourceOwnership(t *testing.T) {
	source, dest := automaticSparkSource(t)
	for _, tc := range []struct {
		name  string
		paths []string
		probe MachineProbe
	}{
		{"platform mismatch", []string{source.Config}, MachineProbe{OS: "darwin", Arch: "arm64"}},
		{"mixed sources", []string{source.Config, filepath.Join(filepath.Dir(source.Config), "other.json")}, MachineProbe{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := PlanMachineInstallWithProbe(context.Background(), dest.Config, tc.paths, nil, tc.probe); err == nil || !strings.Contains(err.Error(), "machine_migration_conflict") {
				t.Fatal("ambiguous source ownership accepted", err)
			}
		})
	}
}
