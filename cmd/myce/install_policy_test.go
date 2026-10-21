package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestInstallCLIPolicyDryRun(t *testing.T) {
	for _, tc := range []struct{ name, role, file, data, reason string }{
		{"retired", "queue", "hosts.yaml", "hosts:\n  fixture:\n    production_idle: 10s\n", "production_idle"},
		{"zero weight", "queue", "projects.yaml", "fixture:\n  weight: 0\n", "positive finite weight"},
		{"deadline call priority", "queue", "projects.yaml", "fixture:\n  weight: 1\n  default_call_priority: deadline\n", "default_call_priority must be experiment or production"},
		{"empty host", "queue", "hosts.yaml", "hosts:\n  '':\n    production_hold: true\n", "host name is required"},
		{"worker ignores policy", "worker", "hosts.yaml", "hosts:\n  fixture:\n    production_idle: 10s\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			path := filepath.Join(home, tc.file)
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			flag := "--host-config"
			if tc.file == "projects.yaml" {
				flag = "--project-config"
			}
			probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "nvidia-smi" {
					if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
						return []byte("GPU-fixture, driver"), nil
					}
					return []byte("Fixture GPU, 24576"), nil
				}
				if name == "systemctl" || name == "launchctl" {
					t.Fatal("dry run touched service manager")
				}
				return nil, nil
			}}
			err := setupCommandWithProbe([]string{"install", tc.role, "--dry-run", flag, path}, probe, home)
			if tc.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("dry run accepted invalid policy")
			}
			wants := []string{tc.file, tc.reason}
			if tc.name == "retired" {
				wants = append(wants, "line 3", "retired")
			}
			for _, want := range wants {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("missing %s: %v", want, err)
				}
			}
		})
	}
}
