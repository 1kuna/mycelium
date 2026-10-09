package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mycelium/internal/myce"
)

func TestInstallCLIDryRunFakeDetection(t *testing.T) {
	for _, platform := range []string{"cuda", "xpu", "metal"} {
		t.Run(platform, func(t *testing.T) {
			home := t.TempDir()
			p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, n string, args ...string) ([]byte, error) {
				switch n {
				case "nvidia-smi":
					if platform == "cuda" {
						return []byte("RTX 4090, 24576"), nil
					}
				case "sycl-ls":
					if platform == "xpu" {
						return []byte("[level_zero:gpu] Intel UHD Integrated GPU"), nil
					}
				case "system_profiler":
					if platform == "metal" {
						return []byte("Metal Support: Metal 3"), nil
					}
				}
				if n == "systemctl" || n == "launchctl" {
					t.Fatal("dry run touched service")
				}
				return nil, errors.New("absent")
			}}
			if platform == "metal" {
				p.OS = "darwin"
				p.Arch = "arm64"
			}
			if e := setupCommandWithProbe([]string{"install", "worker", "--nas", filepath.Join(home, "nas"), "--dry-run"}, p, home); e != nil {
				t.Fatal(e)
			}
			entries, _ := os.ReadDir(home)
			if len(entries) != 0 {
				t.Fatal("dry run wrote files")
			}
		})
	}
	home := t.TempDir()
	if e := setupCommandWithProbe([]string{"install", "queue", "--nas", filepath.Join(home, "nas"), "--dry-run"}, myce.MachineProbe{OS: "linux", Arch: "arm64"}, home); e != nil {
		t.Fatal(e)
	}
}
