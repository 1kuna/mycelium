package myce

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func onboardingProbe(platform string) MachineProbe {
	p := MachineProbe{OS: "linux", Arch: "arm64", Glob: func(pattern string) ([]string, error) {
		if strings.Contains(pattern, "renderD") && platform == "xpu" {
			return []string{"/dev/dri/renderD128", "/dev/dri/renderD129"}, nil
		}
		return nil, nil
	}}
	p.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "/usr/sbin/system_profiler":
			if platform == "metal" {
				return []byte("Metal Support: Metal 3"), nil
			}
			return []byte("No displays"), nil
		case "nvidia-smi":
			if platform == "cuda" {
				if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
					return []byte("GPU-gb10, driver"), nil
				}
				return []byte("NVIDIA GB10, [N/A]"), nil
			}
		case "sycl-ls":
			if platform == "xpu" {
				return []byte("[level_zero:gpu] Intel Arc GPU"), nil
			}
		}
		return nil, exec.ErrNotFound
	}
	p.CommandEnv = func(ctx context.Context, env map[string]string, n string, args ...string) ([]byte, error) {
		if n == "python3" && platform == "xpu" {
			return []byte(`[{"identity":"uuid-a","fingerprint":"driver","pci_address":"0000:01:00.0"},{"identity":"uuid-b","fingerprint":"driver","pci_address":"0000:02:00.0"}]`), nil
		}
		return p.Command(ctx, n, args...)
	}
	if platform == "cuda" {
		facts := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x10de", "0", "NVIDIA GB10", "", true})
		p.Glob, p.ReadFile = facts.Glob, facts.ReadFile
	}
	if platform == "xpu" {
		facts := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x8086", "8", "Intel GPU", "", true}, pciFixture{"0000:02:00.0", "0x8086", "8", "Intel GPU", "", true})
		p.Glob, p.ReadFile = facts.Glob, facts.ReadFile
	}
	if platform == "metal" {
		p.OS = "darwin"
	}
	return p
}

func requirePreflightCode(t *testing.T, err error, code string) {
	t.Helper()
	var typed *PreflightError
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}
