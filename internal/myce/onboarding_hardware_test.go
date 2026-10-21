package myce

import (
	"context"
	"errors"
	"os"
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
		if n == "python3" {
			return []byte(`[{"identity":"uuid-a","fingerprint":"driver"},{"identity":"uuid-b","fingerprint":"driver"}]`), nil
		}
		return p.Command(ctx, n, args...)
	}
	if platform == "metal" {
		p.OS = "darwin"
	}
	return p
}
func TestOnboardingWholeMachineHardware(t *testing.T) {
	for _, platform := range []string{"xpu", "cuda", "metal", "cpu-linux", "cpu-mac"} {
		t.Run(platform, func(t *testing.T) {
			p := onboardingProbe(platform)
			c := ServiceConfig{}
			if platform == "cpu-mac" {
				p.OS = "darwin"
				p.Arch = "amd64"
			}
			if platform == "xpu" {
				c.Detection = Detection{Platform: "xpu", MemoryMode: "fixed", MemoryGB: 24, ReserveGB: 2}
				c.MachineDevices = []DeviceInstall{{"0", Detection{Platform: "xpu", MemoryMode: "fixed", MemoryGB: 24, ReserveGB: 2}}, {"1", Detection{Platform: "xpu", MemoryMode: "fixed", MemoryGB: 16, ReserveGB: 1}}}
			}
			got, err := PreflightHardware(context.Background(), c, p)
			if err != nil {
				t.Fatal(err)
			}
			switch platform {
			case "xpu":
				if len(got.Devices) != 2 || got.Devices[0].Detection.MemoryGB != 24 || got.Devices[1].Detection.MemoryGB != 16 || got.Devices[1].Detection.ReserveGB != 1 {
					t.Fatal(got)
				}
			case "cuda":
				if len(got.Devices) != 1 || got.MemoryMode != "unified" || got.ReserveGB != 16 || got.MemoryGB != 0 {
					t.Fatal(got)
				}
			case "metal":
				if len(got.Devices) != 1 || got.MemoryMode != "unified" || got.ReserveGB != 4 {
					t.Fatal(got)
				}
			default:
				if got.Platform != "cpu" || len(got.Devices) != 0 {
					t.Fatal(got)
				}
			}
		})
	}
}
func TestOnboardingSelectorsAndRecovery(t *testing.T) {
	p := onboardingProbe("xpu")
	c := ServiceConfig{Device: "1,0", Detection: Detection{Platform: "xpu", MemoryMode: "fixed", MemoryGB: 16, ReserveGB: 2}}
	first, err := PreflightHardware(context.Background(), c, p)
	if err != nil || len(first.Devices) != 2 {
		t.Fatal(first, err)
	}
	saved := ApplyHardwarePreflight(c, first)
	if saved.Device != "1,0" {
		t.Fatal("selector list changed")
	}
	original := p.CommandEnv
	p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
		return nil, os.ErrPermission
	}
	if _, err = PreflightHardware(context.Background(), saved, p); err == nil {
		t.Fatal("driver failure accepted")
	}
	p.CommandEnv = original
	recovered, err := PreflightHardware(context.Background(), saved, p)
	if err != nil || len(recovered.Devices) != 2 || recovered.Devices[1].Detection.MemoryGB != 16 {
		t.Fatal(recovered, err)
	}
	saved.Device = "1"
	one, err := PreflightHardware(context.Background(), saved, p)
	if err != nil || len(one.Devices) != 1 || one.Devices[0].Selector != "1" {
		t.Fatal(one, err)
	}
	saved.Device = "2"
	_, err = PreflightHardware(context.Background(), saved, p)
	requirePreflightCode(t, err, "device_selector_unmatched")
}
func requirePreflightCode(t *testing.T, err error, code string) {
	t.Helper()
	var typed *PreflightError
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}
func TestOnboardingPresentGPUWithoutDriverRefuses(t *testing.T) {
	// The previous fixture returned 0x030000 for every sysfs file. That tested
	// the rejected class-only rule, not the supported-accelerator contract.
	p := reviewPCIProbe("0x10de", "0x2b85", "16")
	_, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	requirePreflightCode(t, err, "gpu_driver_missing")
	_, err = p.Detect(context.Background())
	requirePreflightCode(t, err, "gpu_driver_missing")
}
func TestOnboardingInvalidInventoryAndMemoryRefuse(t *testing.T) {
	p := onboardingProbe("cuda")
	original := p.Command
	p.Command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
			return []byte("bad inventory with private-value"), nil
		}
		return original(ctx, n, args...)
	}
	_, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	requirePreflightCode(t, err, "gpu_inventory_invalid")
	if strings.Contains(err.Error(), "private-value") {
		t.Fatal("probe output leaked")
	}
	p = onboardingProbe("xpu")
	_, err = PreflightHardware(context.Background(), ServiceConfig{}, p)
	requirePreflightCode(t, err, "gpu_memory_unavailable")
}

func TestOnboardingExplicitPlatformKeepsDetectedUnifiedDefaults(t *testing.T) {
	for _, platform := range []string{"cuda", "metal"} {
		p := onboardingProbe(platform)
		c := ServiceConfig{Detection: Detection{Platform: platform, MemoryMode: "auto"}}
		got, err := PreflightHardware(context.Background(), c, p)
		reserve := 4.0
		if platform == "cuda" {
			reserve = 16
		}
		if err != nil || got.MemoryMode != "unified" || got.ReserveGB != reserve {
			t.Fatal(got, err)
		}
	}
}
