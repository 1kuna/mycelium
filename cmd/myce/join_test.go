package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestJoinCLIUsesDetectedOrExplicitGPU(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	for _, explicit := range []bool{false, true} {
		calls := 0
		probe := myce.MachineProbe{OS: "darwin", Arch: "arm64", Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
			calls++
			if explicit {
				t.Fatal("explicit CLI GPU ran detector")
			}
			if name != "/usr/sbin/system_profiler" {
				t.Fatalf("unexpected command %s", name)
			}
			return []byte("Metal Support: Metal 3"), nil
		}}
		args := []string{"join", "--heartbeat", "0"}
		if explicit {
			args = append(args, "--gpu", "fake")
		}
		// Stop after local GPU setup, before worker.Run or any queue contact.
		err := runWithProbe(args, probe)
		if err == nil || err.Error() != "heartbeat must be positive" {
			t.Fatalf("join setup: %v", err)
		}
		if !explicit && calls != 1 {
			t.Fatalf("detection calls=%d", calls)
		}
	}
}

// An Apple Silicon host cannot be classified as GPU-less merely because its
// Metal runtime is unavailable. The owner can still explicitly select CPU.
func TestJoinAppleSiliconWithoutMetalRefusesUnlessCPUSelected(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	probe := myce.MachineProbe{OS: "darwin", Arch: "arm64", Command: func(context.Context, string, ...string) ([]byte, error) { return nil, exec.ErrNotFound }}
	_, err := detectJoinGPU(context.Background(), probe, joinGPUOptions{mode: "auto"}, nil)
	var typed *myce.PreflightError
	if !errors.As(err, &typed) || typed.Code != "gpu_driver_unavailable" {
		t.Fatalf("missing Metal lost typed refusal: %v", err)
	}
	err = runWithProbe([]string{"join", "--heartbeat", "0"}, probe)
	if !errors.As(err, &typed) || typed.Code != "gpu_driver_unavailable" {
		t.Fatalf("join accepted unavailable Metal: %v", err)
	}
	got, err := detectJoinGPU(context.Background(), probe, joinGPUOptions{platform: "cpu", mode: "fixed"}, nil)
	if err != nil || got.platform != "cpu" || got.memory != 0 {
		t.Fatal(got, err)
	}
	err = runWithProbe([]string{"join", "--gpu", "cpu", "--heartbeat", "0"}, probe)
	if err == nil || err.Error() != "heartbeat must be positive" {
		t.Fatalf("explicit CPU did not reach local validation: %v", err)
	}
}

// Integrated fixtures provide physical topology, never just a marketing name.
func integratedPCIFixture(p myce.MachineProbe) myce.MachineProbe {
	p.Glob = func(pattern string) ([]string, error) {
		if strings.Contains(pattern, "renderD") {
			return []string{"/dev/dri/renderD128"}, nil
		}
		return []string{"/sys/bus/pci/devices/0000:00:02.0/class"}, nil
	}
	p.ReadFile = func(path string) ([]byte, error) {
		switch filepath.Base(path) {
		case "class":
			return []byte("0x030000"), nil
		case "vendor":
			return []byte("0x8086"), nil
		case "current_link_width":
			return []byte("0"), nil
		}
		return nil, os.ErrNotExist
	}
	if p.CommandEnv == nil {
		p.CommandEnv = func(ctx context.Context, _ map[string]string, name string, args ...string) ([]byte, error) {
			if name == "python3" {
				b, err := p.Command(ctx, "sycl-ls")
				if err != nil {
					return nil, err
				}
				if !strings.Contains(string(b), "[level_zero:gpu") {
					return []byte("[]"), nil
				}
				return []byte(`[{"identity":"intel-fixture","fingerprint":"driver"}]`), nil
			}
			return p.Command(ctx, name, args...)
		}
	}
	return p
}

func cudaPCIFixture(p myce.MachineProbe, integrated bool) myce.MachineProbe {
	return cudaPCIFixtureN(p, integrated, 1)
}

func cudaPCIFixtureN(p myce.MachineProbe, integrated bool, n int) myce.MachineProbe {
	p.Glob = func(string) ([]string, error) {
		paths := []string{}
		for i := 1; i <= n; i++ {
			paths = append(paths, fmt.Sprintf("/sys/bus/pci/devices/0000:%02x:00.0/class", i))
		}
		return paths, nil
	}
	p.ReadFile = func(path string) ([]byte, error) {
		switch filepath.Base(path) {
		case "class":
			return []byte("0x030000"), nil
		case "vendor":
			return []byte("0x10de"), nil
		case "current_link_width":
			if integrated {
				return []byte("0"), nil
			}
			return []byte("16"), nil
		}
		return nil, os.ErrNotExist
	}
	return p
}
