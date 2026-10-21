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

func TestJoinGPUDetection(t *testing.T) {
	for _, tc := range []struct {
		name, os, arch, output, platform, mode string
		memory, reserve                        float64
	}{
		{"CUDA", "linux", "amd64", "RTX 4090, 24576", "cuda", "dedicated", 24, 2},
		{"Metal", "darwin", "arm64", "Metal Support: Metal 3", "metal", "unified", 0, 4},
		{"UnifiedCUDA", "linux", "arm64", "NVIDIA GB10, N/A", "cuda", "unified", 0, 16},
		{"IntegratedXPU", "linux", "amd64", "[level_zero:gpu] Intel UHD Integrated GPU", "xpu", "unified", 0, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := myce.MachineProbe{OS: tc.os, Arch: tc.arch, Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "nvidia-smi" && tc.platform == "cuda" && strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
					return []byte("GPU-fixture, driver, 0000:01:00.0"), nil
				}
				if name == "/usr/sbin/system_profiler" && tc.platform == "metal" || name == "nvidia-smi" && tc.platform == "cuda" || name == "sycl-ls" && tc.platform == "xpu" {
					return []byte(tc.output), nil
				}
				return nil, exec.ErrNotFound
			}}
			if tc.platform == "cuda" {
				probe = cudaPCIFixture(probe, tc.mode == "unified")
			}
			if tc.platform == "xpu" {
				probe = integratedPCIFixture(probe)
			}
			got, err := detectJoinGPU(context.Background(), probe, joinGPUOptions{mode: "auto"}, nil)
			want := joinGPUOptions{platform: tc.platform, mode: tc.mode, memory: tc.memory, reserve: tc.reserve}
			if err != nil || got != want {
				t.Fatalf("got %+v, %v; want %+v", got, err, want)
			}
		})
	}
}

func TestJoinExplicitGPUBypassesDetection(t *testing.T) {
	probe := myce.MachineProbe{Command: func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("explicit GPU ran detector")
		return nil, nil
	}}
	want := joinGPUOptions{platform: "fake", device: "2", mode: "fixed", memory: 8, reserve: 1}
	got, err := detectJoinGPU(context.Background(), probe, want, nil)
	if err != nil || got != want {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestJoinDetectionPreservesExplicitOptions(t *testing.T) {
	probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "nvidia-smi" && strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
			return []byte("GPU-0, driver, 0000:01:00.0\nGPU-1, driver, 0000:02:00.0\nGPU-2, driver, 0000:03:00.0"), nil
		}
		if name != "nvidia-smi" || !strings.Contains(strings.Join(args, " "), "--id=GPU-2") {
			t.Fatalf("unexpected probe %s %v", name, args)
		}
		return []byte("RTX 4090, 24576"), nil
	}}
	probe = cudaPCIFixtureN(probe, false, 3)
	options := joinGPUOptions{device: "2", mode: "auto", memory: 0, reserve: 0}
	got, err := detectJoinGPU(context.Background(), probe, options, map[string]bool{"memory-mode": true, "memory-gb": true, "reserve-memory-gb": true})
	options.platform = "cuda"
	if err != nil || got != options {
		t.Fatalf("got %+v, %v; want %+v", got, err, options)
	}
}

func TestJoinMachineDeviceAllowance(t *testing.T) {
	for _, selection := range []string{"", "0,1", "1"} {
		t.Run("selection="+selection, func(t *testing.T) {
			p := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != "nvidia-smi" {
					return nil, exec.ErrNotFound
				}
				if strings.Contains(strings.Join(args, " "), "--query-gpu=uuid,driver_version") {
					return []byte("GPU-a, driver, 0000:01:00.0\nGPU-b, driver, 0000:02:00.0"), nil
				}
				if strings.Contains(strings.Join(args, " "), "--id=") {
					return []byte("RTX, 8192"), nil
				}
				return []byte("RTX, 8192\nRTX, 8192"), nil
			}}
			p = cudaPCIFixtureN(p, false, 2)
			got, err := detectJoinGPU(context.Background(), p, joinGPUOptions{device: selection, mode: "auto"}, nil)
			if err != nil || got.platform != "cuda" || got.device != selection {
				t.Fatalf("allowance narrowed or rejected: %+v %v", got, err)
			}
		})
	}
}

func TestJoinNoGPUOffersCPUWithoutContactingQueue(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	for _, tc := range []struct{ os, arch string }{{"linux", "amd64"}, {"darwin", "amd64"}} {
		probe := myce.MachineProbe{OS: tc.os, Arch: tc.arch, Glob: func(string) ([]string, error) { return nil, nil }, Command: func(context.Context, string, ...string) ([]byte, error) { return nil, exec.ErrNotFound }}
		got, err := detectJoinGPU(context.Background(), probe, joinGPUOptions{mode: "auto"}, nil)
		if err != nil || got.platform != "cpu" || got.memory != 0 {
			t.Fatalf("CPU enrollment: %+v %v", got, err)
		}
		err = runWithProbe([]string{"join", "--heartbeat", "0"}, probe)
		if err == nil || err.Error() != "heartbeat must be positive" {
			t.Fatalf("unexpected setup error: %v", err)
		}
	}
}

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

func TestJoinRejectsConflictingChassisDeclarations(t *testing.T) {
	err := run([]string{"join", "--desktop", "--laptop"})
	if err == nil || !strings.Contains(err.Error(), "--desktop and --laptop are mutually exclusive") {
		t.Fatal(err)
	}
	err = benchmarkCommand([]string{"--desktop", "--laptop"})
	if err == nil || !strings.Contains(err.Error(), "--desktop and --laptop are mutually exclusive") {
		t.Fatal(err)
	}
}

func TestJoinAcceptsOnboardingBenchmarkFlag(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	for _, value := range []string{"true", "false"} {
		err := run([]string{"join", "--gpu", "fake", "--heartbeat", "0", "--onboarding-benchmark=" + value})
		if err == nil || err.Error() != "heartbeat must be positive" {
			t.Fatalf("flag %s: %v", value, err)
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
