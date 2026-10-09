package main

import (
	"context"
	"errors"
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
				if name == "system_profiler" && tc.platform == "metal" || name == "nvidia-smi" && tc.platform == "cuda" || name == "sycl-ls" && tc.platform == "xpu" {
					return []byte(tc.output), nil
				}
				return nil, errors.New("absent")
			}}
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
		if name != "nvidia-smi" || !strings.Contains(strings.Join(args, " "), "--id=2") {
			t.Fatalf("unexpected probe %s %v", name, args)
		}
		return []byte("RTX 4090, 24576"), nil
	}}
	options := joinGPUOptions{device: "2", mode: "auto", memory: 0, reserve: 0}
	got, err := detectJoinGPU(context.Background(), probe, options, map[string]bool{"memory-mode": true, "memory-gb": true, "reserve-memory-gb": true})
	options.platform = "cuda"
	if err != nil || got != options {
		t.Fatalf("got %+v, %v; want %+v", got, err, options)
	}
}

func TestJoinNoGPUFailsBeforeContactingQueue(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	for _, tc := range []struct{ os, arch, checked string }{{"linux", "amd64", "nvidia-smi"}, {"darwin", "arm64", "system_profiler"}} {
		probe := myce.MachineProbe{OS: tc.os, Arch: tc.arch, Command: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("absent") }}
		err := runWithProbe([]string{"join", "--queue", "http://127.0.0.1:1"}, probe)
		if err == nil || !strings.Contains(err.Error(), tc.checked) || !strings.Contains(err.Error(), "specify --gpu") || strings.Contains(err.Error(), "\n") {
			t.Fatalf("unexpected error: %v", err)
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
			if name != "system_profiler" {
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
