package myce

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// GB10 reports a nonzero PCI transport width despite using shared system RAM.
// CUDA's per-device integrated attribute supplies the memory-topology fact.
func TestCUDAIntegratedPropertyOverridesPCITransport(t *testing.T) {
	for _, value := range []string{"1", "0", "invalid", "unavailable"} {
		t.Run(value, func(t *testing.T) {
			p := withCUDAFacts(MachineProbe{OS: "linux", Arch: "arm64"}, false, 1)
			read := p.ReadFile
			p.ReadFile = func(path string) ([]byte, error) {
				if path == "/proc/meminfo" {
					return []byte("MemTotal: 134217728 kB\nMemAvailable: 33554432 kB\n"), nil
				}
				return read(path)
			}
			queries := 0
			p.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "python3" {
					queries++
					if len(args) != 3 || args[2] != "0000:01:00.0" || !strings.Contains(args[1], "cuDeviceGetByPCIBusId") {
						t.Fatalf("unbound topology query: %v", args)
					}
					if value == "unavailable" {
						return nil, errors.New("driver attribute unavailable")
					}
					return []byte(value), nil
				}
				if name != "nvidia-smi" {
					t.Fatalf("unexpected command %s", name)
				}
				if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
					return []byte("GPU-integrated, driver, 0000:01:00.0"), nil
				}
				return []byte("GPU, [N/A], 0000:01:00.0"), nil
			}
			hardware, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
			if value != "1" {
				if err == nil {
					t.Fatal("N/A alone established unified memory")
				}
				return
			}
			if err != nil || hardware.MemoryMode != "unified" || hardware.ReserveGB != 16 || hardware.MemoryGB != 0 || queries == 0 {
				t.Fatalf("integrated runtime property ignored: %+v %v queries=%d", hardware, err, queries)
			}
			devices, err := (Worker{Machine: Machine{GPU: "cuda"}}).localInventoryWithProbe(context.Background(), p)
			if err != nil || len(devices) != 1 || devices[0].Reason != "" || !devices[0].UnifiedMemory || !devices[0].MemoryMeasured || devices[0].BudgetGB != 16 {
				t.Fatalf("worker disagrees with onboarding: %+v %v", devices, err)
			}
		})
	}
}
