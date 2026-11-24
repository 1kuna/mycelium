package myce

import (
	"context"
	"strings"
	"testing"
)

func TestIndependentDiscreteCUDANAMemory(t *testing.T) {
	p := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x10de", "16", "Generic NVIDIA", "", true})
	command := p.Command
	p.Command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
		if n == "nvidia-smi" && strings.Contains(strings.Join(args, " "), "name,memory.total") {
			out := "Generic NVIDIA, N/A"
			if strings.Contains(strings.Join(args, " "), "pci.bus_id") {
				out += ", 0000:01:00.0"
			}
			return []byte(out), nil
		}
		return command(ctx, n, args...)
	}
	got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	if err == nil {
		t.Fatalf("known PCIe discrete device with unknown VRAM must not become unified: %+v", got)
	}
	requirePreflightCode(t, err, "gpu_memory_unavailable")
}
