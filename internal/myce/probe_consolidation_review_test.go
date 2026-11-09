package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestIndependentWorkerFamilyFailureRetainsIntel(t *testing.T) {
	p := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x10de", "16", "NVIDIA", "", true}, pciFixture{"0000:02:00.0", "0x8086", "8", "Intel GPU", "", true})
	cmd := p.Command
	p.Command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
		if n == "nvidia-smi" {
			return nil, os.ErrPermission
		}
		return cmd(ctx, n, args...)
	}
	w := Worker{Machine: Machine{GPU: "xpu", FreeMemoryGB: 16}, FixedAcceleratorMemory: true}
	got, err := w.localInventoryWithProbe(context.Background(), p)
	if len(got) != 1 || got[0].Platform != "xpu" {
		t.Fatalf("worker lost healthy Intel after independent probe: %+v err=%v", got, err)
	}
}

func TestIndependentIntelXpuSMIRealFormat(t *testing.T) {
	p := pciFixtureProbe(pciFixture{"0000:03:00.0", "0x8086", "1", "Intel(R) Arc(TM) Pro B70 Graphics", "", true})
	cmd := p.Command
	p.Command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
		if n == "sycl-ls" {
			return nil, exec.ErrNotFound
		}
		if n == "xpu-smi" {
			return []byte("| Device ID | Device Information |\n| 0 | Device Name: Intel(R) Arc(TM) Pro B70 Graphics |\n| | Vendor Name: Intel(R) Corporation |\n| | PCI BDF Address: 0000:03:00.0 |\n"), nil
		}
		return cmd(ctx, n, args...)
	}
	env := p.CommandEnv
	p.CommandEnv = func(ctx context.Context, e map[string]string, n string, args ...string) ([]byte, error) {
		if n == "python3" {
			return env(ctx, e, n, args...)
		}
		return p.Command(ctx, n, args...)
	}
	got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	if err == nil {
		t.Logf("preflight=%+v", got)
		return
	}
	var typed *PreflightError
	if errors.As(err, &typed) && typed.Code == "gpu_memory_unavailable" {
		return
	} // valid explicit-budget refusal
	t.Fatalf("healthy Level Zero plus real xpu-smi incorrectly classified as missing driver: %+v err=%v", got, err)
}

func TestIndependentUnknownCUDATopology(t *testing.T) {
	p := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x10de", "16", "Generic NVIDIA", "", true})
	read := p.ReadFile
	p.ReadFile = func(path string) ([]byte, error) {
		if strings.HasSuffix(path, "/current_link_width") || strings.HasSuffix(path, "/local_mem") {
			return nil, os.ErrNotExist
		}
		return read(path)
	}
	got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	if err == nil {
		t.Fatalf("unknown physical topology inferred memory mode: %+v", got)
	}
	requirePreflightCode(t, err, "gpu_memory_unavailable")
}
