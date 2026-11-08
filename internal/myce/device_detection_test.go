package myce

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The compatibility memory helper already uses XPU affinity. Cover both
// vendors so a per-card fallback cannot silently become a host-wide query.
func TestDeviceTopologyHelperAffinity(t *testing.T) {
	p := MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) { return []byte("GPU, N/A, 0000:01:00.0"), nil }}
	for _, platform := range []string{"cuda", "xpu"} {
		called := false
		p.CommandEnv = func(_ context.Context, env map[string]string, name string, args ...string) ([]byte, error) {
			called = true
			if platform == "cuda" && env["CUDA_VISIBLE_DEVICES"] != "GPU-card" || platform == "xpu" && (env["ZE_AFFINITY_MASK"] != "3" || env["ONEAPI_DEVICE_SELECTOR"] != "level_zero:0") {
				t.Fatal("unscoped helper", env)
			}
			if name == "python3" {
				return []byte("1"), nil
			}
			if name == "sycl-ls" && strings.Join(args, " ") == "--verbose" {
				return []byte("global_mem_size: 8589934592"), nil
			}
			return nil, errors.New("unexpected helper")
		}
		selector := "GPU-card"
		if platform == "xpu" {
			selector = "3"
		}
		_, err := p.detectAccelerator(t.Context(), MachineDevice{Platform: platform, Selector: selector, pciFact: &pciAccelerator{address: "0000:01:00.0", topologyKnown: true}})
		if err != nil || !called {
			t.Fatal("scoped topology helper failed", platform, err)
		}
	}
}
