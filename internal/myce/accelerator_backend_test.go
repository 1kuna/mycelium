package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the consumers of every descriptor with injected hardware. These are
// the existing wire names, budgets, process masks and status words, not aliases.
func TestAcceleratorBackendDescriptors(t *testing.T) {
	cases := []struct {
		platform, device, extension, resource, placement string
		free                                             float64
		cuda, affinity, selector                         string
		tiers, multi, fault, telemetry                   bool
	}{
		{"cuda", "GPU-card", "cuda:0", "NVIDIA (CUDA)", "cuda", 6, "GPU-card", "2147483647", "!*:*", true, true, true, false},
		{"xpu", "3", "xpu:0", "Intel (XPU)", "xpu", 7, "", "3", "level_zero:0", true, true, true, true},
		{"metal", "0", "mps", "Apple Metal", "metal", 5, "", "2147483647", "!*:*", false, false, false, false},
		{"fake", "0", "cpu", "test resource (fake)", "cpu-fake", 7, "", "2147483647", "!*:*", false, true, false, false},
		{"cpu", "", "cpu", "CPU", "cpu", 0, "", "2147483647", "!*:*", false, false, false, false},
	}
	if len(cases) != len(acceleratorBackends) {
		t.Fatal("add a behavior case for every registered backend")
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		t.Run(tc.platform, func(t *testing.T) {
			backend := backendFor(tc.platform)
			if !ValidPlatform(tc.platform) || PhysicalAcceleratorPlatform(tc.platform) != (tc.platform != "cpu" && tc.platform != "fake") {
				t.Fatal("CLI platform validation changed")
			}
			for _, mode := range []string{"auto", "fixed", "dedicated", "unified"} {
				want := mode != "fixed" && (tc.platform == "cuda" || tc.platform == "metal" || tc.platform == "xpu" && mode == "unified")
				if AcceleratorMemoryMeasured(tc.platform, mode) != want {
					t.Fatal("CLI memory provenance changed", mode)
				}
			}

			if seen[backend.platform] || !validGPU(tc.platform) {
				t.Fatal("missing or duplicate backend", tc.platform)
			}
			seen[backend.platform] = true
			if ExtensionDevice(tc.platform) != tc.extension || resourceWords(tc.platform) != tc.resource {
				t.Fatal("backend public names changed")
			}
			m := Machine{GPU: tc.platform, Device: tc.device}
			env := placementEnvironment(m)
			if env["MYCE_DEVICE"] != tc.extension || env["MYCE_PLATFORM"] != tc.placement {
				t.Fatal("placement environment", env)
			}
			if isolatesDeviceTiers(&m) != tc.tiers {
				t.Fatal("tier isolation changed")
			}
			model, err := WithModelDevice(Model{}, tc.platform, tc.device)
			if err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]string{"CUDA_VISIBLE_DEVICES": tc.cuda, "ZE_AFFINITY_MASK": tc.affinity, "ONEAPI_DEVICE_SELECTOR": tc.selector, "ZE_ENABLE_PCI_ID_DEVICE_ORDER": "1"} {
				if model.DeviceEnv[key] != want {
					t.Fatalf("%s = %q, want %q", key, model.DeviceEnv[key], want)
				}
			}
			_, err = cardsEnvironment(tc.platform, []MachineDevice{{Selector: tc.device}})
			if (err == nil) != tc.multi {
				t.Fatal("multi-card contract", err)
			}
			source, err := newMemorySource(tc.platform, 8, 1, "auto", "linux",
				func(string) ([]byte, error) { return []byte("MemAvailable: 6291456 kB\n"), nil },
				func(_ context.Context, name string, _ ...string) ([]byte, error) {
					if name != "nvidia-smi" {
						t.Fatal("unexpected memory command", name)
					}
					return []byte("7168"), nil
				})
			if err != nil {
				t.Fatal(err)
			}
			free, err := source(t.Context())
			if err != nil || free != tc.free {
				t.Fatal("memory contract", free, err)
			}
			healthy, err := (MachineProbe{Command: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("probe failed") }}).DeviceHealth(t.Context(), MachineDevice{Platform: tc.platform})
			if healthy || err == nil {
				t.Fatal("unknown health cleared quarantine")
			}
			if (backend.faultMatcher != nil) != tc.fault || (backend.health != nil) != tc.fault || (backend.telemetry != nil) != tc.telemetry {
				t.Fatal("recovery/telemetry support changed")
			}
			kind, _ := allocationKernelFault(MachineDevice{Platform: tc.platform}, "NVRM: NV_ERR_NO_MEMORY")
			if (kind == "nvrm_allocation") != (tc.platform == "cuda") {
				t.Fatal("allocation evidence backend contract", kind)
			}
			if tc.fault {
				line := "xe 0000:03:00.0: GPU hang"
				if tc.platform == "cuda" {
					line = "NVRM: Xid (PCI:0000:03:00): 79"
				}
				reason := backend.faultMatcher(MachineDevice{pciAddress: "0000:03:00.0"}, line)
				if !deviceFaultReason(reason) {
					t.Fatal("fault matcher/reason registry diverged", reason)
				}
			}
			failure := &acceleratorProbeError{families: map[string]bool{tc.platform: true}}
			if !failure.failed(tc.platform) || failure.Error() != tc.platform+": device_probe_unavailable" {
				t.Fatal("partial probe status", failure)
			}
		})
	}
	if validGPU("mps") || validGPU("") || ExtensionDevice("unknown") != "" || resourceWords("unknown") != "unknown resource" {
		t.Fatal("unknown backend must remain unknown")
	}
}

func TestAcceleratorBackendInventoryAndDetection(t *testing.T) {
	for _, platform := range []string{"cuda", "xpu", "metal", "fake", "cpu"} {
		t.Run(platform, func(t *testing.T) {
			backend := backendFor(platform)
			if platform == "fake" || platform == "cpu" {
				if backend.inventory != nil {
					t.Fatal("simulated/CPU backend discovered physical hardware")
				}
				return
			}
			p := MachineProbe{OS: "linux", Arch: "amd64",
				Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
					switch name {
					case "nvidia-smi":
						if strings.Contains(strings.Join(args, " "), "name,memory.total") {
							return []byte("card, 8192, 0000:03:00.0"), nil
						}
						return []byte("GPU-card, driver, 0000:03:00.0"), nil
					case "/usr/sbin/system_profiler":
						return []byte("Metal"), nil
					}
					t.Fatal("unexpected probe", name, args)
					return nil, nil
				},
				CommandEnv: func(context.Context, map[string]string, string, ...string) ([]byte, error) {
					return []byte(`[{"identity":"intel-card","fingerprint":"driver","pci_address":"0000:03:00.0","memory_bytes":8589934592}]`), nil
				},
			}
			pci := []pciAccelerator{{address: "0000:03:00.0", vendor: backend.pciVendor, topologyKnown: true}}
			devices, err := backend.inventory(p, t.Context(), pci)
			if err != nil || len(devices) != 1 || devices[0].Platform != platform {
				t.Fatal("inventory", devices, err)
			}
			if backend.pciVendor != "" {
				if pci[0].platform() != platform || backend.nativeNodes == nil {
					t.Fatal("physical backend mapping missing")
				}
				devices[0].pciFact = &pci[0]
			}
			detected, err := p.detectAccelerator(t.Context(), devices[0])
			mode := "dedicated"
			if platform == "xpu" {
				mode = "fixed"
			}
			if platform == "metal" {
				mode = "unified"
			}
			if err != nil || detected.Platform != platform || detected.MemoryMode != mode {
				t.Fatal("detection", detected, err)
			}
		})
	}
}

func TestAcceleratorBackendEnvironmentRecipes(t *testing.T) {
	lock := filepath.Join(t.TempDir(), "requirements.txt")
	if err := os.WriteFile(lock, []byte("package==1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	recipes, err := (EnvironmentCatalog{}).resolveEnvironment(Environment{LockFile: lock, Backend: "uv", Python: "3.12"})
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range acceleratorBackends {
		recipe, ok := recipes[backend.platform]
		if !ok || recipe.Platform != backend.platform {
			t.Fatal("environment omitted backend", backend.platform, recipe)
		}
	}
	if len(recipes) != len(acceleratorBackends) {
		t.Fatal("environment resolved extra backends")
	}
	// A named family failure must leave other descriptors' inventory usable.
	failure := &acceleratorProbeError{families: map[string]bool{"cuda": true, "xpu": true}}
	if failure.Error() != "cuda: device_probe_unavailable; xpu: device_probe_unavailable" {
		t.Fatal(failure)
	}
}
