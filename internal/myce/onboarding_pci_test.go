package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

type pciFixture struct {
	address, vendor, link, name, class string
	usable                             bool
}

func pciFixtureProbe(devices ...pciFixture) MachineProbe {
	files := map[string]string{}
	var classes, nodes []string
	var intel []map[string]string
	var cuda []string
	intelName := ""
	for _, d := range devices {
		base := "/sys/bus/pci/devices/" + d.address
		class := d.class
		if class == "" {
			class = "0x030000"
		}
		classes = append(classes, base+"/class")
		files[base+"/class"], files[base+"/vendor"], files[base+"/current_link_width"] = class, d.vendor, d.link
		files[base+"/device"] = "0x1234"
		if !d.usable {
			continue
		}
		switch d.vendor {
		case "0x10de":
			cuda = append(cuda, "GPU-"+d.address+", driver, "+d.address)
		case "0x8086":
			node := fmt.Sprintf("renderD%d", 128+len(nodes))
			nodes = append(nodes, "/dev/dri/"+node)
			for _, attr := range []string{"vendor", "current_link_width", "device"} {
				files["/sys/class/drm/"+node+"/device/"+attr] = files[base+"/"+attr]
			}
			intel = append(intel, map[string]string{"identity": "uuid-" + d.address, "fingerprint": "driver", "pci_address": d.address})
			intelName = d.name
		}
	}
	p := MachineProbe{OS: "linux", Arch: "amd64", Glob: func(pattern string) ([]string, error) {
		if strings.Contains(pattern, "renderD") {
			return nodes, nil
		}
		return classes, nil
	}, ReadFile: func(path string) ([]byte, error) {
		if s, ok := files[path]; ok {
			return []byte(s), nil
		}
		return nil, os.ErrNotExist
	}}
	p.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "nvidia-smi":
			if len(cuda) == 0 {
				return nil, exec.ErrNotFound
			}
			if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
				return []byte(strings.Join(cuda, "\n")), nil
			}
			return []byte("NVIDIA GPU, 24576"), nil
		case "sycl-ls":
			if len(intel) == 0 {
				return nil, exec.ErrNotFound
			}
			if len(args) > 0 && args[0] == "--verbose" {
				return []byte("global_mem_size: 17179869184"), nil
			}
			return []byte("[level_zero:gpu:0] " + intelName), nil
		}
		return nil, exec.ErrNotFound
	}
	p.CommandEnv = func(ctx context.Context, _ map[string]string, name string, args ...string) ([]byte, error) {
		if name == "python3" {
			return json.Marshal(intel)
		}
		return p.Command(ctx, name, args...)
	}
	return p
}

func TestOnboardingMixedDriverlessIntel(t *testing.T) {
	p := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x10de", "16", "NVIDIA GPU", "", true}, pciFixture{"0000:02:00.0", "0x8086", "8", "Intel Arc GPU", "", false})
	_, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	requirePreflightCode(t, err, "gpu_driver_missing")
}

func TestOnboardingPCIMatrix(t *testing.T) {
	nvidia := pciFixture{"0000:01:00.0", "0x10de", "16", "NVIDIA GPU", "", true}
	discrete := pciFixture{"0000:02:00.0", "0x8086", "8", "Intel Arc GPU", "", true}
	integrated := pciFixture{"0000:00:02.0", "0x8086", "0", "Intel(R) Graphics [0x7d55]", "", true}
	arcIntegrated := integrated
	arcIntegrated.name = "Intel(R) Arc(TM) Graphics"
	missing := discrete
	missing.usable = false
	bmc := pciFixture{"0000:03:00.0", "0x1a03", "1", "ASPEED", "", false}
	for _, tc := range []struct {
		name    string
		devices []pciFixture
		modes   []string
		code    string
	}{
		{"nvidia_driverless_intel", []pciFixture{nvidia, missing}, nil, "gpu_driver_missing"},
		{"integrated_nvidia", []pciFixture{integrated, nvidia}, []string{"dedicated", "unified"}, ""},
		{"arc_discrete", []pciFixture{discrete}, []string{"fixed"}, ""},
		{"arc_integrated", []pciFixture{arcIntegrated}, []string{"unified"}, ""},
		{"generic_integrated", []pciFixture{integrated}, []string{"unified"}, ""},
		{"bmc", nil, nil, ""},
	} {
		for _, withBMC := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/bmc=%v", tc.name, withBMC), func(t *testing.T) {
				devices := append([]pciFixture(nil), tc.devices...)
				if withBMC {
					devices = append(devices, bmc)
				}
				got, err := PreflightHardware(context.Background(), ServiceConfig{}, pciFixtureProbe(devices...))
				if tc.code != "" {
					requirePreflightCode(t, err, tc.code)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(got.Devices) != len(tc.modes) {
					t.Fatalf("devices=%+v want modes=%v", got.Devices, tc.modes)
				}
				if len(tc.modes) == 0 && got.Platform != "cpu" {
					t.Fatal(got)
				}
				for i, mode := range tc.modes {
					if got.Devices[i].Detection.MemoryMode != mode {
						t.Fatalf("device %d=%+v want %s", i, got.Devices[i], mode)
					}
				}
			})
		}
	}
}

func TestDetectIntelMemoryUsesPCITopology(t *testing.T) {
	for _, name := range []string{"Intel(R) Arc(TM) Graphics", "Intel(R) Graphics [0x7d55]"} {
		for _, verbose := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/verbose=%v", name, verbose), func(t *testing.T) {
				p := pciFixtureProbe(pciFixture{"0000:00:02.0", "0x8086", "0", name, "", true})
				command := p.Command
				p.Command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
					if !verbose && len(args) > 0 && args[0] == "--verbose" {
						return nil, exec.ErrNotFound
					}
					return command(ctx, n, args...)
				}
				got, err := p.Detect(context.Background())
				if err != nil || got.MemoryMode != "unified" || got.MemoryGB != 0 || got.ReserveGB != 8 {
					t.Fatalf("%+v %v", got, err)
				}
			})
		}
	}
}

func withIntegratedPCI(p MachineProbe) MachineProbe {
	fixture := pciFixtureProbe(pciFixture{"0000:00:02.0", "0x8086", "0", "Intel Graphics", "", true})
	p.Glob, p.ReadFile = fixture.Glob, fixture.ReadFile
	p = withIntelRuntimeFixture(p)
	return p
}

func TestOnboardingSameFamilyCompletenessAndTopology(t *testing.T) {
	integrated := pciFixture{"0000:00:02.0", "0x8086", "0", "Intel Arc GPU", "", true}
	discrete := pciFixture{"0000:02:00.0", "0x8086", "8", "Intel UHD GPU", "0x120000", true}
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			discrete.usable = !missing
			p := pciFixtureProbe(integrated, discrete)
			got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
			if missing {
				requirePreflightCode(t, err, "gpu_driver_missing")
				return
			}
			if err != nil || len(got.Devices) != 2 || got.Devices[0].Detection.MemoryMode != "unified" || got.Devices[1].Detection.MemoryMode != "fixed" {
				t.Fatalf("%+v %v", got, err)
			}
			// Foreground detection follows the same selected runtime/PCI identity.
			d, err := p.DetectDevice(context.Background(), "1")
			if err != nil || d.MemoryMode != "fixed" {
				t.Fatalf("%+v %v", d, err)
			}
		})
	}
}

func TestOnboardingFamilyFailureRetainsOtherInventory(t *testing.T) {
	p := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x10de", "16", "NVIDIA", "", true}, pciFixture{"0000:02:00.0", "0x8086", "8", "Intel Arc GPU", "", true})
	command := p.Command
	p.Command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
		if n == "nvidia-smi" {
			return nil, os.ErrPermission
		}
		return command(ctx, n, args...)
	}
	devices, err := p.Accelerators(context.Background())
	requirePreflightCode(t, err, "gpu_driver_unavailable")
	if len(devices) != 1 || devices[0].Platform != "xpu" {
		t.Fatalf("other family hidden: %+v", devices)
	}
	p.Command = command
	p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
		return nil, os.ErrPermission
	}
	devices, err = p.Accelerators(context.Background())
	requirePreflightCode(t, err, "gpu_driver_unavailable")
	if len(devices) != 1 || devices[0].Platform != "cuda" {
		t.Fatalf("other family hidden: %+v", devices)
	}
}

func TestOnboardingTopologyEvidenceAndAmbiguity(t *testing.T) {
	for _, tc := range []struct{ name, localMem, link, mode, code string }{
		{"integrated_local_memory", "0", "missing", "unified", ""},
		{"discrete_local_memory", "17179869184", "0", "fixed", ""},
		{"unknown_topology", "missing", "missing", "", "gpu_memory_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := pciFixtureProbe(pciFixture{"0000:00:02.0", "0x8086", "0", "Intel Arc GPU", "", true})
			read := p.ReadFile
			p.ReadFile = func(path string) ([]byte, error) {
				if strings.HasSuffix(path, "/local_mem") {
					if tc.localMem == "missing" {
						return nil, os.ErrNotExist
					}
					return []byte(tc.localMem), nil
				}
				if strings.HasSuffix(path, "/current_link_width") {
					if tc.link == "missing" {
						return nil, os.ErrNotExist
					}
					return []byte(tc.link), nil
				}
				return read(path)
			}
			got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
			if tc.code != "" {
				requirePreflightCode(t, err, tc.code)
				return
			}
			if err != nil || got.MemoryMode != tc.mode {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
	p := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x8086", "8", "Intel GPU", "", true}, pciFixture{"0000:02:00.0", "0x8086", "8", "Intel GPU", "", true})
	env := p.CommandEnv
	p.CommandEnv = func(ctx context.Context, e map[string]string, n string, args ...string) ([]byte, error) {
		if n == "python3" {
			return []byte(`[{"identity":"a","fingerprint":"driver"},{"identity":"b","fingerprint":"driver"}]`), nil
		}
		return env(ctx, e, n, args...)
	}
	_, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	requirePreflightCode(t, err, "gpu_inventory_invalid")
}

func TestDetectCUDAMemoryUsesPCITopology(t *testing.T) {
	for _, tc := range []struct{ name, link, mode string }{
		{"Generic NVIDIA", "0", "unified"},
		{"NVIDIA GB10", "16", "dedicated"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			p := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x10de", tc.link, tc.name, "", true})
			p.Command = func(_ context.Context, _ string, args ...string) ([]byte, error) {
				if strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
					return []byte("GPU-fixture, driver, 00000000:01:00.0"), nil
				}
				return []byte(tc.name + ", 24576, 00000000:01:00.0"), nil
			}
			got, err := p.Detect(context.Background())
			if err != nil || got.MemoryMode != tc.mode {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
}

func withDiscretePCI(p MachineProbe) MachineProbe {
	fixture := pciFixtureProbe(pciFixture{"0000:01:00.0", "0x8086", "8", "Intel GPU", "", true})
	p.Glob, p.ReadFile = fixture.Glob, fixture.ReadFile
	p = withIntelRuntimeFixture(p)
	return p
}

// Legacy detection fixtures now provide runtime UUIDs as well as memory text.
// Their assertions stay unchanged; non-Level-Zero text cannot invent a device.
func withIntelRuntimeFixture(p MachineProbe) MachineProbe {
	p.CommandEnv = func(ctx context.Context, _ map[string]string, name string, args ...string) ([]byte, error) {
		if name == "python3" {
			b, err := p.Command(ctx, "sycl-ls")
			if err != nil {
				return nil, err
			}
			if !strings.Contains(string(b), "[level_zero:gpu") {
				return []byte("[]"), nil
			}
			return []byte(`[{"identity":"intel-fixture","fingerprint":"driver:Intel GPU"}]`), nil
		}
		return p.Command(ctx, name, args...)
	}
	return p
}

func withCUDAFacts(p MachineProbe, integrated bool, count int) MachineProbe {
	devices := []pciFixture{}
	link := "16"
	if integrated {
		link = "0"
	}
	for i := 1; i <= count; i++ {
		devices = append(devices, pciFixture{fmt.Sprintf("0000:%02x:00.0", i), "0x10de", link, "fixture", "", true})
	}
	facts := pciFixtureProbe(devices...)
	p.Glob, p.ReadFile = facts.Glob, facts.ReadFile
	return p
}
