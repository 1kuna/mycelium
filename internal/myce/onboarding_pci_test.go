package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
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

func withIntegratedPCI(p MachineProbe) MachineProbe {
	fixture := pciFixtureProbe(pciFixture{"0000:00:02.0", "0x8086", "0", "Intel Graphics", "", true})
	p.Glob, p.ReadFile = fixture.Glob, fixture.ReadFile
	p = withIntelRuntimeFixture(p)
	return p
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
