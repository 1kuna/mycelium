package myce

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// pciAccelerator contains only physical facts. Runtime names never determine
// topology, support or memory mode. Unknown topology stays unknown.
type pciAccelerator struct {
	address, vendor, device   string
	integrated, topologyKnown bool
}

func (d pciAccelerator) platform() string {
	switch d.vendor {
	case "0x10de":
		return "cuda"
	case "0x8086":
		return "xpu"
	}
	return ""
}

// withFilesystem adapts both fixture APIs to one reader; neither falls back to
// the host. LocalMachineProbe alone opts into the real root filesystem.
func (p MachineProbe) withFilesystem() MachineProbe {
	if p.Files != nil {
		if p.Glob == nil {
			p.Glob = func(pattern string) ([]string, error) {
				paths, err := fs.Glob(p.Files, strings.TrimPrefix(pattern, "/"))
				for i := range paths {
					paths[i] = "/" + paths[i]
				}
				return paths, err
			}
		}
		if p.ReadFile == nil {
			p.ReadFile = func(path string) ([]byte, error) { return fs.ReadFile(p.Files, strings.TrimPrefix(path, "/")) }
		}
	}
	return p
}

func (p MachineProbe) pciAccelerators() ([]pciAccelerator, error) {
	p = p.withFilesystem()
	if p.OS != "linux" {
		return nil, nil
	}
	if p.Glob == nil {
		return nil, preflightError("gpu_inventory_unavailable", "gpu", "PCI filesystem unavailable")
	}
	paths, err := p.Glob("/sys/bus/pci/devices/*/class")
	if err != nil {
		return nil, preflightError("gpu_inventory_unavailable", "gpu", "cannot inspect PCI inventory")
	}
	out := []pciAccelerator{}
	var failures []error
	for _, path := range paths {
		if p.ReadFile == nil {
			failures = append(failures, preflightError("gpu_inventory_unavailable", "gpu", "PCI reader missing"))
			continue
		}
		b, err := p.ReadFile(path)
		class, e := strconv.ParseUint(strings.TrimSpace(string(b)), 0, 32)
		if err != nil || e != nil {
			failures = append(failures, preflightError("gpu_inventory_unavailable", filepath.Base(filepath.Dir(path)), "cannot inspect PCI class"))
			continue
		}
		if class>>16 != 3 && class>>16 != 0x12 {
			continue
		}
		dir := filepath.Dir(path)
		vendor, e := p.ReadFile(filepath.Join(dir, "vendor"))
		if e != nil {
			failures = append(failures, preflightError("gpu_inventory_unavailable", filepath.Base(dir), "cannot inspect PCI vendor"))
			continue
		}
		device, _ := p.ReadFile(filepath.Join(dir, "device"))
		integrated, known, e := p.pciTopology(dir)
		// Inaccessible topology affects this endpoint's memory evidence only.
		if e != nil {
			known = false
		}
		out = append(out, pciAccelerator{filepath.Base(dir), strings.ToLower(strings.TrimSpace(string(vendor))), strings.TrimSpace(string(device)), integrated, known})
	}
	return out, errors.Join(failures...)
}

func (p MachineProbe) pciTopology(dir string) (integrated, known bool, err error) {
	if p.ReadFile == nil {
		return false, false, nil
	}
	// Local device memory is direct evidence; zero means system memory. Intel's
	// DRM ABI exposes this on devices which implement local_mem.
	if b, e := p.ReadFile(filepath.Join(dir, "local_mem")); e == nil {
		n, e := strconv.ParseUint(strings.TrimSpace(string(b)), 0, 64)
		if e != nil {
			return false, false, e
		}
		return n == 0, true, nil
	} else if !os.IsNotExist(e) {
		return false, false, e
	}
	// A PCIe link belongs to a discrete endpoint. Root-complex integrated
	// graphics report zero (or do not expose this attribute at all).
	if b, e := p.ReadFile(filepath.Join(dir, "current_link_width")); e == nil {
		n, e := strconv.Atoi(strings.TrimSpace(string(b)))
		if e != nil || n < 0 {
			return false, false, fmt.Errorf("invalid PCI link width")
		}
		return n == 0, true, nil
	} else if !os.IsNotExist(e) {
		return false, false, e
	}
	return false, false, nil
}

// Match physical/runtime identities before selectors or saved budgets are
// applied. A single-device legacy runtime can omit its PCI address; ambiguous
// multi-device correlation is refused instead of inferred from runtime order.
func reconcilePCI(pci []pciAccelerator, runtime []MachineDevice) (map[string]pciAccelerator, error) {
	matched := map[string]bool{}
	facts := map[string]pciAccelerator{}
	var failures []error
	for _, r := range runtime {
		candidates := []pciAccelerator{}
		for _, d := range pci {
			if d.platform() == r.Platform && (r.pciAddress == "" || normalizePCIAddress(r.pciAddress) == normalizePCIAddress(d.address)) {
				candidates = append(candidates, d)
			}
		}
		if len(candidates) == 0 && len(pci) == 0 {
			continue
		} // Non-PCI platforms / injected runtime-only probes.
		if len(candidates) != 1 || matched[candidates[0].address] {
			failures = append(failures, preflightError("gpu_inventory_invalid", r.Platform, "runtime components cannot be matched uniquely to PCI hardware; restore complete inventory and re-detect"))
			continue
		}
		d := candidates[0]
		matched[d.address] = true
		facts[r.Platform+":"+r.Selector] = d
	}
	for _, d := range pci {
		if d.platform() == "" || matched[d.address] {
			continue
		}
		if d.platform() == "xpu" && d.topologyKnown && d.integrated {
			continue
		} // Optional absent iGPU runtime; existing CPU-only contract.
		failures = append(failures, preflightError("gpu_driver_missing", d.address, "GPU hardware has no supported runtime inventory; repair the driver and re-detect"))
	}
	return facts, errors.Join(failures...)
}
func normalizePCIAddress(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	parts := strings.Split(s, ":")
	if len(parts) == 3 {
		if domain, e := strconv.ParseUint(parts[0], 16, 32); e == nil {
			return fmt.Sprintf("%04x:%s:%s", domain, parts[1], parts[2])
		}
	}
	return s
}
