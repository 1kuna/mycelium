package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// PreflightError contains a stable reason and a safe operator action. Probe
// output and command environments are deliberately never included.
type PreflightError struct{ Code, Component, Message string }

func (e *PreflightError) Error() string { return e.Code + ": " + e.Component + ": " + e.Message }
func preflightError(code, component, message string) error {
	return &PreflightError{code, component, message}
}
func probeAbsent(err error) bool {
	return errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrNoGPU)
}

// gpuPresent recognizes NVIDIA display controllers and Intel discrete PCIe
// display endpoints. Linux's current_link_width PCI sysfs ABI is positive for
// a discrete PCIe link; integrated root-complex graphics have no such link.
// Runtime inventory remains authoritative for enrolling an accelerator.
func (p MachineProbe) gpuPresent() (bool, error) {
	if p.OS != "linux" || p.Glob == nil {
		return false, nil
	}
	paths, err := p.Glob("/sys/bus/pci/devices/*/class")
	if err != nil {
		return false, err
	}
	present := false
	for _, path := range paths {
		if p.ReadFile == nil {
			return false, fmt.Errorf("PCI reader missing")
		}
		b, err := p.ReadFile(path)
		if err != nil {
			return false, err
		}
		if !strings.HasPrefix(strings.TrimSpace(string(b)), "0x03") {
			continue
		}
		dir := filepath.Dir(path)
		vendor, err := p.ReadFile(filepath.Join(dir, "vendor"))
		if err != nil {
			return false, err
		}
		v := strings.TrimSpace(string(vendor))
		supported := v == "0x10de" || v == "0x8086" && p.intelDiscrete(dir)
		if supported {
			present = true
			continue
		}
		device, _ := p.ReadFile(filepath.Join(dir, "device"))
		if p.Info != nil && v != "0x8086" {
			p.Info(fmt.Sprintf("Ignoring display device %s (PCI %s:%s); no supported accelerator backend", filepath.Base(dir), v, strings.TrimSpace(string(device))))
		}
	}
	return present, nil
}

// NVIDIA tools can remain installed on hosts with only a BMC or other GPU.
func (p MachineProbe) nvidiaPresent() (bool, error) {
	if p.OS != "linux" || p.Glob == nil {
		// Without PCI inventory, absence cannot excuse a failing driver tool.
		return true, nil
	}
	paths, err := p.Glob("/sys/bus/pci/devices/*/class")
	if err != nil {
		return false, err
	}
	for _, path := range paths {
		if p.ReadFile == nil {
			return false, fmt.Errorf("PCI reader missing")
		}
		class, err := p.ReadFile(path)
		if err != nil {
			return false, err
		}
		if !strings.HasPrefix(strings.TrimSpace(string(class)), "0x03") {
			continue
		}
		vendor, err := p.ReadFile(filepath.Join(filepath.Dir(path), "vendor"))
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(string(vendor)) == "0x10de" {
			return true, nil
		}
	}
	return false, nil
}
func (p MachineProbe) nvidiaProbeFailure(present bool) error {
	if present {
		return preflightError("gpu_driver_unavailable", "cuda", "NVIDIA driver probe failed; repair the driver and re-detect")
	}
	if p.Info != nil {
		p.Info("nvidia-smi failed without an NVIDIA PCI display device; continuing CPU/other accelerator detection")
	}
	return nil
}

func (p MachineProbe) intelDiscrete(dir string) bool {
	if p.ReadFile == nil {
		return false
	}
	b, err := p.ReadFile(filepath.Join(dir, "current_link_width"))
	width, parseErr := strconv.Atoi(strings.TrimSpace(string(b)))
	return err == nil && parseErr == nil && width > 0
}

// HardwarePreflight is also the nonsecret detect --config reply. The scalar
// fields retain compatibility; Devices is the complete selected install plan.
type HardwarePreflight struct {
	Detection
	Devices []DeviceInstall `json:"devices,omitempty"`
}

// PreflightHardware reuses M2 physical identities and per-component detection.
// A saved CPU choice is explicit; callers clear Platform for re-detection.
// It is pure with respect to saved configuration and probes afresh each call.
func PreflightHardware(ctx context.Context, c ServiceConfig, p MachineProbe) (HardwarePreflight, error) {
	out := HardwarePreflight{Detection: c.Detection}
	if p.OS != "linux" && p.OS != "darwin" || p.Arch != "amd64" && p.Arch != "arm64" {
		return out, preflightError("hardware_unsupported", "host", "supported Linux/macOS architecture required")
	}
	if c.Detection.Platform == "cpu" {
		out.Detection = Detection{OS: p.OS, Arch: p.Arch, Platform: "cpu", MemoryMode: "fixed"}
		return out, nil
	}
	if c.AllowDeviceless && c.Detection.Platform == "fake" {
		return out, nil
	}
	allowance, err := deviceAllowance(c.Device)
	if err != nil {
		return out, err
	}
	physical, err := p.Accelerators(ctx)
	if err != nil {
		return out, err
	}
	if len(physical) == 0 {
		// Absence of a runtime is not absence of hardware.
		present, e := p.gpuPresent()
		if e != nil {
			return out, preflightError("gpu_inventory_unavailable", "gpu", "cannot inspect PCI inventory; restore access and re-detect")
		}
		if present {
			return out, preflightError("gpu_driver_missing", "gpu", "GPU hardware has no supported inventory; repair the driver and re-detect")
		}
		if c.Detection.Platform != "" {
			return out, preflightError("gpu_inventory_unavailable", "gpu", "configured GPU is absent; repair it and re-detect")
		}
		if len(allowance) > 0 {
			return out, preflightError("device_selector_unmatched", "gpu", "explicit selectors are absent; restore the selected components and re-detect")
		}
		out.Detection = Detection{OS: p.OS, Arch: p.Arch, Platform: "cpu", MemoryMode: "fixed"}
		return out, nil
	}
	matched := map[string]bool{}
	for _, device := range physical {
		selected := len(allowance) == 0
		for _, s := range allowance {
			if s == device.ID || s == device.Selector || s == device.Identity {
				selected = true
				matched[s] = true
			}
		}
		if !selected {
			continue
		}
		probe := p
		if device.Platform == "xpu" {
			command := p.Command
			probe.Command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
				if n == "nvidia-smi" {
					return nil, exec.ErrNotFound
				}
				return command(ctx, n, args...)
			}
		}
		d, e := probe.DetectDevice(ctx, device.Selector)
		var configured *Detection
		perDevice := false
		for _, saved := range c.MachineDevices {
			if saved.Selector == device.Selector || saved.Selector == device.Identity || saved.Selector == device.ID {
				v := saved.Detection
				configured = &v
				perDevice = true
				break
			}
		}
		if configured == nil && c.Detection.Platform != "" {
			v := c.Detection
			configured = &v
		}
		var typed *PreflightError
		fixed := configured != nil && configured.MemoryMode == "fixed" && configured.Platform == device.Platform && configured.MemoryGB > configured.ReserveGB
		if e != nil && !(fixed && device.Platform == "xpu" && errors.As(e, &typed) && typed.Code == "gpu_memory_unavailable") {
			return out, e
		}
		if configured != nil {
			if configured.Platform != device.Platform {
				return out, preflightError("gpu_inventory_invalid", device.ID, "configured platform differs from detected component")
			}
			if perDevice || configured.MemoryMode != "" && configured.MemoryMode != "auto" {
				d.MemoryMode, d.MemoryGB, d.ReserveGB = configured.MemoryMode, configured.MemoryGB, configured.ReserveGB
			} else {
				if configured.MemoryGB > 0 {
					d.MemoryGB = configured.MemoryGB
				}
				if configured.ReserveGB > 0 {
					d.ReserveGB = configured.ReserveGB
				}
			}
		}
		if d.Platform == "" {
			d.Platform = device.Platform
		}
		d.OS, d.Arch = p.OS, p.Arch
		out.Devices = append(out.Devices, DeviceInstall{Selector: device.Selector, Detection: d})
	}
	if len(matched) != len(allowance) {
		return out, preflightError("device_selector_unmatched", "gpu", "one or more explicit selectors are absent; selection was not broadened")
	}
	if len(out.Devices) == 0 {
		return out, preflightError("gpu_inventory_unavailable", "gpu", "no selected components detected")
	}
	if err := ValidateDeviceSettings(out.Devices); err != nil {
		return out, err
	}
	out.Detection = out.Devices[0].Detection
	return out, nil
}

// ApplyHardwarePreflight leaves identity, selector allowance and all host policy
// untouched. Existing per-device budgets win over scalar compatibility fields.
func ApplyHardwarePreflight(c ServiceConfig, p HardwarePreflight) ServiceConfig {
	c.Detection = p.Detection
	c.MachineDevices = slices.Clone(p.Devices)
	return c
}

// OverlayHardwareSettings applies explicit scalar memory flags to each offered
// component. An omitted flag never default-fills a saved per-device value.
func OverlayHardwareSettings(c ServiceConfig, explicit map[string]bool) ServiceConfig {
	c.MachineDevices = slices.Clone(c.MachineDevices)
	for i := range c.MachineDevices {
		d := &c.MachineDevices[i].Detection
		if explicit["memory-mode"] {
			d.MemoryMode = c.Detection.MemoryMode
		}
		if explicit["memory-gb"] {
			d.MemoryGB = c.Detection.MemoryGB
		}
		if explicit["reserve-memory-gb"] {
			d.ReserveGB = c.Detection.ReserveGB
		}
	}
	return c
}

func validateServiceHardware(c ServiceConfig, p HardwarePreflight) error {
	if p.Platform == "" || p.Platform != c.Detection.Platform {
		return fmt.Errorf("no device matching the configured GPU platform")
	}
	for _, expected := range c.MachineDevices {
		if !slices.ContainsFunc(p.Devices, func(d DeviceInstall) bool {
			return d.Selector == expected.Selector && d.Detection.Platform == expected.Detection.Platform && d.Detection.MemoryGB == expected.Detection.MemoryGB && d.Detection.ReserveGB == expected.Detection.ReserveGB && d.Detection.MemoryMode == expected.Detection.MemoryMode
		}) {
			return preflightError("service_inventory_incomplete", "gpu", "service cannot observe every configured component and budget")
		}
	}
	return nil
}

func withoutMyceToken(env []string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		if !strings.HasPrefix(entry, "MYCE_TOKEN=") {
			out = append(out, entry)
		}
	}
	return out
}
