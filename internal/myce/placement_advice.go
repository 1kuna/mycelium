package myce

import "strings"

// PlacementAdvisories explains authored pins without changing frozen intent or
// claiming that a selector proves a hardware requirement. Never echo identifiers
// or free-form reasons: advice can be copied into public support reports.
func PlacementAdvisories(s Spec, reasons map[string]string, variants ...ExtensionVariant) []string {
	if s.ExtensionCheck != nil {
		return nil // Qualification intentionally selects exact hardware.
	}
	var advice []string
	if len(s.AllowedMachines) > 0 && strings.TrimSpace(reasons["host"]) == "" {
		advice = append(advice, "Host pin has no declared host requirement. If the software only needs a platform, accelerator, memory or environment, omit allowed_machines and declare needs, compatible targets and environment coverage; Mycelium chooses the host. Retain a pin for specific hardware, approved production parity or hardware qualification.")
	}
	devicePinned := s.Needs.GPU != nil && len(s.Needs.GPU.Devices) > 0
	for _, v := range variants {
		devicePinned = devicePinned || v.Needs.GPU != nil && len(v.Needs.GPU.Devices) > 0
	}
	if devicePinned && strings.TrimSpace(reasons["device"]) == "" {
		advice = append(advice, "Device pin has no declared device requirement. If any compatible accelerator suffices, omit needs.gpu.devices and use needs.gpu.count: 1 with the required backend and memory; consume MYCE_DEVICE and assigned visibility. Retain an exact selector only when the work requires that device.")
	}
	return advice
}
