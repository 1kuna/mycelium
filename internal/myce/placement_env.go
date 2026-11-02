package myce

import (
	"strconv"
	"strings"
)

// Placement is worker-owned and cannot be overridden by the payload/catalog.
func placementEnvironment(m Machine) map[string]string {
	platform := m.GPU
	if platform == "fake" {
		platform = "cpu-fake"
	}
	// MYCE_GPU_COUNT is the number of visible cards, for example vLLM's
	// --tensor-parallel-size; HF device_map="auto" uses every visible card.
	devices, count := m.Device, "1"
	if len(m.group) > 1 {
		devices, count = strings.Join(m.group, ","), strconv.Itoa(len(m.group))
	} else if m.GPU == "cpu" {
		count = "0"
	}
	return map[string]string{"MYCE_DEVICE": ExtensionDevice(m.GPU), "MYCE_PLATFORM": platform, "MYCE_DEVICES": devices, "MYCE_GPU_COUNT": count, "MYCE_MACHINE": m.ID}
}
func withPlacementEnvironment(model Model, m Machine) Model {
	env := map[string]string{}
	for k, v := range model.DeviceEnv {
		env[k] = v
	}
	for k, v := range placementEnvironment(m) {
		env[k] = v
	}
	model.DeviceEnv = env
	return model
}
