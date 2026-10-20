package myce

// Placement is worker-owned and cannot be overridden by the payload/catalog.
func placementEnvironment(m Machine) map[string]string {
	platform := m.GPU
	if platform == "fake" {
		platform = "cpu-fake"
	}
	return map[string]string{"MYCE_DEVICE": ExtensionDevice(m.GPU), "MYCE_PLATFORM": platform, "MYCE_DEVICES": m.Device, "MYCE_MACHINE": m.ID}
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
