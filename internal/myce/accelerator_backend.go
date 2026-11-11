package myce

import "context"

// acceleratorBackend is the backend contract. Keep vendor choices here; callers
// consume operations and capabilities, never infer them from a platform name.
// Hardware probes remain injected through MachineProbe and run only in workers.
type acceleratorBackend struct {
	platform, pciVendor                                         string
	extensionDevice, resourceName, placementName                string
	cpu, simulated, unifiedMemory, autoUnifiedMemory            bool
	fixedMemoryDefault, optionalIntegratedRuntime, isolateTiers bool
	accessNodes                                                 string

	liveFreeMemory   func(context.Context, func(context.Context, string, ...string) ([]byte, error)) (float64, error)
	hostOS, hostArch string
	detect           func(MachineProbe, context.Context, MachineDevice, Detection) (Detection, error)
	inventory        func(MachineProbe, context.Context, []pciAccelerator) ([]MachineDevice, error)
	telemetry        func(Worker, context.Context, MachineProbe, []MachineDevice)

	faultReasons []string
	faultMatcher DeviceFaultMatcher
	healthName   string
	health       func(MachineProbe, context.Context, MachineDevice) (bool, error)
	healthReport func(MachineProbe, context.Context, MachineDevice) (DeviceHealthReport, error)
}

// Order preserves CUDA-before-XPU inventory, environment recipe resolution and
// partial-probe diagnostics. CPU and fake have no physical discovery operation.
var acceleratorBackends []acceleratorBackend

// Assign in init because probe functions themselves look up backend policy.
func init() {
	acceleratorBackends = []acceleratorBackend{
		{
			platform: "cuda", pciVendor: "0x10de", extensionDevice: "cuda:0", resourceName: "NVIDIA (CUDA)",
			autoUnifiedMemory: true, isolateTiers: true, accessNodes: "/dev/nvidia[0-9]*",
			detect: detectCUDAAccelerator, inventory: cudaAccelerators, liveFreeMemory: cudaFreeMemory,
			faultReasons: []string{"device_fault_nvidia_xid"}, faultMatcher: cudaKernelFault,
			healthName: "nvidia-smi recovery action", health: cudaDeviceHealth,
		},
		{
			platform: "xpu", pciVendor: "0x8086", extensionDevice: "xpu:0", resourceName: "Intel (XPU)",
			fixedMemoryDefault: true, optionalIntegratedRuntime: true, isolateTiers: true, accessNodes: "/dev/dri/renderD*",
			detect: detectXPUAccelerator, inventory: xpuAccelerators, telemetry: Worker.sampleXPUTelemetry,
			faultReasons: []string{"device_fault_xe", "device_fault_i915"}, faultMatcher: intelKernelFault,
			healthName: "xpu-smi health", health: xpuDeviceHealth, healthReport: MachineProbe.XPUHealthReport,
		},
		{
			platform: "metal", extensionDevice: "mps", resourceName: "Apple Metal", unifiedMemory: true,
			inventory: metalAccelerators, hostOS: "darwin", hostArch: "arm64",
		},
		{
			platform: "fake", extensionDevice: "cpu", resourceName: "test resource (fake)",
			placementName: "cpu-fake", simulated: true,
		},
		{
			platform: "cpu", extensionDevice: "cpu", resourceName: "CPU", cpu: true,
		},
	}
}

// Unknown names retain a zero descriptor and are rejected at the same public
// validation boundaries. The registry is immutable after initialization.
func backendFor(platform string) acceleratorBackend {
	for _, backend := range acceleratorBackends {
		if backend.platform == platform {
			return backend
		}
	}
	return acceleratorBackend{}
}

func validGPU(platform string) bool { return backendFor(platform).platform != "" }
