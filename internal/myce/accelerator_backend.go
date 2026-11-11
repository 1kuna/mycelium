package myce

import "context"

// acceleratorBackend is the backend contract. Keep vendor choices here; callers
// consume operations and capabilities, never infer them from a platform name.
// Hardware probes remain injected through MachineProbe and run only in workers.
type acceleratorBackend struct {
	platform, pciVendor                          string
	extensionDevice, resourceName, placementName string
	applicationDevice, variantDevice             string
	cpu, simulated                               bool

	// Memory accounting policy is separate from the device's observed topology.
	unifiedMemory, autoUnifiedMemory, fixedMemoryDefault bool
	deviceHostMemory, unifiedProcessAdmission            bool
	memoryCommand, memorySelectorFlag                    string
	liveFreeMemory                                       func(context.Context, func(context.Context, string, ...string) ([]byte, error)) (float64, error)
	attemptMemory                                        func(Worker, context.Context, map[int]bool, memoryFS, *float64, ...map[int]bool) (float64, bool, error)
	measureMemory                                        func(context.Context, string, float64, func(context.Context, string, ...string) ([]byte, error)) (MemoryReading, error)
	resolveDevice                                        func(context.Context, string) (string, error)

	hostOS, hostArch          string
	telemetryPendingSource    string
	optionalIntegratedRuntime bool
	detect                    func(MachineProbe, context.Context, MachineDevice, Detection) (Detection, error)
	inventory                 func(MachineProbe, context.Context, []pciAccelerator) ([]MachineDevice, error)
	telemetry                 func(Worker, context.Context, MachineProbe, []MachineDevice)
	fingerprint               func(context.Context, MachineProbe, string, []byte, func(context.Context) ([]MachineDevice, error)) ([]byte, [][]string, error)

	visibleDevicesVariable, runtimeSelectorPrefix, onlyDevice string
	onlyDeviceError                                           string
	selectWithoutDevice, multipleDevices, isolateTiers        bool
	accessNodes, nativeReadRoot, isolationPresence            string
	nativeNodes                                               func(string, acceleratorNodeProbe) ([]string, error)
	cooperativeCPUIsolation                                   bool
	verifyCPUIsolation                                        func(MachineProbe) error

	prepareServiceProbe func(MachineProbe, string) (MachineProbe, error)

	faultReasons              []string
	faultMatcher              DeviceFaultMatcher
	repeatedTeardownAttention bool
	healthName                string
	health                    func(MachineProbe, context.Context, MachineDevice) (bool, error)
	healthReport              func(MachineProbe, context.Context, MachineDevice) (DeviceHealthReport, error)
}

// Order preserves CUDA-before-XPU inventory, environment recipe resolution and
// partial-probe diagnostics. CPU and fake have no physical discovery operation.
var acceleratorBackends []acceleratorBackend

// Assign in init because probe functions themselves look up backend policy.
func init() {
	acceleratorBackends = []acceleratorBackend{
		{
			memoryCommand: "nvidia-smi", memorySelectorFlag: "--id=",
			platform: "cuda", pciVendor: "0x10de", extensionDevice: "cuda:0", resourceName: "NVIDIA (CUDA)",
			autoUnifiedMemory: true, isolateTiers: true, multipleDevices: true, unifiedProcessAdmission: true,
			resolveDevice: ResolveCUDADevice, attemptMemory: cudaAttemptDeviceMemory, measureMemory: measureCUDADeviceMemory,
			nativeReadRoot: "/proc/driver/nvidia/gpus", isolationPresence: "dev/nvidiactl",
			visibleDevicesVariable: "CUDA_VISIBLE_DEVICES", accessNodes: "/dev/nvidia[0-9]*",
			detect: detectCUDAAccelerator, inventory: cudaAccelerators, liveFreeMemory: cudaFreeMemory,
			nativeNodes: cudaDeviceNodes, verifyCPUIsolation: verifyCUDASelectNone, fingerprint: cudaFingerprint, faultReasons: []string{"device_fault_nvidia_xid"},
			faultMatcher: cudaKernelFault, healthName: "nvidia-smi recovery action", health: cudaDeviceHealth,
		},
		{
			platform: "xpu", pciVendor: "0x8086", extensionDevice: "xpu:0", resourceName: "Intel (XPU)",
			telemetryPendingSource: "XPU telemetry not yet sampled",
			fixedMemoryDefault:     true, optionalIntegratedRuntime: true, isolateTiers: true, multipleDevices: true, deviceHostMemory: true, repeatedTeardownAttention: true,
			attemptMemory: xpuAttemptDeviceMemory, measureMemory: measureXPUDeviceMemory, isolationPresence: "dev/dri/renderD*",
			visibleDevicesVariable: "ZE_AFFINITY_MASK", runtimeSelectorPrefix: "level_zero:", accessNodes: "/dev/dri/renderD*",
			detect: detectXPUAccelerator, inventory: xpuAccelerators, prepareServiceProbe: xpuServiceProbe,
			nativeNodes: xpuDeviceNodes, verifyCPUIsolation: verifyXPUSelectNone, fingerprint: xpuFingerprint, faultReasons: []string{"device_fault_xe", "device_fault_i915"},
			faultMatcher: intelKernelFault, healthName: "xpu-smi health", health: xpuDeviceHealth,
			healthReport: MachineProbe.XPUHealthReport, telemetry: Worker.sampleXPUTelemetry,
		},
		{
			platform: "metal", extensionDevice: "mps", resourceName: "Apple Metal",
			unifiedMemory: true, onlyDevice: "0", onlyDeviceError: "Metal supports only device 0", cooperativeCPUIsolation: true, applicationDevice: "mps (queue backend metal)", variantDevice: "mps or MLX (queue backend metal)", inventory: metalAccelerators, hostOS: "darwin", hostArch: "arm64",
			verifyCPUIsolation: verifyMetalSelectNone, fingerprint: metalFingerprint,
		},
		{
			platform: "fake", extensionDevice: "cpu", resourceName: "test resource (fake)", placementName: "cpu-fake",
			simulated: true, selectWithoutDevice: true, multipleDevices: true, verifyCPUIsolation: verifySimulatedSelectNone, fingerprint: fakeFingerprint,
		},
		{
			platform: "cpu", extensionDevice: "cpu", resourceName: "CPU", cpu: true, selectWithoutDevice: true, fingerprint: cpuFingerprint,
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

// ValidPlatform reports whether a queue backend name is known (including CPU and fake).
func ValidPlatform(platform string) bool { return validGPU(platform) }

// PhysicalAcceleratorPlatform excludes the explicit CPU and simulated execution modes.
func PhysicalAcceleratorPlatform(platform string) bool {
	b := backendFor(platform)
	return b.platform != "" && !b.cpu && !b.simulated
}

// AcceleratorMemoryMeasured describes the live-memory source selected by a worker's configuration.
func AcceleratorMemoryMeasured(platform, mode string) bool {
	b := backendFor(platform)
	return b.platform != "" && mode != "fixed" && !b.cpu && !b.simulated && (!b.fixedMemoryDefault || mode == "unified")
}
