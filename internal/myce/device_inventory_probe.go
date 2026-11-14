package myce

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
)

// Accelerators is the single device probe for onboarding, refresh and launch.
// Physical PCI facts establish vendors/topology; runtimes establish usability.
func (p MachineProbe) Accelerators(ctx context.Context) ([]MachineDevice, error) {
	p = p.withFilesystem()
	if p.OS != "linux" && p.OS != "darwin" || p.Arch != "amd64" && p.Arch != "arm64" {
		return nil, preflightError("hardware_unsupported", "host", "supported Linux/macOS architecture required")
	}
	if p.Command == nil {
		return nil, preflightError("gpu_inventory_unavailable", "gpu", "inventory command runner missing")
	}
	for _, backend := range acceleratorBackends {
		if backend.hostOS == p.OS && backend.hostArch == p.Arch && backend.inventory != nil {
			return backend.inventory(p, ctx, nil)
		}
	}
	pci, pciErr := p.pciAccelerators()
	failures := &acceleratorProbeError{}
	if pciErr != nil {
		failures.causes = append(failures.causes, pciErr)
	}
	for _, d := range pci {
		if d.platform() == "" && p.Info != nil {
			p.Info("Ignoring display device " + d.address + " (PCI " + d.vendor + ":" + d.device + "); no supported accelerator backend")
		}
	}
	out := []MachineDevice{}
	for _, backend := range acceleratorBackends {
		if backend.pciVendor == "" || backend.inventory == nil {
			continue
		}
		devices, err := backend.inventory(p, ctx, pci)
		out = append(out, devices...)
		if err != nil {
			if failures.families == nil {
				failures.families = map[string]bool{}
			}
			failures.families[backend.platform] = true
			failures.causes = append(failures.causes, err)
		}
	}
	facts, completeErr := reconcilePCI(pci, out)
	if completeErr != nil {
		failures.causes = append(failures.causes, completeErr)
	}
	if p.Info != nil {
		for _, device := range pci {
			if device.platform() == "" {
				continue
			}
			matched := false
			for _, fact := range facts {
				matched = matched || fact.address == device.address
			}
			if !matched {
				p.Info("PCI component " + device.address + " needs driver/runtime; no matching runtime identity")
			}
		}
	}
	for i := range out {
		d := &out[i]
		d.ID = strconv.Itoa(i)
		if fact, ok := facts[d.Platform+":"+d.Selector]; ok {
			d.pciFact = &fact
		} else if len(pci) > 0 {
			d.Reason = "gpu_inventory_invalid"
		}
	}
	if len(failures.families) > 0 || len(failures.causes) > 0 {
		return out, failures
	}
	return out, nil
}

func (p MachineProbe) levelZeroDevices(ctx context.Context) ([]MachineDevice, error) {
	if p.CommandEnv == nil {
		return nil, errors.New("Level Zero private environment runner unavailable")
	}
	b, err := p.CommandEnv(ctx, map[string]string{"ZE_ENABLE_PCI_ID_DEVICE_ORDER": "1", "ZE_AFFINITY_MASK": "", "ONEAPI_DEVICE_SELECTOR": ""}, "python3", "-c", levelZeroInventory)
	if err != nil {
		return nil, err
	}
	var devices []struct {
		Identity    string `json:"identity"`
		Fingerprint string `json:"fingerprint"`
		PCIAddress  string `json:"pci_address"`
		MemoryBytes uint64 `json:"memory_bytes"`
	}
	if err = json.Unmarshal(b, &devices); err != nil {
		return nil, err
	}
	out := []MachineDevice{}
	for i, d := range devices {
		out = append(out, MachineDevice{ID: strconv.Itoa(i), Platform: "xpu", Identity: d.Identity, Selector: strconv.Itoa(i), Fingerprint: d.Fingerprint, pciAddress: d.PCIAddress, TotalMemoryGB: float64(d.MemoryBytes) / (1024 * 1024 * 1024)})
	}
	if err = validateInventory(out); err != nil {
		return nil, err
	}
	return out, nil
}

// A partial probe error withholds only its failed families. The public message
// contains fixed reason codes, never subprocess output or host paths.
type acceleratorProbeError struct {
	families map[string]bool
	causes   []error
}

func (e *acceleratorProbeError) failed(platform string) bool {
	return e.families[platform]
}
func (e *acceleratorProbeError) Error() string {
	reasons := []string{}
	for _, backend := range acceleratorBackends {
		if e.failed(backend.platform) {
			reasons = append(reasons, backend.platform+": device_probe_unavailable")
		}
	}
	if len(reasons) == 0 && len(e.causes) > 0 {
		reasons = append(reasons, "gpu: device_probe_unavailable")
	}
	return strings.Join(reasons, "; ")
}
func (e *acceleratorProbeError) Unwrap() error {
	return errors.Join(errors.Join(e.causes...), reject("device_probe_unavailable", errors.New(e.Error())))
}

// ctypes is only a standard-library binding to the installed Level Zero API;
// it does not require torch, a model, or a separately managed service.
const levelZeroInventory = `
import ctypes as c, json, sys
try: z=c.CDLL("libze_loader.so.1")
except OSError: sys.exit(78)
def check(code):
    if code: raise RuntimeError("Level Zero inventory failed")
class Props(c.Structure):
    _fields_=[("stype",c.c_uint32),("pNext",c.c_void_p),("type",c.c_uint32),("vendorId",c.c_uint32),("deviceId",c.c_uint32),("flags",c.c_uint32),("subdeviceId",c.c_uint32),("coreClockRate",c.c_uint32),("maxMemAllocSize",c.c_uint64),("maxHardwareContexts",c.c_uint32),("maxCommandQueuePriority",c.c_uint32),("numThreadsPerEU",c.c_uint32),("physicalEUSimdWidth",c.c_uint32),("numEUsPerSubslice",c.c_uint32),("numSubslicesPerSlice",c.c_uint32),("numSlices",c.c_uint32),("timerResolution",c.c_uint64),("timestampValidBits",c.c_uint32),("kernelTimestampValidBits",c.c_uint32),("uuid",c.c_ubyte*16),("name",c.c_char*256)]
class MemoryProps(c.Structure):
    _fields_=[("stype",c.c_uint32),("pNext",c.c_void_p),("flags",c.c_uint32),("maxClockRate",c.c_uint32),("maxBusWidth",c.c_uint32),("totalSize",c.c_uint64),("name",c.c_char*256)]
def memory_bytes(device):
    count=c.c_uint32()
    if z.zeDeviceGetMemoryProperties(c.c_void_p(device),c.byref(count),None): return 0
    props=(MemoryProps*count.value)()
    for p in props: p.stype=7
    if z.zeDeviceGetMemoryProperties(c.c_void_p(device),c.byref(count),props): return 0
    return sum(p.totalSize for p in props)
class DriverProps(c.Structure):
    _fields_=[("stype",c.c_uint32),("pNext",c.c_void_p),("uuid",c.c_ubyte*16),("driverVersion",c.c_uint32)]
class PCIAddress(c.Structure):
    _fields_=[("domain",c.c_uint32),("bus",c.c_uint32),("device",c.c_uint32),("function",c.c_uint32)]
class PCISpeed(c.Structure):
    _fields_=[("genVersion",c.c_int32),("width",c.c_int32),("maxBandwidth",c.c_int64)]
class PCIProps(c.Structure):
    _fields_=[("stype",c.c_uint32),("pNext",c.c_void_p),("address",PCIAddress),("maxSpeed",PCISpeed)]
def pci_address(device):
    if not hasattr(z,"zeDevicePciGetPropertiesExt"): return ""
    pci=PCIProps(); pci.stype=0x10008
    if z.zeDevicePciGetPropertiesExt(c.c_void_p(device),c.byref(pci)): return ""
    a=pci.address
    return "%04x:%02x:%02x.%x" % (a.domain,a.bus,a.device,a.function)
check(z.zeInit(1))
n=c.c_uint32(); check(z.zeDriverGet(c.byref(n),None))
drivers=(c.c_void_p*n.value)(); check(z.zeDriverGet(c.byref(n),drivers))
result=[]
for driver in drivers:
    dp=DriverProps(); dp.stype=1
    check(z.zeDriverGetProperties(c.c_void_p(driver),c.byref(dp)))
    count=c.c_uint32(); check(z.zeDeviceGet(c.c_void_p(driver),c.byref(count),None))
    devices=(c.c_void_p*count.value)(); check(z.zeDeviceGet(c.c_void_p(driver),c.byref(count),devices))
    for device in devices:
        p=Props(); p.stype=3
        check(z.zeDeviceGetProperties(c.c_void_p(device),c.byref(p)))
        if p.type==1: result.append(dict(identity=bytes(p.uuid).hex(),fingerprint=str(dp.driverVersion)+":"+bytes(p.name).decode(),pci_address=pci_address(device),memory_bytes=memory_bytes(device)))
# A cross-driver ordinal is ambiguous for the affinity selector.
if len(drivers)>1 and result: raise RuntimeError("ambiguous Level Zero drivers")
print(json.dumps(result))
`

func metalAccelerators(p MachineProbe, ctx context.Context, _ []pciAccelerator) ([]MachineDevice, error) {

	b, err := p.Command(ctx, "/usr/sbin/system_profiler", "SPDisplaysDataType")
	if err != nil || !strings.Contains(string(b), "Metal") {
		return nil, preflightError("gpu_driver_unavailable", "metal", "Apple Silicon Metal unavailable; repair runtime access and re-detect")
	}
	return []MachineDevice{{ID: "0", Platform: "metal", Identity: "Apple Silicon", Selector: "0", Fingerprint: "Apple Silicon"}}, nil
}

func cudaAccelerators(p MachineProbe, ctx context.Context, pci []pciAccelerator) ([]MachineDevice, error) {
	nvidia, intel := false, false
	for _, d := range pci {
		nvidia = nvidia || d.platform() == "cuda"
		intel = intel || d.platform() == "xpu"
	}
	if !intel && nvidia && p.Info != nil {
		p.Info("No Intel PCI display/accelerator device; no XPU devices (runtime not required)")
	}
	failed := false
	var causes []error
	out := []MachineDevice{}
	b, err := p.Command(ctx, "nvidia-smi", "--query-gpu=uuid,driver_version,pci.bus_id", "--format=csv,noheader,nounits")
	if err != nil {
		if nvidia {
			failed = true
			code := "gpu_driver_unavailable"
			if probeAbsent(err) {
				code = "gpu_driver_missing"
			}
			causes = append(causes, errors.Join(preflightError(code, "cuda", "NVIDIA runtime unavailable; install or repair the driver and re-detect"), err, ctx.Err()))
		} else if !probeAbsent(err) && p.Info != nil {
			p.Info("nvidia-smi failed without an NVIDIA PCI display device; continuing CPU/other accelerator detection")
		}
	} else if strings.TrimSpace(string(b)) != "" {
		cuda := []MachineDevice{}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			fields := strings.Split(line, ",")
			if len(fields) < 2 || len(fields) > 3 || !strings.HasPrefix(strings.TrimSpace(fields[0]), "GPU-") {
				failed = true
				break
			}
			uuid := strings.TrimSpace(fields[0])
			d := MachineDevice{ID: strconv.Itoa(len(cuda)), Platform: "cuda", Identity: uuid, Selector: uuid, Fingerprint: strings.TrimSpace(fields[1])}
			if len(fields) == 3 {
				d.pciAddress = strings.TrimSpace(fields[2])
			}
			cuda = append(cuda, d)
		}
		if validateInventory(cuda) != nil {
			failed = true
		}
		if failed {
			failed = nvidia
			if nvidia {
				causes = append(causes, preflightError("gpu_inventory_invalid", "cuda", "invalid CUDA UUID inventory"))
			} else if p.Info != nil {
				p.Info("Invalid CUDA UUID inventory without an NVIDIA PCI display device; continuing CPU/other accelerator detection")
			}
		} else {
			out = append(out, cuda...)
		}
	}
	if failed {
		return out, errors.Join(causes...)
	}
	return out, nil
}

func xpuAccelerators(p MachineProbe, ctx context.Context, pci []pciAccelerator) ([]MachineDevice, error) {
	if !slices.ContainsFunc(pci, func(d pciAccelerator) bool { return d.platform() == "xpu" }) {
		return nil, nil
	}
	failed := false
	var causes []error
	out := []MachineDevice{}

	xpu, e := p.levelZeroDevices(ctx)
	if e != nil {
		optional := true
		for _, d := range pci {
			if d.platform() == "xpu" && (!d.topologyKnown || !d.integrated) {
				optional = false
			}
		}
		if !(optional && len(pci) > 0 && (probeAbsent(e) || installExit(e, 78))) {
			failed = true
			code := "gpu_driver_unavailable"
			if probeAbsent(e) || installExit(e, 78) {
				code = "gpu_driver_missing"
			}
			causes = append(causes, preflightError(code, "xpu", "cannot enumerate Level Zero UUIDs; install or repair the runtime"))
		}
	} else {
		out = append(out, xpu...)
	}
	if failed {
		return out, errors.Join(causes...)
	}
	return out, nil
}
