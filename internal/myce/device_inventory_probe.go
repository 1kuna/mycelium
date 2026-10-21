package myce

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

// Accelerators uses the vendor inventory API. Level Zero's actual GPU order
// and UUIDs are read together, with PCI ordering forced before zeInit.
func (p MachineProbe) Accelerators(ctx context.Context) ([]MachineDevice, error) {
	out := []MachineDevice{}
	if p.OS == "darwin" && p.Arch == "arm64" {
		d, err := p.Detect(ctx)
		if errors.Is(err, ErrNoGPU) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		return []MachineDevice{{ID: "0", Platform: "metal", Identity: d.GPU, Selector: "0", Fingerprint: d.GPU}}, nil
	}
	if p.Command == nil {
		return nil, inventoryPreflightError("gpu_inventory_unavailable", "gpu", "inventory command runner missing")
	}
	failures := []error{}
	nvidia, pciErr := p.nvidiaPresent()
	if pciErr != nil {
		failures = append(failures, inventoryPreflightError("gpu_inventory_unavailable", "gpu", "cannot inspect PCI inventory; restore access and re-detect"))
	}
	b, err := p.Command(ctx, "nvidia-smi", "--query-gpu=uuid,driver_version,pci.bus_id", "--format=csv,noheader,nounits")
	if err != nil && !probeAbsent(err) {
		if e := p.nvidiaProbeFailure(nvidia); e != nil {
			failures = append(failures, reject("device_probe_unavailable", e))
		}
	}
	if err == nil && strings.TrimSpace(string(b)) != "" {
		cuda := []MachineDevice{}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			fields := strings.Split(line, ",")
			if len(fields) < 2 || len(fields) > 3 || !strings.HasPrefix(strings.TrimSpace(fields[0]), "GPU-") {
				failures = append(failures, inventoryPreflightError("gpu_inventory_invalid", "cuda", "invalid CUDA UUID inventory"))
				cuda = nil
				break
			}
			uuid := strings.TrimSpace(fields[0])
			d := MachineDevice{Platform: "cuda", Identity: uuid, Selector: uuid, Fingerprint: strings.TrimSpace(fields[1])}
			if len(fields) == 3 {
				d.pciAddress = strings.TrimSpace(fields[2])
			}
			cuda = append(cuda, d)
		}
		out = append(out, cuda...)
	}
	xpu, err := p.intelAccelerators(ctx)
	out = append(out, xpu...)
	if err != nil {
		failures = append(failures, err)
	}
	for i := range out {
		out[i].ID = strconv.Itoa(i)
	}
	if err := validateInventory(out); err != nil {
		failures = append(failures, inventoryPreflightError("gpu_inventory_invalid", "gpu", "invalid or duplicate component identities"))
	}
	return out, errors.Join(failures...)
}

func (p MachineProbe) intelAccelerators(ctx context.Context) ([]MachineDevice, error) {
	if p.Glob == nil {
		return nil, nil
	}
	nodes, err := p.Glob("/dev/dri/renderD*")
	if err != nil {
		return nil, inventoryPreflightError("gpu_inventory_unavailable", "xpu", "cannot inspect GPU nodes")
	}
	discrete := p.ReadFile == nil
	intel := []string{}
	for _, node := range nodes {
		if p.ReadFile == nil {
			intel = append(intel, node)
			continue
		}
		dir := filepath.Join("/sys/class/drm", filepath.Base(node), "device")
		vendor, e := p.ReadFile(filepath.Join(dir, "vendor"))
		if e != nil {
			return nil, inventoryPreflightError("gpu_inventory_unavailable", "xpu", "cannot inspect render-node vendor")
		}
		if strings.TrimSpace(string(vendor)) != "0x8086" {
			continue
		}
		integrated, known, e := p.pciTopology(dir)
		if e != nil {
			return nil, inventoryPreflightError("gpu_inventory_unavailable", "xpu", "cannot inspect render-node topology")
		}
		discrete = discrete || !known || !integrated
		intel = append(intel, node)
	}
	if len(intel) == 0 {
		return nil, nil
	}
	if p.CommandEnv == nil {
		if !discrete {
			return nil, nil
		}
		return nil, inventoryPreflightError("gpu_inventory_unavailable", "xpu", "Level Zero inventory requires a private environment runner")
	}
	b, err := p.CommandEnv(ctx, map[string]string{"ZE_ENABLE_PCI_ID_DEVICE_ORDER": "1", "ZE_AFFINITY_MASK": "", "ONEAPI_DEVICE_SELECTOR": ""}, "python3", "-c", levelZeroInventory)
	if err != nil {
		code := "gpu_driver_unavailable"
		if probeAbsent(err) || installExit(err, 78) {
			if !discrete {
				if p.Info != nil {
					p.Info("Intel integrated graphics runtime missing; continuing with CPU")
				}
				return nil, nil
			}
			code = "gpu_driver_missing"
		}
		return nil, inventoryPreflightError(code, "xpu", "cannot enumerate Level Zero UUIDs; install or repair the oneAPI/Level Zero runtime")
	}
	var devices []struct {
		Identity    string `json:"identity"`
		Fingerprint string `json:"fingerprint"`
		PCIAddress  string `json:"pci_address"`
	}
	if err = json.Unmarshal(b, &devices); err != nil {
		return nil, inventoryPreflightError("gpu_inventory_invalid", "xpu", "invalid Level Zero inventory")
	}
	out := []MachineDevice{}
	for i, d := range devices {
		out = append(out, MachineDevice{Platform: "xpu", Identity: d.Identity, Selector: strconv.Itoa(i), Fingerprint: d.Fingerprint, pciAddress: d.PCIAddress})
	}
	return out, nil
}

// Keep M2's runtime rejection contract while exposing the onboarding diagnosis.
func inventoryPreflightError(code, component, message string) error {
	return reject("device_probe_unavailable", preflightError(code, component, message))
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
        if p.type==1: result.append(dict(identity=bytes(p.uuid).hex(),fingerprint=str(dp.driverVersion)+":"+bytes(p.name).decode(),pci_address=pci_address(device)))
# A cross-driver ordinal is ambiguous for the affinity selector.
if len(drivers)>1 and result: raise RuntimeError("ambiguous Level Zero drivers")
print(json.dumps(result))
`
