package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
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
	b, err := p.Command(ctx, "nvidia-smi", "--query-gpu=uuid,driver_version", "--format=csv,noheader,nounits")
	if err != nil && !errors.Is(err, exec.ErrNotFound) && !errors.Is(err, os.ErrNotExist) {
		return nil, inventoryPreflightError("gpu_driver_unavailable", "cuda", "CUDA inventory probe unavailable; repair the driver and re-detect")
	}
	if err == nil && strings.TrimSpace(string(b)) != "" {
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			uuid, driver, ok := strings.Cut(line, ",")
			if !ok || !strings.HasPrefix(strings.TrimSpace(uuid), "GPU-") {
				return nil, inventoryPreflightError("gpu_inventory_invalid", "cuda", "invalid CUDA UUID inventory")
			}
			uuid = strings.TrimSpace(uuid)
			out = append(out, MachineDevice{Platform: "cuda", Identity: uuid, Selector: uuid, Fingerprint: strings.TrimSpace(driver)})
		}
	}
	glob := p.Glob
	if glob == nil {
		glob = func(string) ([]string, error) { return nil, nil }
	}
	nodes, globErr := glob("/dev/dri/renderD*")
	if globErr != nil {
		return nil, inventoryPreflightError("gpu_inventory_unavailable", "xpu", "cannot inspect GPU nodes")
	}
	// Only Intel discrete render nodes belong to the Level Zero backend.
	// A BMC, AMD or VM render node does not require Intel's runtime.
	if p.ReadFile != nil {
		intel := nodes[:0]
		for _, node := range nodes {
			dir := filepath.Join("/sys/class/drm", filepath.Base(node), "device")
			vendor, _ := p.ReadFile(filepath.Join(dir, "vendor"))
			if strings.TrimSpace(string(vendor)) == "0x8086" && p.intelDiscrete(dir) {
				intel = append(intel, node)
			}
		}
		nodes = intel
	}
	if len(nodes) > 0 {
		if p.CommandEnv == nil {
			return nil, inventoryPreflightError("gpu_inventory_unavailable", "xpu", "Level Zero inventory requires a private environment runner")
		}
		b, err = p.CommandEnv(ctx, map[string]string{"ZE_ENABLE_PCI_ID_DEVICE_ORDER": "1", "ZE_AFFINITY_MASK": "", "ONEAPI_DEVICE_SELECTOR": ""}, "python3", "-c", levelZeroInventory)
		if err != nil {
			code := "gpu_driver_unavailable"
			if probeAbsent(err) || installExit(err, 78) {
				code = "gpu_driver_missing"
			}
			return nil, inventoryPreflightError(code, "xpu", "cannot enumerate Level Zero UUIDs; install or repair the oneAPI/Level Zero runtime")
		}
		var devices []struct {
			Identity    string `json:"identity"`
			Fingerprint string `json:"fingerprint"`
		}
		if err = json.Unmarshal(b, &devices); err != nil {
			return nil, inventoryPreflightError("gpu_inventory_invalid", "xpu", "invalid Level Zero inventory")
		}
		for i, d := range devices {
			out = append(out, MachineDevice{Platform: "xpu", Identity: d.Identity, Selector: strconv.Itoa(i), Fingerprint: d.Fingerprint})
		}
	}
	for i := range out {
		out[i].ID = strconv.Itoa(i)
	}
	if err := validateInventory(out); err != nil {
		return nil, inventoryPreflightError("gpu_inventory_invalid", "gpu", "invalid or duplicate component identities")
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
        if p.type==1: result.append(dict(identity=bytes(p.uuid).hex(),fingerprint=str(dp.driverVersion)+":"+bytes(p.name).decode()))
# A cross-driver ordinal is ambiguous for the affinity selector.
if len(drivers)>1 and result: raise RuntimeError("ambiguous Level Zero drivers")
print(json.dumps(result))
`
