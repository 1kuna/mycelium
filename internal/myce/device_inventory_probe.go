package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Accelerators uses the vendor inventory API. Level Zero's actual GPU order
// and UUIDs are read together, with PCI ordering forced before zeInit.
func (p MachineProbe) Accelerators(ctx context.Context) ([]MachineDevice, error) {
	out := []MachineDevice{}
	if p.OS == "darwin" && p.Arch == "arm64" {
		d, err := p.Detect(ctx)
		if err != nil {
			return nil, err
		}
		return []MachineDevice{{ID: "0", Platform: "metal", Identity: d.GPU, Selector: "0", Fingerprint: d.GPU}}, nil
	}
	failures := &acceleratorProbeError{}
	b, err := p.Command(ctx, "nvidia-smi", "--query-gpu=uuid,driver_version", "--format=csv,noheader,nounits")
	if err != nil {
		missing := errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist)
		present, pciErr := p.nvidiaDisplayPresent()
		if present || pciErr != nil && !missing {
			failures.cuda = true
		} else if !missing {
			log.Print("CUDA inventory note: nvidia-smi failed; no NVIDIA PCI display device present")
		}
	}
	if err == nil && strings.TrimSpace(string(b)) != "" {
		cuda := []MachineDevice{}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			uuid, driver, ok := strings.Cut(line, ",")
			if !ok || !strings.HasPrefix(strings.TrimSpace(uuid), "GPU-") {
				failures.cuda = true
				break
			}
			uuid = strings.TrimSpace(uuid)
			cuda = append(cuda, MachineDevice{Platform: "cuda", Identity: uuid, Selector: uuid, Fingerprint: strings.TrimSpace(driver)})
		}
		for i := range cuda {
			cuda[i].ID = strconv.Itoa(i)
		}
		if validateInventory(cuda) != nil {
			failures.cuda = true
		}
		if !failures.cuda {
			out = append(out, cuda...)
		}
	}
	var nodes []string
	if p.Files != nil {
		nodes, err = fs.Glob(p.Files, "dev/dri/renderD*")
		if err != nil {
			failures.xpu = true
		}
	}
	if len(nodes) > 0 {
		xpu, err := p.levelZeroDevices(ctx)
		if err != nil {
			failures.xpu = true
		} else {
			out = append(out, xpu...)
		}
	}
	for i := range out {
		out[i].ID = strconv.Itoa(i)
	}
	if failures.cuda || failures.xpu {
		return out, failures
	}
	return out, nil
}

// A partial probe error withholds only its failed families. The public message
// contains fixed reason codes, never subprocess output or host paths.
type acceleratorProbeError struct{ cuda, xpu bool }

func (e *acceleratorProbeError) failed(platform string) bool {
	return platform == "cuda" && e.cuda || platform == "xpu" && e.xpu
}
func (e *acceleratorProbeError) Error() string {
	reasons := []string{}
	if e.cuda {
		reasons = append(reasons, "cuda: device_probe_unavailable")
	}
	if e.xpu {
		reasons = append(reasons, "xpu: device_probe_unavailable")
	}
	return strings.Join(reasons, "; ")
}
func (e *acceleratorProbeError) Unwrap() error {
	return reject("device_probe_unavailable", errors.New(e.Error()))
}

// Unknown PCI evidence fails closed. Only the local probe opts into the real
// filesystem; fixtures supply the complete view, including an empty PCI tree.
func (p MachineProbe) nvidiaDisplayPresent() (bool, error) {
	if p.Files == nil {
		return false, errors.New("PCI inventory unavailable")
	}
	entries, err := fs.ReadDir(p.Files, "sys/bus/pci/devices")
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		base := "sys/bus/pci/devices/" + entry.Name()
		vendor, err := fs.ReadFile(p.Files, base+"/vendor")
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(string(vendor)) != "0x10de" {
			continue
		}
		class, err := fs.ReadFile(p.Files, base+"/class")
		if err != nil {
			return false, err
		}
		value, err := strconv.ParseUint(strings.TrimSpace(string(class)), 0, 32)
		if err != nil {
			return false, err
		}
		if value>>16 == 0x03 {
			return true, nil
		}
	}
	return false, nil
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
	}
	if err := json.Unmarshal(b, &devices); err != nil {
		return nil, fmt.Errorf("Level Zero inventory: %w", err)
	}
	out := []MachineDevice{}
	for i, d := range devices {
		out = append(out, MachineDevice{ID: strconv.Itoa(i), Platform: "xpu", Identity: d.Identity, Selector: strconv.Itoa(i), Fingerprint: d.Fingerprint})
	}
	if err := validateInventory(out); err != nil {
		return nil, err
	}
	return out, nil
}

// ctypes is only a standard-library binding to the installed Level Zero API;
// it does not require torch, a model, or a separately managed service.
const levelZeroInventory = `
import ctypes as c, json
z=c.CDLL("libze_loader.so.1")
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
