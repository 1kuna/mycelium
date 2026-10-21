package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Only documented runtime controls: prefix matching could copy a credential.
func gpuRuntimeEnvironment(environment []string) string {
	allowed := strings.Fields("PATH LD_LIBRARY_PATH LIBRARY_PATH ZE_AFFINITY_MASK ZE_ENABLE_PCI_ID_DEVICE_ORDER ZE_FLAT_DEVICE_HIERARCHY ZE_ENABLE_VALIDATION_LAYER ZE_ENABLE_LOADER_DEBUG_TRACE ZE_DEBUG ONEAPI_DEVICE_SELECTOR ONEAPI_ROOT SYCL_DEVICE_FILTER SYCL_DEVICE_ALLOWLIST SYCL_DEVICE_WHITE_LIST SYCL_PI_TRACE SYCL_UR_TRACE SYCL_CACHE_PERSISTENT SYCL_CACHE_DIR SYCL_ENABLE_DEFAULT_CONTEXTS SYCL_ENABLE_PCI_ID_DEVICE_ORDER CUDA_VISIBLE_DEVICES CUDA_DEVICE_ORDER CUDA_HOME CUDA_PATH CUDA_MODULE_LOADING")
	values := map[string]string{}
	for _, entry := range environment {
		key, value, _ := strings.Cut(entry, "=")
		for _, name := range allowed {
			if key == name && !strings.ContainsAny(value, "\x00\n\r") {
				if key == "PATH" && strings.ContainsAny(value, "\"\\") {
					continue
				}
				values[key] = value
			}
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	for _, key := range keys {
		// EnvironmentFile uses shell-like double quoting, not Go escape syntax.
		value := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `$`, `\$`).Replace(values[key])
		fmt.Fprintf(&out, "%s=\"%s\"\n", key, value)
	}
	return out.String()
}
func (c ServiceConfig) GPUEnvironmentFile() string {
	return filepath.Join(filepath.Dir(c.Config), "gpu.env")
}

func validateWorkerService(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	if c.AllowDeviceless {
		return nil
	}
	b, err := command(ctx, "systemd-run", "--user", "--wait", "--pipe", "--collect", "--property=UMask=0077", "--property=EnvironmentFile="+c.GPUEnvironmentFile(), c.Binary, "detect", "--config", c.Config)
	var detected HardwarePreflight
	if err == nil {
		err = json.Unmarshal(b, &detected)
	}
	if err == nil {
		err = validateServiceHardware(c, detected)
	}
	if err != nil {
		return preflightError("service_environment_invalid", "service", "worker service GPU validation failed; run detect --config under the user manager and check saved PATH, runtime and device permissions")
	}
	return nil
}

// GPUAccessProbe checks the service user's actual node access, not assumed sudo privileges.
type GPUAccessProbe struct {
	Glob   func(string) ([]string, error)
	Open   func(string) error
	Groups func() ([]string, error)
}

func LocalGPUAccessProbe() GPUAccessProbe {
	return GPUAccessProbe{Glob: filepath.Glob, Open: func(path string) error {
		f, e := os.OpenFile(path, os.O_RDWR, 0)
		if e != nil {
			return e
		}
		return f.Close()
	}, Groups: func() ([]string, error) {
		ids, e := os.Getgroups()
		if e != nil {
			return nil, e
		}
		names := []string{}
		for _, id := range ids {
			g, e := user.LookupGroupId(strconv.Itoa(id))
			if e != nil {
				return nil, e
			}
			names = append(names, g.Name)
		}
		return names, nil
	}}
}
func (p GPUAccessProbe) Check(platform string) error {
	pattern := "/dev/dri/renderD*"
	if platform == "cuda" {
		pattern = "/dev/nvidia[0-9]*"
	}
	nodes, err := p.Glob(pattern)
	if err != nil {
		return err
	}
	groups, groupErr := p.Groups()
	problems := []string{}
	if len(nodes) == 0 {
		problems = append(problems, "missing device nodes "+pattern+" (administrator must install/load the GPU driver)")
	}
	accessible := false
	for _, node := range nodes {
		if e := p.Open(node); e == nil {
			accessible = true
		} else {
			problems = append(problems, "cannot open "+node+" read/write: "+e.Error())
		}
	}
	// Sibling cards may intentionally be inaccessible; selected-runtime detection is authoritative.
	if accessible {
		return nil
	}
	if groupErr != nil {
		problems = append(problems, "cannot inspect groups: "+groupErr.Error())
	}
	for _, want := range []string{"render", "video"} {
		found := false
		for _, g := range groups {
			if g == want {
				found = true
			}
		}
		if !found {
			problems = append(problems, "not a member of "+want+"; administrator: sudo usermod -aG "+want+" <worker-user>, then log out/in and restart the user manager")
		}
	}
	return errors.New(strings.Join(problems, "; "))
}

// DetectService loads no credentials and runs the same inventory tools as install/join.
func DetectService(ctx context.Context, c ServiceConfig, p MachineProbe, access GPUAccessProbe) (Detection, error) {
	if err := os.Setenv("PATH", c.Path); err != nil {
		return Detection{}, err
	}
	if c.Detection.Platform == "cpu" {
		return Detection{OS: p.OS, Arch: p.Arch, Platform: "cpu", MemoryMode: "fixed"}, nil
	}
	selector := strings.TrimSpace(strings.Split(c.Device, ",")[0])
	if selector != "" && c.Detection.Platform == "xpu" {
		bound, err := WithModelDevice(Model{}, "xpu", selector)
		if err != nil {
			return Detection{}, err
		}
		for key, value := range bound.DeviceEnv {
			if err = os.Setenv(key, value); err != nil {
				return Detection{}, err
			}
		}
	}
	if c.Detection.OS == "linux" {
		if err := access.Check(c.Detection.Platform); err != nil {
			return Detection{}, err
		}
	}
	if c.Detection.Platform == "xpu" {
		command := p.Command
		p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			if name == "nvidia-smi" {
				return nil, exec.ErrNotFound
			}
			return command(ctx, name, args...)
		}
	}
	d, err := p.DetectDevice(ctx, selector)
	// Explicit budgets must not require telemetry unavailable on discrete XPUs.
	var failure *PreflightError
	if err != nil && c.Detection.Platform == "xpu" && c.Detection.MemoryMode == "fixed" && c.Detection.MemoryGB > c.Detection.ReserveGB && errors.As(err, &failure) && failure.Code == "gpu_memory_unavailable" {
		return c.Detection, nil
	}
	return d, err
}

// DetectServiceHardware runs inside the service manager's probe process and
// reads only nonsecret configuration. Legacy scalar configurations keep their
// compatible diagnostic reply; machine plans verify every selected component.
func DetectServiceHardware(ctx context.Context, c ServiceConfig, p MachineProbe, access GPUAccessProbe) (HardwarePreflight, error) {
	if err := os.Setenv("PATH", c.Path); err != nil {
		return HardwarePreflight{}, err
	}
	if err := validateOnboardingToolEnvironment(ctx, c, p); err != nil {
		return HardwarePreflight{}, err
	}
	if len(c.MachineDevices) == 0 {
		d, err := DetectService(ctx, c, p, access)
		return HardwarePreflight{Detection: d}, err
	}
	if err := os.Setenv("PATH", c.Path); err != nil {
		return HardwarePreflight{}, err
	}
	if p.OS == "linux" {
		for _, s := range c.MachineDevices {
			if err := access.Check(s.Detection.Platform); err != nil {
				return HardwarePreflight{}, preflightError("gpu_permission_denied", s.Selector, "service cannot access the configured GPU nodes; repair driver permissions")
			}
		}
	}
	return PreflightHardware(ctx, c, p)
}
