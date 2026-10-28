package myce

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type nativeLinuxPolicy struct {
	MetricCgroups                  []string // Trusted attempt member leaves, never payload env.
	Read, ReadOnly, Write, Devices []string
	Env                            []string
	Ports                          []uint16
	Namespace                      bool
}

type nativeLinuxSupport struct{ mode, reason, bwrap, nsenter string }

var nativeLinuxDetection = sync.OnceValue(detectNativeLinuxSupport)

// Capability evidence belongs to this service process: a login shell can have
// different AppArmor/seccomp policy. Neither supported mode currently provides
// a qualified outbound transport, so neither advertises native_network.
func (w Worker) nativeIsolationCapabilities() []string {
	s := nativeLinuxDetection()
	caps := []string{"native_isolation_" + s.mode}
	if s.mode != "unavailable" {
		caps = append(caps, "native_sidecars")
	}
	if s.mode == "landlock" {
		caps = append(caps, "native_network_partial")
	}
	return caps
}

func detectNativeLinuxSupport() nativeLinuxSupport {
	s := nativeLinuxSupport{mode: "unavailable"}
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 || abi < 6 {
		s.reason = "landlock_abi6_required"
		return s
	}
	// ABI availability alone is not proof that this service can install the
	// complete confinement stack. Exercise Landlock + seccomp + exec together.
	probePolicy := nativeLinuxPolicy{Read: []string{"/usr", "/bin", "/lib", "/lib64"}}
	args, err := nativePolicyArgs(probePolicy, []string{"/bin/true"})
	if err != nil {
		s.reason = "confinement_probe_failed"
		return s
	}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer probeCancel()
	probe := exec.CommandContext(probeCtx, args[0], args[1:]...)
	probe.Env = environmentList(jobBaseEnvironment())
	if _, err := probe.CombinedOutput(); err != nil {
		s.reason = "confinement_probe_failed"
		return s
	}
	s.bwrap, _ = exec.LookPath("bwrap")
	s.nsenter, _ = exec.LookPath("nsenter")
	if s.bwrap == "" || s.nsenter == "" {
		s.reason = "namespace_tools_missing"
		return s
	}
	// A hung utility must not stall admission. This tiny /bin/true probe gets
	// two seconds; it does no workload computation or network access.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.bwrap, append(nativeBwrapBase(), "--", "/bin/true")...)
	cmd.Env = environmentList(jobBaseEnvironment())
	out, err := cmd.CombinedOutput()
	if err == nil {
		s.mode = "namespace"
		return s
	}
	if ctx.Err() != nil {
		s.reason = "namespace_probe_timeout"
		return s
	}
	// Only a policy refusal authorizes the weaker fallback. Missing tools,
	// broken mounts and other packaging failures must not downgrade silently.
	message := strings.ToLower(string(out))
	if strings.Contains(message, "operation not permitted") || strings.Contains(message, "permission denied") || strings.Contains(message, "no permissions to create") {
		s.mode, s.reason = "landlock", "namespace_policy_denied"
		return s
	}
	s.reason = "namespace_probe_failed"
	return s
}

func nativeBwrapBase() []string {
	return []string{"--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-net", "--die-with-parent", "--cap-drop", "ALL", "--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"}
}

func nativeIsolationSupport(network bool) (nativeLinuxSupport, error) {
	s := nativeLinuxDetection()
	log.Printf("native isolation: mode=%s reason=%s network=%t", s.mode, s.reason, network)
	if network {
		return s, errors.New("native_network_unavailable: Linux outbound transport is not qualified; needs.network requires a capable host")
	}
	if s.mode == "unavailable" {
		return s, fmt.Errorf("native_isolation_unavailable: %s", s.reason)
	}
	return s, nil
}

func nativeCanonical(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(path)
}

func (w Worker) nativeLinuxPolicy(cmd *exec.Cmd, spool, work string, env map[string]string, ports []string) (nativeLinuxPolicy, error) {
	p := nativeLinuxPolicy{Env: environmentList(env), MetricCgroups: slices.Clone(w.nativeMetricCgroups)}
	work, err := nativeCanonical(work)
	if err != nil {
		return p, err
	}
	dir, err := nativeCanonical(cmd.Dir)
	if err != nil {
		return p, err
	}
	if rel, err := filepath.Rel(work, dir); err != nil || rel != "." && !filepath.IsLocal(rel) {
		return p, errors.New("native isolation requires workdir beneath {work}; stage source with inputs or a profile")
	}
	p.Write = []string{work}
	for _, path := range []string{env["MYCE_CHECKPOINT_DIR"], env["MYCE_USAGE_FILE"]} {
		if path == "" {
			continue
		}
		if path == env["MYCE_USAGE_FILE"] {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				return p, err
			}
			f.Close()
		}
		canonical, err := nativeCanonical(path)
		if err != nil {
			return p, err
		}
		p.Write = append(p.Write, canonical)
	}
	// Do not grant /etc, /run, /proc, home or the worker spool recursively.
	// Landlock does not mediate stat/chmod and similar metadata operations;
	// fallback contents/socket protection is not a metadata-isolation claim.
	// CUDA/Level Zero discover PCI/DRM devices and CPU/NUMA topology here.
	// None of these sysfs trees includes /sys/fs/cgroup; grant metrics below
	// that mount only as exact files resolved by the attached exec helper.
	p.Read = []string{"/usr", "/bin", "/sbin", "/lib", "/lib64", "/opt/intel/oneapi", "/sys/bus/pci", "/sys/class/drm", "/sys/devices", "/sys/module/nvidia", "/sys/module/nvidia_uvm", "/sys/module/xe", "/sys/module/i915", "/etc/ld.so.cache", "/etc/ld.so.conf", "/etc/ld.so.conf.d", "/etc/ssl/certs", "/etc/ca-certificates", "/etc/hosts", "/etc/resolv.conf", "/etc/nsswitch.conf", "/etc/passwd", "/etc/group", "/etc/localtime", "/proc/cpuinfo", "/proc/meminfo", "/proc/stat", "/proc/loadavg", "/proc/uptime", "/proc/version", "/proc/sys/kernel/osrelease", "/proc/sys/kernel/ostype", "/proc/sys/vm/overcommit_memory", "/proc/driver/nvidia/version"}
	modelCache := w.CacheRoot
	if modelCache == "" {
		modelCache = filepath.Join(spool, "model-cache")
	}
	for _, path := range []string{filepath.Join(spool, "environment-cache"), w.EnvironmentRoot, w.ExtensionRoot} {
		if path != "" {
			p.Read = append(p.Read, path)
		}
	}
	for _, path := range []string{modelCache, w.ModelRoot} {
		if path != "" {
			p.ReadOnly = append(p.ReadOnly, path)
		}
	}
	declaredRead, declaredExecute, err := w.nativeDeclaredRoots(spool)
	if err != nil {
		return p, err
	}
	p.ReadOnly = append(p.ReadOnly, declaredRead...)
	p.Read = append(p.Read, declaredExecute...)
	// Acquired archive members may execute, but receive no content-write grant.
	p.Read = append(p.Read, w.nativeSharedInputs...)
	if w.environmentCache != nil {
		p.Read = append(p.Read, w.environmentCache.Root)
	}
	// Only operator runtime roots, never authored run.env library/PATH values.
	for _, key := range []string{"LD_LIBRARY_PATH", "LIBRARY_PATH"} {
		for _, path := range filepath.SplitList(os.Getenv(key)) {
			if filepath.IsAbs(path) {
				p.Read = append(p.Read, path)
			}
		}
	}
	// Prepared Python environments can symlink their interpreter into uv's
	// managed installation. Grant that exact installation (stdlib included),
	// never the operator's entire uv cache or home directory.
	prepared := []string{filepath.Join(spool, "environment-cache"), w.EnvironmentRoot}
	if w.environmentCache != nil {
		prepared = append(prepared, w.environmentCache.Root)
	}
	for _, root := range prepared {
		if root == "" {
			continue
		}
		for _, pattern := range []string{filepath.Join(root, "*", "venv", "bin", "python*"), filepath.Join(root, "*", "bin", "python*"), filepath.Join(root, "venv", "bin", "python*")} {
			paths, _ := filepath.Glob(pattern)
			for _, path := range paths {
				if resolved, err := filepath.EvalSymlinks(path); err == nil && filepath.Base(filepath.Dir(resolved)) == "bin" {
					p.Read = append(p.Read, filepath.Dir(filepath.Dir(resolved)))
				}
			}
		}
	}
	executable, err := os.Executable()
	if err != nil {
		return p, err
	}
	p.Read = append(p.Read, executable)
	for _, port := range ports {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return p, errors.New("invalid native sidecar port")
		}
		p.Ports = append(p.Ports, uint16(n))
	}
	for _, port := range w.nativeControlPorts() {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return p, errors.New("native worker control port is invalid")
		}
		if slices.Contains(p.Ports, uint16(n)) {
			return p, errors.New("native sidecar port conflicts with worker control port")
		}
	}
	if err := w.validateNativeReadGrants(p.Read, spool); err != nil {
		return p, err
	}
	if err := w.validateNativeReadGrants(p.ReadOnly, spool); err != nil {
		return p, err
	}
	if err := nativeReadOnlyOverlap(p.ReadOnly, append(slices.Clone(p.Read), p.Write...)); err != nil {
		return p, err
	}
	p.Devices, err = w.nativeGPUDevices(env)
	if w.selectedDevice != nil && w.selectedDevice.Platform == "cuda" {
		pci := w.selectedDevice.pciAddress
		if w.selectedDevice.pciFact != nil {
			pci = w.selectedDevice.pciFact.address
		}
		if pci != "" {
			p.Read = append(p.Read, filepath.Join("/proc/driver/nvidia/gpus", normalizePCIAddress(pci)))
		}
	}
	return p, err
}

// Resolve exact nodes using the already selected physical device. An ordinal
// must never be guessed to be a DRM minor or NVIDIA kernel minor.
func (w Worker) nativeGPUDevices(env map[string]string) ([]string, error) {
	if w.nativeGPUDeviceProbe != nil {
		return w.nativeGPUDeviceProbe(w, env)
	}
	if env["CUDA_VISIBLE_DEVICES"] == "" && env["ONEAPI_DEVICE_SELECTOR"] == "!*:*" || w.Machine.GPU == "cpu" || w.Machine.GPU == "fake" || w.Machine.GPU == "" {
		return nil, nil
	}
	selector := w.DeviceBinding
	if selector == "" {
		selector = w.Machine.Device
	}
	var pci string
	if w.selectedDevice != nil {
		pci = w.selectedDevice.pciAddress
		if w.selectedDevice.pciFact != nil {
			pci = w.selectedDevice.pciFact.address
		}
	}
	for _, d := range w.Machine.Devices {
		if d.Platform == w.Machine.GPU && (d.Selector == selector || d.ID == w.Machine.Device) {
			pci = d.pciAddress
			if d.pciFact != nil {
				pci = d.pciFact.address
			}
			break
		}
	}
	if pci == "" {
		return nil, errors.New("native_isolation_gpu_mapping_unavailable: selected device has no verified PCI mapping")
	}
	pci = normalizePCIAddress(pci)
	switch w.Machine.GPU {
	case "cuda":
		data, err := os.ReadFile(filepath.Join("/proc/driver/nvidia/gpus", pci, "information"))
		if err != nil {
			return nil, errors.New("native_isolation_gpu_mapping_unavailable: NVIDIA kernel minor unavailable")
		}
		for _, line := range strings.Split(string(data), "\n") {
			if key, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "Device Minor" {
				n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
				if err != nil {
					break
				}
				return []string{fmt.Sprintf("/dev/nvidia%d", n), "/dev/nvidiactl", "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"}, nil
			}
		}
	case "xpu":
		paths, _ := filepath.Glob("/sys/class/drm/renderD*/device")
		for _, path := range paths {
			resolved, err := filepath.EvalSymlinks(path)
			if err == nil && normalizePCIAddress(filepath.Base(resolved)) == pci {
				return []string{filepath.Join("/dev/dri", filepath.Base(filepath.Dir(path)))}, nil
			}
		}
	}
	return nil, errors.New("native_isolation_gpu_mapping_unavailable: selected device node not found")
}

func nativePolicyArgs(p nativeLinuxPolicy, payload []string) ([]string, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return append([]string{executable, "_native-exec", "run", base64.RawStdEncoding.EncodeToString(data)}, payload...), nil
}

func nativeWrapPayload(cmd *exec.Cmd, wrapper []string) error {
	if err := nativeWrapPayloadCheck(cmd); err != nil {
		return err
	}
	cmd.Args = append(slices.Clone(cmd.Args[:4]), wrapper...)
	return nil
}

func nativeWrapPayloadCheck(cmd *exec.Cmd) error {
	if len(cmd.Args) < 5 || cmd.Args[1] != "-c" || !(strings.HasPrefix(cmd.Args[3], "myce-member-") || strings.HasPrefix(cmd.Args[3], "myce-supervisor-")) {
		return errors.New("native isolation requires the worker-owned exec gate")
	}
	return nil
}

func nativeBwrapPolicyArgs(p nativeLinuxPolicy) []string {
	args := nativeBwrapBase()
	// The private /tmp must not hide an exact helper binary or a configured
	// prepared environment beneath /tmp (including go test's build directory).
	// Restore only trusted read grants, then put private write overlays on top.
	for _, path := range append(slices.Clone(p.Read), p.ReadOnly...) {
		canonical, err := nativeCanonical(path)
		if err == nil && strings.HasPrefix(canonical, "/tmp/") {
			args = append(args, "--ro-bind", canonical, canonical)
		}
	}
	for _, path := range p.Write {
		args = append(args, "--bind", path, path)
	}
	args = append(args, "--dir", "/dev/dri", "--tmpfs", "/dev/shm")
	for _, path := range p.Devices {
		if _, err := os.Stat(path); err == nil {
			args = append(args, "--dev-bind", path, path)
		}
	}
	return args
}

func (w Worker) isolateNativeCommand(cmd *exec.Cmd, spool, work string, env map[string]string, ports []string, network bool) error {
	if err := nativeWrapPayloadCheck(cmd); err != nil {
		return err
	}
	s, err := nativeIsolationSupport(network)
	if err != nil {
		return err
	}
	p, err := w.nativeLinuxPolicy(cmd, spool, work, env, ports)
	if err != nil {
		return err
	}
	p.Namespace = s.mode == "namespace"
	if !p.Namespace && len(p.Ports) > 0 {
		log.Printf("native isolation: mode=landlock network=partial reserved_tcp_ports=%d", len(p.Ports))
	}
	args, err := nativePolicyArgs(p, cmd.Args[4:])
	if err != nil {
		return err
	}
	if p.Namespace {
		bwrap := nativeBwrapPolicyArgs(p)
		// bwrap 0.9 passes inherited descriptors to its executable by default;
		// --preserve-fds belongs to other launchers and is not a bwrap option.
		args, err = nativeNamespaceMonitorArgs(s.bwrap, append(append(bwrap, "--"), args...))
		if err != nil {
			return err
		}
	}
	if err := nativeWrapPayload(cmd, args); err != nil {
		return err
	}
	// Dynamic loader and shell startup variables are payload authority. They
	// must not execute code in the gate, bwrap, nsenter or helper before entry.
	cmd.Env = environmentList(jobBaseEnvironment())
	return nil
}
