//go:build linux || darwin

package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/probeguard"
)

// Build with CGO_ENABLED=0 GOOS=linux GOARCH=arm64 (or linux/amd64,
// darwin/arm64) go test -c -o myce-hardware.test ./internal/myce.
// Run ONLY as an explicitly reserved scheduler GPU job, with no production
// active, retaining its MYCE_PLATFORM, MYCE_DEVICES and runtime masks:
//
//	MYCE_TEST_NATIVE_HARDWARE=cuda|xpu|metal
//	MYCE_TEST_NATIVE_ENVROOT=/absolute/trusted/runtime-and-probe-root
//	MYCE_TEST_NATIVE_COMMAND='["/absolute/python3","/root/probe.py"]'
//	./myce-hardware.test -test.run='^TestNativeIsolationHardware$' -test.v -test.timeout=3m
//
// The command must emit native-isolation-gpu-ok:<platform> after freeing its
// allocations. scripts/native-isolation-gpu-* supply tiny ctypes/Swift probes.
// ENVROOT is operator Worker.EnvironmentRoot configuration, not a run.env
// permission or a new production path exception. Include interpreter/stdlib
// installations there when they are outside the ordinary runtime grants. Use
// a tools subdirectory, not the outer work root containing the test's temp/state.
// No service, delegated cgroup, driver installation or fleet access is created.
func TestNativeIsolationHardware(t *testing.T) {
	platform := os.Getenv("MYCE_TEST_NATIVE_HARDWARE")
	if platform == "" {
		t.Skip("hardware qualification requires explicit MYCE_TEST_NATIVE_HARDWARE=cuda|xpu|metal")
	}
	if !slices.Contains([]string{"cuda", "xpu", "metal"}, platform) ||
		(platform == "metal") != (runtime.GOOS == "darwin") {
		t.Fatal("hardware platform must be cuda/xpu on Linux or metal on macOS")
	}
	assigned := os.Getenv("MYCE_DEVICES")
	if os.Getenv("MYCE_PLATFORM") != platform || assigned == "" || singleDeviceMask(assigned) != nil {
		t.Fatal("requires the outer scheduler job's matching MYCE_PLATFORM and single assigned MYCE_DEVICES")
	}
	var command []string
	if err := json.Unmarshal([]byte(os.Getenv("MYCE_TEST_NATIVE_COMMAND")), &command); err != nil || len(command) == 0 || !filepath.IsAbs(command[0]) {
		t.Fatal("MYCE_TEST_NATIVE_COMMAND must be a JSON argv with an absolute executable")
	}
	envroot := os.Getenv("MYCE_TEST_NATIVE_ENVROOT")
	info, err := os.Stat(envroot)
	if !filepath.IsAbs(envroot) || filepath.Clean(envroot) == string(filepath.Separator) || err != nil || !info.IsDir() {
		t.Fatal("MYCE_TEST_NATIVE_ENVROOT must name an explicit trusted runtime/probe directory")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Probe only in this opt-in test. Each call restores the ordinary package
	// guard immediately, including the worker's repeated identity checks.
	inventory := func(ctx context.Context) ([]MachineDevice, error) {
		before := probeguard.Before
		probeguard.Before = nil
		defer func() { probeguard.Before = before }()
		return LocalMachineProbe().Accelerators(ctx)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	devices, err := inventory(ctx)
	cancel()
	if err != nil {
		t.Fatalf("actual accelerator inventory: %v", err)
	}
	selector := "0"
	switch platform {
	case "cuda":
		selector = os.Getenv("CUDA_VISIBLE_DEVICES")
		if !strings.HasPrefix(selector, "GPU-") || strings.Contains(selector, ",") {
			t.Fatal("requires the scheduler's single CUDA UUID mask; numeric CUDA ordinals cannot establish physical PCI identity")
		}
	case "xpu":
		selector = os.Getenv("ZE_AFFINITY_MASK")
		if _, err := strconv.ParseUint(selector, 10, 32); err != nil || os.Getenv("ZE_ENABLE_PCI_ID_DEVICE_ORDER") != "1" || os.Getenv("ONEAPI_DEVICE_SELECTOR") != "level_zero:0" {
			t.Fatal("requires scheduler XPU root ordinal ZE_AFFINITY_MASK, PCI ordering and ONEAPI_DEVICE_SELECTOR=level_zero:0")
		}
	}
	var selected *MachineDevice
	for _, d := range devices {
		if d.Platform == platform && d.Selector == selector {
			if selected != nil || d.Reason != "" {
				t.Fatal("assigned runtime selector is ambiguous or unavailable")
			}
			copy := d
			selected = &copy
		}
	}
	if selected == nil {
		t.Fatal("assigned runtime selector is absent from actual accelerator inventory")
	}
	// Scheduler IDs are stable configured IDs, whereas a fresh inventory's ID
	// is an enumeration index. Keep only the actually assigned physical device
	// in this single-device worker, with its outer logical ID and real selector.
	selected.ID = assigned
	w := recoveryWorker(t) // Only queue authorization is a fixture; execution is real.
	w.Machine = Machine{ID: "native-hardware", GPU: platform, Device: assigned, Devices: []MachineDevice{*selected}}
	w.DeviceBinding, w.selectedDevice, w.InventoryProbe = selector, selected, inventory
	w.EnvironmentRoot = envroot
	rootPath, err := filepath.EvalSymlinks(envroot)
	if err != nil {
		t.Fatal(err)
	}
	statePath, err := filepath.EvalSymlinks(w.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if rel, err := filepath.Rel(rootPath, statePath); err != nil || rel == "." || filepath.IsLocal(rel) {
		t.Fatal("trusted ENVROOT must not include private test worker state; use a separate tools/runtime directory")
	}
	w.AcceleratorStopGrace = time.Second
	// Do not initialize service delegation or fabricate a nested cgroup. This
	// inner worker has admission-only caps; its children inherit the real outer
	// scheduler allocation. Linux requires a finite *actual* memory.max below.
	t.Log("inner Worker member caps: admission-only; no own delegated cgroup; no OOM/member-cap qualification")
	var outer nativeHardwareCgroup
	if runtime.GOOS == "linux" {
		outer, err = nativeHardwareReadCgroup(os.Getpid())
		if err != nil {
			t.Fatalf("read outer scheduler cgroup: %v", err)
		}
		if n, e := strconv.ParseUint(outer.Max, 10, 64); e != nil || n == 0 {
			t.Fatal("outer job memory.max is not finite: admission-only; inherited kernel ceiling qualification unavailable")
		}
		t.Logf("live inherited outer job memory.max=%s; cgroup inspected externally, never modified", outer.Max)
	} else {
		t.Log("macOS memory accounting: admission-only; no kernel memory ceiling proof")
	}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	credential := filepath.Join(store.dir, "service", "joining.json")
	sibling := filepath.Join(store.dir, "job-files", "sibling", "1", "secret")
	protected := []string{credential, sibling, filepath.Join(filepath.Dir(sibling), ".tmp", "secret")}
	for _, path := range protected {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("hardware-test-private"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	w.ServiceConfig = &ServiceConfig{JoiningFile: credential}
	bound, err := w.deviceEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	config := nativeHardwareConfig{Platform: platform, Assigned: assigned, Masks: bound, Protected: protected, Command: command}
	for _, group := range []bool{false, true} {
		name := "command"
		if group {
			name = "sidecar-group"
		}
		t.Run(name, func(t *testing.T) {
			config := config
			config.Sidecar = group
			data, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			job := recoveryJob(t, []string{executable, "_native-hardware-test", "main", string(data)})
			job.Spec.Resumable, job.Spec.Logs = false, "keep"
			job.Spec.Needs.MemoryGB = 1.0 / 1024 // Probe allocates only 4096 bytes; outer job owns the reservation.
			job.Spec.Run.Env = map[string]string{"TMPDIR": "/tmp/ignored-authored-temp"}
			if group {
				port := 0
				job.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{executable, "_native-hardware-test", "side", string(data)}, Port: &port, Readiness: &NativeReadiness{Path: "/ready", Timeout: "15s"}}}
			}
			attempt, err := newAttemptStore(store, job)
			if err != nil {
				t.Fatal(err)
			}
			p, err := w.start(attempt, job)
			if err != nil {
				t.Fatalf("real Worker.start: %v", err)
			}
			finished := false
			defer func() {
				if !finished {
					p.stopDirective(w.Clock.Now(), 0, true, true)
					select {
					case f := <-p.done:
						if f.err != nil || f.result.ReleasePending {
							t.Errorf("failure cleanup: %+v", f)
						}
					case <-time.After(10 * time.Second):
						t.Error("failure cleanup unconfirmed")
					}
				}
				out, _ := os.ReadFile(filepath.Join(attempt.dir, attempt.state.Log))
				t.Logf("confined payload log:\n%s", out)
			}()
			work := filepath.Join(store.dir, "job-files", job.ID, strconv.Itoa(job.Attempt))
			// The tiny handshake is bounded to release a stalled qualification's
			// reservation: 30s observation + at most 10s verified failure cleanup.
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				if _, err := os.Stat(filepath.Join(work, "hardware-ready")); err == nil {
					break
				}
				select {
				case f := <-p.done:
					finished = true
					t.Fatalf("confined payload exited before GPU exec gate: %+v", f)
				case <-timer.C:
					t.Fatal("confined payload did not reach GPU exec gate")
				case <-tick.C:
				}
			}
			members, err := workerGroupMembers(p.cmd.Process.Pid)
			if err != nil || len(members) < 2 {
				t.Fatalf("live confined process group: %v, %v", members, err)
			}
			if runtime.GOOS == "linux" {
				for _, pid := range members {
					child, err := nativeHardwareReadCgroup(pid)
					if err != nil || child != outer {
						t.Fatalf("live /proc/%d/cgroup or memory.max differs from outer job: %+v; %v", pid, child, err)
					}
				}
				t.Logf("verified inherited live memory ceiling for %d processes before GPU exec release", len(members))
			}
			if err := os.WriteFile(filepath.Join(work, "hardware-release"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			select {
			case f := <-p.done:
				finished = true
				if f.err != nil || !f.result.Success || f.result.ReleasePending {
					t.Errorf("GPU execution/cleanup: %+v", f)
				}
			case <-timer.C:
				t.Fatal("GPU command/cleanup did not finish")
			}
			out, err := os.ReadFile(filepath.Join(attempt.dir, attempt.state.Log))
			if err != nil || !strings.Contains(string(out), "native-isolation-gpu-ok:"+platform) {
				t.Errorf("missing successful allocation/fill/read/free receipt: %v", err)
			}
			members, err = workerGroupMembers(p.cmd.Process.Pid)
			if err != nil || len(members) != 0 {
				t.Errorf("process/sidecar cleanup incomplete: %v, %v", members, err)
			}
			for _, path := range protected {
				if data, err := os.ReadFile(path); err != nil || string(data) != "hardware-test-private" {
					t.Errorf("protected fixture changed: %v", err)
				}
			}
		})
	}
}

type nativeHardwareConfig struct {
	Platform, Assigned string
	Sidecar            bool
	Masks              map[string]string
	Protected, Command []string
}

// Bypass TestMain in confined helpers: no HOME fixtures, probe-guard changes,
// test storage provisioning or real discovery runs inside the boundary.
func init() {
	if len(os.Args) > 1 && os.Args[1] == "_native-hardware-test" {
		if err := nativeHardwarePayload(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

func nativeHardwarePayload(args []string) error {
	if len(args) != 2 || args[0] != "main" && args[0] != "side" {
		return errors.New("invalid native hardware helper arguments")
	}
	var config nativeHardwareConfig
	if err := json.Unmarshal([]byte(args[1]), &config); err != nil {
		return err
	}
	if os.Getenv("MYCE_PLATFORM") != config.Platform || os.Getenv("MYCE_DEVICES") != config.Assigned {
		return errors.New("payload lost scheduler placement")
	}
	for key, want := range config.Masks {
		if os.Getenv(key) != want {
			return fmt.Errorf("payload lost assigned runtime mask %s", key)
		}
	}
	for _, key := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE", "SSH_AUTH_SOCK", "DBUS_SESSION_BUS_ADDRESS"} {
		if os.Getenv(key) != "" {
			return fmt.Errorf("payload inherited worker control/session variable %s", key)
		}
	}
	work, err := os.Getwd()
	if err != nil {
		return err
	}
	tmp, err := filepath.EvalSymlinks(os.Getenv("TMPDIR"))
	if err != nil || tmp != filepath.Join(work, ".tmp") {
		return errors.New("payload temp escaped managed workdir")
	}
	for _, path := range config.Protected {
		if _, err := os.ReadFile(path); err == nil {
			return errors.New("confined payload read protected sibling/credential contents")
		}
		if err := os.WriteFile(path, []byte("escaped"), 0600); err == nil {
			return errors.New("confined payload wrote protected sibling/credential contents")
		}
	}
	if err := os.WriteFile(filepath.Join(tmp, args[0]+"-own"), []byte("own"), 0600); err != nil {
		return err
	}
	if args[0] == "side" {
		listener, err := net.Listen("tcp4", "127.0.0.1:"+os.Getenv("MYCE_SIDECAR_PORT"))
		if err != nil {
			return err
		}
		defer listener.Close()
		return http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "native-hardware-sidecar")
		}))
	}
	if config.Sidecar {
		address := os.Getenv("MYCE_SIDECAR_HELPER_URL")
		if address == "" {
			return errors.New("native group lost its reserved sidecar URL")
		}
		client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}}
		defer client.CloseIdleConnections()
		response, err := client.Get(address + "/ready")
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, 128))
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || string(data) != "native-hardware-sidecar" {
			return errors.New("confined main could not use its real sidecar")
		}
		if data, err := os.ReadFile(filepath.Join(tmp, "side-own")); err != nil || string(data) != "own" {
			return errors.New("main and sidecar do not share managed attempt temp")
		}
	}
	if err := os.WriteFile("hardware-ready", nil, 0600); err != nil {
		return err
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat("hardware-release"); err == nil {
			// Same process/cgroup; all confinement survives exec. The supplied
			// command must emit its success receipt only after explicit GPU free.
			return syscall.Exec(config.Command[0], config.Command, os.Environ())
		}
		<-tick.C
	}
}

type nativeHardwareCgroup struct{ Group, Max string }

// Read only. Resolve membership against the existing cgroup2 mount (including
// a cgroup namespace mount root); never touch cgroup.procs/subtree_control.
func nativeHardwareReadCgroup(pid int) (nativeHardwareCgroup, error) {
	var result nativeHardwareCgroup
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return result, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::/") {
			result.Group = strings.TrimPrefix(line, "0::")
		}
	}
	if result.Group == "" {
		return result, errors.New("cgroup v2 membership unavailable")
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return result, err
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for _, line := range strings.Split(string(mounts), "\n") {
		before, after, ok := strings.Cut(line, " - ")
		fields := strings.Fields(before)
		if !ok || !strings.HasPrefix(after, "cgroup2 ") || len(fields) < 5 {
			continue
		}
		rel, err := filepath.Rel(unescape.Replace(fields[3]), result.Group)
		if err != nil || rel != "." && !filepath.IsLocal(rel) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(unescape.Replace(fields[4]), rel, "memory.max"))
		if err != nil {
			return result, err
		}
		result.Max = strings.TrimSpace(string(data))
		return result, nil
	}
	return result, errors.New("existing cgroup2 mount unavailable")
}
