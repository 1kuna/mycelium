package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func gpuHostFixturePeak(driver, drop float64) *GPUHostMemory {
	outside := max(driver, .2)
	return &GPUHostMemory{DriverGB: &driver, HostDropGB: &drop, OutsideGB: &outside}
}

func xeHostFD(client, pci string, kib int) string {
	return fmt.Sprintf("drm-driver: xe\ndrm-client-id: %s\ndrm-pdev: %s\ndrm-total-system: 0\ndrm-resident-system: 0\ndrm-total-gtt: %d KiB\ndrm-resident-gtt: %d KiB\ndrm-resident-vram0: 8192 MiB\n", client, pci, kib, kib)
}

// Redirect only proc reads; the cgroup and fd inventories remain real filesystem
// reads, so duplicated/inherited clients and the attempt boundary are exercised.
type gpuHostFS struct {
	memoryFS
	proc string
}

func (f gpuHostFS) ReadFile(p string) ([]byte, error) {
	if strings.HasPrefix(p, "/proc/") {
		p = filepath.Join(f.proc, strings.TrimPrefix(p, "/proc/"))
	} else if strings.HasPrefix(p, "/sys/kernel/debug/dri/") {
		p = filepath.Join(f.proc, "debug", strings.TrimPrefix(p, "/sys/kernel/debug/dri/"))
	}
	return f.memoryFS.ReadFile(p)
}
func (f gpuHostFS) ReadDir(p string) ([]os.DirEntry, error) {
	if strings.HasPrefix(p, "/proc/") {
		p = filepath.Join(f.proc, strings.TrimPrefix(p, "/proc/"))
	}
	return f.memoryFS.ReadDir(p)
}

func gpuTelemetryTestHelper(t *testing.T, body string) string {
	t.Helper()
	// Spaces and shell metacharacters must remain literal path characters.
	path := filepath.Join(t.TempDir(), "telemetry helper;literal")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGPUHostMemoryTelemetryHelperBoundary(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "private-test-token")
	t.Setenv("MYCE_JOB_ID", "must-not-inherit")
	clock := &workerFakeClock{now: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}
	valid := `{"sampled_at":"2026-10-30T00:00:00Z","gtt_mm":{"0000:03:00.0":"use_type: 1\nuse_tt: 1\nusage: 28672\n"}}`
	for _, mode := range []string{"fresh", "future", "missing timestamp", "malformed", "oversized", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			check := func(t *testing.T) {
				body := valid
				switch mode {
				case "future":
					body = strings.ReplaceAll(body, "00:00:00Z", "00:00:01Z")
				case "missing timestamp":
					body = `{"gtt_mm":{"0000:03:00.0":"usage: 0"}}`
				case "malformed":
					body = `{broken`
				case "oversized":
					body += strings.Repeat(" ", 64<<10)
				}
				script := "[ \"$#\" -eq 0 ] && [ -z \"$MYCE_TOKEN\" ] && [ -z \"$MYCE_JOB_ID\" ] || exit 7\ncat <<'SAMPLE'\n" + body + "\nSAMPLE\n"
				if mode == "timeout" {
					// Outlives the test's backstop, so returning proves the helper timeout.
					script = "exec /bin/sleep 120\n"
				}
				path := gpuTelemetryTestHelper(t, script)
				var got map[string]string
				var err error
				returnsWhileBlocked(t, "telemetry helper", func() { got, err = readGPUTelemetryHelper(context.Background(), clock, path) })
				if (err == nil) != (mode == "fresh") {
					t.Fatalf("helper %s: evidence=%v err=%v", mode, got, err)
				}
				if mode == "fresh" && got["0000:03:00.0"] != "use_type: 1\nuse_tt: 1\nusage: 28672\n" {
					t.Fatal("helper did not preserve raw manager accounting")
				}
			}
			if mode == "timeout" {
				// A blocked real process must still be canceled by the real deadline.
				check(t)
			} else {
				// Inject timer time: OS process startup is external I/O, so it cannot
				// consume a successful fixture's 200 ms budget under shared-host load.
				synctest.Test(t, check)
			}
		})
	}
}

type blockedDeviceMemoryFS struct {
	memoryFS
	blocked chan struct{}
	release chan struct{}
	resumed chan struct{}
	once    sync.Once
}

func (f *blockedDeviceMemoryFS) ReadFile(p string) ([]byte, error) {
	if strings.Contains(p, "/fdinfo/") {
		f.once.Do(func() {
			close(f.blocked)
			<-f.release
			defer close(f.resumed)
		})
	}
	return f.memoryFS.ReadFile(p)
}

func TestGPUHostMemoryBlockedDeviceProbeFinishes(t *testing.T) {
	a, err := fakeCeilings(t).create("attempt-device-block", 24<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	proc := t.TempDir()
	fd := filepath.Join(proc, "7/fdinfo/3")
	if err := os.MkdirAll(filepath.Dir(fd), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fd, []byte(xeHostFD("7", "0000:03:00.0", 83176)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.path, "cgroup.procs"), []byte("7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f := &blockedDeviceMemoryFS{memoryFS: gpuHostFS{memoryFS: a.fs, proc: proc}, blocked: make(chan struct{}), release: make(chan struct{}), resumed: make(chan struct{})}
	a.fs = f
	w := Worker{Machine: Machine{GPU: "xpu"}, selectedDevice: &MachineDevice{pciAddress: "0000:03:00.0"}}
	spec := cpuSpec(1, 0)
	spec.Needs.Platform = Platform{Required: []string{"xpu"}}
	spec.Needs.Memory = "auto"
	// A short device period reaches the blocked probe without waiting out the
	// production five-second cadence; the completion bound below is unchanged.
	finish := w.observeAttemptMemoryEvery(a, Job{Spec: spec}, nil, 20*time.Millisecond)
	defer finish()
	defer func() {
		close(f.release)
		select {
		case <-f.blocked:
			select {
			case <-f.resumed:
			case <-time.After(testWaitTimeout):
				t.Error("device read did not return after release")
			}
		default:
		}
	}()
	select {
	case <-f.blocked:
	case <-time.After(testWaitTimeout):
		t.Fatal("device probe did not enter fdinfo")
	}
	done := make(chan *AttemptMemoryPeak, 1)
	go func() { done <- finish() }()
	select {
	case peak := <-done:
		if peak != nil && (peak.GPUHost != nil || peak.NonreclaimableGB != nil || peak.DeviceGB != nil) {
			t.Fatalf("incomplete device probe published evidence: %+v", peak)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked device fdinfo stalled attempt completion")
	}
}

func gpuHostFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "xpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	m.Devices = m.Devices[:2]
	m.HostMemoryBudgetGB = 24
	m.HostAvailableMemoryGB = 28
	m.MemoryCeilingSlackGB = 0
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "xpu", MemoryBytes: 31 << 30}}
	for i := range m.Devices {
		d := &m.Devices[i]
		d.Platform = "xpu"
		d.ExtensionFingerprint = "synthetic-xe"
		d.TotalMemoryGB = 31
		d.BudgetGB = 30
		d.FreeMemoryGB = 30
		target := d.machine(m)
		r := newExtensionReport(e, &target, "extension:sample")
		r.Setup, r.Qualified, r.CheckID = true, true, "check"
		d.ExtensionReports = []ExtensionReport{r}
	}
	machineCheck(t, q, m)
	s := Spec{Name: "memory", Project: "synthetic", Priority: "experiment", Extension: extensionKey(e), Targets: []ExecutionTarget{v.Target}, Needs: ResourceNeeds{Memory: "auto"}, Workload: &Workload{InputShape: "small-v1", Units: 10}}
	return q, clock, m, s
}

// Unknown pressure cannot replace accepted memory. Native ceiling pressure
// disproves the old bound even on success; OOM retains class invalidation.
