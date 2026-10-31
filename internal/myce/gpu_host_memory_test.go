package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func gpuHostFixturePeak(driver, drop float64) *GPUHostMemory {
	outside := max(driver, .2)
	return &GPUHostMemory{DriverGB: &driver, HostDropGB: &drop, OutsideGB: &outside}
}

func TestGPUHostMemoryStallFraction(t *testing.T) {
	// Historical stalls are baseline, not new pressure. At a 200 ms window,
	// exactly 2,000 microseconds is 1%; avg10 need not have refreshed yet.
	if memoryStallPressure(1000000, 1001999, 200*time.Millisecond) || !memoryStallPressure(1000000, 1002000, 200*time.Millisecond) {
		t.Fatal("cumulative PSI did not respect the 1% elapsed-time boundary")
	}
}

func xeHostFD(client, pci string, kib int) string {
	return fmt.Sprintf("drm-driver: xe\ndrm-client-id: %s\ndrm-pdev: %s\ndrm-total-system: 0\ndrm-resident-system: 0\ndrm-total-gtt: %d KiB\ndrm-resident-gtt: %d KiB\ndrm-resident-vram0: 8192 MiB\n", client, pci, kib, kib)
}

func TestGPUHostMemoryDRMAccounting(t *testing.T) {
	raw := xeHostFD("211974", "0000:03:00.0", 83176)
	key, gb, err := drmHostMemory([]byte(raw))
	if err != nil || key != "0000:03:00.0/211974" || gb != float64(83176)/(1<<20) {
		t.Fatalf("system zero hid GTT, or VRAM entered host RAM: %s %v %v", key, gb, err)
	}
	for _, counter := range []string{"drm-total-system: 0\n", "drm-resident-gtt: 83176 KiB\n"} {
		if _, _, err := drmHostMemory([]byte(strings.ReplaceAll(raw, counter, ""))); err == nil {
			t.Fatal("missing host counter became zero")
		}
	}
	if _, _, err := drmHostMemory([]byte(strings.ReplaceAll(raw, "drm-client-id: 211974\n", ""))); !errors.Is(err, errAttemptMemoryUnsupported) {
		t.Fatal("DRM without client identity was treated as an ordinary descriptor")
	}
	if _, _, err := drmHostMemory([]byte("pos: 0\nflags: 0100000\n")); !errors.Is(err, errAttemptMemoryPending) {
		t.Fatal("ordinary descriptor became incomplete DRM")
	}
	if _, _, err := drmHostMemory([]byte(strings.ReplaceAll(raw, "xe\n", "i915\n"))); err == nil {
		t.Fatal("uninvestigated driver enabled reduction")
	}
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

func TestGPUHostMemoryCompleteAttempt(t *testing.T) {
	for _, mode := range []string{"complete", "b70 212 KiB", "b70 startup pages", "host PSI", "cgroup PSI", "host avg10 only", "cgroup avg10 only", "host PSI reset", "cgroup PSI reset", "invalid window", "low available", "missing host PSI", "missing cgroup PSI", "host PSI total", "cgroup PSI total", "helper", "helper missing card", "helper malformed manager", "helper stale", "helper failed", "missing host field", "swap growth", "swap churn", "host OOM", "missing driver field", "missing client identity", "missing debugfs", "malformed debugfs", "masked host drop", "missing events"} {
		t.Run(mode, func(t *testing.T) {
			clock := &workerFakeClock{now: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}
			c := fakeCeilings(t)
			c.clock = clock
			a, err := c.create("attempt-xe", 16<<30, 0)
			if err != nil {
				t.Fatal(err)
			}
			a.pageSize = 4096 // b70 kernel pages, independent of the test host.
			proc := t.TempDir()
			put := func(p, v string) {
				t.Helper()
				p = filepath.Join(proc, p)
				if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(v), 0600); err != nil {
					t.Fatal(err)
				}
			}
			put("meminfo", "MemAvailable: 24000000 kB\nSwapTotal: 8388608 kB\nSwapFree: 6388608 kB\n")
			if strings.HasPrefix(mode, "b70 ") || strings.HasSuffix(mode, "PSI total") {
				// The issue's carried swap baseline is 2,069,843,968 bytes.
				put("meminfo", "MemAvailable: 24000000 kB\nSwapTotal: 8388608 kB\nSwapFree: 6367276 kB\n")
			}
			put("vmstat", "oom_kill 3\npswpout 100\n")
			put("pressure/memory", "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
			a.fs = gpuHostFS{memoryFS: a.fs, proc: proc}
			a.hostBaseline, err = readGPUHostState(a.fs)
			if err != nil {
				t.Fatal(err)
			}
			put("meminfo", "MemAvailable: 15000000 kB\nSwapTotal: 8388608 kB\nSwapFree: 6388608 kB\n")
			if mode == "missing host field" {
				put("meminfo", "MemAvailable: 15000000 kB\n")
			}
			if mode == "swap growth" {
				put("meminfo", "MemAvailable: 15000000 kB\nSwapTotal: 8388608 kB\nSwapFree: 6257536 kB\n") // +128 MiB
			}
			if mode == "host OOM" {
				put("vmstat", "oom_kill 4\npswpout 100\n")
			}
			if mode == "swap churn" {
				put("vmstat", "oom_kill 3\npswpout 32868\n") // 128 MiB out, even with flat swap used.
			}
			if mode == "b70 212 KiB" || strings.HasSuffix(mode, "PSI total") {
				// #303 incident: 212 KiB growth, 23 pages out, >=17.8 GiB
				// MemAvailable, zero attempt swap/events. Carried swap is baseline.
				put("meminfo", "MemAvailable: 18664653 kB\nSwapTotal: 8388608 kB\nSwapFree: 6367064 kB\n")
				put("vmstat", "oom_kill 3\npswpout 123\n")
				for _, name := range []string{"memory.swap.current", "memory.swap.peak"} {
					if err := os.WriteFile(filepath.Join(a.path, name), []byte("0\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if strings.HasSuffix(mode, "PSI total") {
				// Review reproduction: 200 ms stalled while avg10 is still zero.
				if mode == "host PSI total" {
					put("pressure/memory", "some avg10=0.00 avg60=0.00 avg300=0.00 total=200000\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=200000\n")
				} else if err := os.WriteFile(filepath.Join(a.path, "memory.pressure"), []byte("some avg10=0.00 avg60=0.00 avg300=0.00 total=200000\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=200000\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "b70 startup pages" {
				put("meminfo", "MemAvailable: 15000000 kB\nSwapTotal: 8388608 kB\nSwapFree: 6367276 kB\n")
				put("vmstat", "oom_kill 3\npswpout 102\n")
			}
			if mode == "host PSI" {
				put("pressure/memory", "some avg10=1.00 avg60=0.00 avg300=0.00 total=100000\n")
			}
			if mode == "cgroup PSI" {
				if err := os.WriteFile(filepath.Join(a.path, "memory.pressure"), []byte("some avg10=1.00 avg60=0.00 avg300=0.00 total=100000\nfull avg10=1.00 avg60=0.00 avg300=0.00 total=100000\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasSuffix(mode, "avg10 only") {
				// #487: avg10=1.44% but only 120 us / 200 ms = 0.06% stalls.
				psi := "some avg10=1.44 avg60=0.00 avg300=0.00 total=120\nfull avg10=1.44 avg60=0.00 avg300=0.00 total=120\n"
				if mode == "host avg10 only" {
					put("pressure/memory", psi)
				} else {
					memoryFactsFile(t, a, "memory.pressure", psi)
				}
			}
			if mode == "low available" {
				put("meminfo", "MemAvailable: 131072 kB\nSwapTotal: 8388608 kB\nSwapFree: 6388608 kB\n")
			}
			if mode == "missing host PSI" {
				put("pressure/memory", "some avg10=unknown\n")
			}
			if mode == "missing cgroup PSI" {
				if err := os.Remove(filepath.Join(a.path, "memory.pressure")); err != nil {
					t.Fatal(err)
				}
			}
			raw := xeHostFD("7", "0000:03:00.0", 83176)
			if mode == "missing driver field" {
				raw = strings.ReplaceAll(raw, "drm-total-system: 0\n", "")
			}
			put("7/fdinfo/3", raw)
			put("7/fdinfo/4", raw)
			// A second process inherits that client. A client on the other card also
			// consumes host RAM and must be included, even though not selected.
			put("8/fdinfo/3", raw)
			put("8/fdinfo/4", xeHostFD("8", "0000:07:00.0", 40))
			if mode == "missing client identity" {
				put("8/fdinfo/4", strings.ReplaceAll(xeHostFD("8", "0000:07:00.0", 40), "drm-client-id: 8\n", ""))
			}
			for _, pci := range []string{"0000:03:00.0", "0000:07:00.0"} {
				put("debug/"+pci+"/gtt_mm", "use_type: 1\nuse_tt: 1\nusage: 28672\n")
			}
			if mode == "missing debugfs" {
				if err := os.Remove(filepath.Join(proc, "debug/0000:03:00.0/gtt_mm")); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "malformed debugfs" {
				put("debug/0000:03:00.0/gtt_mm", "use_type: 1\nuse_tt: 1\nusage: unknown\n")
			}
			if mode == "masked host drop" {
				// Another workload releases 8 GiB while unreported driver
				// storage grows by 8 GiB. A flat host delta must not hide it.
				put("meminfo", "MemAvailable: 24000000 kB\nSwapTotal: 8388608 kB\nSwapFree: 6388608 kB\n")
				put("debug/0000:03:00.0/gtt_mm", "use_type: 1\nuse_tt: 1\nusage: 8589934592\n")
			}
			if err := os.WriteFile(filepath.Join(a.path, "cgroup.procs"), []byte("7\n8\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(a.path, "memory.stat"), []byte("anon 8230000000\nkernel 30000000\nshmem 0\nunevictable 0\nfile_dirty 0\nfile_writeback 0\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(a.path, "memory.events"), []byte("high 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "missing events" {
				if err := os.WriteFile(filepath.Join(a.path, "memory.events"), []byte("oom 0\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			f := &nonreclaimableSamplingFS{memoryFS: a.fs, sampled: make(chan struct{})}
			a.fs = f
			if mode == "host PSI reset" {
				a.hostBaseline.psiTotalUS = 1000
			}
			if mode == "cgroup PSI reset" {
				*a.pressureBaseline = 1000
			}
			a.pressureStart = clock.Now()
			w := Worker{Clock: clock, Machine: Machine{GPU: "xpu"}, selectedDevice: &MachineDevice{pciAddress: "0000:03:00.0"}}
			if strings.HasPrefix(mode, "helper") {
				managers := map[string]string{"0000:03:00.0": "use_type: 1\nuse_tt: 1\nusage: 28672\n", "0000:07:00.0": "use_type: 1\nuse_tt: 1\nusage: 28672\n"}
				if mode == "helper missing card" {
					delete(managers, "0000:07:00.0")
				}
				if mode == "helper malformed manager" {
					managers["0000:03:00.0"] = "use_type: 1\nuse_tt: 1\nusage: unknown\n"
				}
				at := clock.Now()
				if mode == "helper stale" {
					at = at.Add(-time.Second)
				}
				body, _ := json.Marshal(map[string]any{"sampled_at": at, "gtt_mm": managers})
				script := "cat <<'SAMPLE'\n" + string(body) + "\nSAMPLE\n"
				if mode == "helper failed" {
					script += "exit 1\n"
				}
				w.GPUTelemetryHelper = gpuTelemetryTestHelper(t, script)
				// A valid helper replaces unreadable debugfs, including for
				// the client's second card. Invalid helper output must fail
				// even when direct debugfs would have been readable.
				if mode == "helper" {
					if err := os.RemoveAll(filepath.Join(proc, "debug")); err != nil {
						t.Fatal(err)
					}
				}
			}
			spec := cpuSpec(1, 0)
			spec.Needs.Platform = Platform{Required: []string{"xpu"}}
			finish := w.observeAttemptMemory(a, Job{Spec: spec}, nil)
			defer finish()
			select {
			case <-f.sampled:
			case <-time.After(5 * time.Second):
				t.Fatal("no sample")
			}
			if strings.HasPrefix(mode, "helper") {
				// Static helper timestamps still belong to every invocation;
				// the empty-cgroup baseline precedes this 200 ms window.
				a.pressureStart = clock.Now().Add(-200 * time.Millisecond)
			} else if mode != "invalid window" {
				clock.Advance(200 * time.Millisecond)
			}
			peak := finish()
			if strings.HasSuffix(mode, "PSI reset") || mode == "invalid window" || strings.HasSuffix(mode, "avg10 only") || mode == "b70 212 KiB" || mode == "host PSI" || mode == "swap growth" || mode == "low available" || mode == "missing host PSI" || mode == "missing cgroup PSI" || strings.HasSuffix(mode, "PSI total") {
				// Send the actual sampler result through learning, rather than
				// stopping at the diagnostic flag. Identity/timing use fake time.
				q, clock, m, spec := observationFixture(t)
				spec.Needs.Memory = "auto"
				var before time.Time
				missing := mode == "missing host PSI" || mode == "missing cgroup PSI" || strings.HasSuffix(mode, "PSI reset") || mode == "invalid window"
				if missing {
					// Unknown telemetry must preserve an existing qualified peak.
					seed := startObservation(t, q, m, spec)
					clock.Advance(time.Second)
					r := result(&seed, true)
					two := 2.0
					r.MemoryPeak = &AttemptMemoryPeak{HostGB: &two}
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					before = clock.Now()
				}
				run := startObservation(t, q, m, spec)
				clock.Advance(time.Second)
				r := result(&run, true)
				r.MemoryPeak = peak
				// Exercise the real Result JSON boundary, not a private flag.
				body, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				var wire Result
				if err := json.Unmarshal(body, &wire); err != nil {
					t.Fatal(err)
				}
				if err := q.Complete(wire); err != nil {
					t.Fatal(err)
				}
				learned := false
				for _, point := range q.s.WorkObservations {
					learned = learned || point.MemoryRecordedAt.After(before)
					if missing && (!point.MemoryRecordedAt.Equal(before) || point.MemoryPeak == nil || point.MemoryPeak.HostGB == nil || *point.MemoryPeak.HostGB != 2) {
						t.Fatalf("missing pressure discarded qualified evidence: %+v", point)
					}
				}
				if learned != (mode == "b70 212 KiB" || strings.HasSuffix(mode, "avg10 only")) {
					t.Fatalf("pressure eligibility %s: learned=%t", mode, learned)
				}
				expectedReason := map[string]string{
					"b70 212 KiB": "eligible", "host avg10 only": "eligible", "cgroup avg10 only": "eligible",
					"host PSI": "host_psi_total", "host PSI total": "host_psi_total", "cgroup PSI total": "cgroup_psi_total",
					"swap growth": "host_swap_growth", "low available": "available_below_floor",
					"missing host PSI": "host_pressure_unavailable_or_reset", "missing cgroup PSI": "cgroup_psi_unavailable_or_reset",
					"host PSI reset": "host_pressure_unavailable_or_reset", "cgroup PSI reset": "cgroup_psi_unavailable_or_reset", "invalid window": "invalid_pressure_window",
				}[mode]
				expectedUS := float64(200000)
				if mode == "invalid window" {
					expectedUS = 0
				}
				if peak.Pressure == nil || peak.Pressure.Reason != expectedReason || peak.Pressure.ElapsedUS != expectedUS {
					t.Fatalf("unexplained pressure decision: %+v want %s", peak.Pressure, expectedReason)
				}
				if strings.HasSuffix(mode, "avg10 only") {
					e := peak.Pressure
					if e.HostStallUS == nil || e.CgroupStallUS == nil || *e.HostStallUS+*e.CgroupStallUS != 120 || max(e.HostAvg10, e.CgroupAvg10) != 1.44 {
						t.Fatalf("lost PSI operands: %+v", e)
					}
				}
				if mode == "b70 212 KiB" {
					e := peak.Pressure
					if e.HostSwapGrowthBytes != 212*1024 || e.HostSwapOutBytes != 23*4096 || e.MinAvailableGB == nil || *e.MinAvailableGB != float64(18664653)/(1<<20) || e.SwapBudgetGB != min(e.ResidentGB, *e.MinAvailableGB)*.01 {
						t.Fatalf("lost swap/headroom operands: %+v", e)
					}
				}
				q = memoryFactsRestart(t, q, clock)
				memoryFactsGET(t, q, run, wire.MemoryPeak, false)
				if missing {
					if !peak.PressureUnavailable || peak.HostGB == nil {
						t.Fatalf("lost diagnostic peak or unknown pressure: %+v", peak)
					}
					for _, point := range q.s.WorkObservations {
						if !point.MemoryRecordedAt.Equal(before) || *point.MemoryPeak.HostGB != 2 {
							t.Fatal("restart changed prior memory evidence", point)
						}
					}
				}
			}
			if peak.PressureUnavailable || strings.HasPrefix(mode, "helper ") || mode == "missing host field" || mode == "missing host PSI" || mode == "missing cgroup PSI" || mode == "missing driver field" || mode == "missing client identity" || mode == "missing debugfs" || mode == "malformed debugfs" || mode == "missing events" {
				if peak.GPUHost != nil || peak.NonreclaimableGB != nil {
					t.Fatalf("incomplete attempt enabled GPU reduction: %+v", peak)
				}
				return
			}
			driver := float64(83176+40)/(1<<20) + float64(2*28672)/(1<<30)
			drop := float64(9000000) / (1 << 20)
			if mode == "b70 212 KiB" || strings.HasSuffix(mode, "PSI total") {
				drop = float64(24000000-18664653) / (1 << 20)
			}
			if mode == "low available" {
				drop = float64(24000000-131072) / (1 << 20)
			}
			if mode == "masked host drop" {
				driver += 8 - float64(28672)/(1<<30)
				drop = 0
			}
			if peak.GPUHost == nil || *peak.GPUHost.DriverGB != driver || *peak.GPUHost.HostDropGB != drop {
				t.Fatalf("wrong attributed host memory: %+v", peak.GPUHost)
			}
			wantOutside := max(driver, drop-float64(8260000000)/(1<<30))
			if peak.GPUHost.OutsideGB == nil || math.Abs(*peak.GPUHost.OutsideGB-wantOutside) > 1e-8 {
				t.Fatalf("wrong synchronized outside peak %+v", peak.GPUHost)
			}
			if peak.constrained() != (mode == "swap growth" || mode == "swap churn" || mode == "host OOM" || mode == "host PSI" || mode == "cgroup PSI" || mode == "low available" || strings.HasSuffix(mode, "PSI total")) {
				t.Fatalf("host swap growth not retained: %+v", peak)
			}

		})
	}
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
				script = "exec /bin/sleep 10\n"
			}
			path := gpuTelemetryTestHelper(t, script)
			start := time.Now()
			got, err := readGPUTelemetryHelper(context.Background(), clock, path)
			if (err == nil) != (mode == "fresh") {
				t.Fatalf("helper %s: evidence=%v err=%v", mode, got, err)
			}
			if mode == "fresh" && got["0000:03:00.0"] != "use_type: 1\nuse_tt: 1\nusage: 28672\n" {
				t.Fatal("helper did not preserve raw manager accounting")
			}
			if mode == "timeout" && time.Since(start) > 2*time.Second {
				t.Fatal("helper timeout stalled telemetry")
			}
		})
	}
}

type blockedGPUHostFS struct {
	memoryFS
	reads    atomic.Int32
	resident atomic.Int64
	blocked  chan struct{}
	release  chan struct{}
	spike    chan struct{}
	sawSpike atomic.Bool
	resumed  chan struct{}
	doneRead atomic.Bool
}

func (f *blockedGPUHostFS) ReadFile(p string) ([]byte, error) {
	if strings.Contains(p, "/fdinfo/") && f.reads.Add(1) == 2 {
		close(f.blocked)
		<-f.release
	}
	if filepath.Base(p) == "memory.stat" {
		resident := f.resident.Load()
		if resident == 16 && f.sawSpike.CompareAndSwap(false, true) {
			close(f.spike)
		}
		return []byte(fmt.Sprintf("anon %d\nkernel 0\nshmem 0\nunevictable 0\nfile_dirty 0\nfile_writeback 0\n", resident<<30)), nil
	}
	b, err := f.memoryFS.ReadFile(p)
	if strings.HasSuffix(p, "/gtt_mm") && f.reads.Load() >= 2 && f.doneRead.CompareAndSwap(false, true) {
		close(f.resumed)
	}
	return b, err
}

func TestGPUHostMemoryBlockedDriverPreservesResidentSpike(t *testing.T) {
	for _, blockedAtFinish := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked_at_finish=%t", blockedAtFinish), func(t *testing.T) {
			testGPUHostMemoryBlockedDriver(t, blockedAtFinish)
		})
	}
}

func testGPUHostMemoryBlockedDriver(t *testing.T, blockedAtFinish bool) {
	a, err := fakeCeilings(t).create("attempt-blocked-xe", 24<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	proc := t.TempDir()
	put := func(p, text string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	put(filepath.Join(proc, "meminfo"), "MemAvailable: 24000000 kB\nSwapTotal: 8388608 kB\nSwapFree: 6388608 kB\n")
	put(filepath.Join(proc, "vmstat"), "oom_kill 0\npswpout 0\n")
	put(filepath.Join(proc, "pressure/memory"), "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
	put(filepath.Join(proc, "7/fdinfo/3"), xeHostFD("7", "0000:03:00.0", 83176))
	put(filepath.Join(proc, "debug/0000:03:00.0/gtt_mm"), "use_type: 1\nuse_tt: 1\nusage: 28672\n")
	put(filepath.Join(a.path, "cgroup.procs"), "7\n")
	put(filepath.Join(a.path, "memory.events"), "high 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n")
	f := &blockedGPUHostFS{memoryFS: gpuHostFS{memoryFS: a.fs, proc: proc}, blocked: make(chan struct{}), release: make(chan struct{}), spike: make(chan struct{}), resumed: make(chan struct{})}
	f.resident.Store(8)
	a.fs = f
	a.hostBaseline, err = readGPUHostState(a.fs)
	if err != nil {
		t.Fatal(err)
	}
	clock := &workerFakeClock{now: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}
	w := Worker{Clock: clock, Machine: Machine{GPU: "xpu"}, selectedDevice: &MachineDevice{pciAddress: "0000:03:00.0"}}
	spec := cpuSpec(1, 0)
	spec.Needs.Platform = Platform{Required: []string{"xpu"}}
	finish := w.observeAttemptMemory(a, Job{Spec: spec}, nil)
	defer finish()
	released := false
	defer func() {
		if !released {
			close(f.release)
		}
	}()
	select {
	case <-f.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("driver did not enter blocking read")
	}
	f.resident.Store(16)
	select {
	case <-f.spike:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked fdinfo prevented the resident sampler from observing 16 GiB")
	}
	f.resident.Store(8)
	clock.Advance(time.Second) // The driver read has missed its coverage window.
	if !blockedAtFinish {
		close(f.release)
		released = true
	}
	done := make(chan *AttemptMemoryPeak, 1)
	go func() { done <- finish() }()
	var peak *AttemptMemoryPeak
	select {
	case peak = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked DRM read stalled attempt completion")
	}
	if blockedAtFinish {
		close(f.release)
		released = true
		select {
		case <-f.resumed:
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled collector did not finish its read")
		}
	}
	if peak == nil || peak.HostGB == nil || *peak.HostGB != 16 {
		t.Fatalf("lost transient resident spike: %+v", peak)
	}
	if peak.GPUHost != nil || peak.NonreclaimableGB != nil {
		t.Fatalf("slow driver telemetry enabled a reduced GPU envelope: %+v", peak)
	}
}

type blockedDeviceMemoryFS struct {
	memoryFS
	blocked chan struct{}
	release chan struct{}
	resumed chan struct{}
}

func (f *blockedDeviceMemoryFS) ReadFile(p string) ([]byte, error) {
	if strings.Contains(p, "/fdinfo/") {
		close(f.blocked)
		<-f.release
		defer close(f.resumed)
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
	a.hostBaseline = nil // Isolate the five-second device probe, without host collection.
	w := Worker{Machine: Machine{GPU: "xpu"}, selectedDevice: &MachineDevice{pciAddress: "0000:03:00.0"}}
	spec := cpuSpec(1, 0)
	spec.Needs.Platform = Platform{Required: []string{"xpu"}}
	spec.Needs.Memory = "auto"
	finish := w.observeAttemptMemory(a, Job{Spec: spec}, nil)
	defer finish()
	defer func() {
		close(f.release)
		select {
		case <-f.blocked:
			select {
			case <-f.resumed:
			case <-time.After(time.Second):
				t.Error("device read did not return after release")
			}
		default:
		}
	}()
	select {
	case <-f.blocked:
	case <-time.After(10 * time.Second):
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

func TestGPUHostMemoryTwoCardAdmission(t *testing.T) {
	for _, pair := range [][2]string{{"separation", "asr"}, {"separation", "separation"}} {
		t.Run(strings.Join(pair[:], "+"), func(t *testing.T) {
			q, clock, m, base := gpuHostFixture(t)
			// Retain #303's historical resident floors and complete components.
			// Driver, host-drop and synchronous residual values are from the
			// public sequential measurement. Keep the unadjusted separation
			// residual: subtracting the overlapping CPU job would understate
			// what the production sampler conservatively charges.
			// Both native attempts had host swap-out, so their actual records
			// are ineligible. These pressure-free fixtures test the arithmetic
			// if complete local repetitions establish those same bounds; they
			// do not certify the native attempts or enable their learning.
			peaks := map[string][2]float64{"separation": {7.645931243896484, 7.8166961669921875}, "asr": {3.725048065185547, 4.041187286376953}}
			components := map[string][2]float64{"separation": {7.582687, 7.678570}, "asr": {3.768425, 3.768913}}
			// Add the independent manager maxima without discounting overlap
			// with the client counters. The measured raw residuals are larger.
			drivers := map[string]float64{"separation": .162628173828125 + float64(167231488)/(1<<30), "asr": .0813140869140625 + float64(87408640)/(1<<30)}
			drops := map[string]float64{"separation": 8.355888366699219, "asr": 3.8674354553222656}
			outside := map[string]float64{"separation": .8351707458496094, "asr": .22780990600585938}
			totals := map[string]float64{"separation": 13.084995, "asr": 9.590683}
			wants := map[string]float64{}
			for _, kind := range []string{"separation", "asr"} {
				spec := base
				spec.Args = []string{kind}
				for n, host := range peaks[kind] {
					j := submit(t, q, spec)
					run := assignedJob(machineCheck(t, q, m))
					if run == nil || run.ID != j.ID {
						t.Fatalf("training not admitted: %+v", job(t, q, j.ID))
					}
					if err := q.Started(result(run, false)); err != nil {
						t.Fatal(err)
					}
					if n == 1 && math.Abs(run.Spec.Needs.HostMemoryGB-totals[kind]*1.25) > 1e-8 {
						t.Fatalf("single complete attempt shrank: %+v", run.MemoryEnvelope)
					}
					clock.Advance(time.Second)
					r := result(run, true)
					device := 8.0
					total := totals[kind]
					component, driver, drop, residual := components[kind][n], drivers[kind], drops[kind], outside[kind]
					r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &component, CgroupGB: &total, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: &GPUHostMemory{DriverGB: &driver, HostDropGB: &drop, OutsideGB: &residual}}
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
				}
				wants[kind] = (peaks[kind][1] + outside[kind]) * 1.25
			}
			// Exercise persistence and identity revalidation before using either card.
			var db string
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&db); err != nil {
				t.Fatal(err)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(db, q.root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			q = reopened
			machineCheck(t, q, m)
			var runs []*Job
			for _, kind := range pair {
				spec := base
				spec.Args = []string{kind}
				queued := submit(t, q, spec)
				run := assignedJob(machineCheck(t, q, m, runs...))
				if run == nil || run.ID != queued.ID {
					t.Fatalf("concurrent work did not fit: %+v", job(t, q, queued.ID))
				}
				if math.Abs(run.Spec.Needs.HostMemoryGB-wants[kind]) > 1e-8 {
					t.Fatalf("wrong envelope %+v want %v", run.MemoryEnvelope, wants[kind])
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				runs = append(runs, run)
			}
			if runs[0].AllocatedDevices[0] == runs[1].AllocatedDevices[0] {
				t.Fatal("both jobs assigned same card")
			}
			want := wants[pair[0]] + wants[pair[1]]
			if got := q.s.Machines[m.ID].HostMemoryInUseGB; math.Abs(got-want) > 1e-8 || got > 24 {
				t.Fatalf("under-reserved concurrent jobs: got %v want %v", got, want)
			}
			// A true OOM must still discard the comparable accepted ranges.
			clock.Advance(time.Second)
			r := result(runs[0], false)
			r.OutOfMemory = true
			host := 7.7
			total := 13.1
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &total, Events: &MemoryEventCounts{Max: 1}, GPUHost: &GPUHostMemory{}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			for _, p := range q.s.WorkObservations {
				if p.MemoryPeak.constrained() && p.GPUHostRange != nil {
					t.Fatal("pressure retained GPU range")
				}
			}
		})
	}
}

func TestGPUHostMemoryFallbackAndFloors(t *testing.T) {
	for _, mode := range []string{"complete", "legacy GPU sample", "missing driver peak", "missing host drop", "missing outside peak", "single attempt", "incomplete sibling history", "stale", "transfer", "unified", "resident floor", "driver floor", "variance"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, m, base := gpuHostFixture(t)
			queued := submit(t, q, base)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != queued.ID {
				t.Fatal("no training job")
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			r := result(run, true)
			host, total, device := 7.7, 13.085, 8.0
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &total, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: gpuHostFixturePeak(.16, 7.9)}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			for id, p := range q.s.WorkObservations {
				p.GPUHostRange = &MemoryPeakRange{Samples: 2, MinGB: 7.8, MaxGB: 7.9}
				switch mode {
				case "legacy GPU sample":
					p.MemoryPeak.GPUHost = nil
				case "missing driver peak":
					p.MemoryPeak.GPUHost.DriverGB = nil
				case "missing host drop":
					p.MemoryPeak.GPUHost.HostDropGB = nil
				case "missing outside peak":
					p.MemoryPeak.GPUHost.OutsideGB = nil
				case "single attempt":
					p.GPUHostRange.Samples = 1
				case "incomplete sibling history":
					old := p
					old.Key.Allowance = "older"
					old.GPUHostRange = nil
					q.s.WorkObservations[identityHash(old.Key)] = old
				case "transfer":
					p.Key.Host = "other-host"
					outside := 20.0
					p.MemoryPeak.GPUHost.OutsideGB = &outside
				case "resident floor":
					oldHost := 9.0
					p.MemoryPeak.HostGB = &oldHost
				case "driver floor":
					driver := 2.0
					p.MemoryPeak.GPUHost.DriverGB = &driver
					p.MemoryPeak.GPUHost.OutsideGB = &driver
				case "variance":
					p.GPUHostRange.MinGB = 3
				}
				q.s.WorkObservations[id] = p
				break
			}
			if mode == "stale" {
				clock.Advance(workCurveMaxAge + time.Second)
			}
			target := m.Devices[0].machine(m)
			if mode == "unified" {
				target.UnifiedMemory = true
			}
			probe := *q.s.Jobs[run.ID]
			probe.State = "queued"
			estimate := q.memoryEstimate(&probe, &target)
			switch mode {
			case "complete":
				if math.Abs(estimate.measuredHostGB-9.875) > 1e-8 {
					t.Fatalf("wrong complete sizing: %+v", estimate)
				}
			case "resident floor":
				if estimate.measuredHostGB < (9+.16)*1.25 {
					t.Fatalf("lost historical resident+driver floor: %+v", estimate)
				}
			case "driver floor":
				if estimate.measuredHostGB < (7.7+2)*1.25 {
					t.Fatalf("lost driver floor: %+v", estimate)
				}
			case "variance":
				if math.Abs(estimate.measuredHostGB-(7.9+4.9)) > 1e-8 {
					t.Fatalf("lost measured variance: %+v", estimate)
				}
			default:
				if estimate.measuredHostGB != 0 {
					t.Fatalf("incomplete/nonlocal evidence reduced GPU memory: %+v", estimate)
				}
				if mode == "transfer" && (estimate.host == nil || hostMemoryPeakGB(estimate.host)*estimate.hostFactor < (host+20)*1.25) {
					t.Fatalf("transfer discarded observed outside-cgroup memory: %+v", estimate)
				}
			}
		})
	}
}

// #486: a rejected successful neighbour must not replace any accepted memory,
// even when throughput can still learn. True OOMs retain class invalidation.
func TestGPUHostMemoryConstrainedNeighbourPreservesAccepted(t *testing.T) {
	for _, outcome := range []string{"constrained success", "pressure unavailable success", "OOM", "ceiling failure"} {
		t.Run(outcome, func(t *testing.T) {
			q, clock, m, spec := gpuHostFixture(t)
			start := func() *Job {
				t.Helper()
				queued := submit(t, q, spec)
				run := assignedJob(machineCheck(t, q, m))
				if run == nil || run.ID != queued.ID {
					t.Fatal("no GPU attempt", queued.ID)
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				return run
			}
			seed := start()
			clock.Advance(time.Second)
			host, total, device, component, drop, outside := 7.79, 9.54, 5.67, 8.48, 7.79, .31464340209961
			r := result(seed, true)
			r.Usage = &SizeHint{MemoryGB: 2, HostMemoryGB: 4}
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, DeviceGB: &device, NonreclaimableGB: &component, Events: &MemoryEventCounts{}, GPUHost: &GPUHostMemory{DriverGB: &outside, HostDropGB: &drop, OutsideGB: &outside}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			// Model already accepted comparable observations on another card
			// and another host; the invalidator formerly swept all these keys.
			for _, point := range q.s.WorkObservations {
				if point.GPUHostRange == nil || point.MemoryRange == nil {
					t.Fatal("fixture did not accept complete memory", point)
				}
				sibling := point
				sibling.Key.Component = m.Devices[1].ID
				sibling.Key.Hardware = m.Devices[1].Identity
				q.s.WorkObservations[identityHash(sibling.Key)] = sibling
				remote := point
				remote.Key.Host = "another-host"
				q.s.WorkObservations[identityHash(remote.Key)] = remote
				break
			}
			run := start()
			before := preservationSnapshot(t, q)
			clock.Advance(time.Second)
			success := strings.HasSuffix(outcome, "success")
			r = result(run, success)
			r.Usage = &SizeHint{MemoryGB: 40, HostMemoryGB: 20}
			r.OutOfMemory = outcome == "OOM"
			if outcome == "ceiling failure" {
				r.Error = "exceeded its 16 GiB memory ceiling"
			}
			host = 20 // Rejected measurements cannot overwrite accepted peaks.
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, Constrained: outcome != "pressure unavailable success", PressureUnavailable: outcome == "pressure unavailable success", Events: &MemoryEventCounts{}}
			body, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			var wire Result
			if err := json.Unmarshal(body, &wire); err != nil {
				t.Fatal(err)
			}
			if err := q.Complete(wire); err != nil {
				t.Fatal(err)
			}
			after := preservationSnapshot(t, q)
			if len(before.WorkCurves) != len(after.WorkCurves) {
				t.Fatal("curve key removed")
			}
			for _, old := range before.WorkCurves {
				point, ok := q.s.WorkObservations[identityHash(old.Key)]
				if !ok {
					t.Fatal("curve key removed", old.Key)
				}
				if success {
					if !reflect.DeepEqual(old.MemoryPeak, point.MemoryPeak) || !reflect.DeepEqual(old.MemoryRange, point.MemoryRange) || !reflect.DeepEqual(old.GPUHostRange, point.GPUHostRange) || !old.MemoryRecordedAt.Equal(point.MemoryRecordedAt) || old.MemoryConfiguration != point.MemoryConfiguration || old.MemoryLayout != point.MemoryLayout || old.HostMemoryGB != point.HostMemoryGB || old.DeviceMemoryGB != point.DeviceMemoryGB || old.IncrementalHostMemoryGB != point.IncrementalHostMemoryGB || old.IncrementalDeviceMemoryGB != point.IncrementalDeviceMemoryGB {
						t.Fatal("constrained neighbour changed accepted memory", old, point)
					}
				} else if !point.MemoryRecordedAt.IsZero() || point.MemoryRange != nil || point.GPUHostRange != nil {
					t.Fatal("failure retained accepted memory", point)
				}
			}
			if success {
				page, err := q.JobLedger(run.ID, 0, -1, 256)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := VerifyCurvePreservation(before, after, []JobLedgerPage{page}); err != nil {
					t.Fatal(err)
				}
				q = memoryFactsRestart(t, q, clock)
				memoryFactsGET(t, q, *run, wire.MemoryPeak, false)
				if _, err := VerifyCurvePreservation(after, preservationSnapshot(t, q), nil); err != nil {
					t.Fatal("restart", err)
				}
			}
		})
	}
}
