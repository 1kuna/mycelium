package myce

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func gpuHostFixturePeak(driver, drop float64) *GPUHostMemory {
	outside := max(driver, .2)
	return &GPUHostMemory{DriverGB: &driver, HostDropGB: &drop, OutsideGB: &outside}
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
	for _, mode := range []string{"complete", "missing host field", "swap growth", "swap churn", "host OOM", "missing driver field", "missing events"} {
		t.Run(mode, func(t *testing.T) {
			a, err := fakeCeilings(t).create("attempt-xe", 16<<30, 0)
			if err != nil {
				t.Fatal(err)
			}
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
			put("vmstat", "oom_kill 3\npswpout 100\n")
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
				put("meminfo", "MemAvailable: 15000000 kB\nSwapTotal: 8388608 kB\nSwapFree: 6388607 kB\n")
			}
			if mode == "host OOM" {
				put("vmstat", "oom_kill 4\npswpout 100\n")
			}
			if mode == "swap churn" {
				put("vmstat", "oom_kill 3\npswpout 101\n")
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
			w := Worker{Machine: Machine{GPU: "xpu"}, selectedDevice: &MachineDevice{pciAddress: "0000:03:00.0"}}
			spec := cpuSpec(1, 0)
			spec.Needs.Platform = Platform{Required: []string{"xpu"}}
			finish := w.observeAttemptMemory(a, Job{Spec: spec}, nil)
			defer finish()
			select {
			case <-f.sampled:
			case <-time.After(5 * time.Second):
				t.Fatal("no sample")
			}
			peak := finish()
			if mode == "missing host field" || mode == "missing driver field" || mode == "missing events" {
				if peak.GPUHost != nil || peak.NonreclaimableGB != nil {
					t.Fatalf("incomplete attempt enabled GPU reduction: %+v", peak)
				}
				return
			}
			if peak.GPUHost == nil || *peak.GPUHost.DriverGB != float64(83176+40)/(1<<20) || *peak.GPUHost.HostDropGB != float64(9000000)/(1<<20) {
				t.Fatalf("wrong attributed host memory: %+v", peak.GPUHost)
			}
			wantOutside := max(float64(83176+40)/(1<<20), float64(9000000)/(1<<20)-float64(8260000000)/(1<<30))
			if peak.GPUHost.OutsideGB == nil || math.Abs(*peak.GPUHost.OutsideGB-wantOutside) > 1e-8 {
				t.Fatalf("wrong synchronized outside peak %+v", peak.GPUHost)
			}
			if peak.constrained() != (mode == "swap growth" || mode == "swap churn" || mode == "host OOM") {
				t.Fatalf("host swap growth not retained: %+v", peak)
			}
		})
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
			// Component peaks from #303's complete attempts. Driver/host drops are
			// explicit real-shaped fixtures, pending the isolated native measurement.
			peaks := map[string][2]float64{"separation": {7.582687, 7.678570}, "asr": {3.768425, 3.768913}}
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
					r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &total, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: gpuHostFixturePeak(.16, host+.2)}
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
				}
				wants[kind] = (peaks[kind][1] + .2) * 1.25
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
			// Existing pressure policy must discard the new range too.
			clock.Advance(time.Second)
			r := result(runs[0], true)
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
