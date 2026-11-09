package myce

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Static fake sensors exercise the real worker sampler without GPU hardware.
type xpuAttributionFS struct {
	memoryFS
	attempt string
	reads   atomic.Int32
	sampled chan struct{}
}

func (f *xpuAttributionFS) ReadFile(p string) ([]byte, error) {
	b, err := f.memoryFS.ReadFile(p)
	if p == filepath.Join(f.attempt, "memory.stat") && f.reads.Add(1) == 2 {
		close(f.sampled)
	}
	return b, err
}

func sampleXPUAttribution(t *testing.T, resident, outside, siblingBefore, siblingAfter float64) *AttemptMemoryPeak {
	t.Helper()
	clock := &workerFakeClock{now: time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)}
	c := fakeCeilings(t)
	c.clock = clock
	stat := func(gb float64) string {
		return fmt.Sprintf("anon %.0f\nkernel 0\nshmem 0\nunevictable 0\nfile_dirty 0\nfile_writeback 0\n", gb*(1<<30))
	}
	put := func(p, value string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	put(filepath.Join(c.root, "memory.stat"), stat(siblingBefore))
	a, err := c.create("attempt-xpu", 24<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	proc := t.TempDir()
	put(filepath.Join(proc, "meminfo"), fmt.Sprintf("MemAvailable: %.0f kB\nSwapTotal: 8388608 kB\nSwapFree: 8388608 kB\n", (64-resident-outside-siblingAfter+siblingBefore)*(1<<20)))
	put(filepath.Join(proc, "vmstat"), "oom_kill 0\npswpout 0\n")
	put(filepath.Join(proc, "pressure/memory"), "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
	put(filepath.Join(proc, "7/fdinfo/3"), xeHostFD("7", "0000:03:00.0", 0))
	put(filepath.Join(proc, "debug/0000:03:00.0/gtt_mm"), "use_type: 1\nuse_tt: 1\nusage: 134217728\n")
	put(filepath.Join(c.root, "memory.stat"), stat(resident+siblingAfter))
	put(filepath.Join(a.path, "memory.stat"), stat(resident))
	put(filepath.Join(a.path, "memory.peak"), fmt.Sprintf("%.0f", resident*(1<<30)))
	put(filepath.Join(a.path, "memory.events"), "high 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n")
	put(filepath.Join(a.path, "cgroup.procs"), "7\n")
	f := &xpuAttributionFS{memoryFS: gpuHostFS{memoryFS: a.fs, proc: proc}, attempt: a.path, sampled: make(chan struct{})}
	a.fs = f
	w := Worker{Clock: clock, Machine: Machine{GPU: "xpu"}, selectedDevice: &MachineDevice{pciAddress: "0000:03:00.0"}}
	spec := cpuSpec(1, 0)
	spec.Needs.Platform = Platform{Required: []string{"xpu"}}
	finish := w.observeAttemptMemory(a, Job{Spec: spec}, nil)
	defer finish()
	select {
	case <-f.sampled:
	case <-time.After(testWaitTimeout):
		t.Fatal("host sampler did not run")
	}
	clock.Advance(200 * time.Millisecond)
	p := finish()
	if p == nil || p.GPUHost == nil || p.NonreclaimableGB == nil || p.constrained() || p.PressureUnavailable {
		t.Fatalf("incomplete fake sample: %+v", p)
	}
	return p
}

func TestXPUHostMemoryCorrectedSampleReplacesLegacyAfterReopen(t *testing.T) {
	p := sampleXPUAttribution(t, 7.25, .25, 0, 8)
	if math.Abs(*p.GPUHost.OutsideGB-.25) > 1e-8 || math.Abs(*p.GPUHost.HostDropGB-7.5) > 1e-8 {
		t.Fatalf("sibling memory attributed to attempt: %+v", p.GPUHost)
	}
	q, clock, m, spec := gpuHostFixture(t)
	complete := func(peak *AttemptMemoryPeak) *Job {
		t.Helper()
		queued := submit(t, q, spec)
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.ID != queued.ID {
			t.Fatalf("not assigned: %+v", job(t, q, queued.ID))
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r := result(run, true)
		r.MemoryPeak = peak
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		return run
	}
	host, outside, device := 10.75, 8.0, 8.0
	legacy := &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &host, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: &GPUHostMemory{DriverGB: &outside, OutsideGB: &outside, HostDropGB: &host}}
	complete(legacy)
	p.DeviceGB = &device
	complete(p)
	// A late accepted result from an old worker must not restore the old
	// aggregate, even during a rolling upgrade.
	complete(legacy)
	q = memoryFactsRestart(t, q, clock)
	machineCheck(t, q, m)
	queued := submit(t, q, spec)
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != queued.ID {
		t.Fatalf("corrected job not assigned: %+v", job(t, q, queued.ID))
	}
	if got := next.Spec.Needs.HostMemoryGB; math.Abs(got-(7.5+minimumHostMemoryGB)) > 1e-8 {
		t.Fatalf("legacy history still dominates after reopen: %.3f, want accepted 7.5 GiB plus page rounding", got)
	}
}
