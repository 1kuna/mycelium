package myce

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func cudaHostFixture(t *testing.T, mode string) *AttemptMemoryPeak {
	t.Helper()
	var peak *AttemptMemoryPeak
	synctest.Test(t, func(t *testing.T) {
		clock := RealClock{} // synctest supplies deterministic sensor time.
		c := fakeCeilings(t)
		put := func(p, text string) {
			t.Helper()
			if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
		}
		put(filepath.Join(c.root, "memory.stat"), "anon 8589934592\nkernel 0\nshmem 0\n")
		a, err := c.create("attempt-cuda", 32<<30, 0)
		if err != nil {
			t.Fatal(err)
		}
		proc := t.TempDir()
		a.fs = gpuHostFS{memoryFS: a.fs, proc: proc}
		a.pageSize = 4096
		meminfo := "MemTotal: 67108864 kB\nMemFree: 20971520 kB\nBuffers: 1048576 kB\nCached: 31457280 kB\nShmem: 131072 kB\nSReclaimable: 2097152 kB\nUnevictable: 0 kB\nMlocked: 0 kB\nDirty: 0 kB\nWriteback: 0 kB\nMemAvailable: 50331648 kB\nSwapTotal: 8388608 kB\nSwapFree: 8388608 kB\n"
		put(filepath.Join(proc, "meminfo"), meminfo)
		vmstat := "oom_kill 3\npswpout 100\nnr_foll_pin_acquired 8192\nnr_foll_pin_released 4096\n"
		put(filepath.Join(proc, "vmstat"), vmstat)
		put(filepath.Join(proc, "pressure/memory"), "some avg10=0.00 total=0\n")
		a.hostBaseline, err = readGPUHostState(a.fs)
		if err != nil {
			t.Fatal(err)
		}
		a.pressureStart = clock.Now()
		// A sibling's 8 GiB stays constant. Only this exact attempt's two PIDs
		// (including a sidecar in a child cgroup) contribute proc pin readings.
		put(filepath.Join(a.path, "cgroup.procs"), "7\n")
		put(filepath.Join(a.path, "sidecar/cgroup.procs"), "8\n")
		put(filepath.Join(a.path, "sidecar/memory.pressure"), "some avg10=0.00 total=0\n")
		put(filepath.Join(proc, "7/status"), "VmPin: 32768 kB\nVmLck: 65536 kB\n")
		put(filepath.Join(proc, "8/status"), "VmPin: 0 kB\nVmLck: 0 kB\n")
		stat := "anon 2147483648\nkernel 268435456\nshmem 134217728\nunevictable 67108864\nfile_dirty 33554432\nfile_writeback 16777216\n"
		put(filepath.Join(a.path, "memory.stat"), stat)
		put(filepath.Join(a.path, "memory.peak"), "12884901888\n")
		put(filepath.Join(c.root, "memory.stat"), strings.Replace(stat, "anon 2147483648", "anon 10737418240", 1))
		put(filepath.Join(proc, "meminfo"), strings.Replace(meminfo, "MemAvailable: 50331648", "MemAvailable: 47185920", 1))
		switch mode {
		case "missing cgroup":
			put(filepath.Join(a.path, "memory.stat"), strings.ReplaceAll(stat, "file_writeback 16777216\n", ""))
		case "missing proc pin":
			put(filepath.Join(proc, "8/status"), "VmLck: 0 kB\n")
		case "missing physical":
			put(filepath.Join(proc, "meminfo"), strings.ReplaceAll(meminfo, "SReclaimable: 2097152 kB\n", ""))
		case "missing global pins":
			put(filepath.Join(proc, "vmstat"), strings.ReplaceAll(vmstat, "nr_foll_pin_released 4096\n", ""))
		case "reset global pins":
			put(filepath.Join(proc, "vmstat"), strings.ReplaceAll(vmstat, "nr_foll_pin_released 4096", "nr_foll_pin_released 9999"))
		case "masked host drop":
			put(filepath.Join(proc, "meminfo"), meminfo) // Unrelated frees mask MemAvailable drop, never absolute occupancy.
		case "host OOM":
			put(filepath.Join(proc, "vmstat"), strings.ReplaceAll(vmstat, "oom_kill 3", "oom_kill 4"))
		}
		w := Worker{Clock: clock, Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}}
		s := cpuSpec(1, 0)
		s.Needs.Platform = Platform{Required: []string{"cuda"}}
		// Device counters are exercised separately below; this test isolates
		// the independent 200 ms cgroup/proc collector without launching CUDA.
		finish := w.observeAttemptMemoryEvery(a, Job{Spec: s}, nil, time.Hour)
		defer finish()
		time.Sleep(600 * time.Millisecond)
		peak = finish()
	})
	return peak
}

func TestCUDAHostMemoryCompleteCalibration(t *testing.T) {
	for _, mode := range []string{"complete", "masked host drop", "missing cgroup", "missing proc pin", "missing physical", "missing global pins", "reset global pins", "host OOM"} {
		t.Run(mode, func(t *testing.T) {
			p := cudaHostFixture(t, mode)
			complete := mode == "complete" || mode == "masked host drop"
			if p == nil || (p.NonreclaimableGB != nil) != (complete || mode == "host OOM") {
				t.Fatalf("wrong complete evidence: %+v", p)
			}
			const nr = 2 + .25 + .125 + .0625 + .03125 + .015625
			const pins = .03125 + .0625 + .015625
			const combined = nr + 1 + pins
			if complete {
				got, ok := p.gpuHostPeakGB()
				if !ok || got != combined || *p.NonreclaimableGB != nr || p.GPUHost.Source != cudaHostMemorySource || *p.GPUHost.PinnedGB != pins || *p.GPUHost.PhysicalGB != 11.125 {
					t.Fatalf("pins/uncharged physical demand lost: %+v, combined %v", p, got)
				}
			}
			q, clock, m, s := unifiedAutoFixture(t, 0)
			var last *Job
			for i := 0; i < 2; i++ {
				queued := submit(t, q, s)
				last = assignedJob(machineCheck(t, q, m))
				if last == nil || last.ID != queued.ID {
					t.Fatal("not assigned", job(t, q, queued.ID).Reason)
				}
				if err := q.Started(result(last, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
				r := result(last, true)
				r.MemoryPeak = sampledMemoryPeak(nil, p)
				device := 4.0
				r.MemoryPeak.DeviceGB = &device
				body, _ := json.Marshal(r)
				var wire Result
				if err := json.Unmarshal(body, &wire); err != nil {
					t.Fatal(err)
				}
				if err := q.Complete(wire); err != nil {
					t.Fatal(err)
				}
				facts := job(t, q, last.ID).MemoryAttempts
				if len(facts) != 1 || facts[0].Attempt != last.Attempt || facts[0].Session != last.Session || (facts[0].LearningReason == "eligible") != (mode != "host OOM") {
					t.Fatal("attempt facts or pressure exclusion lost", facts)
				}
			}
			probe := *q.s.Jobs[last.ID]
			probe.State = "queued"
			target := m.Devices[0].machine(m)
			estimate := q.memoryEstimate(&probe, &target)
			if mode == "host OOM" {
				if estimate.measuredHostGB != 0 {
					t.Fatal("OOM evidence trained the envelope", estimate)
				}
				return
			}
			want := 12 + minimumHostMemoryGB
			if complete {
				want = combined + .125
			}
			if math.Abs(estimate.measuredHostGB-want) > 1e-9 {
				t.Fatalf("calibrated %g, want %g (complete %v)", estimate.measuredHostGB, want, complete)
			}
		})
	}
}
