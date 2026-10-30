package myce

import (
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestAttemptMemoryCeilingEventsConstrainPeak(t *testing.T) {
	for _, event := range []string{"max", "high"} {
		t.Run(event, func(t *testing.T) {
			a, err := fakeCeilings(t).create("attempt-bound", 2<<30, 0)
			if err != nil {
				t.Fatal(err)
			}
			for name, value := range map[string]string{
				"memory.stat":   "anon 2147483648\nkernel 0\nshmem 0\npgscan_file 100\n",
				"memory.events": event + " 1\noom 0\noom_kill 0\n",
			} {
				if err := os.WriteFile(filepath.Join(a.path, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if p := a.peak(); p == nil || p.HostGB == nil || *p.HostGB != 2 || !p.Constrained {
				t.Fatalf("ceiling-bound resident peak was unconstrained: %+v", p)
			}
		})
	}
}

type nonreclaimableSamplingFS struct {
	memoryFS
	reads   atomic.Int32
	sampled chan struct{}
	missing bool
}

func (f *nonreclaimableSamplingFS) ReadFile(path string) ([]byte, error) {
	b, err := f.memoryFS.ReadFile(path)
	if filepath.Base(path) == "memory.stat" && f.reads.Add(1) == 2 {
		close(f.sampled)
		if f.missing {
			return []byte("anon 4021977088\nkernel 17985536\nshmem 4096\n"), nil
		}
	}
	return b, err
}

func TestAttemptMemoryNonreclaimableRequiresCompleteSampling(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(strconv.FormatBool(missing), func(t *testing.T) {
			a, err := fakeCeilings(t).create("attempt-complete", 16<<30, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(a.path, "memory.stat"), []byte("anon 4021977088\nkernel 17985536\nshmem 4096\nunevictable 0\n"), 0600); err != nil {
				t.Fatal(err)
			}
			f := &nonreclaimableSamplingFS{memoryFS: a.fs, sampled: make(chan struct{}), missing: missing}
			a.fs = f
			finish := (Worker{}).observeAttemptMemory(a, Job{Spec: cpuSpec(1, 0)}, nil)
			defer finish()
			select {
			case <-f.sampled:
			case <-time.After(5 * time.Second):
				t.Fatal("host sampler did not produce a sample")
			}
			p := finish()
			if (p.NonreclaimableGB == nil) != missing {
				t.Fatalf("complete=%v peak=%+v", !missing, p)
			}
			if !missing && *p.NonreclaimableGB != float64(4021977088+17985536+4096)/(1<<30) {
				t.Fatal("wrong sampled peak")
			}
		})
	}
}

func TestAttemptMemoryCachePressureAndMemberCaps(t *testing.T) {
	for _, tc := range []struct {
		name                string
		resident, cap       int64
		member, constrained bool
	}{
		{"file-backed pressure despite resident headroom", 8 << 30, 10 << 30, false, true},
		{"near parent ceiling", 19 << 29, 10 << 30, false, true},
		{"binding member cap", 19 << 29, 10 << 30, true, true},
		{"member file-backed pressure", 2 << 30, 10 << 30, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := fakeCeilings(t).create("attempt-parent", tc.cap, 0)
			if err != nil {
				t.Fatal(err)
			}
			observed := a
			if tc.member {
				a.bytes = 40 << 30
				children, err := a.children()
				if err != nil {
					t.Fatal(err)
				}
				observed, err = children.create("main", tc.cap, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(a.path, "memory.stat"), []byte("anon 10737418240\nkernel 0\nshmem 0\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			for name, value := range map[string]string{
				"memory.stat":   "anon " + strconv.FormatInt(tc.resident, 10) + "\nkernel 0\nshmem 0\n",
				"memory.events": "max 559\noom 0\noom_kill 0\n",
			} {
				if err := os.WriteFile(filepath.Join(observed.path, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if p := a.peak(); p == nil || p.Constrained != tc.constrained {
				t.Fatalf("resident pressure misclassified: %+v; want constrained=%v", p, tc.constrained)
			}
		})
	}
}

func TestAttemptMemoryDeviceAccounting(t *testing.T) {
	peak, err := cudaAttemptMemory([]byte("GPU-one, 7, 2048\nGPU-one, 9, 4096\nGPU-two, 7, 8192\n"), "GPU-one", map[int]bool{7: true})
	if err != nil || peak != 2 {
		t.Fatal("charged another process/device", peak, err)
	}
	if _, err := cudaAttemptMemory([]byte("GPU-one, 7, N/A\n"), "GPU-one", map[int]bool{7: true}); err != errAttemptMemoryUnsupported {
		t.Fatal("N/A became zero")
	}
	client, peak, err := drmAttemptMemory([]byte("drm-client-id: 7\ndrm-pdev: 0000:03:00.0\ndrm-resident-vram0: 2048 MiB\n"), "0000:03:00.0")
	if err != nil || peak != 2 || client != "0000:03:00.0/7" {
		t.Fatal("bad DRM client memory", client, peak, err)
	}
	if _, _, err := drmAttemptMemory([]byte("drm-client-id: 7\ndrm-pdev: 0000:03:00.0\n"), "0000:03:00.0"); err != errAttemptMemoryPending {
		t.Fatal("uninitialised DRM counters disabled sampling", err)
	}
}

func TestAttemptMemoryNonreclaimableRetainsLockedPages(t *testing.T) {
	a, err := fakeCeilings(t).create("attempt-cache", 16<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A live-shaped sample: 3.746 GiB anon, 1.103 GiB file, 17 MiB
	// kernel. Include locked file pages and shmem without counting file cache.
	for name, value := range map[string]string{
		"memory.stat":   "anon 4021977088\nfile 1183789056\nkernel 17985536\nshmem 4096\nunevictable 67108864\nslab_reclaimable 3814504\n",
		"memory.events": "max 0\nhigh 0\noom 0\noom_kill 0\noom_group_kill 0\n",
	} {
		if err := os.WriteFile(filepath.Join(a.path, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	p := a.peak()
	want := float64(4021977088+17985536+4096+67108864) / (1 << 30)
	if p.nonreclaimableSampleGB == nil || *p.nonreclaimableSampleGB != want || p.constrained() {
		t.Fatalf("bad non-reclaimable measurement: %+v", p)
	}
	if p.NonreclaimableGB != nil {
		t.Fatal("raw/release-only counter read claimed a complete peak")
	}
	if err := os.WriteFile(filepath.Join(a.path, "memory.stat"), []byte("anon 4021977088\nkernel 17985536\nshmem 4096\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if p := a.peak(); p.nonreclaimableSampleGB != nil {
		t.Fatalf("missing locked-page counter became zero: %+v", p)
	}
}
