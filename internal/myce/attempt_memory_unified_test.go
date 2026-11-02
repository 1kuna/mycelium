package myce

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Host MemAvailable 10 GiB below the 64 GiB baseline fakeMemoryFS reports.
type unifiedDropFS struct{ fakeMemoryFS }

func (f unifiedDropFS) ReadFile(p string) ([]byte, error) {
	if p == "/proc/meminfo" {
		return []byte("MemAvailable: 56623104 kB\nSwapTotal: 8388608 kB\nSwapFree: 8388608 kB\n"), nil
	}
	return f.fakeMemoryFS.ReadFile(p)
}

// I3 (#585): a unified CUDA host that lists the attempt's processes without
// per-process memory learns the device figure from the host drop outside
// Mycelium's cgroups, unless another process shares the GPU.
func TestAttemptMemoryUnifiedCUDAHostDelta(t *testing.T) {
	for _, tc := range []struct {
		name, rows string
		wantGB     float64
		wantReason string
	}{
		{"exclusive", "GPU-one, 42, [N/A]", 8, ""},
		{"shared", "GPU-one, 42, [N/A]\nGPU-one, 99, [N/A]", 0, deviceMemoryShared},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("MYCE_TEST_DEVICE_PROBE", dir)
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			script := "#!/bin/sh\nprintf '" + tc.rows + "\\n'\n: > \"$MYCE_TEST_DEVICE_PROBE/queried\"\n"
			if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			c := fakeCeilings(t)
			rootStat := filepath.Join(c.root, "memory.stat")
			if err := os.WriteFile(rootStat, []byte("anon 1073741824\nkernel 0\nshmem 0\n"), 0600); err != nil {
				t.Fatal(err)
			}
			a, err := c.create("attempt-unified", 64<<30, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(a.path, "cgroup.procs"), []byte("42"), 0600); err != nil {
				t.Fatal(err)
			}
			// 2 GiB of the 10 GiB drop is charged to Mycelium cgroups.
			if err := os.WriteFile(rootStat, []byte("anon 3221225472\nkernel 0\nshmem 0\n"), 0600); err != nil {
				t.Fatal(err)
			}
			a.fs = unifiedDropFS{}
			w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}}
			j := Job{Spec: Spec{Needs: ResourceNeeds{MemoryGB: 4, HostMemoryGB: 4, Platform: Platform{Required: []string{"cuda"}}}}}
			finish := w.observeAttemptMemoryEvery(a, j, nil, 20*time.Millisecond)
			deadline := time.After(testWaitTimeout)
			for {
				if _, err := os.Stat(filepath.Join(dir, "queried")); err == nil {
					break
				}
				select {
				case <-deadline:
					t.Fatal("device query never ran")
				case <-time.After(10 * time.Millisecond):
				}
			}
			p := finish()
			if tc.wantReason != "" {
				if p == nil || p.DeviceGB != nil || p.DeviceUnavailable != tc.wantReason {
					t.Fatalf("shared GPU drop was attributed: %+v", p)
				}
				return
			}
			if p == nil || p.DeviceGB == nil || *p.DeviceGB != tc.wantGB || p.DeviceSource != deviceHostDeltaSource {
				t.Fatalf("unified device use not measured from the host drop: %+v", p)
			}
			// The label follows the larger device figure through aggregation.
			small, large := 4.0, 12.0
			merged := mergeMemoryPeaks(&AttemptMemoryPeak{DeviceGB: &small}, &AttemptMemoryPeak{DeviceGB: p.DeviceGB, DeviceSource: p.DeviceSource})
			if merged.DeviceSource != deviceHostDeltaSource {
				t.Fatalf("device source lost: %+v", merged)
			}
			if merged = mergeMemoryPeaks(merged, &AttemptMemoryPeak{DeviceGB: &large}); merged.DeviceSource != "" {
				t.Fatalf("per-process peak kept the delta label: %+v", merged)
			}
		})
	}
}
