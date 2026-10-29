package myce

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
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
