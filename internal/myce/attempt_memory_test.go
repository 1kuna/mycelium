package myce

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAttemptMemoryDeviceQueryInheritedPipe(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	pidFile := filepath.Join(dir, "child")
	t.Setenv("MYCE_TEST_DEVICE_CHILD", pidFile)
	// The query exits, but a descendant retains stdout/stderr. CommandContext
	// alone cannot interrupt Output's pipe-copy goroutines after that exit.
	// The descendant outlives the test's backstop; cleanup kills it.
	script := "#!/bin/sh\nsleep 120 &\necho $! > \"$MYCE_TEST_DEVICE_CHILD\"\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		b, err := os.ReadFile(pidFile)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err == nil && pid > 0 {
				p, err := os.FindProcess(pid)
				if err == nil {
					_ = p.Kill()
				}
			}
		}
	})
	w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one"}}
	var err error
	returnsWhileBlocked(t, "device query with inherited pipe", func() {
		_, _, err = w.attemptDeviceMemoryReading(context.Background(), map[int]bool{42: true}, nil, nil)
	})
	if err != errAttemptMemoryPending {
		t.Fatalf("incomplete query must remain unknown: %v", err)
	}
}

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

func TestAttemptMemoryDeviceAccounting(t *testing.T) {
	peak, foreign, err := cudaAttemptMemoryReading([]byte("GPU-one, 7, 2048\nGPU-one, 9, 4096\nGPU-two, 7, 8192\n"), "GPU-one", map[int]bool{7: true}, nil)
	if err != nil || peak != 2 || !foreign {
		t.Fatal("charged another process/device", peak, foreign, err)
	}
	if _, _, err := cudaAttemptMemoryReading([]byte("GPU-one, 7, N/A\n"), "GPU-one", map[int]bool{7: true}, nil); err != errDeviceMemoryNotReported {
		t.Fatal("N/A became zero")
	}
	for _, inventory := range []string{"", "GPU-one, 9, 4096\n", "GPU-two, 7, 8192\n"} {
		gb, _, err := cudaAttemptMemoryReading([]byte(inventory), "GPU-one", map[int]bool{7: true}, nil)
		if err != nil || gb != 0 {
			t.Fatal("successful inventory without an attempt CUDA context is not a zero sample", gb, err)
		}
	}
	client, peak, err := drmAttemptMemory([]byte("drm-client-id: 7\ndrm-pdev: 0000:03:00.0\ndrm-resident-vram0: 2048 MiB\n"), "0000:03:00.0")
	if err != nil || peak != 2 || client != "0000:03:00.0/7" {
		t.Fatal("bad DRM client memory", client, peak, err)
	}
	if _, _, err := drmAttemptMemory([]byte("drm-client-id: 7\ndrm-pdev: 0000:03:00.0\n"), "0000:03:00.0"); err != errAttemptMemoryPending {
		t.Fatal("uninitialised DRM counters disabled sampling", err)
	}
}

func TestAttemptMemoryDeviceUnavailable(t *testing.T) {
	w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}}
	j := Job{Spec: Spec{Needs: ResourceNeeds{MemoryGB: 40, HostMemoryGB: 24, Platform: Platform{Required: []string{"cuda"}}}}}
	// A short declared job can finish before the five-second device tick.
	a, err := fakeCeilings(t).create("attempt-short-cuda", 64<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		attempt *memoryAttempt
		reason  string
	}{{a, deviceMemoryNoSample}, {nil, deviceMemoryNoCgroup}} {
		p := w.observeAttemptMemory(tc.attempt, j, nil)()
		if p == nil || p.DeviceGB != nil || p.DeviceUnavailable != tc.reason {
			t.Fatalf("missing device telemetry was silently omitted: %+v", p)
		}
	}
	// NVIDIA N/A is unknown, not zero. A reason-only peak survives aggregation,
	// but a subsequent usable process sample replaces the unavailable label.
	for _, value := range []string{"N/A", "[N/A]", "Not Supported"} {
		if _, _, err := cudaAttemptMemoryReading([]byte("GPU-one, 42, "+value+"\n"), "GPU-one", map[int]bool{42: true}, nil); err != errDeviceMemoryNotReported {
			t.Fatalf("%s became a device measurement: %v", value, err)
		}
	}
	p := sampledMemoryPeak(nil, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryUnsupported})
	if p == nil || p.DeviceUnavailable != deviceMemoryUnsupported || p.DeviceGB != nil {
		t.Fatal("reason-only peak lost", p)
	}
	gb := 18556.0 / 1024
	p = sampledMemoryPeak(p, &AttemptMemoryPeak{DeviceGB: &gb})
	p = sampledMemoryPeak(p, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryUnsupported})
	if p.DeviceUnavailable != "" || p.DeviceGB == nil || *p.DeviceGB != gb {
		t.Fatal("unavailable label replaced a retained measurement", p)
	}
	if p := sampledMemoryPeak(nil, &AttemptMemoryPeak{DeviceUnavailable: "private-command-output"}); p.DeviceUnavailable != "" {
		t.Fatal("published an unrecognized device diagnostic", p)
	}
}
