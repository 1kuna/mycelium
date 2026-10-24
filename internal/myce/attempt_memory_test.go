package myce

import "testing"

func TestAttemptMemoryDeviceAccounting(t *testing.T) {
	peak, ok := cudaAttemptMemory([]byte("GPU-one, 7, 2048\nGPU-one, 9, 4096\nGPU-two, 7, 8192\n"), "GPU-one", map[int]bool{7: true})
	if !ok || peak != 2 {
		t.Fatal("charged another process/device", peak, ok)
	}
	if _, ok := cudaAttemptMemory([]byte("GPU-one, 7, N/A\n"), "GPU-one", map[int]bool{7: true}); ok {
		t.Fatal("N/A became zero")
	}
	client, peak, ok := drmAttemptMemory([]byte("drm-client-id: 7\ndrm-pdev: 0000:03:00.0\ndrm-resident-vram0: 2097152 KiB\n"), "0000:03:00.0")
	if !ok || peak != 2 || client != "0000:03:00.0/7" {
		t.Fatal("bad DRM client memory", client, peak, ok)
	}
}
