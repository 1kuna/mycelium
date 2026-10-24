package myce

import "testing"

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
