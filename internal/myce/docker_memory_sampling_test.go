package myce

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestDockerSharedDeviceSamplesContainerDescendants(t *testing.T) {
	w, s, clock, root := dockerFixture(t)
	j := dockerJob()
	j.Spec.Needs.HostMemoryGB = 4
	j.Spec.Run.DockerMemoryGB = 2
	// The managed attempt starts its sampler concurrently. Configure the
	// fixture before launch instead of replacing its worker after startup.
	w.Machine.GPU = "cuda"
	w.Machine.Device = "GPU-one"
	w.selectedDevice = nil
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.stop(clock.Now(), 0, true)
		clock.advance(defaultGPUStopGrace + 5*time.Second)
		p.docker.waitShutdown(clock)
		p.docker.close()
	})
	await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
	if err := os.WriteFile(filepath.Join(root, "pids"), []byte("42\n43"), 0600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte("#!/bin/sh\necho query >> '"+filepath.Join(dir, "queries")+"'\nprintf 'GPU-one, 42, 1024\\nGPU-one, 43, 2048\\nGPU-one, 99, 8192\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	finish := p.docker.observeMemory(j, 20*time.Millisecond)
	await(t, func() bool {
		b, _ := os.ReadFile(filepath.Join(dir, "queries"))
		return bytes.Count(b, []byte("\n")) >= 2
	})
	peak := finish()
	if peak == nil || peak.DeviceGB == nil || *peak.DeviceGB != 3 || peak.DeviceUnavailable != "" {
		t.Fatalf("container device sample: %+v", peak)
	}
	ids, err := p.docker.processIDs(context.Background())
	if err != nil || !ids[42] || !ids[43] || ids[99] {
		t.Fatal("incorrect host PIDs", ids, err)
	}
	gb, foreign, err := cudaAttemptMemoryReading([]byte("GPU-one, 42, 1024\nGPU-one, 99, 8192"), "GPU-one", ids, nil, map[int]bool{99: true})
	if err != nil || foreign || gb != 1 {
		t.Fatal("known co-tenant counted as foreign", gb, foreign, err)
	}
	// The fixture's exact cap is inspected by the real managed adapter.
	limit, _ := os.ReadFile(filepath.Join(root, "--memory"))
	if string(limit) != strconv.FormatInt(2<<30, 10) {
		t.Fatal("missing cap", string(limit))
	}
}
