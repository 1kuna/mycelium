package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAttemptMemoryCheckinRefreshesDeviceSample(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	a, b := sharedPair(t, q, m)
	dir := t.TempDir()
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	// Fake driver inventory: two co-tenants on one unified CUDA card, plus
	// an unrelated card that must never contribute to either attempt.
	if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte("#!/bin/sh\nprintf 'GPU-one, 42, 30720\\nGPU-one, 43, 13312\\nGPU-other, 42, 65536\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	var attempts []*memoryAttempt
	var reports []AttemptReport
	for _, j := range []*Job{a, b} {
		memory, err := fakeCeilings(t).create("attempt-"+j.ID, 64<<30, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(memory.path, "memory.current"), []byte("0"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(memory.path, "memory.stat"), []byte("anon 0\n"), 0600); err != nil {
			t.Fatal(err)
		}
		attempts = append(attempts, memory)
		reports = append(reports, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: memory.currentHostMemory(clock)})
	}
	// Capture the actors before CUDA initialization, then leave those reports
	// untouched while their samplers continue during slow actor log/result I/O.
	clock.Advance(12 * time.Second)
	workerClock := &lockedTestClock{now: clock.Now()}
	w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}, Clock: workerClock}
	ctx, cancel := context.WithCancel(context.Background())
	var results []<-chan deviceMemoryResult
	for i, memory := range attempts {
		pid := 42 + i
		results = append(results, w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
			return map[int]bool{pid: true}, nil, nil
		}, memory.fs, 20*time.Millisecond, workerClock, memory))
	}
	defer func() {
		cancel()
		for _, result := range results {
			select {
			case <-result:
			case <-time.After(testWaitTimeout):
				t.Error("device sampler did not stop")
			}
		}
	}()
	deadline := time.After(testWaitTimeout)
	for attempts[0].device.Load() == nil || attempts[1].device.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("fake driver did not supply both per-process samples")
		case <-time.After(10 * time.Millisecond):
		}
	}
	for i, want := range []float64{30, 13} {
		cached := reports[i].CurrentHostMemory
		reports[i].CurrentHostMemory = cached.aged(clock.Now())
		sample := reports[i].CurrentHostMemory
		if sample == nil || sample.DeviceGB == nil || *sample.DeviceGB != want || sample.DeviceAgeSeconds != 0 || sample.AgeSeconds != 12 {
			t.Fatalf("cached actor hid fresh device evidence or renewed old host evidence: %+v", sample)
		}
		if cached.DeviceGB != nil {
			t.Fatal("refresh mutated the actor's published snapshot")
		}
	}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports}); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, a.ID); got.State != "stopping" || priorityStop(got) == nil || priorityStop(got).Cause != sharedGPUPressureCause {
		t.Fatal("fresh sampler evidence did not reach the growth guard", got)
	}
	requireState(t, q, b.ID, "running")
	// Refreshing cannot renew a stalled sensor's timestamp.
	clock.Advance(deviceMemorySampleFreshFor)
	for i := range reports {
		reports[i].CurrentHostMemory = reports[i].CurrentHostMemory.aged(clock.Now())
	}
	for _, sample := range currentHostMemorySamples(reports, clock.Now()) {
		if !sample.deviceAt.IsZero() {
			t.Fatal("stale device evidence was refreshed into fresh evidence")
		}
	}
}
