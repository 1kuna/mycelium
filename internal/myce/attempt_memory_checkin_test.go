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
		if err := os.WriteFile(filepath.Join(memory.path, "memory.stat"), []byte("anon 0\nshmem 0\nkernel 0\n"), 0600); err != nil {
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
	for i, j := range []*Job{a, b} {
		sample := q.hostMemorySamples[m.ID][jobKey(*j)]
		if sample.deviceGB != []float64{30, 13}[i] {
			t.Fatal("queue lost the attempt's device footprint", sample)
		}
	}
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

func TestAttemptMemoryCheckinBeforeCUDAContext(t *testing.T) {
	clock := &lockedTestClock{now: time.Unix(100, 0)}
	w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}, Clock: clock,
		cudaMemoryQuery: func(context.Context) ([]byte, error) { return []byte("GPU-one, 42, 29953\n"), nil }}
	memory := &memoryAttempt{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ticks := make(chan time.Time, 2)
	probes := 0
	done := w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
		probes++
		if probes == 2 {
			cancel()
			return nil, nil, context.Canceled
		}
		return map[int]bool{43: true}, map[int]bool{42: true}, nil
	}, nil, deviceMemorySamplePeriod, clock, memory, ticks)
	ticks <- clock.Now()
	ticks <- clock.Now()
	<-done
	sample := memory.device.Load()
	if sample == nil || sample.gb != 0 || !sample.at.Equal(clock.Now()) {
		t.Fatal("pre-context enumeration did not produce exact fresh zero", sample)
	}
}

func TestAttemptMemoryUnreportedCUDATickBreaksCoverage(t *testing.T) {
	clock := &lockedTestClock{now: time.Unix(100, 0)}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	queries := 0
	w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}, Clock: clock,
		cudaMemoryQuery: func(context.Context) ([]byte, error) {
			queries++
			clock.advance(deviceMemorySamplePeriod)
			if queries == 2 {
				return []byte("GPU-one, 42, [N/A]\n"), nil
			}
			return []byte("GPU-one, 42, 2048\n"), nil
		}}
	ticks := make(chan time.Time, 4)
	for range cap(ticks) {
		ticks <- clock.Now()
	}
	done := w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
		if queries == 3 {
			cancel()
			return nil, nil, nil
		}
		return map[int]bool{42: true}, nil, nil
	}, nil, deviceMemorySamplePeriod, clock, nil, ticks)
	got := <-done
	if s := got.peak.DeviceSampling; s == nil || s.Samples != 2 || s.Complete || !got.unreported {
		t.Fatalf("N/A between valid samples reported complete CUDA attribution: %+v", s)
	}
}
