package myce

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Driver-shaped fixtures exercise the actual CUDA sampler and Result wire
// boundary. A startup load stays in the conservative growth forecast; the
// measured tail and missing-cycle evidence survive independently for #1044.
func TestDeviceMemorySamplingCUDAEvidence(t *testing.T) {
	for _, mode := range []string{"complete", "missed interval", "late first sample", "inventory failure", "stale finish"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("MYCE_TEST_CUDA_READING", filepath.Join(dir, "reading"))
			if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte("#!/bin/sh\ncat \"$MYCE_TEST_CUDA_READING\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			clock := &lockedTestClock{now: time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)}
			a := &memoryAttempt{}
			w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}}
			ctx, cancel := context.WithCancel(context.Background())
			ticks := make(chan time.Time)
			inventoryError := false
			out := w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
				if inventoryError {
					return nil, nil, os.ErrPermission
				}
				return map[int]bool{42: true}, nil, nil
			}, nil, time.Second, clock, a, ticks)
			defer cancel()
			for i, raw := range []string{"GPU-one, 42, 1024\nGPU-other, 42, 65536\nGPU-one, 99, 8192\n", "GPU-one, 42, 17408\n", "GPU-one, 42, 17664\n"} {
				if err := os.WriteFile(filepath.Join(dir, "reading"), []byte(raw), 0600); err != nil {
					t.Fatal(err)
				}
				at := clock.Now().Add(time.Second)
				if mode == "missed interval" && i == 2 {
					at = at.Add(2 * time.Second)
				}
				if mode == "late first sample" && i == 0 {
					at = at.Add(2 * time.Second)
				}
				clock.advance(at.Sub(clock.Now()))
				ticks <- at
				await(t, func() bool { s := a.device.Load(); return s != nil && s.at == at })
			}
			if mode == "inventory failure" {
				// The unbuffered tick establishes happens-before for the flag;
				// cancellation joins the collector before inspecting its summary.
				inventoryError = true
				ticks <- clock.Now()
			}
			if mode == "stale finish" {
				clock.advance(3 * time.Second)
			}
			cancel()
			var p *AttemptMemoryPeak
			select {
			case r := <-out:
				p = r.peak
			case <-time.After(testWaitTimeout):
				t.Fatal("device sampler did not stop")
			}
			body, _ := json.Marshal(Result{MemoryPeak: p})
			var wire Result
			if err := json.Unmarshal(body, &wire); err != nil {
				t.Fatal(err)
			}
			s := wire.MemoryPeak.DeviceSampling
			if s == nil || s.Samples != 3 || s.MinGB != 1 || s.MaxGB != 17.25 || s.LastGB != 17.25 || s.MaxStepGB != 16 || s.LastStepGB != .25 || s.Complete != (mode == "complete") {
				t.Fatalf("lost measured CUDA variation/coverage: %+v", s)
			}
			if *wire.MemoryPeak.DeviceGrowthGB < 16 {
				t.Fatal("tail diagnostic discounted conservative startup growth")
			}
			if mode == "complete" {
				// Exercise real acceptance and SQLite retention with both sensors,
				// rather than only inspecting a worker-local sampling summary.
				peak := sampledMemoryPeak(cudaHostFixture(t, "complete"), wire.MemoryPeak)
				q, queueClock, m, spec := unifiedAutoFixture(t, 0)
				queued := submit(t, q, spec)
				run := assignedJob(machineCheck(t, q, m))
				if run == nil || run.ID != queued.ID {
					t.Fatal("not assigned")
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				queueClock.Advance(time.Second)
				r := result(run, true)
				r.MemoryPeak = peak
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				q = memoryFactsRestart(t, q, queueClock)
				facts := job(t, q, run.ID).MemoryAttempts
				if len(facts) != 1 || facts[0].Attempt != run.Attempt || facts[0].Session != run.Session || facts[0].LearningReason != "eligible" || facts[0].MemoryPeak.DeviceSampling == nil || *facts[0].MemoryPeak.DeviceSampling != *s || facts[0].MemoryPeak.GPUHost.PinnedGB == nil {
					t.Fatal("acceptance/restart lost exact-attempt CUDA evidence", facts)
				}
				host := 2.0
				if merged := mergeMemoryPeaks(peak, &AttemptMemoryPeak{HostGB: &host}); merged.DeviceSampling != nil {
					t.Fatal("a new incomplete row inherited an older attempt's coverage")
				}
			}
		})
	}
}
