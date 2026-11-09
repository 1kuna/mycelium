package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSharedDeviceHostOnlyLearningKeepsCard(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, layout := range []string{"separate", "unified"} {
			t.Run(layout, func(t *testing.T) {
				q, clock := fixture(t)
				m := sharedUnifiedMachine()
				m.CapabilityProfile.MemoryLayout = layout
				m.Devices[0].MemoryMode = layout
				m.Devices[0].UnifiedMemory = layout == "unified"
				m.Devices[0].ExtensionFingerprint = "synthetic-cuda-v1"
				s := plainGPUSpec()
				s.Needs.Memory = "auto"
				if layout == "separate" {
					// A recipe/bootstrap hint survives host-only learning as a
					// positive device default, which used to authorize sharing.
					s.Needs.MemoryGB, s.Needs.HostMemoryGB = 8, 1
				}
				seed := assignGPU(t, q, m, submit(t, q, s))
				if err := q.Started(result(seed, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
				host := 1.0
				r := result(seed, true)
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}

				// A host peak alone must not authorize joining a declared tenant.
				blocker := assignGPU(t, q, m, submit(t, q, sharedSpec("experiment", 13)))
				queued := submit(t, q, s)
				if got := assignedJob(machineCheck(t, q, m, blocker)); got != nil {
					t.Fatal("host-only learning authorized joining a busy card", got.MemoryEnvelope)
				}
				finish(t, q, blocker, true)
				unknown := assignedJob(machineCheck(t, q, m))
				if unknown == nil || unknown.ID != queued.ID {
					t.Fatal("unmeasured work could not run alone", job(t, q, queued.ID).Reason)
				}
				if layout == "separate" && (unknown.MemoryEnvelope.Basis != "exploratory" || !strings.Contains(unknown.MemoryEnvelope.Source, "unknown device peak; exclusive card")) {
					t.Fatal("host-only evidence lost exploratory card exclusivity", unknown.MemoryEnvelope)
				}
				if err := q.Started(result(unknown, false)); err != nil {
					t.Fatal(err)
				}
				// Nor may a declared tenant join an already running default vector.
				peer := submit(t, q, sharedSpec("experiment", 13))
				if got := assignedJob(machineCheck(t, q, m, unknown)); got != nil {
					t.Fatal("declared work joined an unmeasured tenant", got.ID)
				}

				// Once a complete run measures device use, auto work may share again.
				clock.Advance(time.Second)
				device := 8.0
				r = result(unknown, true)
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				runningPeer := assignedJob(machineCheck(t, q, m))
				if runningPeer == nil || runningPeer.ID != peer.ID {
					t.Fatal("declared peer did not start after release")
				}
				next := submit(t, q, s)
				measured := assignedJob(machineCheck(t, q, m, runningPeer))
				if measured == nil || measured.ID != next.ID || measured.MemoryEnvelope.Basis != "learned" || measured.Spec.Needs.MemoryGB != 8+minimumHostMemoryGB || strings.Contains(measured.MemoryEnvelope.Source, "GPU recipe default") {
					t.Fatal("accepted device evidence did not enable sharing", measured, job(t, q, next.ID).Reason)
				}
			})
		}

	})

	t.Run("UnifiedAttemptMemoryLearningSurvivesRestart", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
		if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte("#!/bin/sh\nprintf 'GPU-one, 42, N/A\\n'\n"), 0700); err != nil {
			t.Fatal(err)
		}
		fs := &unifiedResidentFS{values: map[string]string{
			"/synthetic/attempt/memory.stat": "anon 8589934592\nkernel 0\nshmem 0\n",
			"/synthetic/memory.stat":         "anon 8589934592\nkernel 0\nshmem 0\n",
			"/proc/42/status":                "VmRSS: 8388608 kB\n",
			"/proc/meminfo":                  "MemAvailable: 58720256 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n",
		}}
		q, clock := fixture(t)
		m := sharedUnifiedMachine()
		m.Devices[0].ExtensionFingerprint = "synthetic-cuda-v1"
		s := plainGPUSpec()
		s.Needs.Memory = "auto"
		seed := assignGPU(t, q, m, submit(t, q, s))
		if err := q.Started(result(seed, false)); err != nil {
			t.Fatal(err)
		}
		a := &memoryAttempt{fs: fs, path: "/synthetic/attempt", pageSize: 4096, hostBaseline: &gpuHostState{available: 64}, rootBaseline: new(float64)}
		w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}}
		ctx, cancel := context.WithCancel(context.Background())
		ticks := make(chan time.Time, 1)
		device := w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) { return map[int]bool{42: true}, nil, nil }, fs, time.Second, clock, a, ticks)
		ticks <- clock.Now()
		await(t, func() bool { return a.device.Load() != nil })
		cancel()
		var peak *AttemptMemoryPeak
		select {
		case r := <-device:
			peak = r.peak
		case <-time.After(testWaitTimeout):
			t.Fatal("sampler did not stop")
		}
		if peak.DeviceGB == nil || *peak.DeviceGB != 8 || peak.DeviceSource != deviceUnifiedResidentSource {
			t.Fatal("complete fallback lost its labelled device evidence", peak)
		}
		host := 1.0
		peak.HostGB = &host
		clock.Advance(time.Second)
		r := result(seed, true)
		r.MemoryPeak = peak
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		q = memoryFactsRestart(t, q, clock)
		facts := job(t, q, seed.ID).MemoryAttempts
		if len(facts) != 1 || facts[0].LearningReason != "eligible" || facts[0].MemoryPeak.DeviceSource != deviceUnifiedResidentSource {
			t.Fatal("restart lost fallback provenance", facts)
		}
		peer := assignGPU(t, q, m, submit(t, q, sharedSpec("experiment", 13)))
		next := submit(t, q, s)
		learned := assignedJob(machineCheck(t, q, m, peer))
		if learned == nil || learned.ID != next.ID || learned.MemoryEnvelope.Basis != "learned" || learned.Spec.Needs.MemoryGB < 8 || learned.Spec.Needs.MemoryGB >= m.Devices[0].BudgetGB {
			t.Fatal("accepted fallback did not enable learned sharing after restart", learned, job(t, q, next.ID).Reason)
		}

	})
}
