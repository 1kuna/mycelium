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

func TestAttemptMemoryCheckinBeforeCUDAContext(t *testing.T) {
	for _, elapsed := range []time.Duration{958 * time.Second, 60 * time.Second} {
		t.Run(elapsed.String(), func(t *testing.T) {
			q, clock := fixture(t)
			m := sharedUnifiedMachine()
			m.Devices[0].BudgetGB, m.Devices[0].FreeMemoryGB = 96, 96
			m.Devices[0].TotalMemoryGB = 121
			m.HostMemoryBudgetGB = 96
			m.MemoryObservation = &HostMemoryObservation{TotalGB: 128, UsedGB: 10}
			s := sharedSpec("experiment", 36)
			s.Needs.HostMemoryGB = 24
			first := submit(t, q, s)
			a := assignedJob(machineCheck(t, q, m))
			if a == nil || a.ID != first.ID {
				t.Fatal("model job not assigned")
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(elapsed - deviceMemorySampleFreshFor)
			s = sharedSpec("experiment", 6)
			s.Needs.HostMemoryGB = 8
			second := submit(t, q, s)
			b := assignedJob(growthCheck(t, q, m, []*Job{a}, []float64{29.2509765625}, 0))
			if b == nil || b.ID != second.ID {
				t.Fatal("fitting co-tenant not assigned", job(t, q, second.ID).Reason)
			}
			if err := q.Started(result(b, false)); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			// The co-tenant is still importing/loading CPU data: a complete
			// driver enumeration contains the model, but no CUDA context for it.
			if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte("#!/bin/sh\nprintf 'GPU-one, 42, 29953\\n'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			clock.Advance(deviceMemorySampleFreshFor)
			workerClock := &lockedTestClock{now: clock.Now()}
			w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}, Clock: workerClock}
			memory := &memoryAttempt{}
			ctx, cancel := context.WithCancel(context.Background())
			ticks := make(chan time.Time, 2)
			probes := 0
			done := w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
				probes++
				if probes == 2 {
					cancel()
					return nil, nil, context.Canceled
				}
				return map[int]bool{43: true}, map[int]bool{42: true}, nil
			}, nil, deviceMemorySamplePeriod, workerClock, memory, ticks)
			t.Cleanup(cancel)
			ticks <- workerClock.Now()
			// The second inventory callback cancels after the first complete
			// driver command, without a wall-clock wait for the fake sensor.
			ticks <- workerClock.Now()
			<-done
			sample := memory.device.Load()
			if sample == nil || sample.gb != 0 || sample.exclusive || !sample.at.Equal(clock.Now()) {
				t.Fatal("pre-context enumeration did not produce exact fresh zero", sample)
			}
			modelGB, zero := 29.2509765625, sample.gb
			reports := []AttemptReport{
				{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{DeviceGB: &modelGB, DeviceGrowthGB: 15.947265625}},
				{Execution: Execution{ID: b.ID, Attempt: b.Attempt}, Session: b.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{DeviceGB: &zero}},
			}
			if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports}); err != nil {
				t.Fatal(err)
			}
			requireState(t, q, a.ID, "running")
			requireState(t, q, b.ID, "running")
			// Declared or measured needs cannot borrow the unused part of a
			// device vector: refuse a third job exceeding either bound.
			for _, used := range []float64{29.2509765625, 70} {
				s = sharedSpec("experiment", 61)
				s.Needs.HostMemoryGB = 1
				third := submit(t, q, s)
				modelGB = used
				worker := Worker{Machine: m, Clock: clock, hostMemorySamples: currentHostMemorySamples(reports, clock.Now())}
				if err := localAttemptAdmission(worker, []Job{*a, *b}, *third, nil); err == nil || admissionDetail(err, Job{}) == nil || admissionDetail(err, Job{}).Subcheck != "device_budget" {
					t.Fatal("worker did not enforce the same device budget", used, err)
				}
				if r := growthCheck(t, q, m, []*Job{a, b}, []float64{used, 0}, 0); assignedJob(r) != nil {
					t.Fatal("non-fitting device vectors shared", used)
				}
				requireState(t, q, third.ID, "queued")
				if err := q.Cancel(third.ID); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
