package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

type unifiedResidentFS struct {
	fakeMemoryFS
	mu     sync.Mutex
	values map[string]string
}

func (f *unifiedResidentFS) ReadFile(p string) ([]byte, error) {
	f.mu.Lock()
	v, ok := f.values[p]
	f.mu.Unlock()
	if ok {
		return []byte(v), nil
	}
	return f.fakeMemoryFS.ReadFile(p)
}
func (f *unifiedResidentFS) set(p, v string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[p] = v
}

// Exercise the real independent sampler, send-time refresh, admission and guard
// with N/A driver counters. Scale every workload with physical RAM; there are
// no assumptions about a particular fleet device's capacity.
func TestUnifiedAttemptMemorySharing(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, total := range []float64{4, 1024} {
			t.Run(fmt.Sprint(total), func(t *testing.T) {
				q, clock := fixture(t)
				m := sharedUnifiedMachine()
				m.HostAvailableMemoryGB = total
				m.CapabilityProfile.SystemMemoryBytes = uint64(total * (1 << 30))
				m.MemoryObservation = &HostMemoryObservation{TotalGB: total}
				m.Devices[0].ReserveMemoryGB = 0
				m.Devices[0].BudgetGB, m.Devices[0].TotalMemoryGB, m.Devices[0].FreeMemoryGB = total, total, total
				s := sharedSpec("experiment", total/8)
				s.Needs.HostMemoryGB = total / 32
				first := submit(t, q, s)
				a := assignedJob(machineCheck(t, q, m))
				if a == nil || a.ID != first.ID {
					t.Fatal("first attempt not assigned")
				}
				if err := q.Started(result(a, false)); err != nil {
					t.Fatal(err)
				}
				second := submit(t, q, s)
				// No current evidence: a declared number alone cannot authorize sharing.
				if r := growthCheck(t, q, m, []*Job{a}, nil, 0); assignedJob(r) != nil {
					t.Fatal("missing evidence shared")
				}
				ceilings := fakeCeilings(t)
				fs := &unifiedResidentFS{values: map[string]string{}}
				rootResident := total / 4
				stat := func(gb float64) string { return fmt.Sprintf("anon %d\nkernel 0\nshmem 0\n", uint64(gb*(1<<30))) }
				fs.set(filepath.Join(ceilings.root, "memory.stat"), stat(rootResident))
				fs.set("/proc/meminfo", fmt.Sprintf("MemAvailable: %d kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n", uint64((total-rootResident)*(1<<20))))
				var memory []*memoryAttempt
				for i := 0; i < 2; i++ {
					mem, err := ceilings.create(fmt.Sprint("attempt-", i), int64(total*(1<<30)), 0)
					if err != nil {
						t.Fatal(err)
					}
					mem.fs, mem.hostBaseline, mem.rootBaseline = fs, &gpuHostState{available: total}, new(float64)
					pid := 42 + i
					fs.set(filepath.Join(mem.path, "memory.stat"), stat(total/8))
					fs.set(filepath.Join(mem.path, "memory.current"), strconv.FormatUint(uint64(total/8*(1<<30)), 10))
					fs.set(filepath.Join("/proc", strconv.Itoa(pid), "status"), fmt.Sprintf("VmRSS: %d kB\n", uint64(total/8*(1<<20))))
					memory = append(memory, mem)
				}
				dir := t.TempDir()
				t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
				if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte("#!/bin/sh\nprintf 'GPU-one, 42, N/A\\nGPU-one, 43, N/A\\n'\n"), 0700); err != nil {
					t.Fatal(err)
				}
				workerClock := &dockerTestClock{now: clock.Now()}
				w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one", UnifiedMemory: true}, Clock: workerClock}
				ctx, cancel := context.WithCancel(context.Background())
				var results []<-chan deviceMemoryResult
				var ticks []chan time.Time
				for i, mem := range memory {
					trigger := make(chan time.Time, 1)
					ticks = append(ticks, trigger)
					pid, other := 42+i, 43-i
					results = append(results, w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
						return map[int]bool{pid: true}, map[int]bool{other: true}, nil
					}, fs, time.Second, workerClock, mem, trigger))
					trigger <- workerClock.Now()
				}
				t.Cleanup(func() {
					cancel()
					for _, r := range results {
						select {
						case <-r:
						case <-time.After(testWaitTimeout):
							t.Error("sampler did not stop")
						}
					}
				})
				await(t, func() bool { return memory[0].device.Load() != nil && memory[1].device.Load() != nil })
				report := func(j *Job, i int) AttemptReport {
					return AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: memory[i].currentHostMemory(workerClock).aged(clock.Now())}
				}
				r, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report(a, 0)}})
				if err != nil {
					t.Fatal(err)
				}
				b := assignedJob(r)
				if b == nil || b.ID != second.ID {
					t.Fatal("two small jobs did not share", job(t, q, second.ID).Reason, memory[0].device.Load())
				}
				if err := q.Started(result(b, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(maxGPUStopGrace + deviceMemorySamplePeriod)
				// The grower consumes another 1/32 of RAM in one sample horizon.
				// Its forecast consumes the remaining physical headroom before zero.
				fs.mu.Lock()
				workerClock.advance(maxGPUStopGrace + deviceMemorySamplePeriod)
				fs.values[filepath.Join(memory[0].path, "memory.stat")] = stat(total * 5 / 32)
				fs.values["/proc/42/status"] = fmt.Sprintf("VmRSS: %d kB\n", uint64(total*5/32*(1<<20)))
				fs.values[filepath.Join(ceilings.root, "memory.stat")] = stat(total * 9 / 32)
				fs.values["/proc/meminfo"] = fmt.Sprintf("MemAvailable: %d kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n", uint64(total*23/32*(1<<20)))
				fs.mu.Unlock()
				for _, tick := range ticks {
					tick <- workerClock.Now()
				}
				await(t, func() bool {
					return memory[0].device.Load().at.Equal(workerClock.Now()) && memory[1].device.Load().at.Equal(workerClock.Now())
				})
				if sample := memory[0].device.Load(); sample.gb != total*5/32 || sample.growthGB != total/32 || sample.exclusive {
					t.Fatal("fake-clock growth signal incorrect", sample)
				}
				// Host pressure from other users leaves less than one observed growth
				// horizon, though both reservations and current samples fit the card.
				m.MemoryObservation.UsedGB = total * 63 / 64
				_, err = q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report(a, 0), report(b, 1)}})
				if err != nil {
					t.Fatal(err)
				}
				requireState(t, q, a.ID, "stopping")
				requireState(t, q, b.ID, "running")
				if got := job(t, q, a.ID); priorityStop(got) == nil || got.Reason != "shared GPU growth consumes physical memory headroom" {
					t.Fatal("physical headroom did not trigger a planned stop", got.Reason)
				}
			})
		}

	})

	t.Run("UnifiedAttemptMemoryUncertainKeepsExclusive", func(t *testing.T) {
		q, clock := fixture(t)
		m := sharedUnifiedMachine()
		a, b := sharedPair(t, q, m)
		gb := 17.0
		report := AttemptReport{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session, Started: true,
			CurrentHostMemory: &CurrentHostMemory{DeviceGB: &gb, DeviceSource: deviceUnifiedResidentSource, DeviceExclusive: true}}
		_, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report, {Execution: Execution{ID: b.ID, Attempt: b.Attempt}, Session: b.Session, Started: true}}})
		if err != nil {
			t.Fatal(err)
		}
		// Unknown attribution sheds a co-tenant without claiming it grew. Cleanup
		// custody prevents any replacement from launching before exact release.
		if job(t, q, a.ID).State != "stopping" && job(t, q, b.ID).State != "stopping" {
			t.Fatal("uncertainty kept sharing")
		}
		stop, survivor := a, b
		if job(t, q, b.ID).State == "stopping" {
			stop, survivor = b, a
		}
		finishInterrupted(t, q, stop)
		report.ID, report.Attempt, report.Session = survivor.ID, survivor.Attempt, survivor.Session
		third := submit(t, q, sharedSpec("experiment", 2))
		if r, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil || assignedJob(r) != nil {
			t.Fatal("uncertain survivor admitted co-tenant", r, err)
		}
		requireState(t, q, third.ID, "queued")
		w := Worker{Machine: m, Clock: clock, hostMemorySamples: currentHostMemorySamples([]AttemptReport{report}, clock.Now())}
		if err := localAttemptAdmission(w, []Job{*survivor}, *job(t, q, third.ID), nil); err == nil {
			t.Fatal("worker accepted uncertain sharing")
		}

	})

	t.Run("UnifiedAttemptMemoryStaleShedsCoTenant", func(t *testing.T) {
		q, clock := fixture(t)
		m := sharedUnifiedMachine()
		a, b := sharedPair(t, q, m)
		clock.Advance(deviceMemorySampleFreshFor)
		growthCheck(t, q, m, []*Job{a, b}, []float64{17, 13}, deviceMemorySampleFreshFor.Seconds())
		if job(t, q, a.ID).State != "stopping" && job(t, q, b.ID).State != "stopping" {
			t.Fatal("stale unified telemetry retained sharing")
		}

	})

	t.Run("UnifiedAttemptMemoryStartupWaitsForFirstSample", func(t *testing.T) {
		for _, ready := range []bool{false, true} {
			t.Run(fmt.Sprint(ready), func(t *testing.T) {
				q, clock := fixture(t)
				m := sharedUnifiedMachine()
				a, b := sharedPair(t, q, m)
				clock.Advance(time.Second)
				// Heartbeats precede the newcomer's first five-second driver tick.
				growthCheck(t, q, m, []*Job{a, b}, []float64{17}, 0)
				requireState(t, q, a.ID, "running")
				requireState(t, q, b.ID, "running")
				third := submit(t, q, sharedSpec("experiment", 2))
				if r := growthCheck(t, q, m, []*Job{a, b}, []float64{17}, 0); assignedJob(r) != nil {
					t.Fatal("unmeasured startup admitted another tenant", third.ID)
				}
				if ready {
					clock.Advance(deviceMemorySamplePeriod - time.Second)
					growthCheck(t, q, m, []*Job{a, b}, []float64{17, 13}, 0)
					requireState(t, q, a.ID, "running")
					requireState(t, q, b.ID, "running")
					return
				}
				clock.Advance(deviceMemorySampleFreshFor - time.Second)
				growthCheck(t, q, m, []*Job{a, b}, []float64{17}, 0)
				if job(t, q, a.ID).State != "stopping" && job(t, q, b.ID).State != "stopping" {
					t.Fatal("first-sample wait bypassed stale fallback indefinitely")
				}
			})
		}

	})

	t.Run("UnifiedCUDAAdmissionUnknownDeviceRemainsFull", func(t *testing.T) {
		q, clock := fixture(t)
		m := unifiedCUDAAdmissionMachine()
		check(t, q, m, nil)
		// Current cgroup RAM exceeds the host vector. It cannot cancel the
		// device vector, whose telemetry is absent (the incident's default).
		j := &Job{ID: "holder", State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, Spec: spec("experiment")}
		j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB = 32, 21
		q.s.Jobs[j.ID] = j
		q.hostMemorySamples[m.ID] = map[attemptKey]hostMemorySample{jobKey(*j): {gb: 45, at: clock.Now()}}
		m.HostAvailableMemoryGB = 50
		m.MemoryObservation.UsedGB = 46 // 75 raw - 25 reserve = 50 usable.
		q.s.Machines[m.ID].HostAvailableMemoryGB = m.HostAvailableMemoryGB
		q.s.Machines[m.ID].MemoryObservation = m.MemoryObservation
		q.refreshHostMemory()
		if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 18 {
			t.Fatalf("device growth discounted: got %g GiB, want 50 - 32 = 18", got)
		}
		clock.Advance(hostMemorySampleFreshFor)
		q.refreshHostMemory()
		if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 0 {
			t.Fatal("stale physical sample admitted CUDA", got)
		}
		// Worker launch uses the same device floor and a new raw measurement.
		w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16, HostMemorySource: func(context.Context) (float64, error) { return 59, nil }}
		w.hostMemorySamples = map[attemptKey]hostMemorySample{jobKey(*j): {gb: 45, at: clock.Now()}}
		next := Job{Spec: cpuSpec(1, 20)}
		if err := localAttemptAdmission(w, []Job{*j}, next, nil); err == nil {
			t.Fatal("worker discounted unknown CUDA device usage")
		}
		w.HostMemorySource = func(context.Context) (float64, error) { return 16, nil } // raw 32; usable 7.
		if free, err := w.sharedHostLiveMemory(context.Background()); err != nil || free != 7 {
			t.Fatalf("worker floor not rechecked: free=%g err=%v", free, err)
		}

	})
}
