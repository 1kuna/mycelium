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
			workerClock := &lockedTestClock{now: clock.Now()}
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
}

func TestUnifiedAttemptMemoryUncertainKeepsExclusive(t *testing.T) {
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
}

func TestUnifiedAttemptMemoryReconciliation(t *testing.T) {
	for _, total := range []float64{4, 1024} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			stat := func(gb float64) string { return fmt.Sprintf("anon %d\nkernel 0\nshmem 0\n", uint64(gb*(1<<30))) }
			meminfo := func(available float64) string {
				return fmt.Sprintf("MemAvailable: %d kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n", uint64(available*(1<<20)))
			}
			fs := &unifiedResidentFS{values: map[string]string{}}
			a := &memoryAttempt{fs: fs, path: "/synthetic/attempt", pageSize: 4096, hostBaseline: &gpuHostState{available: total}, rootBaseline: new(float64)}
			fs.set("/synthetic/attempt/memory.stat", stat(total/8))
			fs.set("/synthetic/memory.stat", stat(total/4))
			fs.set("/proc/42/status", fmt.Sprintf("VmRSS: %d kB\n", uint64(total/8*(1<<20))))
			fs.set("/proc/meminfo", meminfo(total*3/4))
			gb, exclusive, err := a.unifiedDeviceMemory(map[int]bool{42: true}, false)
			if err != nil || exclusive || gb != total/8 {
				t.Fatal("cgroup/RSS footprint not reconciled", gb, exclusive, err)
			}
			// Uncharged device/foreign host growth has no safe division among
			// attempts. Keep an upper diagnostic bound, but require exclusive.
			fs.set("/proc/meminfo", meminfo(total/2))
			gb, exclusive, err = a.unifiedDeviceMemory(map[int]bool{42: true}, false)
			if err != nil || !exclusive || gb != total*3/8 {
				t.Fatal("outside growth guessed attributable", gb, exclusive, err)
			}
			fs.set("/proc/meminfo", meminfo(total*3/4))
			if _, exclusive, err = a.unifiedDeviceMemory(map[int]bool{42: true}, true); err != nil || !exclusive {
				t.Fatal("foreign GPU process shared", exclusive, err)
			}
			fs.set("/proc/42/status", "unavailable")
			if _, exclusive, err = a.unifiedDeviceMemory(map[int]bool{42: true}, false); err == nil || !exclusive {
				t.Fatal("missing RSS became attributed", exclusive, err)
			}
		})
	}
}

func TestUnifiedAttemptMemoryStaleShedsCoTenant(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	a, b := sharedPair(t, q, m)
	clock.Advance(deviceMemorySampleFreshFor)
	growthCheck(t, q, m, []*Job{a, b}, []float64{17, 13}, deviceMemorySampleFreshFor.Seconds())
	if job(t, q, a.ID).State != "stopping" && job(t, q, b.ID).State != "stopping" {
		t.Fatal("stale unified telemetry retained sharing")
	}
}

func TestUnifiedAttemptMemoryStartupWaitsForFirstSample(t *testing.T) {
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
}

func TestUnifiedAttemptMemoryLearningSurvivesRestart(t *testing.T) {
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
}
