package myce

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestOwnHelperUnifiedLaunch(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, tc := range []struct {
			name                        string
			device, allowance, neighbor float64
			want                        bool
		}{
			{"resident helper fits", 22.3, 96, 0, true},
			{"neighbor occupies room", 22.3, 96, 5, false},
			{"helper overshoot fits larger charge", 50, 96, 0, true},
			{"helper overshoot exceeds allowance", 60, 96, 0, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, clock := fixture(t)
				m := unifiedCUDAAdmissionMachine()
				m.CapabilityProfile.SystemMemoryBytes = 121 << 30
				m.CapabilityProfile.LogicalCPUs = 32
				m.HostMemoryBudgetGB = tc.allowance
				j := Job{ID: "own", Machine: m.ID, State: "running", Attempt: 1, Session: m.Session, Spec: spec("production")}
				j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB = 38.73198699951172, 44.519424227328635
				rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: []Job{j}, batchSupervisors: map[attemptKey]*attemptSupervisor{}}
				// At launch, a single helper has loaded CUDA memory, plus 1.22
				// GiB uniquely charged anon. Remaining physical occupancy, including
				// that anon, is 5.44 GiB: the incident's 68.26 GiB launch headroom.
				samples := map[attemptKey]hostMemorySample{jobKey(j): {gb: 1.22, at: clock.Now(), deviceGB: tc.device, residentGB: tc.device, deviceAt: clock.Now()}}
				raw := 121 - tc.device - 5.44 - tc.neighbor
				w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16, HostMemorySource: func(context.Context) (float64, error) { return raw - 16, nil }}
				rt.hostMemorySource = w.sharedHostLiveMemory
				rt.batchHostMemorySamples = samples
				report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 1.22, sampledAt: clock.Now(), DeviceGB: &tc.device, deviceSampledAt: clock.Now(), deviceResidentGB: tc.device}}
				rt.batchSupervisors[jobKey(j)] = &attemptSupervisor{job: j, report: report, admissionMu: &rt.admissionMu}
				if tc.neighbor > 0 {
					n := Job{ID: "neighbor", State: "running", Attempt: 1, Session: m.Session, Spec: spec("production")}
					n.Spec.Needs.MemoryGB, n.Spec.Needs.HostMemoryGB = 0, tc.neighbor
					rt.batchJobs = append(rt.batchJobs, n)
				}
				err := rt.checkBatchLaunchMemory(t.Context(), HostLaunch{Machine: m.ID, Session: m.Session, Key: j.ID, Attempt: "1"})
				if (err == nil) != tc.want {
					t.Fatalf("launch=%v; want fit=%t", err, tc.want)
				}
				if tc.device > j.Spec.Needs.MemoryGB {
					e := hostNeed(&m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
					charge := helperHostCharge(&j, &m, samples[jobKey(j)], clock.Now(), e)
					if math.Abs(charge-(e+tc.device-j.Spec.Needs.MemoryGB)) > 1e-9 {
						t.Fatalf("device overshoot lost from static charge: %g", charge)
					}
				}
			})
		}

	})

	t.Run("OwnHelperCreditDoesNotSpendNeighborsAnonTwice", func(t *testing.T) {
		_, clock := fixture(t)
		m := unifiedCUDAAdmissionMachine()
		j := Job{ID: "own", State: "running", Attempt: 1, Spec: spec("production")}
		j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB = 8, 24
		n := Job{ID: "neighbor", State: "running", Attempt: 1, Spec: cpuSpec(1, 20)}
		if err := normalizeResources(&n.Spec); err != nil {
			t.Fatal(err)
		}
		rt := modelRuntime{clock: clock, memoryMachine: m, batchJobs: []Job{j, n}}
		// All CUDA pages might also be in the neighbor's uniquely charged anon.
		// The union credit is max(20,22), not 20+22 or 20+1+22.
		s := map[attemptKey]hostMemorySample{
			jobKey(j): {gb: 1, at: clock.Now(), deviceGB: 22, residentGB: 22, deviceAt: clock.Now()},
			jobKey(n): {gb: 20, at: clock.Now()},
		}
		if err := rt.checkHostAdmissionLocked(20, true, s, jobKey(j)); !errors.Is(err, errWorkerMemory) {
			t.Fatal("overlapping neighbor anon/device occupancy earned duplicate credit", err)
		}
		// A neighbor is never given the launching attempt's device credit.
		if err := rt.checkHostAdmissionLocked(31, true, s, jobKey(j)); err != nil {
			t.Fatal("one union credit did not admit the exact launch", err)
		}
		if err := rt.checkHostAdmissionLocked(31, true, s); !errors.Is(err, errWorkerMemory) {
			t.Fatal("non-launching job received helper credit", err)
		}

	})
}

func TestOwnHelperRetainsRemainingDeviceDemand(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		device, forecast, free float64
	}{
		{"unreached device envelope", 22, 0, 1},
		{"device growth forecast", 10, 20, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, clock := fixture(t)
			m := unifiedCUDAAdmissionMachine()
			j := Job{ID: "own", Machine: m.ID, Session: m.Session, Attempt: 1, State: "running", Spec: spec("production")}
			j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB = 8, 24
			rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: []Job{j}, batchSupervisors: map[attemptKey]*attemptSupervisor{}, hostMemorySource: func(context.Context) (float64, error) { return tc.free, nil }}
			// Anonymous occupancy remains below the composite32.125GiB ceiling.
			// It cannot prove that the future CUDA allocation already exists.
			r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 32, sampledAt: clock.Now(), DeviceGB: &tc.device, deviceSampledAt: clock.Now(), deviceResidentGB: tc.device, DeviceGrowthGB: tc.forecast}}
			rt.batchSupervisors[jobKey(j)] = &attemptSupervisor{job: j, report: r, admissionMu: &rt.admissionMu}
			if err := rt.checkBatchLaunchMemory(t.Context(), HostLaunch{Machine: m.ID, Session: m.Session, Key: j.ID, Attempt: "1"}); !errors.Is(err, errWorkerMemory) {
				t.Fatal("host occupancy erased pending device demand", err)
			}
		})
	}
}

func TestOwnHelperDiscreteHostLaunch(t *testing.T) {
	_, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.UnifiedMemory = false
	j := Job{ID: "own", State: "running", Attempt: 1, Spec: spec("production")}
	j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB = 11, 27
	rt := modelRuntime{clock: clock, memoryMachine: m, batchJobs: []Job{j}}
	s := map[attemptKey]hostMemorySample{jobKey(j): {gb: 8, at: clock.Now(), deviceGB: 22.3, residentGB: 22.3, deviceAt: clock.Now()}}
	if err := rt.checkHostAdmissionLocked(3.125, true, s, jobKey(j)); err != nil {
		t.Fatal(err)
	}
	if err := rt.checkHostAdmissionLocked(3, true, s, jobKey(j)); !errors.Is(err, errWorkerMemory) {
		t.Fatal("discrete device bytes incorrectly credited host RAM", err)
	}
	s[jobKey(j)] = hostMemorySample{gb: 100, at: clock.Now()}
	if err := rt.checkHostAdmissionLocked(120, true, s, jobKey(j)); !errors.Is(err, errWorkerMemory) {
		t.Fatal("resident host overshoot escaped static allowance", err)
	}
}

func TestHelperResidentCounterIsLocalUniqueAndFresh(t *testing.T) {
	_, clock := fixture(t)
	resident := 0.0
	gb, _, err := cudaAttemptMemoryReading([]byte("GPU-one,42,22835.2\nGPU-one,43,22835.2\nGPU-one,99,40960\n"), "GPU-one", map[int]bool{42: true, 43: true}, &resident)
	if err != nil || math.Abs(resident-22.3) > 1e-9 || math.Abs(gb-44.6) > 1e-9 {
		t.Fatalf("counter union bound: sum=%g resident=%g err=%v", gb, resident, err)
	}
	var latest atomic.Pointer[hostMemorySample]
	latest.Store(&hostMemorySample{gb: gb, residentGB: resident, at: clock.Now()})
	c := (&CurrentHostMemory{sampledAt: clock.Now(), deviceSample: &latest}).aged(clock.Now())
	r := AttemptReport{Execution: Execution{ID: "own", Attempt: 1}, CurrentHostMemory: c}
	s := currentHostMemorySamples([]AttemptReport{r}, clock.Now())[attemptKey{"own", 1, ""}]
	m := unifiedCUDAAdmissionMachine()
	if helperDeviceResident(&m, s, clock.Now()) != resident {
		t.Fatal("fresh local helper bound lost")
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var remote CurrentHostMemory
	if err := json.Unmarshal(data, &remote); err != nil {
		t.Fatal(err)
	}
	if remote.deviceResidentGB != 0 {
		t.Fatal("remote reports can mint local resident credit")
	}
	clock.Advance(deviceMemorySampleFreshFor)
	if helperDeviceResident(&m, s, clock.Now()) != 0 {
		t.Fatal("stale helper counter grants credit")
	}
	s.deviceAt, s.source = clock.Now(), deviceUnifiedResidentSource
	if helperDeviceResident(&m, s, clock.Now()) != 0 {
		t.Fatal("RSS fallback grants physical credit")
	}
	s.source, s.deviceAt = "", clock.Now().Add(time.Second)
	if helperDeviceResident(&m, s, clock.Now()) != 0 {
		t.Fatal("future helper counter grants credit")
	}
}
