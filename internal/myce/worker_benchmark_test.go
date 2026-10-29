package myce

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

func TestDeviceMaintenanceBenchmarkWorkerContinues(t *testing.T) {
	for _, scenario := range []string{"primary", "sibling", "after-offer"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			machineCheck(t, q, m)
			disabled, enabled := "0", "1"
			if scenario == "sibling" {
				disabled, enabled = "1", "0"
			}
			if scenario != "after-offer" {
				changeDevice(t, q, m.HostID, disabled, "disable")
			}
			handler := API{q, "test"}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if scenario == "after-offer" && r.URL.Path == "/api/workers/launch" {
					changeDevice(t, q, m.HostID, disabled, "disable")
				}
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			measured := false
			w := Worker{Machine: m, Clock: clock, StateDir: newTestStateRoot(t), Interval: time.Second,
				Client:           Client{URL: server.URL, Token: "test"},
				HostMemorySource: func(context.Context) (float64, error) { return 32, nil },
				Benchmark: func(context.Context, BenchmarkConfig, *CapabilityProfile) error {
					measured = true
					return nil
				}}
			w.availability = newAvailabilityMonitor(clock, false, false, false)
			w.availability.sample(OwnerSignals{Power: "desktop"})
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			if err := w.runBenchmark(context.Background(), store); err != nil {
				t.Fatal("deferred onboarding aborted worker startup", err)
			}
			if measured {
				t.Fatal("whole-host benchmark touched disabled inventory")
			}
			// The ordinary checkin following onboarding must release its hold and
			// retain the enabled GPU's entire budget plus CPU admission.
			gpuSpec := spec("experiment")
			gpuSpec.Needs.MemoryGB = 8
			gpu := submit(t, q, gpuSpec)
			cpu := submit(t, q, cpuSpec(1, 1))
			gotGPU, gotCPU := false, false
			var running []*Job
			for _, a := range machineCheck(t, q, m).Attempts {
				if a.Job != nil {
					running = append(running, a.Job)
					gotGPU = gotGPU || a.Job.ID == gpu.ID && slices.Equal(a.Job.AllocatedDevices, []string{enabled})
					gotCPU = gotCPU || a.Job.ID == cpu.ID
				}
			}
			for _, a := range machineCheck(t, q, m, running...).Attempts {
				gotCPU = gotCPU || a.Job != nil && a.Job.ID == cpu.ID
			}
			if !gotGPU || !gotCPU {
				t.Fatal("deferred onboarding withheld sibling/CPU capacity", gotGPU, gotCPU)
			}
			if len(q.s.PreparationPlans) != 0 {
				t.Fatal("deferred onboarding retained preparation custody")
			}
		})
	}
}

func TestDeviceMaintenanceBenchmarkStartedContinues(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.BenchmarkPending = true
	r := machineCheck(t, q, m)
	if r.BenchmarkPlan == nil {
		t.Fatal("missing initial onboarding offer")
	}
	launch := *r.BenchmarkPlan
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	changeDevice(t, q, m.HostID, "1", "disable")
	r = machineCheck(t, q, m)
	if r.PreparationStopAt != nil || r.Leave {
		t.Fatal("device drain stopped authorized benchmark", r)
	}
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal("device drain rejected existing benchmark custody", err)
	}
	m.BenchmarkPending = false
	if _, err := q.Checkin(Checkin{Machine: m, BenchmarkDone: launch.Attempt}); err != nil {
		t.Fatal(err)
	}
	if len(q.s.PreparationPlans) != 0 {
		t.Fatal("completed benchmark retained custody")
	}
}
