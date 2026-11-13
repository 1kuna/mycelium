package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A learned envelope already protects future occupancy. A fresh growth
// forecast protects the next observation horizon, including sample age. These
// are alternative bounds on the same bytes, not additive reservations.
func TestHostGrowthEnvelopeOverlapQueueAndLaunch(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		envelope, resident, forecast float64
		rate, age                    float64
		unenforced, want             bool
	}{
		{name: "two envelopes fit", envelope: 11, resident: 8, forecast: 4, want: true},
		{name: "two envelopes exceed capacity", envelope: 13, resident: 8, forecast: 4},
		// Growth past the envelope needs a host without enforced limits: a cgroup
		// v2 memory.max is the envelope and bounds the forecast (#1487).
		{name: "aged growth exceeds envelope", envelope: 11, resident: 10, forecast: 1, rate: 2, age: 2, unenforced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 24, 24
			if !tc.unenforced {
				m.MemoryEnforcement = cgroupMemoryEnforcement
			}
			s := spec("production")
			s.Needs.HostMemoryGB = tc.envelope
			first := submit(t, q, s)
			holder := assignedJob(machineCheck(t, q, m))
			if holder == nil || holder.ID != first.ID {
				t.Fatal("first job not assigned")
			}
			if err := q.Started(result(holder, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(3 * time.Second)
			m.HostAvailableMemoryGB = 24 - tc.resident
			current := &CurrentHostMemory{NonreclaimableGB: tc.resident, FootprintGB: &tc.resident, GrowthObserved: true, GrowthGB: tc.forecast, GrowthRateGB: tc.rate, AgeSeconds: tc.age,
				sampledAt: clock.Now().Add(-time.Duration(tc.age * float64(time.Second)))}
			report := AttemptReport{Execution: Execution{ID: holder.ID, Attempt: holder.Attempt}, Session: holder.Session, Started: true, CurrentHostMemory: current}
			samples := currentHostMemorySamples([]AttemptReport{report}, clock.Now())
			second := submit(t, q, s)
			next := *second
			next.Attempt, next.Machine, next.Session, next.State = 1, m.ID, m.Session, "running"
			next.AllocatedDevices = []string{m.Devices[1].ID}
			w := Worker{Machine: m, Clock: clock, hostMemorySamples: samples,
				HostMemorySource: func(context.Context) (float64, error) { return m.HostAvailableMemoryGB, nil }}
			if err := localAttemptAdmission(w, []Job{*holder}, next, nil); (err == nil) != tc.want {
				t.Errorf("assignment admission=%v, want fit=%t", err, tc.want)
			}
			rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: []Job{*holder, next}, batchHostMemorySamples: samples,
				batchSupervisors: map[attemptKey]*attemptSupervisor{}, hostMemorySource: w.HostMemorySource}
			rt.batchSupervisors[jobKey(*holder)] = &attemptSupervisor{job: *holder, report: report, admissionMu: &rt.admissionMu}
			rt.batchSupervisors[jobKey(next)] = &attemptSupervisor{job: next, admissionMu: &rt.admissionMu}
			launch := HostLaunch{Machine: m.ID, Session: next.Session, Key: next.ID, Attempt: "1"}
			if err := rt.checkBatchLaunchMemory(t.Context(), launch); (err == nil) != tc.want {
				t.Errorf("launch admission=%v, want fit=%t", err, tc.want)
			}
			reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}})
			if err != nil {
				t.Fatal(err)
			}
			got := assignedJob(reply)
			if tc.want {
				if got == nil || got.ID != second.ID {
					t.Fatalf("fitting second job remained queued: %s", job(t, q, second.ID).Reason)
				}
				return
			}
			if got != nil {
				t.Fatal("unsafe concurrent assignment", got.ID)
			}
			// A serialization decision must clear on exact physical release.
			if err := q.Complete(result(holder, true)); err != nil {
				t.Fatal(err)
			}
			m.HostAvailableMemoryGB = 24
			if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != second.ID {
				t.Fatal("second job did not run after release")
			}
		})
	}
}

func TestHostGrowthResidentOvershootBindsAllowance(t *testing.T) {
	_, clock := fixture(t)
	m := m3Machine()
	m.HostMemoryBudgetGB = 22
	j := Job{ID: "holder", State: "running", Attempt: 1, Session: m.Session, AllocatedDevices: []string{"0"}, Spec: spec("production")}
	j.Spec.Needs.HostMemoryGB = 11
	sample := hostMemorySample{gb: 12, at: clock.Now()}
	charge, pending := memoryCharge(&j, &m, 11, sample, clock.Now())
	if charge < 12 || pending != 0 {
		t.Fatalf("resident overshoot lost: charge=%g pending=%g", charge, pending)
	}
	next := j
	next.ID, next.AllocatedDevices = "next", []string{"1"}
	w := Worker{Machine: m, Clock: clock, hostMemorySamples: map[attemptKey]hostMemorySample{jobKey(j): sample},
		HostMemorySource: func(context.Context) (float64, error) { return 24, nil }}
	err := localAttemptAdmission(w, []Job{j}, next, nil)
	detail := admissionDetail(err, next)
	if !errors.Is(err, errWorkerMemory) || detail == nil || detail.Subcheck != "aggregate_host_allowance" {
		t.Fatal("physical headroom bypassed the occupied allowance", err)
	}
}
