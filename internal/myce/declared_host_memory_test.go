package myce

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestDeclaredHostMemoryForecastContainment(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		anon, footprint, available float64
		neighbor, charge, pending  float64
		want                       bool
	}{
		// #1064's fake-clock reproduction: 2 + 2 + 20*3 = 64 GiB
		// forecast, but the entire native attempt is contained at 6.125.
		{"inflated forecast admits fitting neighbor", 2, 2, 46, 40, 6.125, 4.125, true},
		{"near ceiling protects remaining growth", 6, 6.12, .2, .1, 6.125, .125, false},
		// An actual observation contradicting containment must remain honest.
		{"observed overuse remains a floor", 7, 8, 46, 40, 8, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 16, 64)
			m.Devices = []MachineDevice{}
			m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 48, .125
			m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
			holder := submit(t, q, cpuSpec(2, 6))
			a := assignedJob(machineCheck(t, q, m))
			if a == nil || a.ID != holder.ID {
				t.Fatal("declared job not assigned")
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(3 * time.Second)
			current := &CurrentHostMemory{NonreclaimableGB: tc.anon, FootprintGB: &tc.footprint,
				GrowthObserved: true, GrowthGB: 2, GrowthRateGB: 20, AgeSeconds: 3,
				sampledAt: clock.Now().Add(-3 * time.Second)}
			report := AttemptReport{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session, Started: true, CurrentHostMemory: current}
			samples := currentHostMemorySamples([]AttemptReport{report}, clock.Now())
			w := Worker{Machine: m, Clock: clock, hostMemorySamples: samples,
				HostMemorySource: func(context.Context) (float64, error) { return tc.available, nil }}
			ceiling, err := w.jobCeilingBytes(*a)
			if err != nil || ceiling != int64(6.125*(1<<30)) {
				t.Fatal("worker containment differs from declaration plus slack", ceiling, err)
			}
			charge, pending := memoryCharge(a, &m, hostNeed(&m, 0, 6), samples[jobKey(*a)], clock.Now())
			if charge != tc.charge || pending != tc.pending {
				t.Errorf("charge=%g pending=%g, want %g/%g", charge, pending, tc.charge, tc.pending)
			}
			m.HostAvailableMemoryGB = tc.available
			next := submit(t, q, cpuSpec(2, tc.neighbor))
			launching := *next
			launching.Attempt, launching.Machine, launching.Session, launching.State = 1, m.ID, m.Session, "running"
			if err := localAttemptAdmission(w, []Job{*a}, launching, nil); (err == nil) != tc.want {
				t.Errorf("worker admission=%v, want fit=%t", err, tc.want)
			}
			rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: []Job{*a, launching},
				batchHostMemorySamples: samples, hostMemorySource: w.HostMemorySource,
				batchSupervisors: map[attemptKey]*attemptSupervisor{}}
			rt.batchSupervisors[jobKey(*a)] = &attemptSupervisor{job: *a, report: report, admissionMu: &rt.admissionMu}
			rt.batchSupervisors[jobKey(launching)] = &attemptSupervisor{job: launching, admissionMu: &rt.admissionMu}
			launch := HostLaunch{Machine: m.ID, Session: m.Session, Key: next.ID, Attempt: "1"}
			if err := rt.checkBatchLaunchMemory(t.Context(), launch); (err == nil) != tc.want {
				t.Errorf("final launch admission=%v, want fit=%t", err, tc.want)
			}
			reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}})
			if err != nil {
				t.Fatal(err)
			}
			got := assignedJob(reply)
			if (got != nil && got.ID == next.ID) != tc.want {
				t.Fatalf("queue admission=%v, want fit=%t; %s", got, tc.want, job(t, q, next.ID).Reason)
			}
		})
	}
}

func TestDeclaredHostMemoryCeilingAndFreshness(t *testing.T) {
	_, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 64)
	m.MemoryEnforcement, m.MemoryCeilingSlackGB = "memory ceilings enforced (cgroup v2)", .125
	j := Job{State: "running", Spec: cpuSpec(2, 2)}
	j.Spec.Needs = ResourceNeeds{CPUCores: 2, HostMemoryGB: 2, Platform: Platform{Required: []string{"cpu"}}}
	if err := j.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	ceiling := hostNeed(&m, 0, 2)
	sample := hostMemorySample{gb: 2.1, footprintGB: 2.12, growthGB: 2, rateGB: 20, at: clock.Now()}
	clock.Advance(3 * time.Second)
	charge, growth := memoryCharge(&j, &m, ceiling, sample, clock.Now())
	if charge != ceiling || math.Abs(growth-.025) > 1e-9 {
		t.Fatal("near-ceiling use lost containment headroom", charge, growth)
	}
	clock.Advance(hostMemorySampleFreshFor)
	charge, growth = memoryCharge(&j, &m, ceiling, sample, clock.Now())
	if charge != ceiling || growth != ceiling {
		t.Fatal("expired sample granted resident credit", charge, growth)
	}
	charge, growth = memoryCharge(&j, &m, ceiling, hostMemorySample{}, clock.Now())
	if charge != ceiling || growth != ceiling {
		t.Fatal("missing sample granted resident credit", charge, growth)
	}
}

func TestDeclaredHostMemoryCapRequiresNativeCPUContainment(t *testing.T) {
	_, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 64)
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	j := Job{State: "running", Spec: cpuSpec(2, 6)}
	j.Spec.Needs = ResourceNeeds{CPUCores: 2, HostMemoryGB: 6, Platform: Platform{Required: []string{"cpu"}}}
	if err := j.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	sample := hostMemorySample{gb: 2, footprintGB: 2, growthGB: 2, rateGB: 20, at: clock.Now()}
	clock.Advance(3 * time.Second)
	for _, kind := range []string{"unenforced", "RSS", "auto", "GPU"} {
		t.Run(kind, func(t *testing.T) {
			job, machine := j, m
			switch kind {
			case "unenforced":
				machine.MemoryEnforcement = ""
			case "RSS":
				machine.MemoryEnforcement = darwinRSSGuardStatus
			case "auto":
				job.Spec.Needs.Memory = "auto"
			case "GPU":
				job.Spec.Needs.Platform = Platform{Required: []string{"cuda"}}
			}
			charge, pending := memoryCharge(&job, &machine, 6.125, sample, clock.Now())
			wantCharge, wantPending := 64.0, 62.0
			if kind == "auto" {
				// A cgroup limit contains these CPU attempts, so the
				// forecast is bounded by that ceiling: charge and growth alike.
				wantCharge, wantPending = 6.125, 4.125
			}
			if charge != wantCharge || pending != wantPending {
				t.Fatal("lost forecast charge or physical growth bound", charge, pending)
			}
		})
	}
}

func TestDeclaredHostMemoryRetainsActualPeakAndAttemptFloor(t *testing.T) {
	_, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 64)
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	j := Job{ID: "declared", Attempt: 1, Session: "current", State: "running", Spec: cpuSpec(2, 6)}
	j.Spec.Needs = ResourceNeeds{CPUCores: 2, HostMemoryGB: 6, Platform: Platform{Required: []string{"cpu"}}}
	j.HostMemoryCharge = &AttemptHostMemoryCharge{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, GiB: 8}
	// Retained forecast-only evidence cannot invent use above containment;
	// actual peaks and exact-attempt reservation floors must survive it.
	sample := hostMemorySample{gb: 2, footprintGB: 2, peakGB: 7, retainedGB: 100, at: clock.Now()}
	charge, growth := memoryCharge(&j, &m, 6.125, sample, clock.Now())
	if charge != 8 || growth != 6 {
		t.Fatal("lost retained attempt floor or charged unbounded forecast", charge, growth)
	}
	clock.Advance(hostMemorySampleFreshFor)
	charge, growth = memoryCharge(&j, &m, 6.125, sample, clock.Now())
	if charge != 8 || growth != 8 {
		t.Fatal("stale sample released floor or granted resident credit", charge, growth)
	}
	j.HostMemoryCharge.Session = "previous"
	charge, growth = memoryCharge(&j, &m, 6.125, sample, clock.Now())
	if charge != 7 || growth != 7 {
		t.Fatal("lost actual peak or inherited another session's floor", charge, growth)
	}
	charge, growth = memoryCharge(&j, &m, 10, hostMemorySample{}, clock.Now())
	if charge != 10 || growth != 10 {
		t.Fatal("lost larger known reservation floor", charge, growth)
	}
}
