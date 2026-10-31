package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

func sharedRunning(t *testing.T, resumable bool) (*Queue, *fakeClock, Machine, *Job) {
	t.Helper()
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	check(t, q, m, nil)
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Resumable = resumable
	s.SubmissionKey = "owner-pressure-logical-job"
	j := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("missing assignment", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	return q, clock, m, run
}

func TestSharedHostPressureYieldHysteresisAndExactRelease(t *testing.T) {
	for _, sensor := range []string{"darwin memory", "linux PSI", "owner latency"} {
		t.Run(sensor, func(t *testing.T) {
			q, clock, m, run := sharedRunning(t, true)
			p := SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
			switch sensor {
			case "darwin memory":
				p.MemoryLevel = 2
			case "linux PSI":
				p.MemoryPSI = sharedPSIHigh
			default:
				p.WakeDelayMS = sharedWakeHighMS
			}
			m.SharedHostPressure = &p
			clock.Advance(time.Second)
			r := check(t, q, m, run)
			j := job(t, q, run.ID)
			if j.State != "stopping" || priorityStop(j) == nil || priorityStop(j).Cause != sharedHostPressureCause || !r.Stop || r.Kill || r.GraceSeconds != q.preemptionGrace().Seconds() {
				t.Fatalf("missing planned grace: %+v %+v", j, r)
			}
			stopAt := *j.StopAt
			// A middling sample does not clear the latch or renew stop grace.
			p.MemoryLevel = 1
			p.MemoryPSI = (sharedPSIHigh + sharedPSILow) / 2
			p.WakeDelayMS = (sharedWakeHighMS + sharedWakeLowMS) / 2
			clock.Advance(time.Second)
			check(t, q, m, run)
			if j = job(t, q, run.ID); j.StopAt == nil || !j.StopAt.Equal(stopAt) || q.s.Machines[m.ID].SharedHostYieldReason == "" {
				t.Fatal("flapped or renewed grace")
			}
			clock.Advance(q.preemptionGrace())
			r = check(t, q, m, run)
			if !r.Kill || job(t, q, run.ID).State != "stopping" || q.s.Machines[m.ID].CPUInUseCores != 1 {
				t.Fatal("elapsed grace released custody", r)
			}
			finishInterrupted(t, q, run)
			j = job(t, q, run.ID)
			if j.State != "queued" || j.InfrastructureRetries != 0 || j.Losses != 0 || j.Spec.SubmissionKey != "owner-pressure-logical-job" {
				t.Fatal("yield changed logical work/accounting", j)
			}
			if next := check(t, q, m, nil).Job; next != nil {
				t.Fatal("readmitted before recovery")
			}
			p.MemoryPSI = sharedPSILow
			p.WakeDelayMS = sharedWakeLowMS
			r = check(t, q, m, nil)
			if r.Job == nil || r.Job.ID != run.ID || r.Job.Attempt <= run.Attempt {
				t.Fatal("fresh recovery did not requeue same job", r)
			}
			if err := q.Complete(result(run, true)); !errors.Is(err, ErrStale) {
				t.Fatal("stale attempt accepted", err)
			}
		})
	}
}

func TestSharedHostPressureUnknownAndStale(t *testing.T) {
	q, clock, m, run := sharedRunning(t, false)
	for _, p := range []*SharedHostPressure{nil, {}, {MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, AgeSeconds: signalMaxAge.Seconds()}, {MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, MemoryPSI: -1}} {
		m.SharedHostPressure = p
		check(t, q, m, run)
		if job(t, q, run.ID).State != "running" || q.s.Machines[m.ID].SharedHostReason == "" {
			t.Fatal("unknown became pressure or admitted work")
		}
	}
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
	check(t, q, m, run)
	clock.Advance(signalMaxAge)
	q.mu.Lock()
	q.refreshHostMemory()
	reason := q.s.Machines[m.ID].SharedHostReason
	q.mu.Unlock()
	if reason == "" {
		t.Fatal("cached sample did not expire with injected clock")
	}
	// Queue-owned times and latches cannot be supplied by a worker.
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, ReceivedAt: clock.Now().Add(time.Hour)}
	m.SharedHostYieldReason = "forged pressure"
	check(t, q, m, run)
	if q.s.Machines[m.ID].SharedHostYieldReason != "" || !q.s.Machines[m.ID].SharedHostPressure.ReceivedAt.Equal(clock.Now()) {
		t.Fatal("trusted worker timestamp/latch")
	}
	// Stale evidence must not clear a real pressure latch.
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 4}
	check(t, q, m, run)
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, AgeSeconds: signalMaxAge.Seconds()}
	check(t, q, m, run)
	if q.s.Machines[m.ID].SharedHostYieldReason == "" {
		t.Fatal("stale recovery cleared latch")
	}
}

func TestSharedHostPressureFailureAndCancellationPrecedence(t *testing.T) {
	for _, ending := range []string{"independent failure", "cancellation", "completed before signal"} {
		t.Run(ending, func(t *testing.T) {
			q, _, m, run := sharedRunning(t, false)
			m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 2}
			r := check(t, q, m, run)
			if !r.Kill || r.GraceSeconds != 0 {
				t.Fatal("nonresumable work got checkpoint delay")
			}
			out := result(run, false)
			want := "failed"
			if ending == "cancellation" {
				if err := q.Cancel(run.ID); err != nil {
					t.Fatal(err)
				}
				out.Interrupted = true
				want = "cancelled"
			} else if ending == "completed before signal" {
				out.Success = true
				want = "succeeded"
			}
			if err := q.Complete(out); err != nil {
				t.Fatal(err)
			}
			requireState(t, q, run.ID, want)
		})
	}
}

func TestSharedHostPressureDedicatedHostsUnchanged(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("ordinary", "ordinary", "cpu", 2, 8)
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 4}
	j := submit(t, q, cpuSpec(1, 1))
	r := check(t, q, m, nil)
	if r.Job == nil || r.Job.ID != j.ID || r.Stop || r.Kill {
		t.Fatal("pressure changed dedicated admission", r)
	}
}

func TestSharedHostPressureRetiredWorkerDoesNotStopReplacement(t *testing.T) {
	q, _, m, run := sharedRunning(t, true)
	q.mu.Lock()
	retired := *q.s.Machines[m.ID]
	retired.ID = "retired-worker"
	retired.Gone = true
	retired.SharedHostYieldReason = "shared host memory pressure"
	q.s.Machines[retired.ID] = &retired
	q.mu.Unlock()
	r := check(t, q, m, run)
	if r.Stop || r.Kill || job(t, q, run.ID).State != "running" {
		t.Fatal("retired pressure stopped healthy replacement", r)
	}
}

func TestSharedHostPressurePSIParsing(t *testing.T) {
	for _, value := range []string{"", "some avg10=NaN", "some avg10=101", "full avg10=1.00"} {
		if _, err := sharedMemoryPSI([]byte(value)); err == nil {
			t.Error("unknown PSI accepted", value)
		}
	}
	if n, err := sharedMemoryPSI([]byte("some avg10=1.25 avg60=0.50 total=123\nfull avg10=0.1\n")); err != nil || n != 1.25 {
		t.Fatal(n, err)
	}
}

func TestSharedHostCPUConcurrencyCap(t *testing.T) {
	_, clock, m, run := sharedRunning(t, false)
	m.SharedHostReason = ""
	w := Worker{sharedCPUDev: true, Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 12, nil }}
	next := Job{ID: "next", Spec: cpuSpec(2, 1)}
	if err := normalizeResources(&next.Spec); err != nil {
		t.Fatal(err)
	}
	if err := localAttemptAdmission(w, []Job{*run}, next, nil); err == nil {
		t.Fatal("concurrent declarations exceeded two-core profile")
	}
	next.Spec.Needs.CPUCores = 1
	if err := localAttemptAdmission(w, []Job{*run}, next, nil); err != nil {
		t.Fatal("fitting concurrent work rejected", err)
	}
}
