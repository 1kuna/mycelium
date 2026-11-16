package myce

import (
	"testing"
	"time"
)

func TestAdmissionWaitExactHolderSamples(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	low := submit(t, q, cpuSpec(4, 3))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != low.ID {
		t.Fatal("missing lower-tier holder")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	// This is the existing minimum-runtime protection after repeated stops.
	if err := q.update(func() error { q.touchJob(run.ID).PreemptionCount = 2; return nil }); err != nil {
		t.Fatal(err)
	}
	footprint := 2.0
	report := AttemptReport{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: run.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{SampleID: 1, NonreclaimableGB: 2, FootprintGB: &footprint, GrowthObserved: true}}
	if _, err := q.Checkin(Checkin{Machine: m, Attempts: []AttemptReport{report}, AttemptProtocol: keyedAttemptProtocol, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Priority = "production"
	demand := submit(t, q, s)
	read := func() AdmissionWaitHolder {
		e := admissionWaitAPI(t, q, demand.ID)
		for _, target := range e.Latest.Targets {
			for _, h := range target.Holders {
				if h.Job == run.ID && h.Attempt == run.Attempt {
					return h
				}
			}
		}
		t.Fatal("exact holder missing")
		return AdmissionWaitHolder{}
	}
	fresh := read()
	if !fresh.SampleFresh || fresh.ForecastHostGB == nil || *fresh.ForecastHostGB != 2 || fresh.SampleAgeSeconds == nil || *fresh.SampleAgeSeconds != 0 || fresh.Preemptible || fresh.PreemptionReason != "minimum_runtime" {
		t.Fatalf("lost fresh forecast or protection: %+v", fresh)
	}
	// The first overdue check-in adds a new actual admission decision (#1158).
	// Establish it before asserting that identical telemetry/decisions reuse it.
	clock.Advance(time.Second)
	report.CurrentHostMemory.SampleID++
	if _, err := q.Checkin(Checkin{Machine: m, Attempts: []AttemptReport{report}, AttemptProtocol: keyedAttemptProtocol, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	observedAt := admissionWaitAPI(t, q, demand.ID).Latest.At
	clock.Advance(time.Second)
	report.CurrentHostMemory.SampleID++
	if _, err := q.Checkin(Checkin{Machine: m, Attempts: []AttemptReport{report}, AttemptProtocol: keyedAttemptProtocol, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, demand.ID); !got.Latest.At.Equal(observedAt) {
		t.Fatal("identical fresh telemetry rewrote an unchanged explanation")
	}
	clock.Advance(hostMemorySampleFreshFor)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	stale := read()
	if stale.SampleFresh || stale.ForecastHostGB != nil || stale.SampleAgeSeconds == nil || *stale.SampleAgeSeconds != hostMemorySampleFreshFor.Seconds() {
		t.Fatalf("stale sample renewed: %+v", stale)
	}
	// Make a newer attempt visible while the old exact release remains. Its
	// sample/charge/enforcement must never be attributed to old cleanup custody.
	if err := q.update(func() error {
		holder := q.touchJob(run.ID)
		q.retainRelease(holder)
		holder.Attempt++
		holder.Spec.Needs.HostMemoryGB = 7
		q.hostMemorySamples[m.ID][jobKey(*holder)] = hostMemorySample{id: 2, at: clock.Now(), gb: 7, footprintGB: 7, growthObserved: true}
		q.maintain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	old := read()
	if old.Kind != "release" || old.ForecastHostGB != nil || old.SampleAgeSeconds != nil || old.EnforcedHostGB != nil || old.AdmissionHostGB != nil || old.ChargedHostGB == nil || *old.ChargedHostGB != 3 {
		t.Fatalf("old execution borrowed newer telemetry: %+v", old)
	}
}
