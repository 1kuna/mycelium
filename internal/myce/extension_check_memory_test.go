package myce

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// #645: an identifiable pre-fix XPU aggregate must not prevent the accepted
// corrected sample that replaces it. Reliable oversized peaks still refuse.
func TestExtensionCheckMemoryRecoversInflatedLegacyXPU(t *testing.T) {
	for _, tc := range []struct {
		name              string
		hint, resident    float64
		corrected, refuse bool
	}{
		{name: "size hint", hint: 12, resident: 10.75},
		{name: "bounded exploration", resident: 10.75},
		{name: "oversized corrected peak", hint: 12, resident: 25, corrected: true, refuse: true},
		{name: "oversized resident peak", hint: 12, resident: 25, refuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m, s := gpuHostFixture(t)
			m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 48, 52
			machineCheck(t, q, m)
			s.Workload = nil
			s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
			s.RequireTarget = true
			s.Needs = ResourceNeeds{Memory: "auto", GPU: &GPUSelection{Devices: []string{"0"}}}
			s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			s.SizeHint = SizeHint{MemoryGB: 8, HostMemoryGB: 40}
			start := func() *Job {
				t.Helper()
				queued := submit(t, q, s)
				run := assignedJob(machineCheck(t, q, m))
				if run == nil || run.ID != queued.ID {
					t.Fatalf("check not assigned: %+v", job(t, q, queued.ID))
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				return run
			}
			complete := func(run *Job, peak *AttemptMemoryPeak) {
				t.Helper()
				clock.Advance(time.Second)
				r := result(run, true)
				r.MemoryPeak = peak
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			outside, device := 11.0, 8.0
			legacy := &AttemptMemoryPeak{HostGB: &tc.resident, NonreclaimableGB: &tc.resident, CgroupGB: &tc.resident, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: &GPUHostMemory{SiblingAdjusted: tc.corrected, DriverGB: &outside, OutsideGB: &outside, HostDropGB: &tc.resident}}
			complete(start(), legacy)
			m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 24, 28
			machineCheck(t, q, m)
			s.SizeHint = SizeHint{HostMemoryGB: tc.hint}
			if tc.refuse {
				_, err := q.Submit(s)
				var refusal *HTTPError
				if !errors.As(err, &refusal) || refusal.Code != "no_qualified_target" || !strings.Contains(refusal.Message, "exceeds allowance 24 GiB") || !strings.Contains(refusal.Message, memoryCapacityRemedies) {
					t.Fatalf("reliable oversized peak was not refused actionably: %v", err)
				}
				return
			}
			run := start()
			wantBasis, wantHost := "exploratory", 6.0
			if tc.hint > 0 {
				wantBasis, wantHost = "hint", tc.hint
			}
			if run.MemoryEnvelope.Basis != wantBasis || run.Spec.Needs.HostMemoryGB != wantHost {
				t.Fatalf("legacy figure still blocks fallback: %+v", run.MemoryEnvelope)
			}
			host, residual := 7.25, .25
			corrected := &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &host, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: &GPUHostMemory{SiblingAdjusted: true, DriverGB: &residual, OutsideGB: &residual, HostDropGB: &host}}
			complete(run, corrected)
			next := start()
			if next.MemoryEnvelope.Basis != "learned" || next.Spec.Needs.HostMemoryGB != 9.375 {
				t.Fatalf("next accepted sample did not replace legacy figure: %+v", next.MemoryEnvelope)
			}
		})
	}
}

// A qualification check runs its small reference fixture, so it is sized from
// its own accepted checks, never from the workload's peak (#618). Live: a check
// peaking near 0.5 GiB was charged 16 GiB of a 24 GiB host allowance because
// it matched the workload's 12.8 GiB accepted peak. The workload in turn keeps
// its own envelope; a check's small peak never shrinks it.
func TestExtensionCheckMemoryLearnsFromChecksNotWorkload(t *testing.T) {
	q, clock, m, s := floorFreeFitFixture(t, 24)
	workloadHost, workloadDevice := 12.8, 4.5
	for id, p := range q.s.WorkObservations { // the workload's accepted peak
		p.MemoryPeak = &AttemptMemoryPeak{HostGB: &workloadHost, DeviceGB: &workloadDevice}
		q.s.WorkObservations[id] = p
	}
	checkSpec := s
	checkSpec.Workload = nil
	checkSpec.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
	checkSpec.RequireTarget = true
	checkSpec.AllowedMachines = []string{m.ID}
	checkSpec.Needs = ResourceNeeds{Memory: "auto", GPU: &GPUSelection{Devices: []string{"0"}}}
	checkSpec.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
	runCheck := func(host, device float64) *Job {
		t.Helper()
		queued := submit(t, q, checkSpec)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("check not assigned: %s", job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &host, DeviceGB: &device}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		return j
	}

	first := runCheck(.4, .18)
	if e := first.MemoryEnvelope; e == nil || e.Source != boundedQuarter || first.Spec.Needs.HostMemoryGB != 6 {
		t.Fatalf("first check not bounded to a quarter of the allowance: host=%.2f GiB %+v", first.Spec.Needs.HostMemoryGB, e)
	}
	second := runCheck(.4, .18)
	if e := second.MemoryEnvelope; e == nil || e.Basis != "learned" || math.Abs(second.Spec.Needs.HostMemoryGB-.5) > 1e-9 || math.Abs(second.Spec.Needs.MemoryGB-.225) > 1e-9 {
		t.Fatalf("check not charged from its own accepted peak: host=%.3f device=%.3f GiB %+v", second.Spec.Needs.HostMemoryGB, second.Spec.Needs.MemoryGB, e)
	}

	queued := submit(t, q, s)
	work := assignedJob(machineCheck(t, q, m))
	if work == nil || work.ID != queued.ID {
		t.Fatalf("workload not assigned: %s", job(t, q, queued.ID).Reason)
	}
	if e := work.MemoryEnvelope; e == nil || e.Source != "accepted peak × 1.25" || work.Spec.Needs.HostMemoryGB != workloadHost*1.25 || work.Spec.Needs.MemoryGB != workloadDevice*1.25 {
		t.Fatalf("check peaks changed the workload's envelope: host=%.2f device=%.2f GiB %+v", work.Spec.Needs.HostMemoryGB, work.Spec.Needs.MemoryGB, e)
	}
}
