package myce

import (
	"math"
	"strings"
	"testing"
	"time"
)

// #645: a single CUDA/unified check's total-cgroup peak transfers to XPU
// with a 2.5 margin, exceeding the allowance despite the measured need fitting.
func TestExtensionCheckMemoryCrossFamilyRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		hint, total                    float64
		oom, missingDevice, sameTarget bool
	}{
		{"hint", 12, 10.855361938476562, false, false, false},
		{"no hint", 0, 10.855361938476562, false, false, false},
		{"oversized cgroup despite small RSS", 12, 25, false, false, false},
		{"failed probe cannot repeat", 12, 10.855361938476562, true, false, false},
		{"missing device telemetry", 12, 10.855361938476562, true, true, false},
		{"same target different layout", 12, 10.855361938476562, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m, s := gpuHostFixture(t)
			e := registeredExtension(t, q, s.Extension)
			e.Version = "2"
			cuda := e.Variants[0]
			cuda.Target = ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "cuda"}
			if tc.sameTarget {
				e.Variants[0] = cuda
				m.CapabilityProfile.Arch = "arm64"
				m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 31 << 30}}
				for i := range m.Devices {
					m.Devices[i].Platform, m.Devices[i].MemoryMode = "cuda", "dedicated"
				}
			} else {
				e.Variants = append(e.Variants, cuda)
			}
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			source := cloneMachine(&m)
			source.ID, source.HostID, source.Session = "source", "source", "source-session"
			source.UnifiedMemory = true
			source.HostMemoryBudgetGB, source.HostAvailableMemoryGB = 64, 64
			source.CapabilityProfile.Arch = "arm64"
			source.CapabilityProfile.SystemMemoryBytes = 64 << 30
			source.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 31 << 30}}
			source.Devices = source.Devices[:1]
			source.Devices[0].Platform, source.Devices[0].MemoryMode = "cuda", "unified"
			for _, owner := range []*Machine{&source, &m} {
				for i := range owner.Devices {
					target := owner.Devices[i].machine(*owner)
					report := newExtensionReport(e, &target, "extension:sample")
					report.Setup, report.Qualified, report.CheckID = true, true, "check"
					owner.Devices[i].ExtensionReports = []ExtensionReport{report}
				}
				machineCheck(t, q, *owner)
			}
			s.Extension, s.ExtensionDigest, s.Workload = extensionKey(e), e.Digest, nil
			s.RequireTarget = true
			s.Needs = ResourceNeeds{Memory: "auto", GPU: &GPUSelection{Devices: []string{"0"}}}
			s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			s.SizeHint = SizeHint{MemoryGB: 8, HostMemoryGB: 40}
			s.Targets = []ExecutionTarget{cuda.Target}
			start := func(owner Machine) *Job {
				t.Helper()
				queued := submit(t, q, s)
				run := assignedJob(machineCheck(t, q, owner))
				if run == nil || run.ID != queued.ID {
					t.Fatalf("check not assigned: %+v", job(t, q, queued.ID))
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				return run
			}
			complete := func(run *Job, peak *AttemptMemoryPeak, oom bool) {
				t.Helper()
				clock.Advance(time.Second)
				r := result(run, !oom)
				r.MemoryPeak, r.OutOfMemory = peak, oom
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			rss, device := 5.0111541748046875, 5.7197265625
			seed := &AttemptMemoryPeak{HostGB: &rss, CgroupGB: &tc.total, DeviceGB: &device, Events: &MemoryEventCounts{}}
			if tc.missingDevice {
				seed.DeviceGB = nil
			}
			complete(start(source), seed, false)
			s.Targets = []ExecutionTarget{e.Variants[0].Target}
			s.AllowedMachines = []string{m.ID}
			s.SizeHint = SizeHint{HostMemoryGB: tc.hint}
			if tc.total > 24 {
				memoryExplorationRefused(t, q, s)
				return
			}
			run := start(m)
			wantSource := boundedGPUExploration
			if tc.missingDevice {
				wantSource += "; GPU recipe default with host estimate floor"
			}
			if run.MemoryEnvelope.Basis != "exploratory" || run.MemoryEnvelope.Source != wantSource || run.Spec.Needs.HostMemoryGB != 23.875 || len(q.s.MemoryExplorations) != 1 {
				t.Fatalf("single cross-family prior still blocks bounded measurement: %+v", run.MemoryEnvelope)
			}
			host, residual := 7.25, .25
			peak := &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &host, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: &GPUHostMemory{SiblingAdjusted: true, DriverGB: &residual, OutsideGB: &residual, HostDropGB: &host}}
			complete(run, peak, tc.oom)
			if tc.oom {
				if got := job(t, q, run.ID); got.State != "failed" || got.OOMRetries != 0 || !strings.Contains(got.Reason, memoryCapacityRemedies) {
					t.Fatalf("bounded measurement retried after OOM: %+v", got)
				}
				q = memoryFactsRestart(t, q, clock)
				machineCheck(t, q, source)
				machineCheck(t, q, m)
				memoryExplorationRefused(t, q, s)
				if tc.sameTarget {
					// A failed discrete probe cannot erase the accepted unified
					// observation or replace it with the discrete spent prior.
					s.AllowedMachines = []string{source.ID}
					local := start(source)
					if local.MemoryEnvelope.Basis != "learned" || local.Spec.Needs.HostMemoryGB != tc.total*1.25 {
						t.Fatalf("probe changed the other layout's accepted need: %+v", local.MemoryEnvelope)
					}
					complete(local, seed, false)
					s.AllowedMachines = []string{m.ID}
				}
				// The durable refusal remains valid without any live device
				// estimate after the source observation's normal expiry.
				clock.Advance(workCurveMaxAge + time.Second)
				machineCheck(t, q, source)
				machineCheck(t, q, m)
				memoryExplorationRefused(t, q, s)
				return
			}
			next := start(m)
			if next.MemoryEnvelope.Basis != "learned" || next.Spec.Needs.HostMemoryGB != 9.375 {
				t.Fatalf("accepted target sample did not replace transferred prior: %+v", next.MemoryEnvelope)
			}
			// Once measured on this family, a genuinely oversized peak is binding.
			host = 25
			complete(next, peak, false)
			memoryExplorationRefused(t, q, s)
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
