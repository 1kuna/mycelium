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
		available                      float64
		oom, missingDevice, sameTarget bool
	}{
		{name: "hint", hint: 12, total: 10.855361938476562},
		{name: "no hint", total: 10.855361938476562},
		{name: "oversized cgroup despite small RSS", hint: 12, total: 25},
		{name: "shared host", total: 9.93, available: 13.7},
		{name: "drained host below allowance", total: 9.93, available: 23},
		{name: "cgroup prior exceeds live room", total: 9.93, available: 8},
		{name: "hint exceeds live room", hint: 14, total: 9.93, available: 13.7},
		{name: "failed probe cannot repeat", hint: 12, total: 10.855361938476562, available: 13.7, oom: true},
		{name: "missing device telemetry", hint: 12, total: 10.855361938476562, oom: true, missingDevice: true},
		{name: "same target different layout", hint: 12, total: 10.855361938476562, oom: true, sameTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m, s := gpuHostFixture(t)
			if tc.available > 0 {
				// Start a new sensor session at the captured headroom, rather
				// than inventing a large drop from the generic fixture's 28 GiB.
				m.ID, m.HostID, m.Session = "live-headroom-target", "live-headroom-target", "live-headroom-session"
				m.HostAvailableMemoryGB, m.MemoryCeilingSlackGB = tc.available, .2
			}
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
			source := unifiedCUDAAdmissionMachine()
			source.ID, source.HostID, source.Session = "source", "source", "source-session"
			source.CapabilityProfile.Arch = "arm64"
			source.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 31 << 30}}
			source.Devices = cloneMachine(&m).Devices[:1]
			source.Devices[0].Platform, source.Devices[0].MemoryMode = "cuda", "unified"
			source.Devices[0].UnifiedMemory = true
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
			s.AllowedMachines = []string{source.ID}
			start := func(owner Machine) *Job {
				t.Helper()
				queued := submit(t, q, s)
				run := assignedJob(machineCheck(t, q, owner))
				if run == nil || run.ID != queued.ID {
					got := job(t, q, queued.ID)
					t.Fatalf("check not assigned (%s): %s", got.State, got.Reason)
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
			if tc.available > 0 {
				rss, device = 4.42, 1.10
			}
			seed := &AttemptMemoryPeak{HostGB: &rss, CgroupGB: &tc.total, DeviceGB: &device, Events: &MemoryEventCounts{}}
			if tc.missingDevice {
				seed.DeviceGB = nil
			}
			complete(start(source), seed, false)
			s.Targets = []ExecutionTarget{e.Variants[0].Target}
			s.AllowedMachines = []string{m.ID}
			s.SizeHint = SizeHint{HostMemoryGB: tc.hint}
			budget := 24 - m.MemoryCeilingSlackGB - exploratoryDriftMarginGB(&m)
			if tc.available > 0 {
				budget = min(budget, tc.available-m.MemoryCeilingSlackGB-exploratoryDriftMarginGB(&m))
			}
			if tc.total > 24 {
				memoryExplorationRefused(t, q, s)
				return
			}
			if tc.total > budget || tc.hint > budget {
				queued := submit(t, q, s)
				if run := assignedJob(machineCheck(t, q, m)); run != nil {
					t.Fatal("probe started below the total-cgroup prior or host hint")
				}
				if got := job(t, q, queued.ID); got.State != "queued" || !strings.Contains(got.Reason, "RAM envelope") {
					t.Fatalf("live headroom shortage did not wait: %s %s", got.State, got.Reason)
				}
				if len(q.s.MemoryExplorations) != 0 {
					t.Fatal("refusal spent the exploration claim")
				}
				return
			}
			run := start(m)
			wantSource := boundedGPUExploration
			if tc.missingDevice {
				wantSource += "; unknown device peak; exclusive card"
			}
			if run.MemoryEnvelope.Basis != "exploratory" || run.MemoryEnvelope.Source != wantSource || math.Abs(run.Spec.Needs.HostMemoryGB-budget) > 1e-9 || len(q.s.MemoryExplorations) != 1 {
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
					if local.MemoryEnvelope.Basis != "learned" || local.Spec.Needs.HostMemoryGB != tc.total+minimumHostMemoryGB {
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
			if next.MemoryEnvelope.Basis != "learned" || next.Spec.Needs.HostMemoryGB != 7.5+minimumHostMemoryGB {
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
	if e := first.MemoryEnvelope; e == nil || e.Source != physicalMemoryProbe || first.Spec.Needs.HostMemoryGB != 24-minimumHostMemoryGB {
		t.Fatalf("first check did not reserve physical headroom: host=%.2f GiB %+v", first.Spec.Needs.HostMemoryGB, e)
	}
	second := runCheck(.4, .18)
	if e := second.MemoryEnvelope; e == nil || e.Basis != "learned" || math.Abs(second.Spec.Needs.HostMemoryGB-(.4+minimumHostMemoryGB)) > 1e-9 || math.Abs(second.Spec.Needs.MemoryGB-(.18+minimumHostMemoryGB)) > 1e-9 {
		t.Fatalf("check not charged from its own accepted peak: host=%.3f device=%.3f GiB %+v", second.Spec.Needs.HostMemoryGB, second.Spec.Needs.MemoryGB, e)
	}

	queued := submit(t, q, s)
	work := assignedJob(machineCheck(t, q, m))
	if work == nil || work.ID != queued.ID {
		t.Fatalf("workload not assigned: %s", job(t, q, queued.ID).Reason)
	}
	if e := work.MemoryEnvelope; e == nil || e.Source != "accepted peak + measured uncertainty" || work.Spec.Needs.HostMemoryGB != workloadHost+minimumHostMemoryGB || work.Spec.Needs.MemoryGB != workloadDevice+minimumHostMemoryGB {
		t.Fatalf("check peaks changed the workload's envelope: host=%.2f device=%.2f GiB %+v", work.Spec.Needs.HostMemoryGB, work.Spec.Needs.MemoryGB, e)
	}
}
