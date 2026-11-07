package myce

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// Replay the saved 2026-11-06 times and ratios on a sub-GiB pair and a large
// installation. A much larger incompatible target must not set the bound.
func TestMemoryRecoveryIncident(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hosts int
		scale float64
	}{{"tiny", 2, 1.0 / 16}, {"large", 40, 64}} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			clock.now = time.Date(2026, 11, 6, 6, 30, 0, 0, time.UTC)
			scale := tc.scale
			var machines []Machine
			for i := 0; i < tc.hosts; i++ {
				id := fmt.Sprintf("worker-%d", i)
				m := cpuMachine(id, id, "cpu", 8, 8*scale)
				m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
				m.CapabilityProfile.SystemMemoryBytes = uint64(8 * scale * (1 << 30))
				m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB, m.ExtensionFingerprint = 4.4*scale, .125*scale, "cpu-v1"
				check(t, q, m, nil)
				machines = append(machines, m)
			}
			other := cpuMachine("incompatible", "other", "cpu", 8, 4096*scale)
			other.HostMemoryBudgetGB = 4096 * scale
			check(t, q, other, nil)
			s := cpuSpec(1, 0)
			s.RequireTarget = true
			s.Targets = []ExecutionTarget{{OS: "darwin", Arch: "arm64", Platform: "cpu"}}
			seed := 3.8001251220703125*scale - minimumHostMemoryGB
			for i := 0; i < 2; i++ {
				s.AllowedMachines = []string{machines[i].ID}
				runMemory(t, q, clock, machines[i], s, seed, nil)
			}
			start := func(i int) *Job {
				s.AllowedMachines = []string{machines[i].ID}
				queued := submit(t, q, s)
				run := check(t, q, machines[i], nil).Job
				if run == nil || run.ID != queued.ID {
					t.Fatal("not assigned")
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				return run
			}
			stopped, survivor := start(0), start(1)
			peak, limit := 4.4857635498046875*scale, 3.9251251220703125*scale
			if math.Abs(stopped.MemoryEnvelope.HostGB-limit) > 1e-9 {
				t.Fatal(stopped.MemoryEnvelope)
			}
			clock.now = time.Date(2026, 11, 6, 6, 32, 19, 0, time.UTC)
			r := result(stopped, false)
			r.OutOfMemory, r.ReleasePending = true, true
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true, Source: processGroupMemorySource}
			r.ExecutionCause = &RuntimeCause{Class: rssLimitExceeded, RSSLimit: &RSSLimitEvidence{ObservedBytes: uint64(peak * (1 << 30)), LimitBytes: int64(limit * (1 << 30)), Quality: "owned_session_rss"}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			failed := job(t, q, stopped.ID)
			if failed.MemoryOOMFloor == nil || math.Abs(failed.MemoryOOMFloor.HostMemoryGB-(peak+(peak-limit))) > 1e-9 || failed.ReleaseConfirmed {
				t.Fatal("retry floor or custody lost", failed)
			}
			clock.now = time.Date(2026, 11, 6, 6, 38, 36, 0, time.UTC)
			s.AllowedMachines = nil
			s.SubmissionKey = "recovering-incident"
			pending := submit(t, q, s)
			if got := job(t, q, pending.ID); got.DiagnosticCode != memoryEvidenceRecovering {
				t.Fatal("missing typed diagnostic", got.DiagnosticCode, got.Reason)
			}
			if why := q.memoryEvidenceWaitingReason(pending); !strings.HasPrefix(why, memoryEvidenceRecovering+":") {
				t.Fatalf("why=%q failed=%+v facts=%+v sized=%+v", why, failed, failed.MemoryAttempts, q.memoryCandidate(pending, q.s.Machines[machines[0].ID]).MemoryEnvelope)
			}
			candidate := q.memoryCandidate(pending, q.s.Machines[machines[0].ID])
			if candidate.Spec.Needs.HostMemoryGB < peak || candidate.Spec.Needs.HostMemoryGB > peak+minimumHostMemoryGB {
				t.Fatal("cap lowered physical evidence or used incompatible host", candidate.MemoryEnvelope)
			}
			if run := check(t, q, machines[0], nil).Job; run != nil {
				t.Fatal("ran below observed need", run.ID)
			}
			q = memoryFactsRestart(t, q, clock)
			for i, m := range machines {
				var live *Job
				if i == 1 {
					live = survivor
				}
				check(t, q, m, live)
			}
			pending = submit(t, q, s)
			if pending.ID == "" || !strings.HasPrefix(q.memoryEvidenceWaitingReason(pending), memoryEvidenceRecovering+":") {
				t.Fatal("wait not durable")
			}
			clock.now = time.Date(2026, 11, 6, 6, 38, 48, 0, time.UTC)
			r = result(survivor, true)
			clean := 3.5586090087890625 * scale
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &clean, Source: processGroupMemorySource}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if why := q.memoryEvidenceWaitingReason(pending); why != "" {
				t.Fatal("clean evidence did not recover", why)
			}
			if run := check(t, q, machines[1], nil).Job; run == nil || run.ID != pending.ID {
				t.Fatal("same durable job not placed")
			}
			// Explicit needs are a hard refusal even in auto mode.
			s.SubmissionKey = "declared-impossible"
			s.Needs.HostMemoryGB = 10 * scale
			_, err := q.Submit(s)
			var api *HTTPError
			if !errors.As(err, &api) || api.Code != "no_qualified_target" {
				t.Fatal("declared impossibility accepted", err)
			}
		})
	}
}

// A constrained RSS stop supplies observed variance, not proof of exclusive
// physical OOM. The existing guard consumes the enlarged reservation; explicit
// limits and the guard's ownership/sample-completeness fences remain intact.
func TestMemoryRecoveryGuardUsesObservedVariance(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	m.CapabilityProfile.OS = "darwin"
	m.MemoryCeilingSlackGB = .125
	check(t, q, m, nil)
	s := cpuSpec(1, 0)
	seed := 3.8001251220703125 - minimumHostMemoryGB
	runMemory(t, q, clock, m, s, seed, nil)
	queued := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID {
		t.Fatal("not assigned")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	peak := 4.4857635498046875
	r := result(run, false)
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true, Source: processGroupMemorySource}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// The retry is still queued: its ended attempt must already supply the
	// observed lower bound and overshoot to comparable new submissions.
	probe := *queued
	probe.State = "queued"
	wantRecovery := peak + (peak - 3.9251251220703125) + .125
	if e := q.memoryCandidate(&probe, q.s.Machines[m.ID]).MemoryEnvelope; math.Abs(e.HostGB-wantRecovery) > 1e-9 {
		t.Fatal("queued retry lost class evidence", e)
	}
	if err := q.Cancel(run.ID); err != nil {
		t.Fatal(err)
	}
	clean := 3.5586090087890625
	recovery := runMemory(t, q, clock, m, s, clean, nil)
	want := peak + (peak - 3.9251251220703125) + .125
	if math.Abs(recovery.MemoryEnvelope.HostGB-want) > 1e-9 {
		t.Fatalf("overshoot margin: %+v want %g", recovery.MemoryEnvelope, want)
	}
	queued = submit(t, q, s)
	learned := q.memoryCandidate(queued, q.s.Machines[m.ID])
	if math.Abs(learned.MemoryEnvelope.HostGB-want) > 1e-9 || learned.MemoryEnvelope.Basis != "learned" {
		t.Fatal("clean evidence forgot observed variance", learned.MemoryEnvelope)
	}
	limit, _ := memoryBytes(learned.MemoryEnvelope.HostGB)
	guard := rssGuard{limit: limit}
	declared := rssGuard{limit: 4 << 30}
	roots := map[int]string{7: "same-birth"}
	var explicitStop *RSSLimitEvidence
	for i := 0; i < 3; i++ {
		if guard.observe(clock.Now(), roots, uint64(peak*(1<<30)), 2, nil) != nil {
			t.Fatal("normal observed variance stopped again")
		}
		explicitStop = declared.observe(clock.Now(), roots, uint64(peak*(1<<30)), 2, nil)
		clock.Advance(time.Second)
	}
	if explicitStop == nil {
		t.Fatal("explicit guard was weakened")
	}
}

func TestMemoryRecoveryNewEligibleCapacity(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	s := cpuSpec(1, 0)
	s.RequireTarget = true
	runMemory(t, q, clock, m, s, 40, nil)
	pending := submit(t, q, s)
	if !strings.HasPrefix(q.memoryEvidenceWaitingReason(pending), memoryEvidenceRecovering+":") {
		t.Fatal("not waiting")
	}
	larger := cpuMachine("additional", "additional-host", "cpu", 8, 64)
	larger.HostMemoryBudgetGB, larger.ExtensionFingerprint = 64, "cpu-v1"
	check(t, q, larger, nil)
	if why := q.memoryEvidenceWaitingReason(pending); why != "" {
		t.Fatal("capacity change did not recover", why)
	}
	if run := check(t, q, larger, nil).Job; run == nil || run.ID != pending.ID {
		t.Fatal("same job not assigned")
	}
}

// Authored containment remains a hard requirement even when the variant itself
// selects auto memory. Qualification must not recursively size this diagnostic.
func TestMemoryRecoveryAutoVariantContainment(t *testing.T) {
	q, _ := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
	v.Execution.Main.MemoryGB = 40
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("worker", "host", "cpu", 8, 32)
	m.HostMemoryBudgetGB, m.ExtensionFingerprint = 32, "cpu-v1"
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	s := Spec{Name: "sample", Project: "sample", Extension: extensionKey(e), RequireTarget: true, Workload: &Workload{InputShape: "small-v1", Units: 10}}
	_, err := q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "no_qualified_target" {
		t.Fatal("auto variant lost authored containment", err)
	}
}

func TestMemoryRecoveryVariantAutoSizingWaits(t *testing.T) {
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("worker", "host", "cpu", 8, 32)
	m.HostMemoryBudgetGB, m.ExtensionFingerprint = 32, "cpu-v1"
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	s := Spec{Name: "sample", Project: "sample", Extension: extensionKey(e), RequireTarget: true, Workload: &Workload{InputShape: "small-v1", Units: 10}}
	runMemory(t, q, clock, m, s, 40, nil)
	pending := submit(t, q, s)
	if pending.Spec.Needs.Memory != "" {
		t.Fatal("test no longer exercises variant-owned mode")
	}
	if why := q.memoryEvidenceWaitingReason(pending); !strings.HasPrefix(why, memoryEvidenceRecovering+":") {
		t.Fatal("variant-owned auto missed typed wait", why)
	}
	if run := check(t, q, m, nil).Job; run != nil {
		t.Fatal("ran below accepted variant peak")
	}
}

func TestMemoryRecoveryCapRetainsAcceptedMeasurement(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	s := cpuSpec(1, 0)
	s.RequireTarget = true
	runMemory(t, q, clock, m, s, 12, nil)
	runMemory(t, q, clock, m, s, 4, func(r *Result) {
		r.Success = false
		r.MemoryPeak.Constrained = true
		growth := 1.0
		r.MemoryPeak.GrowthGB = &growth
	})
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	budget := 8.0
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &budget}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	m.HostMemoryBudgetGB = budget
	check(t, q, m, nil)
	pending := submit(t, q, s)
	if e := q.memoryCandidate(pending, q.s.Machines[m.ID]).MemoryEnvelope; e.HostGB < 12 {
		t.Fatal("uncertainty cap discarded the accepted measurement", e)
	}
	if why := q.memoryEvidenceWaitingReason(pending); !strings.HasPrefix(why, memoryEvidenceRecovering+":") {
		t.Fatal("unrecovered measurement must wait", why)
	}
	if run := check(t, q, m, nil).Job; run != nil {
		t.Fatal("ran below unrecovered accepted peak")
	}
}
