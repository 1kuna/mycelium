package myce

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// Retained #884 evidence: these are different attempts, not a 7.5423 GiB
// observation. A larger permanently qualified target must not block this one.
func TestMemoryRecoveryTargetPairedEvidence(t *testing.T) {
	for _, margin := range []float64{2.1867791904561273, 12} {
		t.Run(fmt.Sprint(margin), func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "worker", "cpu", 8, 32)
			m.ExtensionFingerprint = "cpu-v1"
			clock.now = time.Date(2026, 11, 7, 18, 29, 0, 0, time.UTC)
			m.CapabilityProfile.OS = "darwin"
			m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 6.5, .125
			check(t, q, m, nil)
			larger := cpuMachine("larger", "larger", "cpu", 8, 32)
			larger.CapabilityProfile.OS = "darwin"
			larger.HostMemoryBudgetGB, larger.ExtensionFingerprint = 32, "cpu-v1"
			larger.Leaving = true // Permanent qualification still sees its capacity.
			check(t, q, larger, nil)
			s := cpuSpec(1, 0)
			s.Targets = []ExecutionTarget{{OS: "darwin", Arch: m.CapabilityProfile.Arch, Platform: "cpu"}}
			oomPeak, constrainedPeak := 3.3578033447265625, 5.3555145263671875
			stopped := runMemory(t, q, clock, m, s, oomPeak, func(r *Result) {
				r.Success, r.OutOfMemory = false, true
				r.MemoryPeak.Constrained = true
				r.MemoryPeak.GrowthGB = &margin
			})
			if err := q.Cancel(stopped.ID); err != nil {
				t.Fatal(err)
			}
			runMemory(t, q, clock, m, s, constrainedPeak, func(r *Result) {
				r.MemoryPeak.Constrained = true
			})
			// Retained matching evidence remains valid through a long idle
			// interval; elapsed age alone cannot erase the paired lower bound.
			clock.Advance(365 * 24 * time.Hour)
			check(t, q, m, nil)
			queued := submit(t, q, s)
			candidate := q.memoryCandidate(queued, q.s.Machines[m.ID])
			room, why := q.exploratoryMemory(queued, q.s.Machines[m.ID])
			want := max(constrainedPeak, min(oomPeak+margin, room)) + .125
			if why != "" || math.Abs(candidate.MemoryEnvelope.HostGB-want) > 1e-9 {
				t.Fatalf("paired target envelope=%+v want=%g refusal=%q", candidate.MemoryEnvelope, want, why)
			}
			for _, member := range []bool{false, true} {
				floored := *queued
				if member {
					floored.Spec.Run.MemoryGB = 7
				} else {
					floored.MemoryOOMFloor = &SizeHint{HostMemoryGB: 7}
				}
				if e := q.memoryCandidate(&floored, q.s.Machines[m.ID]); e.Spec.Needs.HostMemoryGB < 7 || hostFits(q.s.Machines[m.ID], 0, e.Spec.Needs.HostMemoryGB) {
					t.Fatal("clipped authored/member or job-local floor", e.MemoryEnvelope)
				}
			}
			physical := *q.s.Machines[m.ID]
			physical.FreeHostMemoryGB = constrainedPeak + .1 // Less than peak + slack.
			if e := q.memoryCandidate(queued, &physical); e.Spec.Needs.HostMemoryGB < constrainedPeak || hostFits(&physical, 0, e.Spec.Needs.HostMemoryGB) {
				t.Fatal("clipped measured need to scarce physical room", e.MemoryEnvelope)
			}
			if q.s.HostReleases == nil {
				q.s.HostReleases = map[string]HostRelease{}
			}
			q.s.HostReleases["unconfirmed"] = HostRelease{Host: hostID(&m), Job: "unconfirmed", HostChargeGB: 1}
			if e := q.memoryCandidate(queued, q.s.Machines[m.ID]); e.MemoryEnvelope.Refusal == "" {
				t.Fatal("recovery bypassed unconfirmed release custody", e.MemoryEnvelope)
			}
			delete(q.s.HostReleases, "unconfirmed")
			if run := check(t, q, m, nil).Job; run == nil || run.ID != queued.ID {
				t.Fatal("fitting paired recovery did not dispatch", job(t, q, queued.ID).Reason)
			}
		})
	}
}

func TestMemoryRecoveryTargetRetainsOOMFloor(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker", "cpu", 8, 32)
	m.ExtensionFingerprint = "cpu-v1"
	m.CapabilityProfile.OS = "darwin"
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 6.5, .125
	check(t, q, m, nil)
	s := cpuSpec(1, 0)
	stopped := runMemory(t, q, clock, m, s, 6.5, func(r *Result) {
		r.Success, r.OutOfMemory = false, true
		r.MemoryPeak.Constrained = true
		margin := 2.1867791904561273
		r.MemoryPeak.GrowthGB = &margin
	})
	retry := job(t, q, stopped.ID)
	if retry.MemoryOOMFloor == nil || retry.MemoryOOMFloor.HostMemoryGB < 6.5 {
		t.Fatal("real OOM floor lost", retry.MemoryOOMFloor)
	}
	if candidate := q.memoryCandidate(retry, q.s.Machines[m.ID]); candidate.Spec.Needs.HostMemoryGB < retry.MemoryOOMFloor.HostMemoryGB || hostFits(q.s.Machines[m.ID], 0, candidate.Spec.Needs.HostMemoryGB) {
		t.Fatal("real OOM floor clipped to target", candidate.MemoryEnvelope)
	}
	if run := check(t, q, m, nil).Job; run != nil {
		t.Fatal("dispatched below real OOM floor", run.ID)
	}
	if err := q.Cancel(stopped.ID); err != nil {
		t.Fatal(err)
	}
	queued := submit(t, q, s)
	if e := q.memoryCandidate(queued, q.s.Machines[m.ID]); e.Spec.Needs.HostMemoryGB < 6.5 || hostFits(q.s.Machines[m.ID], 0, e.Spec.Needs.HostMemoryGB) {
		t.Fatal("comparable OOM peak clipped on a new job", e.MemoryEnvelope)
	}
}

// An accepted sample selects the clean-history reader, but cannot erase a
// larger constrained measurement when other work consumes live target room.
func TestMemoryRecoveryTargetAcceptedHistoryRetainsLower(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	m.MemoryCeilingSlackGB = .125
	check(t, q, m, nil)
	s := cpuSpec(1, 0)
	runMemory(t, q, clock, m, s, 3, func(r *Result) {
		r.Success = false
		r.MemoryPeak.Constrained = true
		growth := 2.0
		r.MemoryPeak.GrowthGB = &growth
	})
	runMemory(t, q, clock, m, s, 4, nil)
	runMemory(t, q, clock, m, s, 8, func(r *Result) {
		r.MemoryPeak.Constrained = true
	})
	clock.Advance(365 * 24 * time.Hour)
	check(t, q, m, nil)
	queued := submit(t, q, s)
	physical := *q.s.Machines[m.ID]
	physical.FreeHostMemoryGB = 6
	candidate := q.memoryCandidate(queued, &physical)
	if candidate.Spec.Needs.HostMemoryGB < 8 || hostFits(&physical, 0, candidate.Spec.Needs.HostMemoryGB) {
		t.Fatal("accepted history clipped constrained measured need", candidate.MemoryEnvelope)
	}
}
