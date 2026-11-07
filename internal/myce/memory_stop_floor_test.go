package myce

import (
	"math"
	"strings"
	"testing"
	"time"
)

// Replay the two retained stops, not an invented first-attempt 1.92 GB peak.
// The first small cold ceiling is possible on either OS when ledger headroom
// is small; it is not an accepted workload estimate. Neither timing identity
// loss on retry nor moving hosts may erase the job's measured lower bound.
func TestMemoryStopFloorIncidentRecovery(t *testing.T) {
	for _, platform := range []string{"darwin", "linux"} {
		t.Run(platform, func(t *testing.T) {
			q, clock, m := learnedMemoryFixture(t)
			m.CapabilityProfile.OS = platform
			m.MemoryCeilingSlackGB = .125
			m.HostAvailableMemoryGB = .2079925537109375 + .125
			check(t, q, m, nil)
			s := cpuSpec(2, 0)
			queued := submit(t, q, s)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != queued.ID || math.Abs(run.MemoryEnvelope.HostGB-.2079925537109375) > 1e-9 || run.MemoryEnvelope.Evidence != 0 {
				t.Fatalf("cold physical remainder: %+v", run)
			}
			for i, measurement := range []struct{ peak, growth float64 }{
				{.30828857421875, .2067968019513712},
				{1.7888946533203125, 1.0494580599730645},
			} {
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(8 * time.Second)
				r := result(run, false)
				r.OutOfMemory = true
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &measurement.peak, GrowthGB: &measurement.growth, Constrained: true}
				if platform == "darwin" {
					r.MemoryPeak.Source = processGroupMemorySource
					r.ExecutionCause = &RuntimeCause{Class: rssLimitExceeded, RSSLimit: &RSSLimitEvidence{ObservedBytes: uint64(measurement.peak * (1 << 30)), LimitBytes: int64(run.MemoryEnvelope.HostGB * (1 << 30)), Quality: "owned_session_rss"}}
				} else {
					resident := measurement.peak / 2
					r.MemoryPeak.HostGB = &resident
					r.MemoryPeak.CgroupGB = &measurement.peak
					r.MemoryPeak.Events = &MemoryEventCounts{OOM: 1, OOMKill: 1}
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				stopped := job(t, q, run.ID)
				want := measurement.peak + max(measurement.growth, measurement.peak-run.MemoryEnvelope.HostGB)
				if stopped.State != "queued" || stopped.MemoryOOMFloor == nil || stopped.MemoryOOMFloor.HostMemoryGB < want || stopped.OOMRetries != i+1 {
					t.Fatalf("measured retry lost: state=%s floor=%+v want=%g retries=%d", stopped.State, stopped.MemoryOOMFloor, want, stopped.OOMRetries)
				}
				q = memoryFactsRestart(t, q, clock)
				m.HostAvailableMemoryGB = 32
				other := m
				if i == 1 {
					other.ID, other.HostID = "retry-worker", "retry-host"
				}
				run = check(t, q, other, nil).Job
				if run == nil || run.ID != queued.ID || run.MemoryEnvelope.HostGB < want {
					t.Fatalf("retry below observed peak and uncertainty: %+v want %g state=%s reason=%s", run, want, job(t, q, queued.ID).State, job(t, q, queued.ID).Reason)
				}
				if i == 0 && math.Abs(run.MemoryEnvelope.HostGB-.6400853761701212) > 1e-9 {
					t.Fatalf("first measured recovery differs from receipt: %+v", run.MemoryEnvelope)
				}
				m = other
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(8 * time.Second)
			peak := 1.7888946533203125
			r := result(run, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, run.ID); got.State != "succeeded" || !got.ReleaseConfirmed {
				t.Fatal("replayed job did not finish and release", got.State)
			}
		})
	}
}

func TestMemoryStopFloorRunawayExceedsEligibleCapacity(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 2)
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB, m.ExtensionFingerprint = 2, 2, "cpu-v1"
	check(t, q, m, nil)
	s := cpuSpec(1, 0)
	queued := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID {
		t.Fatal("not assigned")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	peak, growth := 3.0, 1.0
	limit, err := memoryBytes(run.MemoryEnvelope.HostGB)
	if err != nil {
		t.Fatal(err)
	}
	guard := rssGuard{limit: limit}
	var stop *RSSLimitEvidence
	for i := 0; i < 3; i++ {
		stop = guard.observe(clock.Now(), map[int]string{7: "owned-birth"}, uint64(peak*(1<<30)), 20+i, nil)
		clock.Advance(time.Second)
	}
	if stop == nil {
		t.Fatal("growing owned process group escaped unchanged guard")
	}
	r := result(run, false)
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth, Constrained: true}
	r.ExecutionCause = &RuntimeCause{Class: rssLimitExceeded, RSSLimit: stop}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, run.ID)
	if got.State != "failed" || got.MemoryOOMFloor.HostMemoryGB < peak+growth || !strings.Contains(got.Reason, "needs more memory than any eligible host") {
		t.Fatal("runaway retried below measured need", got.State, got.MemoryOOMFloor, got.Reason)
	}
}

func TestMemoryStopFloorRetainsGuardObservation(t *testing.T) {
	peak, growth := 1.0, .2
	j := Job{Attempt: 2, Machine: "worker", Session: "session", Spec: cpuSpec(1, .64),
		MemoryEnvelope: &JobMemoryEnvelope{HostGB: .64},
		MemoryAttempts: []AttemptMemoryFacts{{Attempt: 2, Machine: "worker", Session: "session", MemoryPeak: &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth}}},
		ExecutionCause: &RuntimeCause{Class: rssLimitExceeded, RSSLimit: &RSSLimitEvidence{ObservedBytes: 1920811008}},
	}
	observed := float64(1920811008) / (1 << 30)
	if floor := memoryStopFloor(&j); floor < observed+(observed-.64) {
		t.Fatal("smaller final peak erased guard observation", floor)
	}
}
