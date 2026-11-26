package myce

import (
	"strings"
	"testing"
	"time"
)

// The unified-memory replay from #2091: a 96 GiB pool allowance on a 128 GiB
// host, a peer, and a canary whose authored hints charge it 60 GiB.
type aloneReplay struct {
	q      *Queue
	clock  *fakeClock
	m      Machine
	free   float64
	peer   *Job
	canary *Job
}

const aloneReplayKeep = 4.0

func (r *aloneReplay) setFree(free float64) {
	r.free = free
	r.m.MemoryObservation = &HostMemoryObservation{TotalGB: 128, UsedGB: 128 - free, ReserveGB: aloneReplayKeep}
	r.m.HostAvailableMemoryGB = free - aloneReplayKeep
}

// check reports the given attempts as running with their charges resident.
func (r *aloneReplay) check(t *testing.T, running ...*Job) Reply {
	t.Helper()
	r.clock.Advance(time.Second)
	c := Checkin{Machine: r.m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
	for _, j := range running {
		gb := memoryPools(j).RAM.ChargeGB
		c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true,
			CurrentHostMemory: &CurrentHostMemory{SampleID: r.clock.Now().UnixNano(), NonreclaimableGB: gb, FootprintGB: &gb}})
	}
	reply, err := r.q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

func (r *aloneReplay) start(t *testing.T, j *Job, running ...*Job) *Job {
	t.Helper()
	run := assignedJob(r.check(t, running...))
	if run == nil || run.ID != j.ID {
		t.Fatalf("%s was not admitted: %s", j.Spec.Name, job(t, r.q, j.ID).Reason)
	}
	if err := r.q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	return run
}

// burst replays attempt 12's stop: a load burst at 4.34 GiB/s, sampled every
// second, forecast across the 1 s cadence plus the 10 s stop grace.
func (r *aloneReplay) burst(t *testing.T, run *Job, others ...*Job) *Job {
	t.Helper()
	for range 40 {
		r.setFree(r.free - 4.34)
		r.check(t, append(others, run)...)
		if got := job(t, r.q, run.ID); got.State == "stopping" {
			return got
		}
	}
	t.Fatalf("burst did not stop %s", run.Spec.Name)
	return nil
}

func newAloneReplay(t *testing.T) *aloneReplay {
	t.Helper()
	q, clock := fixture(t)
	r := &aloneReplay{q: q, clock: clock, m: sharedUnifiedMachine()}
	r.m.HostMemoryBudgetGB = 96
	r.m.Capabilities = append(r.m.Capabilities, pooledMemoryCapability)
	r.setFree(110)
	peer := sharedSpec("experiment", 20)
	peer.Resumable = true
	r.peer = r.start(t, submit(t, q, peer))
	canary := sharedSpec("experiment", 52)
	canary.Needs.HostMemoryGB, canary.Resumable = 8, true
	r.canary = r.start(t, submit(t, q, canary), r.peer)
	if got := memoryPools(r.canary).RAM; got.ChargeGB != 60 || got.Alone {
		t.Fatalf("fixture canary charge %+v, want 60 GiB shared", got)
	}
	return r
}

func (r *aloneReplay) finishPeer(t *testing.T) {
	t.Helper()
	done := result(r.peer, true)
	done.Error = ""
	if err := r.q.Complete(done); err != nil {
		t.Fatal(err)
	}
	requireState(t, r.q, r.peer.ID, "succeeded")
}

func pressureFinish(t *testing.T, q *Queue, stopped *Job, releaseOnly bool) {
	t.Helper()
	host, device := 20.0, 35.0 // Within the charge: a victim, as in attempt 12.
	r := result(stopped, false)
	r.Error, r.Interrupted, r.ReleaseOnly = "", !releaseOnly, releaseOnly
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
}

// #2091: after a memory_pressure stop, the retry used to start again at the
// same 60 GiB charge beside the same peer. It now waits for the whole pool.
func TestMemoryPressureRetryRunsAlone(t *testing.T) {
	for _, releaseOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "result", true: "release first"}[releaseOnly], func(t *testing.T) {
			r := newAloneReplay(t)
			stopped := r.burst(t, r.canary, r.peer)
			if stop := priorityStop(stopped); stop == nil || stop.Cause != memoryPressureCause || stop.MemoryPool.Trigger != "stop_grace" {
				t.Fatalf("expected a forecast pressure stop: %+v", stop)
			}
			requireState(t, r.q, r.peer.ID, "running")
			pressureFinish(t, r.q, stopped, releaseOnly)
			requireState(t, r.q, r.canary.ID, "queued")

			r.setFree(110 - 20) // The canary released; the peer still holds its charge.
			if next := assignedJob(r.check(t, r.peer)); next != nil {
				t.Fatalf("pressure retry started beside the peer at %.3f GiB", memoryPools(next).RAM.ChargeGB)
			}
			if got := job(t, r.q, r.canary.ID); !strings.Contains(got.Reason, "held by "+r.peer.ID) {
				t.Fatalf("waiting retry does not name the peer: %s", got.Reason)
			}
			r.finishPeer(t)
			r.setFree(110)
			retry := r.start(t, r.canary)
			if got := memoryPools(retry).RAM; !got.Alone || got.ChargeGB != 60 {
				t.Fatalf("retry pool %+v, want alone at the unchanged 60 GiB charge", got)
			}
		})
	}
}

// Running alone, the same linear forecast of a load burst still stops the
// canary with tens of GiB of physical room left; the stop rule is unchanged
// here. Alone, that stop is held for the owner instead of retried in a loop.
func TestMemoryPressureStopWhileAloneHolds(t *testing.T) {
	for _, releaseOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "result", true: "release first"}[releaseOnly], func(t *testing.T) {
			r := newAloneReplay(t)
			pressureFinish(t, r.q, r.burst(t, r.canary, r.peer), false)
			r.finishPeer(t)
			r.setFree(110)
			alone := r.start(t, r.canary)
			stopped := r.burst(t, alone)
			d := priorityStop(stopped).MemoryPool
			if d.Trigger != "stop_grace" || d.AvailableGB-d.ReserveGB < 40 {
				t.Fatalf("alone stop comparison %+v", d)
			}
			t.Logf("alone burst stop: %.2f GiB room above reserve, %.2f GiB forecast loss, %.2f GiB used by the canary",
				d.AvailableGB-d.ReserveGB, d.GraceLossGB, 110-d.AvailableGB)
			pressureFinish(t, r.q, stopped, releaseOnly)
			got := job(t, r.q, r.canary.ID)
			if got.State != "paused" || got.Pause == nil || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("pressure stop while alone was retried: %s %s", got.State, got.Reason)
			}
			for _, want := range []string{"memory_capacity: attempt 2 was stopped for memory_pressure while running alone", "host 20.000 GiB", "device 35.000 GiB", "charge 60.000 GiB of a 96.000 GiB pool allowance", "queue trigger stop_grace"} {
				if !strings.Contains(got.Reason, want) {
					t.Fatalf("hold reason lacks %q: %s", want, got.Reason)
				}
			}
			r.check(t)
			requireState(t, r.q, r.canary.ID, "paused")
		})
	}
}

// A clean alone run feeds the ordinary exact-peak learning, and the next run of
// the workload shares the pool again under ordinary admission.
func TestMemoryPressureCleanAloneRunReturnsToSharing(t *testing.T) {
	r := newAloneReplay(t)
	pressureFinish(t, r.q, r.burst(t, r.canary, r.peer), false)
	r.finishPeer(t)
	r.setFree(110)
	alone := r.start(t, r.canary)
	done := result(alone, true)
	done.Error = ""
	peak := 45.0
	done.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CombinedHostGB: &peak, CgroupGB: &peak}
	if err := r.q.Complete(done); err != nil {
		t.Fatal(err)
	}
	requireState(t, r.q, r.canary.ID, "succeeded")

	peerRun := r.start(t, submit(t, r.q, r.peer.Spec))
	again := r.canary.Spec
	next := r.start(t, submit(t, r.q, again), peerRun)
	// Authored hints still bound the charge, as before this rule.
	if got := memoryPools(next).RAM; got.Alone || got.TypicalGB != peak || got.Evidence.Runs != 1 || got.ChargeGB != 60 {
		t.Fatalf("next run pool %+v, want shared with the %.0f GiB clean peak learned", got, peak)
	}
}

// Production still reclaims a lower-tier attempt running alone after a
// pressure stop. That priority stop is not a pressure stop, so the lower-tier
// job requeues and later runs alone again rather than being held.
func TestMemoryPressureAloneYieldsToProduction(t *testing.T) {
	r := newAloneReplay(t)
	pressureFinish(t, r.q, r.burst(t, r.canary, r.peer), false)
	r.finishPeer(t)
	r.setFree(110)
	alone := r.start(t, r.canary)
	production := sharedSpec("production", 10)
	production.Class = "production"
	queued := submit(t, r.q, production)
	if next := assignedJob(r.check(t, alone)); next != nil {
		t.Fatal("production started beside the alone attempt", next.ID)
	}
	stopped := job(t, r.q, alone.ID)
	if stop := priorityStop(stopped); stopped.State != "stopping" || stop == nil || stop.Cause != priorityPreemptionCause || stop.Demand != queued.ID {
		t.Fatalf("production did not reclaim the alone attempt: %s %+v", stopped.State, stop)
	}
	finishInterrupted(t, r.q, stopped)
	requireState(t, r.q, alone.ID, "queued")
	r.start(t, queued)
}
