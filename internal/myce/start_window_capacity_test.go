package myce

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Keep the routine deadline-maintenance path cheap with a production backlog.
// The fake clock leaves all relative windows unexpired throughout the sample.
// Run the identical fixture against release sources for performance comparisons.
func BenchmarkStartWindowDeadlineMaintenance(b *testing.B) {
	q, clock := fixture(b)
	for i := range 5 {
		m := cpuBackfillMachine(fmt.Sprintf("worker-%d", i), 64)
		if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}); err != nil {
			b.Fatal(err)
		}
	}
	for range 100 {
		s := cpuSpec(1, 4)
		s.Priority, s.StartWindow = "production", "600s"
		submit(b, q, s)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		q.maintainStartDeadlines()
	}
	b.StopTimer()
	for _, j := range q.liveJobs() {
		if j.State != "queued" || j.StartCapacityWaiting || expired(j.Spec.StartBy, clock.Now()) {
			b.Fatal("routine maintenance changed a fresh relative window", j.ID)
		}
	}
}

func TestStartWindowHostBackfill(t *testing.T) {
	for _, absolute := range []bool{false, true} {
		name := "relative"
		if absolute {
			name = "absolute"
		}
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuBackfillMachine("worker", 64)
			machineCheck(t, q, m)
			holderSpec := cpuSpec(1, 32)
			holderSpec.Priority = "production"
			holder := submit(t, q, holderSpec)
			running := assignedJob(machineCheck(t, q, m))
			if running == nil || running.ID != holder.ID {
				t.Fatal("missing running holder", running)
			}
			if err := q.Started(result(running, false)); err != nil {
				t.Fatal(err)
			}
			large := cpuSpec(1, 48)
			large.Priority = "production"
			demand := submit(t, q, large)
			clock.Advance(time.Second)
			s := cpuSpec(1, 4)
			s.Priority, s.StartWindow = "production", "60s"
			cutoff := clock.Now().Add(time.Minute)
			if absolute {
				s.StartBy = &cutoff
			}
			j := submit(t, q, s)
			for range 90 {
				if next := assignedJob(machineCheck(t, q, m, running)); next != nil {
					t.Fatal("fenced job dispatched", next)
				}
				clock.Advance(time.Second)
			}
			machineCheck(t, q, m, running)
			waiting := job(t, q, j.ID)
			if absolute {
				if waiting.State != "failed" || !waiting.Spec.StartBy.Equal(cutoff) || !strings.Contains(waiting.Reason, "production start deadline exceeded") {
					t.Fatal("absolute cutoff moved or was ignored", waiting.State, waiting.Reason)
				}
				return
			}
			if waiting.State != "queued" || !waiting.StartCapacityWaiting || !strings.Contains(waiting.Reason, "host_capacity_accumulation") || !strings.Contains(waiting.Reason, "paused for capacity") {
				t.Fatal("fenced relative window expired or was not paused", waiting.State, waiting.Reason)
			}
			finish(t, q, running, true)
			next := assignedJob(machineCheck(t, q, m))
			if next == nil || next.ID != demand.ID {
				t.Fatal("earlier demand did not dispatch", next)
			}
			offer := assignedJob(machineCheck(t, q, m, next))
			if offer == nil || offer.ID != j.ID || offer.StartCapacityWaiting || offer.Spec.StartBy.Sub(clock.Now()) != time.Minute {
				t.Fatal("fence recovery did not restore a fresh start budget", offer)
			}
			if err := q.Started(result(offer, false)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Reproduce a production recovery whose entire relative window was consumed
// by an uncharged live-host-growth refusal, including retained helper custody.
func TestStartWindowCapacityRecovery(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake", "host")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	s := spec("production")
	s.StartWindow, s.SubmissionKey = "600s", "capacity-recovery"
	j := submit(t, q, s)
	first := check(t, q, m, nil).Job
	if err := q.Started(result(first, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	finishInterrupted(t, q, first)
	recovery := check(t, q, m, nil).Job
	if recovery == nil || recovery.ID != j.ID || recovery.Spec.StartBy.Sub(clock.Now()) != 600*time.Second {
		t.Fatal("missing bounded recovery", recovery)
	}
	// The refusal can arrive after durable start authorization, before main exec.
	if err := q.Started(result(recovery, true)); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(capacityResult(recovery, true)); err != nil {
		t.Fatal(err)
	}
	before := job(t, q, j.ID)
	clock.Advance(30 * time.Minute)
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("unreleased helper reused", next)
	}
	q = reopenCancellationQueue(t, q, clock)
	waiting := job(t, q, j.ID)
	if waiting.State != "queued" || waiting.Losses != before.Losses || waiting.InfrastructureRetries != before.InfrastructureRetries || submit(t, q, s).ID != j.ID {
		t.Fatalf("capacity wait expired, charged, or duplicated: state=%s reason=%s", waiting.State, waiting.Reason)
	}
	if !strings.Contains(waiting.Reason, "600s left to start (paused for capacity)") {
		t.Fatal("missing paused start budget", waiting.Reason)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/jobs/"+j.ID, nil)
	request.Header.Set("Authorization", "Bearer test")
	reply := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(reply, request)
	var observed Job
	if reply.Code != http.StatusOK || json.Unmarshal(reply.Body.Bytes(), &observed) != nil {
		t.Fatal("job observation failed", reply.Code)
	}
	if !strings.Contains(observed.Reason, "600s left to start (paused for capacity)") || !strings.Contains(JobReasonDetail(&observed), "600s left to start (paused for capacity)") {
		t.Fatal("API/status display lost paused budget", observed.Reason, JobReasonDetail(&observed))
	}
	release := result(recovery, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	m.HostAvailableMemoryGB++
	next := check(t, q, m, nil).Job
	if next == nil || next.ID != j.ID || next.Spec.StartBy.Sub(clock.Now()) != 600*time.Second {
		t.Fatal("capacity recovery did not restore the start budget", next)
	}
	if err := q.Started(result(next, true)); err != nil {
		t.Fatal(err)
	}
	finish(t, q, next, true)
	requireState(t, q, j.ID, "succeeded")
}

func TestStartWindowCapacityRecoveryStillBoundsStuckStart(t *testing.T) {
	for _, mode := range []string{"preparation", "unreachable"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake", "host")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 1
			check(t, q, m, nil)
			s := spec("production")
			s.StartWindow, s.Needs.HostMemoryGB = "600s", 4
			j := submit(t, q, s)
			clock.Advance(30 * time.Minute)
			if got := job(t, q, j.ID); got.State != "queued" || !strings.Contains(got.Reason, "paused for capacity") {
				t.Fatal("initial capacity wait expired", got.State, got.Reason)
			}
			m.HostAvailableMemoryGB = 16
			offer := check(t, q, m, nil).Job
			if offer == nil || offer.ID != j.ID {
				t.Fatal("no offer after capacity recovered", offer)
			}
			clock.Advance(599 * time.Second)
			if mode == "preparation" {
				check(t, q, m, offer)
			}
			if got := job(t, q, j.ID); got.State != "running" || strings.Contains(got.Reason, "paused for capacity") || !strings.Contains(got.Reason, "1s left to start") {
				t.Fatal("assigned startup is not counting down", got.State, got.Reason)
			}
			clock.Advance(time.Second)
			if got := job(t, q, j.ID); got.State != "failed" || !strings.Contains(got.Reason, "start deadline exceeded") {
				t.Fatal("stuck startup escaped its window", got.State, got.Reason)
			}
			if err := q.Started(result(offer, true)); err == nil {
				t.Fatal("expired start was authorized")
			}
		})
	}
}

func TestStartWindowCapacityAbsoluteCutoffs(t *testing.T) {
	for _, kind := range []string{"start_by", "give_up_at", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake", "host")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
			s := spec("production")
			s.StartWindow = "600s"
			cutoff := clock.Now().Add(time.Minute)
			switch kind {
			case "start_by":
				s.StartBy = &cutoff
			case "give_up_at":
				s.GiveUpAt = &cutoff
			case "deadline":
				s.Deadline = &cutoff // Existing ordering hint, not an expiry policy.
			}
			j := submit(t, q, s)
			offer := check(t, q, m, nil).Job
			if err := q.Complete(capacityResult(offer, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Minute)
			got := job(t, q, j.ID)
			if kind == "deadline" {
				if got.State != "queued" || got.Spec.Deadline == nil || !got.Spec.Deadline.Equal(cutoff) {
					t.Fatal("ordering deadline changed", got.State, got.Spec.Deadline)
				}
			} else if got.State != "failed" || kind == "start_by" && !got.Spec.StartBy.Equal(cutoff) || kind == "give_up_at" && !got.Spec.GiveUpAt.Equal(cutoff) {
				t.Fatal("absolute cutoff moved or was ignored", got.State, got.Reason)
			}
		})
	}
}

func TestStartWindowCapacityPreparationDisplay(t *testing.T) {
	for _, observation := range []string{"building", "stale"} {
		t.Run(observation, func(t *testing.T) {
			q, clock := fixture(t)
			r := binaryRecipe(t, "cpu")
			if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
				t.Fatal(err)
			}
			m := cpuMachine("worker", "worker", "cpu", 8, 0.5)
			m.EnvironmentBuilder = true
			check(t, q, m, nil)
			s := cpuSpec(1, 4)
			s.Priority, s.StartWindow, s.Environment.Name = "production", "600s", "tools"
			j := submit(t, q, s)
			frozen, _, err := q.captureReadObservation(j.ID, nil)
			if err != nil || !frozen.inventoryFresh(frozen.s.Machines[m.ID]) {
				t.Fatal("initial preparation inventory was not fresh", err)
			}
			clock.Advance(30 * time.Minute)
			// The old response is point-in-time; a new job read must age the
			// same committed contact even without another queue transaction.
			if !frozen.inventoryFresh(frozen.s.Machines[m.ID]) {
				t.Fatal("later time changed the detached preparation observation")
			}
			want := "unknown_observation"
			if observation == "building" {
				m.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "building"}}
				check(t, q, m, nil)
				want = "building"
			}
			got, err := q.jobSnapshot(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Hold == nil || got.Hold.Kind != want || !strings.Contains(JobReasonDetail(got), "600s left to start (paused for capacity)") {
				t.Fatalf("preparation observation hid paused budget: blocker=%+v display=%s", got.Hold, JobReasonDetail(got))
			}
		})
	}
}
