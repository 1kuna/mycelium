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

// Model the incident mix, then measure routine maintenance after the first
// expiry decisions. The fake clock stays fixed: a fenced render's re-armed
// budget must avoid repeating backlog planning on every worker check-in.
// The old release fails the three renders during warmup; report that difference.
func BenchmarkStartWindowDeadlineMaintenance(b *testing.B) {
	q, clock := fixture(b)
	m := cpuBackfillMachine("worker-0", 64)
	check(b, q, m, nil)
	holderSpec := cpuSpec(1, 32)
	holderSpec.Priority = "production" // Same-tier work must finish, not be reclaimed.
	holder := submit(b, q, holderSpec)
	running := assignedJob(check(b, q, m, nil))
	if running == nil || running.ID != holder.ID {
		b.Fatal("missing holder")
	}
	if err := q.Started(result(running, false)); err != nil {
		b.Fatal(err)
	}
	for i := 1; i < 5; i++ {
		check(b, q, cpuBackfillMachine(fmt.Sprintf("worker-%d", i), 8), nil)
	}
	s := cpuSpec(1, 48)
	s.Priority, s.StartWindow = "production", "600s"
	s.AllowedMachines = []string{m.ID}
	demand := submit(b, q, s)
	clock.Advance(time.Second)
	s.Needs.HostMemoryGB = 4
	s.Needs.MemoryGB = 0
	template := submit(b, q, s)
	drainAdmissionWait(b, q)
	q.mu.Lock()
	defer q.mu.Unlock()
	expiredAt := clock.Now().Add(-20 * time.Minute)
	// Cold, structurally valid queue records isolate this pass from submission I/O.
	for i := range 24 {
		j := cloneJob(template)
		j.ID = fmt.Sprintf("parent-%02d", i)
		j.Spec.StartBy = &expiredAt
		j.Spec.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "piece"}}}
		j.FanOut = &FanOutProgress{}
		q.s.Jobs[j.ID] = &j
	}
	for i := range 100 {
		j := cloneJob(template)
		j.ID = fmt.Sprintf("stage-%03d", i)
		j.Spec.StartBy, j.StartWindow = nil, 0
		j.Spec.After = &PieceDependency{Job: "parent-00", Outputs: map[string]string{"input": "output"}}
		j.Dependency = &PieceBinding{Job: "parent-00", State: "waiting"}
		q.s.Jobs[j.ID] = &j
	}
	renders := []*Job{q.s.Jobs[template.ID]}
	for i := range 2 {
		j := cloneJob(template)
		j.ID = fmt.Sprintf("render-%d", i)
		q.s.Jobs[j.ID] = &j
		renders = append(renders, &j)
	}
	for _, j := range renders {
		j.Spec.StartBy = &expiredAt
	}
	// Confirm the fixture really exercises host accumulation, not ordinary fit.
	scan := q.placementScan()
	for _, j := range renders {
		waiting, ready := q.queuedCapacityStatus(j)
		reason := q.hostBackfillWaitingReason(j, scan)
		if waiting || !ready || reason == "" {
			b.Fatalf("render is not ready but fenced: waiting=%t ready=%t reason=%q holder_stop=%v", waiting, ready, reason, q.s.Jobs[holder.ID].StopAt)
		}
	}
	q.maintainStartDeadlines()
	q.maintainStartDeadlines()
	failed := 0
	for _, j := range renders {
		if j.State == "failed" {
			failed++
		}
	}
	b.Logf("warmup: %d/3 renders failed; 24 expired parents, 100 blocked stages, 5 machines; demand=%s", failed, demand.ID)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		q.maintainStartDeadlines()
	}
	b.StopTimer()
	for i := range 24 {
		j := q.s.Jobs[fmt.Sprintf("parent-%02d", i)]
		if j.State != "queued" {
			b.Fatal("logical parent expired")
		}
	}
}

// Logical parents do not consume physical launch budgets. A fenced ordinary
// job re-arms at expiry, but must still fail if that renewed budget later
// expires with an unavailable route and no host fence.
func TestStartWindowFenceExpiryExemptions(t *testing.T) {
	q, clock := fixture(t)
	m := cpuBackfillMachine("worker", 64)
	machineCheck(t, q, m)
	s := cpuSpec(1, 4)
	s.Priority, s.StartWindow = "production", "60s"
	parent := submit(t, q, s)
	j := submit(t, q, s)
	drainAdmissionWait(t, q)
	q.mu.Lock()
	p := q.s.Jobs[parent.ID]
	p.Spec.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "piece"}}}
	p.FanOut = &FanOutProgress{}
	cutoff := *p.Spec.StartBy
	q.mu.Unlock()
	clock.Advance(time.Minute)
	q.mu.Lock()
	// No check-in/assignment: expiry decisions alone must preserve the parent
	// and fail the ordinary job, with no fence and no deadline extension.
	q.maintainStartDeadlines()
	p = q.s.Jobs[parent.ID]
	got := q.s.Jobs[j.ID]
	parentOK := p.State == "queued" && p.Spec.StartBy.Equal(cutoff)
	jobOK := got.State == "failed" && got.Spec.StartBy.Equal(cutoff)
	q.mu.Unlock()
	if !parentOK || !jobOK {
		t.Fatal("expiry exemptions or unfenced expiry changed", parentOK, jobOK)
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
