package myce

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"runtime/pprof"
	"sort"
	"testing"
	"time"
)

func largeRetainedHistoryFixture(t testing.TB) (*Queue, []Machine) {
	t.Helper()
	q, workers := candidateCostFixture(t)
	for i := 160; i < 6500; i++ {
		source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j := *source
		j.ID = fmt.Sprintf("retained-%04d", i)
		timing := *source.Timing
		timing.Execution.ID = j.ID
		j.Timing = &timing
		q.s.Jobs[j.ID] = &j
	}
	return q, workers
}

// Frozen on the uncached health implementation. Compare all permanent admission
// predicates, resolved needs/envelopes and health outcomes for the incident shape,
// including the next pass after new evidence and changed inventory identity.
func TestCheckinRetainedEvidenceDecisionEquivalence(t *testing.T) {
	q, _ := largeRetainedHistoryFixture(t)
	type decision struct {
		Job, Target string
		Failed      []string
		Needs       ResourceNeeds
		Envelope    *JobMemoryEnvelope
		Oversized   bool
	}
	compare := func(stage, want string) {
		t.Helper()
		q.memoryPlanning = newMemoryPlanningCache()
		var decisions []decision
		for _, j := range q.s.Jobs {
			if j.State != "queued" {
				continue
			}
			oversized := q.learnedEnvelopeOversized(j)
			q.evaluateTargets(j, func(fit targetFit, candidate *Job, m *Machine) bool {
				decisions = append(decisions, decision{j.ID, targetKey(m), fit.failed, candidate.Spec.Needs, candidate.MemoryEnvelope, oversized})
				return true
			})
		}
		q.memoryPlanning = nil
		sort.Slice(decisions, func(i, j int) bool {
			if decisions[i].Job != decisions[j].Job {
				return decisions[i].Job < decisions[j].Job
			}
			return decisions[i].Target < decisions[j].Target
		})
		data, err := json.Marshal(decisions)
		if err != nil {
			t.Fatal(err)
		}
		got := fmt.Sprintf("%x", sha256.Sum256(data))
		oversized := 0
		for _, d := range decisions {
			if d.Oversized {
				oversized++
			}
		}
		t.Logf("%s: %d decisions, %d oversized; digest %s", stage, len(decisions), oversized, got)
		if got != want {
			t.Errorf("%s decision digest %s, want %s", stage, got, want)
		}
	}
	compare("fitting", "96c4e3fbe5185c413b629540b9ddea888643d0047ee115549111caebab8b30e4")
	for id, observation := range q.s.WorkObservations {
		peak := 200.0
		copy := *observation.MemoryPeak
		copy.HostGB = &peak
		observation.MemoryPeak = &copy
		q.s.WorkObservations[id] = observation
	}
	for _, j := range q.s.Jobs {
		if j.State == "succeeded" {
			facts := j.MemoryAttempts[0]
			peak := *facts.MemoryPeak
			large := 200.0
			peak.HostGB = &large
			facts.MemoryPeak = &peak
			j.MemoryAttempts = []AttemptMemoryFacts{facts}
		}
	}
	q.memoryObservationIndex = nil
	compare("new evidence", "ff969673176a103779cc2335886ac65e7353b33d12246358f9ef3048a544c90e")
	for _, m := range q.s.Machines {
		m.ExtensionFingerprint, m.CPUExtensionFingerprint = "changed-identity", "changed-identity"
		for i := range m.Devices {
			m.Devices[i].ExtensionFingerprint = "changed-device"
		}
	}
	q.memoryObservationIndex = nil
	compare("changed identity", "46cc7217e338d91f0a0113977e7ce9f7845ef8e7530626d1e2df80eb9e90ac7b")
}

func TestCheckinRetainedEvidenceScans(t *testing.T) {
	q, workers := largeRetainedHistoryFixture(t)
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	scans, uncached := 0, 0
	q.observeMemoryHistory = func(jobs int, indexed bool) {
		// Live-only planning views do not scan retained history.
		if jobs >= 6500 {
			scans++
			if !indexed {
				uncached++
			}
		}
	}
	for i, worker := range workers {
		scans, uncached = 0, 0
		_, err := q.Checkin(Checkin{Machine: worker, AttemptProtocol: keyedAttemptProtocol,
			Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: worker.Session, Started: true}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d full-history evidence scans, %d uncached", worker.ID, scans, uncached)
		if scans > 1 || uncached != 0 {
			t.Errorf("check-in evidence scans: %d total, %d uncached; want at most 1 total and 0 uncached", scans, uncached)
		}
	}
}

// The incident fixture uses real private SQLite commits and an injected decision
// clock. Inclusive child phases (notably retained_index) must not be summed.
func BenchmarkCheckinBodyPhases(b *testing.B) {
	q, workers := largeRetainedHistoryFixture(b)
	benchmarkCheckinBodyPhases(b, q, workers)
}

// Exercise the retained attempt payloads and a long-running attempt's ledger,
// as well as the incident's backlog and observation catalog. No live data is
// loaded: these records are generated from the same laptop fixture.
func BenchmarkCheckinBodyAttemptScale(b *testing.B) {
	q, workers := largeRetainedHistoryFixture(b)
	for _, j := range q.s.Jobs {
		if j.State != "succeeded" {
			continue
		}
		fact := j.MemoryAttempts[0]
		for attempt := 2; attempt <= 8; attempt++ {
			fact.Attempt = attempt
			j.MemoryAttempts = append(j.MemoryAttempts, fact)
		}
	}
	for n := range 17400 {
		q.appendJobLedger(q.s.Jobs["running-0"], JobLedgerEntry{Attempt: 1, Kind: "custody", Machine: workers[0].ID, Session: workers[0].Session, Reason: "synthetic retained execution history", Release: "confirmed", ProcessID: fmt.Sprint(n)})
	}
	benchmarkCheckinBodyPhases(b, q, workers)
}

func benchmarkCheckinBodyPhases(b *testing.B, q *Queue, workers []Machine) {
	if err := q.update(func() error { return nil }); err != nil {
		b.Fatal(err)
	}
	totals := map[string]time.Duration{}
	q.observeBodyPhase = func(name string, elapsed time.Duration) { totals[name] += elapsed }
	q.observeUpdatePhases = func(phases [5]time.Duration) {
		totals["body"] += phases[holdBody]
		totals["persist"] += phases[holdPersist]
		totals["snapshot"] += phases[holdRecords]
	}
	b.ReportAllocs()
	b.ResetTimer()
	pprof.Do(context.Background(), pprof.Labels("work", "checkin-body"), func(ctx context.Context) {
		for n := range b.N {
			i := n % len(workers)
			m := workers[i]
			span := &requestSpan{}
			ctx := context.WithValue(ctx, requestSpanKey{}, span)
			_, err := q.CheckinContext(ctx, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true}}})
			totals["all_holds"] += span.hold
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	for name, elapsed := range totals {
		b.ReportMetric(float64(elapsed)/float64(b.N)/1e6, name+"-ms/op")
	}
}

// Frozen on the pre-change planner as well as the existing 660 admission and
// 402 dispatch decisions. Cover the maintenance release pass and every queued
// waiting reason with both occupied and available CPU capacity.
func TestCheckinPlanningOutcomeEquivalence(t *testing.T) {
	for _, occupied := range []bool{true, false} {
		t.Run(fmt.Sprint(occupied), func(t *testing.T) {
			q, _ := candidateCostFixture(t)
			if !occupied {
				for i := range 5 {
					delete(q.s.Jobs, fmt.Sprintf("running-%d", i))
				}
			}
			q.refreshHostMemory()
			q.refreshHostCPU()
			q.maintain()
			type jobOutcome struct {
				ID, State, Machine, Session, Reason string
				Attempt                             int
				Needs                               ResourceNeeds
			}
			var jobs []jobOutcome
			for _, j := range q.liveJobs() {
				jobs = append(jobs, jobOutcome{j.ID, j.State, j.Machine, j.Session, j.Reason, j.Attempt, j.Spec.Needs})
			}
			sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
			type pick struct{ Target, Job string }
			var picks []pick
			scan := q.placementScan()
			for _, m := range scan.machines {
				id := ""
				if j := q.plannedPick(m, scan); j != nil {
					id = j.ID
				}
				picks = append(picks, pick{targetKey(m), id})
			}
			data, err := json.Marshal(struct {
				Jobs  []jobOutcome
				Picks []pick
			}{jobs, picks})
			if err != nil {
				t.Fatal(err)
			}
			got := fmt.Sprintf("%x", sha256.Sum256(data))
			want := map[bool]string{true: "80e3a367392c6882691f3933a050cc97f4fee73a47d37f907a1611a494cb41e8", false: "7b41b03ef3b1540261f76675346d941484131e96bcdcc3631e8f64de83bc8726"}[occupied]
			t.Logf("occupied=%v jobs=%d picks=%d digest=%s", occupied, len(jobs), len(picks), got)
			if got != want {
				t.Fatalf("planning outcomes %s, want %s", got, want)
			}
		})
	}
}
