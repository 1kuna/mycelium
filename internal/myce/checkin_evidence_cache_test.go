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

// incidentRetainedJobs is the incident's retained-history size. Tests whose
// assertions are counts rather than digests use a smaller history.
const incidentRetainedJobs = 6500

func largeRetainedHistoryFixture(t testing.TB, retained int) (*Queue, []Machine) {
	t.Helper()
	q, workers := candidateCostFixture(t)
	for i := 160; i < retained; i++ {
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
// The guarded fixture now charges cold CPU work its small startup seed. These
// digests retain every other admission field and all 660 decision records.
// Constrained host peaks also floor submitter hints: after the 200 GiB update,
// 108 hinted GPU target records retain that need and fail host-memory admission.

// The incident fixture uses real private SQLite commits and an injected decision
// clock. Inclusive child phases (notably retained_index) must not be summed.
func BenchmarkCheckinBodyPhases(b *testing.B) {
	q, workers := largeRetainedHistoryFixture(b, incidentRetainedJobs)
	benchmarkCheckinBodyPhases(b, q, workers)
}

// Exercise the retained attempt payloads and a long-running attempt's ledger,
// as well as the incident's backlog and observation catalog. No live data is
// loaded: these records are generated from the same laptop fixture.
func BenchmarkCheckinBodyAttemptScale(b *testing.B) {
	q, workers := largeRetainedHistoryFixture(b, incidentRetainedJobs)
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
			drainAdmissionWait(t, q)
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
			// The unoccupied case includes the cold accelerator diagnostic
			// with its positive RAM bootstrap floor. Keep every job and pick
			// in the snapshot.
			want := map[bool]string{true: "a77a8cc52a630c2f00d1dfabbaf7c11d289cddf7dfa2b9358edf717d0cae2455", false: "1da036d216cf72d0a910369c59db1ef4a1c26b25fb77a32652b76ccc5a9ebb3f"}[occupied]
			t.Logf("occupied=%v jobs=%d picks=%d digest=%s", occupied, len(jobs), len(picks), got)
			if got != want {
				t.Logf("planning records: %s", data)
				t.Fatalf("planning outcomes %s, want %s", got, want)
			}
		})
	}
}
