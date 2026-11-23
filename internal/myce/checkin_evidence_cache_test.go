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
func TestCheckinRetainedEvidenceDecisionEquivalence(t *testing.T) {
	q, _ := largeRetainedHistoryFixture(t, incidentRetainedJobs)
	type decision struct {
		Job, Target string
		Failed      []string
		Needs       ResourceNeeds
		Envelope    *JobMemoryEnvelope
		Oversized   bool
	}
	compare := func(stage, want string) {
		t.Helper()
		collect := func(cache *memoryPlanningCache, reference bool) []decision {
			q.memoryPlanning = cache
			var decisions []decision
			for _, j := range q.s.Jobs {
				if j.State != "queued" || reference && j.ID >= "queued-012" {
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
			return decisions
		}
		decisions := collect(newMemoryPlanningCache(), false)
		data, err := json.Marshal(decisions)
		if err != nil {
			t.Fatal(err)
		}
		// Twelve distinct recipes cover CPU/GPU and cold/accepted evidence.
		// The full snapshot below still covers all 660 decisions; repeating
		// all retained-history scans uncached would dominate this regression.
		var reference []decision
		for _, d := range decisions {
			if d.Job < "queued-012" {
				reference = append(reference, d)
			}
		}
		referenceData, err := json.Marshal(reference)
		if err != nil {
			t.Fatal(err)
		}
		uncached, err := json.Marshal(collect(nil, true))
		if err != nil || string(referenceData) != string(uncached) {
			t.Fatalf("%s cached admission differs from uncached decisions: %v", stage, err)
		}
		got := fmt.Sprintf("%x", sha256.Sum256(data))
		oversized := 0
		for _, d := range decisions {
			if d.Oversized {
				oversized++
			}
			if stage == "new evidence" && d.Envelope != nil && d.Envelope.Basis == "hint" {
				if d.Needs.HostMemoryGB != 200 || d.Envelope.HostGB != 200 {
					t.Errorf("%s/%s: hint hid the 200 GiB constrained host peak: needs=%g envelope=%g", d.Job, d.Target, d.Needs.HostMemoryGB, d.Envelope.HostGB)
				}
			}
		}
		t.Logf("%s: %d decisions, %d oversized; digest %s", stage, len(decisions), oversized, got)
		if got != want {
			t.Logf("%s admission records: %s", stage, data)
			t.Errorf("%s decision digest %s, want %s", stage, got, want)
		}
	}
	compare("fitting", "0865ba8ae50554a335e234a4e5c0b5fdfccea71a0fc8b99f032d50ac637a5b26")
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
	compare("new evidence", "96a82003362a1930af5c043b17721a6ac0fc76309a256756615847e8e18b560b")
	for _, m := range q.s.Machines {
		m.ExtensionFingerprint, m.CPUExtensionFingerprint = "changed-identity", "changed-identity"
		for i := range m.Devices {
			m.Devices[i].ExtensionFingerprint = "changed-device"
		}
	}
	q.memoryObservationIndex = nil
	// After the identity change, 150 shell-sized CPU starts with deadlines
	// add only the guard-unavailable refusal and its fit predicate (#1967).
	compare("changed identity", "d415204cce98441b2c958219772d9f4e22cf4855cc894add104c062ceb39dd54")
}

func TestCheckinRetainedEvidenceScans(t *testing.T) {
	q, workers := largeRetainedHistoryFixture(t, 480)
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	full := len(q.s.Jobs)
	scans, uncached := 0, 0
	q.observeMemoryHistory = func(jobs int, indexed bool) {
		// Live-only planning views do not scan retained history.
		if jobs >= full {
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
			// The small first-run charge changes available-capacity placements;
			// occupied capacity still has the same jobs, reasons and six picks.
			// #1686 removes memory details for ineligible targets. Restoring only
			// the former noFitDetail reproduces the old available-capacity digest;
			// the changed snapshot retains all 270 jobs and six picks.
			want := map[bool]string{true: "80e3a367392c6882691f3933a050cc97f4fee73a47d37f907a1611a494cb41e8", false: "8348d394dd1639faa5d718d9a648c8887a2b501744b4c2538edebe0d8a6197d5"}[occupied]
			t.Logf("occupied=%v jobs=%d picks=%d digest=%s", occupied, len(jobs), len(picks), got)
			if got != want {
				t.Logf("planning records: %s", data)
				t.Fatalf("planning outcomes %s, want %s", got, want)
			}
		})
	}
}
