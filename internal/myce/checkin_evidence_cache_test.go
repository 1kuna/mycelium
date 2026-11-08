package myce

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
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
		if scans == 0 || scans > 7 || uncached != 0 {
			t.Errorf("check-in evidence scans: %d total, %d uncached; want 1..7 total and 0 uncached", scans, uncached)
		}
	}
}
