package myce

import (
	"context"
	"fmt"
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

// The incident fixture uses real private SQLite commits and an injected decision
// clock. Inclusive child phases (notably retained_index) must not be summed.
func BenchmarkCheckinBodyPhases(b *testing.B) {
	q, workers := largeRetainedHistoryFixture(b)
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
	for n := range b.N {
		i := n % len(workers)
		m := workers[i]
		span := &requestSpan{}
		ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
		_, err := q.CheckinContext(ctx, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true}}})
		totals["all_holds"] += span.hold
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	for name, elapsed := range totals {
		b.ReportMetric(float64(elapsed)/float64(b.N)/1e6, name+"-ms/op")
	}
}
