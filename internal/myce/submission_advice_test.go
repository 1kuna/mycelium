package myce

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// The incident-size history preserves constrained execution provenance used by
// memory planning: 6,500 retained executions, 110 queued, 1,400 curves, 5 workers.
func submissionAdviceFixture(t testing.TB) *Queue {
	t.Helper()
	q, _ := candidateCostFixture(t)
	for i := 160; i < 6500; i++ {
		source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j := *source
		j.ID = fmt.Sprintf("retained-%04d", i)
		timing := *source.Timing
		timing.Execution.ID = j.ID
		j.Timing = &timing
		q.s.Jobs[j.ID] = &j
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	return q
}

func BenchmarkSubmissionAdvice(b *testing.B) {
	q := submissionAdviceFixture(b)
	var phases [4]time.Duration
	q.observeSubmissionAdvice = func(timing submissionAdviceTiming) {
		for i, d := range timing.phases {
			phases[i] += d
		}
	}
	var hold time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		j := cloneJob(q.s.Jobs["queued-081"])
		span := &requestSpan{}
		q.submissionDeadlineGuidance(&j, context.WithValue(context.Background(), requestSpanKey{}, span))
		hold += span.hold
	}
	b.StopTimer()
	b.ReportMetric(float64(hold.Nanoseconds())/float64(b.N), "mutex-ns/op")
	for i, name := range []string{"capture-ns/op", "wait-reason-ns/op", "status-reason-ns/op", "deadline-ns/op"} {
		b.ReportMetric(float64(phases[i].Nanoseconds())/float64(b.N), name)
	}
}
