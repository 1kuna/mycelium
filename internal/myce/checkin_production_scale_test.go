package myce

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

// Synthetic #1568 workload: 10,200 retained executions, 75 current jobs,
// five occupied workers, a 29-piece production fan-out and 40 waiting stages.
// Retain the existing fixture's 1,400 learned curves and constrained attempts.
func productionCheckinFixture(t testing.TB) (*Queue, []Machine) {
	t.Helper()
	q, workers := largeRetainedHistoryFixture(t, 10200)
	parentSpec := q.s.Jobs["queued-001"].Spec
	for id, j := range q.s.Jobs {
		if j.State == "queued" {
			delete(q.s.Jobs, id)
		}
	}
	parentSpec.Priority = "production"
	parentSpec.Outputs = map[string]string{"out": "results"}
	parentSpec.FanOut = &FanOut{}
	parent := &Job{ID: "producer", Spec: parentSpec, State: "queued", Queued: q.clock.Now(), FanOut: &FanOutProgress{Next: 29}}
	for i := range 29 {
		piece := FanOutPiece{Key: fmt.Sprintf("piece-%02d", i)}
		parentSpec.FanOut.Pieces = append(parentSpec.FanOut.Pieces, piece)
		child := *parent
		child.ID, child.FanOut, child.Spec.FanOut = piece.Key, nil, nil
		child.Batch = &BatchWork{Parent: parent.ID, Pieces: []FanOutPiece{piece}, Concurrency: 1}
		q.s.Jobs[child.ID] = &child
		parent.FanOut.Children = append(parent.FanOut.Children, child.ID)
	}
	q.s.Jobs[parent.ID] = parent
	for i := range 40 {
		s := parentSpec
		s.FanOut = nil
		s.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"input": "piece-00/out"}}
		id := fmt.Sprintf("stage-%02d", i)
		q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: q.clock.Now()}
	}
	if len(q.s.Jobs) != 10275 {
		t.Fatalf("fixture has %d jobs, want 10275", len(q.s.Jobs))
	}
	q.s.Submissions = map[string]*submissionRecord{}
	for id, j := range q.s.Jobs {
		j.Spec.SubmissionKey = id
		q.s.Submissions[submissionDigest(j.Spec.Project, id)] = &submissionRecord{
			Kind: submissionBinding, Project: j.Spec.Project, Key: id, JobID: id, Job: j,
		}
	}
	return q, workers
}

func BenchmarkCheckinProductionScale(b *testing.B) {
	q, workers := productionCheckinFixture(b)
	if err := q.update(func() error { return nil }); err != nil {
		b.Fatal(err)
	}
	totals := map[string]time.Duration{}
	var maximum, maxBody time.Duration
	capture := false
	q.observeBodyPhase = func(name string, elapsed time.Duration) {
		if capture {
			totals[name] += elapsed
		}
	}
	q.observePersistenceHold = func(p queueHoldPhases) {
		if !capture {
			return
		}
		// Only the first transaction is the check-in. Post-unlock diagnostic
		// builders may commit their own transactions before this call returns.
		capture = false
		hold := p.last.Sub(p.started)
		totals["update_hold"] += hold
		totals["body"] += p.phases[holdBody]
		totals["persist"] += p.phases[holdPersist]
		maximum, maxBody = max(maximum, hold), max(maxBody, p.phases[holdBody])
	}
	b.ReportAllocs()
	b.ResetTimer()
	pprof.Do(context.Background(), pprof.Labels("work", "checkin-body"), func(ctx context.Context) {
		for n := range b.N {
			i := n % len(workers)
			if i == 0 {
				q.clock.(*fakeClock).Advance(time.Second)
			}
			m := workers[i]
			span := &requestSpan{}
			ctx := context.WithValue(ctx, requestSpanKey{}, span)
			capture = true
			_, err := q.CheckinContext(ctx, Checkin{Machine: m, period: time.Second,
				AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{
					Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true,
				}}})
			if err != nil {
				b.Fatal(err)
			}
			totals["all_holds"] += span.hold
		}
	})
	b.StopTimer()
	for name, elapsed := range totals {
		b.ReportMetric(float64(elapsed)/float64(b.N)/1e6, name+"-ms/op")
	}
	b.ReportMetric(float64(maximum)/1e6, "max-hold-ms")
	b.ReportMetric(float64(maxBody)/1e6, "max-body-ms")
	verifyProductionCheckinFixture(b, q, workers)
}

// One operation is a round of five simultaneous arrivals. All transaction
// holds include post-unlock diagnostic commits, unlike the serial benchmark's
// isolated check-in transaction. Request spans sum only that request's holds.
func BenchmarkCheckinProductionScaleConcurrent(b *testing.B) {
	q, workers := productionCheckinFixture(b)
	if err := q.update(func() error { return nil }); err != nil {
		b.Fatal(err)
	}
	var maximum, maxBody, allHolds time.Duration
	q.observePersistenceHold = func(p queueHoldPhases) {
		maximum = max(maximum, p.last.Sub(p.started))
		maxBody = max(maxBody, p.phases[holdBody])
	}
	b.ResetTimer()
	for range b.N {
		q.clock.(*fakeClock).Advance(time.Second)
		start := make(chan struct{})
		spans := make([]requestSpan, len(workers))
		var wg sync.WaitGroup
		for i, m := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ctx := context.WithValue(context.Background(), requestSpanKey{}, &spans[i])
				_, err := q.CheckinContext(ctx, Checkin{Machine: m, period: time.Second,
					AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{
						Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true,
					}}})
				if err != nil {
					b.Error(err)
				}
			}()
		}
		close(start)
		wg.Wait()
		for i := range spans {
			allHolds += spans[i].hold
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(allHolds)/float64(b.N*len(workers))/1e6, "all-holds-ms/checkin")
	b.ReportMetric(float64(maximum)/1e6, "max-any-tx-hold-ms")
	b.ReportMetric(float64(maxBody)/1e6, "max-any-tx-body-ms")
	verifyProductionCheckinFixture(b, q, workers)
}

func verifyProductionCheckinFixture(b *testing.B, q *Queue, workers []Machine) {
	b.Helper()
	for i := range workers {
		j := q.s.Jobs[fmt.Sprintf("running-%d", i)]
		if j.State != "running" || j.Attempt != 1 || j.Losses != 0 {
			b.Fatalf("check-in disturbed active custody: %s %s attempt=%d losses=%d", j.ID, j.State, j.Attempt, j.Losses)
		}
	}
	for i := range 40 {
		j := q.s.Jobs[fmt.Sprintf("stage-%02d", i)]
		if j.State != "queued" || dependencyReady(j) || j.Spec.StartBy != nil {
			b.Fatalf("waiting production stage changed: %+v", j)
		}
	}
}
