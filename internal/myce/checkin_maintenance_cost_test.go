package myce

import (
	"context"
	"fmt"
	"runtime/pprof"
	"slices"
	"testing"
	"time"
)

// A synthetic 225-job load. Production is eligible on four occupied
// workers, and a previously preempted experiment checks for a production gap
// on the fifth. Relative and absolute windows exercise both deadline paths.
func BenchmarkCheckinMaintenanceDeepBacklog(b *testing.B) {
	q, workers := candidateCostFixture(b)
	clock := q.clock.(*fakeClock)
	if err := q.update(func() error {
		for i := range workers {
			m := q.touchMachine(workers[i].ID)
			m.Capabilities = append(m.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
			workers[i].Capabilities = m.Capabilities
		}
		// A large same-tier holder leaves a small CPU/RAM margin. The queued
		// head can use the host when it releases and fences the margin meanwhile.
		holder := q.touchJob("running-0")
		holder.Spec.Needs.CPUCores = 12
		holder.Spec.Needs.HostMemoryGB = 8
		for i := 0; i < 220; i++ {
			id := fmt.Sprintf("queued-%03d", i)
			if i >= 110 {
				job := *q.s.Jobs[fmt.Sprintf("queued-%03d", i-110)]
				job.ID = id
				q.putJob(id, &job)
			}
			job := q.touchJob(id)
			if job.Spec.Priority == "production" {
				job.Spec.ExcludedMachines = []string{workers[4].ID}
				if i%4 == 0 {
					deadline, explicit := clock.Now().Add(time.Hour), true
					job.Spec.StartBy, job.StartByExplicit = &deadline, &explicit
				}
			} else if i == 95 {
				job.ProductionBackoff = map[string]ProductionBackoff{hostID(&workers[4]): {LastAt: clock.Now()}}
			}
		}
		head := q.touchJob("queued-000")
		head.Spec.Needs.HostMemoryGB = 28
		head.Spec.Needs.CPUCores = 8
		head.Spec.AllowedMachines = []string{workers[0].ID}
		deadline := clock.Now()
		head.Spec.Deadline = &deadline
		head.PreemptionPlacement = &PreemptionPlacement{Machine: workers[0].ID, Session: workers[0].Session, Device: "0"}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	checkin := func(ctx context.Context, i int) {
		m := workers[i]
		r, err := q.CheckinContext(ctx, Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol,
			Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true}}})
		if err != nil || r.Job != nil || r.Stop {
			b.Fatalf("check-in changed custody on worker %d: job=%v stop=%v err=%v", i, r.Job, r.Stop, err)
		}
		for _, attempt := range r.Attempts {
			if attempt.Stop || attempt.Job != nil && attempt.ID != fmt.Sprintf("running-%d", i) {
				b.Fatalf("check-in changed custody on worker %d: %+v", i, attempt)
			}
		}
	}
	for i := range workers {
		checkin(context.Background(), i)
	}
	var maintenance time.Duration
	q.observeBodyPhase = func(name string, elapsed time.Duration) {
		if name == "maintain" {
			maintenance += elapsed
		}
	}
	holds := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	pprof.Do(context.Background(), pprof.Labels("work", "deep-backlog-checkin"), func(ctx context.Context) {
		for n := range b.N {
			i := n % len(workers)
			if i == 0 {
				clock.Advance(time.Second)
			}
			var span requestSpan
			checkin(context.WithValue(ctx, requestSpanKey{}, &span), i)
			holds = append(holds, span.hold)
		}
	})
	b.StopTimer()
	slices.Sort(holds)
	median := (holds[(len(holds)-1)/2] + holds[len(holds)/2]) / 2
	b.ReportMetric(holdMS(median), "median-hold-ms/checkin")
	b.ReportMetric(holdMS(maintenance)/float64(b.N), "mean-maintenance-ms/checkin")
}
