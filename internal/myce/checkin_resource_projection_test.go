package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestScarceHostYieldProjectionKeepsDemand(t *testing.T) {
	q, clock, _, _, victim := yieldFixture(t, false)
	drainAdmissionWait(t, q)
	s := spec("experiment")
	s.Resumable = false
	s.Needs.CPUCores, s.Needs.HostMemoryGB = 1, 1
	s.Needs.Platform = Platform{Required: []string{"fake"}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	var target *Machine
	for _, m := range q.plannerMachines() {
		if m.GPU == "fake" {
			target = m
			break
		}
	}
	if target == nil {
		t.Fatal("missing scarce target")
	}
	set := &candidateSet{q: q, m: target, resources: map[batchResourceKey]batchResourceRoom{}}
	// A portable demand can populate the ordinary projection first. It must
	// neither suppress later yields nor donate another demand's stop authority.
	portable := &Job{ID: "portable-demand", State: "queued", Spec: s, Queued: clock.Now()}
	q.batchResourcesFitWithScan(portable, target, set.resources)
	for _, id := range []string{"demand-a", "demand-b"} {
		j := *portable
		j.ID = id
		j.Spec.RequirementReasons = map[string]string{"backend:fake": "requires simulated accelerator"}
		if !q.batchResourcesFitWithScan(&j, target, set.resources) {
			t.Fatal("yield demand lost admission", id)
		}
		p := set.batchPlan(&j)
		if p == nil || p.yields[victim.ID] == nil || p.yields[victim.ID].Demand != id {
			t.Fatal("yield plan reused a different demand", id, p)
		}
		if !reflect.DeepEqual(p, q.roomPlan(&j, target, true)) {
			t.Fatal("cached yield differs from uncached plan", id)
		}
	}
}

// The same synthetic fixture can be copied unchanged to older releases.
// JSON applies the new annotation only where the release supports it.
func BenchmarkCheckinResourceProjection(b *testing.B) {
	for _, shape := range []string{"plain", "reasons", "reasons-readers"} {
		b.Run(shape, func(b *testing.B) {
			q, workers := candidateCostFixture(b)
			clock := &workerFakeClock{now: q.clock.Now()}
			q.clock = clock
			for i := 160; i < 6500; i++ {
				source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
				j := *source
				j.ID = fmt.Sprintf("retained-%04d", i)
				timing := *source.Timing
				timing.Execution.ID = j.ID
				j.Timing = &timing
				q.s.Jobs[j.ID] = &j
			}
			if shape != "plain" {
				for _, j := range q.s.Jobs {
					if j.State == "queued" {
						if err := json.Unmarshal([]byte(`{"requirement_reasons":{"os:linux":"requires Linux"}}`), &j.Spec); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			if err := q.update(func() error { return nil }); err != nil {
				b.Fatal(err)
			}
			var holds, waits []time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if i%len(workers) == 0 {
					clock.Advance(time.Second)
				}
				var readers sync.WaitGroup
				readWaits := make([]time.Duration, 4)
				if shape == "reasons-readers" {
					for n := range readWaits {
						readers.Add(1)
						go func() {
							defer readers.Done()
							span := &requestSpan{}
							ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
							id := fmt.Sprintf("queued-%03d", (i*4+n)%30)
							if _, err := q.jobSnapshot(id, ctx); err != nil {
								b.Error(err)
							}
							readWaits[n] = span.wait
						}()
					}
				}
				m := workers[i%len(workers)]
				span := &requestSpan{}
				ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
				_, err := q.CheckinContext(ctx, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i%len(workers)), Attempt: 1}, Session: m.Session, Started: true}}})
				readers.Wait()
				if err != nil {
					b.Fatal(err)
				}
				holds = append(holds, span.hold)
				if shape == "reasons-readers" {
					waits = append(waits, readWaits...)
				}
			}
			b.StopTimer()
			for name, values := range map[string][]time.Duration{"hold": holds, "get-wait": waits} {
				if len(values) == 0 {
					continue
				}
				sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
				b.ReportMetric(float64(values[(len(values)-1)/2].Nanoseconds()), name+"-p50-ns")
				b.ReportMetric(float64(values[(99*len(values)+99)/100-1].Nanoseconds()), name+"-p99-ns")
			}
		})
	}
}
