package myce

import (
	"strings"
	"testing"
	"time"
)

// #1299: between canary stages on b70, lower-tier jobs that production had
// just preempted were placed back on the host and evicted again seconds later,
// churning memory while the next stage prepared. While outranking production
// work that could start on that host is pending, the preempted job places on
// another host instead; it may return once the stage is placed or a lease's
// brief window ends.
func TestPreemptedWorkSkipsHostBetweenProductionStages(t *testing.T) {
	for _, pending := range []string{"queued-stage", "open-lease"} {
		t.Run(pending, func(t *testing.T) {
			q, clock := fixture(t)
			a, b := machine("a", "fake", "host-a"), machine("b", "fake", "host-b")
			check(t, q, a, nil)
			check(t, q, b, nil)
			low := spec("experiment")
			low.Name, low.Needs.MemoryGB = "low", 4
			lj := submit(t, q, low)
			if err := q.update(func() error {
				j := q.touchJob(lj.ID)
				j.ProductionBackoff = map[string]ProductionBackoff{"host-a": {LastAt: clock.Now()}}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if pending == "queued-stage" {
				// The pipeline's next stage is ready and can start on host-a,
				// but has not been placed yet.
				s := spec("production")
				s.Name, s.AllowedMachines = "stage", []string{"a"}
				submit(t, q, s)
			} else if _, err := q.OpenProductionLease(ProductionLeaseRequest{Project: "test", Key: "pipeline", TTL: "2h"}); err != nil {
				t.Fatal(err)
			}
			reason := func(m Machine) string {
				t.Helper()
				var out string
				if err := q.update(func() error {
					out = holdDetail(q.productionAdmissionHold(q.s.Jobs[lj.ID], q.s.Machines[m.ID]))
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				return out
			}
			if got := reason(a); !strings.Contains(got, "pending after preemption") {
				t.Fatalf("preempted work not held off host-a: %q", got)
			}
			if got := reason(b); got != "" {
				t.Fatalf("hold leaked to another host: %q", got)
			}
			if reply := check(t, q, b, nil); reply.Job == nil || reply.Job.ID != lj.ID {
				t.Fatalf("preempted work did not place elsewhere: %+v", reply.Job)
			}
			if err := q.update(func() error {
				q.requeue(q.touchJob(lj.ID), "test requeue", false)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if pending == "queued-stage" {
				if reply := check(t, q, a, nil); reply.Job == nil || reply.Job.Spec.Name != "stage" {
					t.Fatalf("production stage not placed first: %+v", reply.Job)
				}
			} else {
				clock.Advance(ProductionStartTimeout + time.Second)
			}
			if got := reason(a); got != "" {
				t.Fatalf("hold outlived the pending production work: %q", got)
			}
		})
	}
}
