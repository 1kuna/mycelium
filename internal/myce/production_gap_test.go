package myce

import (
	"strings"
	"testing"
	"time"
)

// #1299: between production stages, lower-tier jobs that production had
// just preempted were placed back on the host and evicted again seconds later,
// churning memory while the next stage prepared. While outranking production
// work that could start on that host is pending, the preempted job places on
// another host instead; it may return once the command starts or a lease's
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
			} else if _, err := q.OpenProductionLease(ProductionLeaseRequest{Project: "test", Key: "pipeline", TTL: "30s"}); err != nil {
				t.Fatal(err)
			}
			reason := func(m Machine) string {
				t.Helper()
				var out string
				if err := q.update(func() error {
					j, target := q.s.Jobs[lj.ID], q.s.Machines[m.ID]
					out = holdDetail(q.productionAdmissionHold(j, target))
					q.memoryPlanning = newMemoryPlanningCache()
					defer func() { q.memoryPlanning = nil }()
					for range 2 {
						if got := holdDetail(q.productionAdmissionHold(j, target)); got != out {
							t.Fatalf("cached gap %q differs from %q", got, out)
						}
					}
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
				reply := check(t, q, a, nil)
				if reply.Job == nil || reply.Job.Spec.Name != "stage" {
					t.Fatalf("production stage not placed first: %+v", reply.Job)
				}
				// This stage has no release plan; the assigned-attempt claim is
				// covered below with actual capacity preemption.
			} else {
				if err := q.update(func() error {
					q.memoryPlanning = newMemoryPlanningCache()
					defer func() { q.memoryPlanning = nil }()
					j, target := q.s.Jobs[lj.ID], q.s.Machines[a.ID]
					if got := holdDetail(q.productionAdmissionHold(j, target)); !strings.Contains(got, "production lease pipeline pending") {
						t.Fatalf("active lease did not protect the host: %q", got)
					}
					clock.Advance(30 * time.Second)
					if got := holdDetail(q.productionAdmissionHold(j, target)); got != "" {
						t.Fatalf("expired lease kept the host fenced during the same pass: %q", got)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if got := reason(a); got != "" {
				t.Fatalf("hold outlived the pending production work: %q", got)
			}
		})
	}
}

// #1619: placement and final live launch admission can see different room.
// A refused production attempt must keep the preemption commitment while its
// helpers release and the retry prepares, even after queue Started commits.
func TestLaunchCapacityPreemptionKeepsRoomUntilCommandStart(t *testing.T) {
	for _, boundary := range []string{"command-start", "release", "cancel", "restart", "refused-again", "other-host"} {
		t.Run(boundary, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "fake", 8, 32)
			m.Devices = []MachineDevice{}
			// Twelve total requested GiB cannot fit this 11 GiB of observed room.
			// That ordinary queue-side shortfall earns the preemption claim.
			m.HostAvailableMemoryGB = 11
			low := submit(t, q, cpuSpec(1, 8))
			victim := assignedJob(machineCheck(t, q, m))
			p := cpuSpec(1, 4)
			p.Priority = "production"
			demand := submit(t, q, p)
			machineCheck(t, q, m, victim)
			if got := job(t, q, low.ID); got.State != "stopping" {
				t.Fatal("ordinary RAM shortfall did not preempt", got.State)
			}
			finishInterrupted(t, q, victim)
			m.HostAvailableMemoryGB = 32 // Spare room can fit the victim once the claim ends.
			offer := assignedJob(machineCheck(t, q, m))
			if offer == nil || offer.ID != demand.ID {
				t.Fatal("missing production offer", offer)
			}
			if err := q.Started(result(offer, false)); err != nil {
				t.Fatal(err)
			}
			if err := q.Complete(capacityResult(offer, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Minute)
			offer = assignedJob(machineCheck(t, q, m))
			if offer == nil || offer.ID != demand.ID || offer.Attempt != 2 {
				t.Fatal("missing production retry", offer)
			}
			if err := q.Started(result(offer, false)); err != nil {
				t.Fatal(err)
			}
			if boundary == "restart" {
				q = reopenCancellationQueue(t, q, clock)
			}
			// Raw heartbeat: the queue authorized exec, but main has not started.
			pull := func(started bool) Reply {
				r, err := q.Checkin(Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol,
					Attempts: []AttemptReport{{Execution: Execution{ID: offer.ID, Attempt: offer.Attempt}, Session: offer.Session, Started: started}}})
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			// A delayed report for the released attempt cannot end the new claim.
			if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol,
				Attempts: []AttemptReport{{Execution: Execution{ID: offer.ID, Attempt: 1}, Session: offer.Session, Started: true},
					{Execution: Execution{ID: offer.ID, Attempt: offer.Attempt}, Session: offer.Session}}}); err != nil {
				t.Fatal(err)
			}
			if boundary == "refused-again" {
				if err := q.Complete(capacityResult(offer, true)); err != nil {
					t.Fatal(err)
				}
				if next := assignedJob(pull(false)); next != nil {
					t.Fatal("retry bypassed helper release", next.ID)
				}
				r := result(offer, false)
				r.ReleaseOnly = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				clock.Advance(2 * time.Minute)
				offer = assignedJob(machineCheck(t, q, m))
				if offer == nil || offer.ID != demand.ID || offer.Attempt != 3 {
					t.Fatal("lost production claim after second refusal", offer)
				}
				if err := q.Started(result(offer, false)); err != nil {
					t.Fatal(err)
				}
			}
			for range 3 {
				clock.Advance(time.Second)
				if next := assignedJob(pull(false)); next != nil {
					t.Fatal("yielded work reclaimed preparing production room", next.ID)
				}
			}
			if got := job(t, q, low.ID); got.Attempt != 1 || got.Losses != 0 {
				t.Fatal("hold spent attempt or loss", got.Attempt, got.Losses)
			}
			// MY-50: an unrelated job can still use spare host capacity.
			other := submit(t, q, cpuSpec(1, 1))
			if next := assignedJob(pull(false)); next == nil || next.ID != other.ID {
				t.Fatal("whole host reserved", next)
			}
			if err := q.Complete(result(job(t, q, other.ID), true)); err != nil {
				t.Fatal(err)
			}
			switch boundary {
			case "command-start", "restart", "refused-again":
				if next := assignedJob(pull(true)); next == nil || next.ID != low.ID {
					t.Fatal("worker command start did not return spare room", next, job(t, q, low.ID).Reason)
				}
				if err := q.Complete(result(offer, true)); err != nil {
					t.Fatal(err)
				}
				requireState(t, q, demand.ID, "succeeded")
			case "other-host":
				otherHost := cpuMachine("elsewhere", "other-host", "fake", 8, 32)
				otherHost.Devices = []MachineDevice{}
				if next := assignedJob(machineCheck(t, q, otherHost)); next == nil || next.ID != low.ID {
					t.Fatal("claim withheld another host", next)
				}
			case "release":
				r := result(offer, false)
				r.ReleaseOnly = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				if next := assignedJob(machineCheck(t, q, m)); next == nil || next.ID != low.ID {
					t.Fatal("exact release did not end claim", next)
				}
			case "cancel":
				if err := q.Cancel(demand.ID); err != nil {
					t.Fatal(err)
				}
				if next := assignedJob(pull(false)); next == nil || next.ID != low.ID {
					t.Fatal("cancelled demand held spare room", next, job(t, q, low.ID).Reason)
				}
			}
		})
	}
}

func TestPreemptionClaimTransfersToFanOutExecution(t *testing.T) {
	q, _, m, s := fanOutFixture(t, 1)
	s.Priority = "production"
	parent := submit(t, q, s)
	// The planner has paid for this target. Materialization must transfer
	// that turn to the exact child, without retaining a parent-wide hold.
	if err := q.update(func() error {
		q.touchJob(parent.ID).PreemptionPlacement = &PreemptionPlacement{Machine: m.ID, Session: m.Session, Device: m.Device}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	child := assignedJob(machineCheck(t, q, m))
	if child == nil || child.Batch == nil || child.Batch.Parent != parent.ID {
		t.Fatal("missing child", child)
	}
	if job(t, q, parent.ID).PreemptionPlacement != nil || job(t, q, child.ID).PreemptionPlacement == nil {
		t.Fatal("preparation claim did not transfer to execution")
	}
}
