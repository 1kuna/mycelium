package myce

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// Production on a unified-memory CUDA host waited behind a running,
// non-resumable deadline command that held most of the host RAM allowance.
func TestUnifiedProductionPreemptsDeadlineHostMemory(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.MemoryObservation.UsedGB = 6
	check(t, q, m, nil)
	deadline := spec("deadline")
	deadline.Project, deadline.Resumable = "editor", false
	deadline.Needs.MemoryGB, deadline.Needs.HostMemoryGB, deadline.Needs.CPUCores = 64, 8, 2
	deadline.AllowedMachines = []string{m.ID}
	victim := submit(t, q, deadline)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != victim.ID {
		t.Fatal("missing deadline holder", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	// The holder's live use (~41 GiB) already left the measured headroom while
	// its unknown unified-device growth still reserves the full envelope.
	m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 49.3, 46.6
	check(t, q, m, run)
	prod := spec("production")
	prod.Project, prod.Resumable = "naming", false
	prod.Needs.MemoryGB, prod.Needs.HostMemoryGB, prod.Needs.CPUCores = 27.9, 25.5, 4
	prod.AllowedMachines = []string{m.ID}
	demand := submit(t, q, prod)
	clock.Advance(time.Second)
	reply := check(t, q, m, run)
	if !reply.Stop {
		t.Fatalf("production did not preempt the deadline holder: reason=%q", job(t, q, demand.ID).Reason)
	}
	if reply.Job != nil {
		t.Fatal("production admitted before the holder released", reply.Job)
	}
	// The holder exits; its unified allocation returns to live headroom.
	if err := q.Complete(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("release receipt bypassed the live RAM check", got.ID)
	}
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 90, 6
	if got := check(t, q, m, nil).Job; got == nil || got.ID != demand.ID {
		t.Fatalf("production not admitted after release: %v reason=%q", got, job(t, q, demand.ID).Reason)
	}
}

// A unified device's unattributed footprint can depress live RAM more than
// all the ledger charges account for. Production can still reclaim the pool.
func TestUnifiedProductionReclaimsPooledMemory(t *testing.T) {
	q, clock := fixture(t)
	policy := defaultPriorityPolicy()
	policy.Tiers[2].Selectors = []string{"*/experiment"}
	policy.Tiers = append(policy.Tiers, PriorityTier{"background", []string{"*/*"}})
	if _, err := q.SetPriorityPolicy(0, policy); err != nil {
		t.Fatal(err)
	}
	m := sharedUnifiedMachine()
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
	m.CapabilityProfile.LogicalCPUs = 20
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 113.609573, 113.609573
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 128, ReserveGB: 4}
	const need = 102.615402
	production := sharedSpec("production", 0)
	production.Name, production.Project, production.Resumable = "production", "customer", false
	production.Needs.Memory, production.Needs.HostMemoryGB, production.Needs.CPUCores = "auto", 0, 4
	production.StartWindow = "10m"

	holders := []struct {
		class                    string
		charge, footprint, freed float64
		resumable                bool
	}{
		{"experiment", 44.125, 6.692459, 15.9, true}, // Device slot and unmeasured unified use.
		{"experiment", 5.625, 4.625809, 4.625809, true},
		{"experiment", 4, 2.046009, 2.046009, false},
		{"test", 8, 0.178535, 0.178535, false},
		{"test", 4.346595, 0.210388, 0.210388, true},
	}
	active := make([]*Job, len(holders))
	pull := func() Reply {
		t.Helper()
		c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
		for i, j := range active {
			if j == nil {
				continue
			}
			gb := holders[i].footprint
			c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true,
				CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: gb, FootprintGB: &gb}})
		}
		r, err := q.Checkin(c)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	for i, holder := range holders {
		s := cpuSpec(2, holder.charge)
		if i == 0 {
			s = sharedSpec("experiment", 0)
			s.Needs.HostMemoryGB, s.Needs.CPUCores = holder.charge, 6
		}
		s.Name, s.Class, s.Resumable = fmt.Sprintf("holder-%d", i), holder.class, holder.resumable
		if s.Class == "test" {
			s.Priority = ""
		}
		s.Needs.Memory = "auto"
		queued := submit(t, q, s)
		run := assignedJob(pull())
		if run == nil || run.ID != queued.ID || math.Abs(run.Memory.RAM.ChargeGB-holder.charge) > .001 {
			t.Fatalf("holder %d not admitted with its charge: %v, %s", i, run, job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(run, true)); err != nil {
			t.Fatal(err)
		}
		active[i] = run
	}
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 85.866444, 128-4-85.866444
	pull()
	ledger := q.hostMemoryLedgers[m.ID]
	if math.Abs(ledger.ReservedGB-66.096595) > .001 || math.Abs(ledger.GrowthGB-52.343395) > .001 || math.Abs(ledger.SignedRoomGB-33.523049) > .001 {
		t.Fatalf("unexpected replay ledger: %+v", ledger)
	}
	seedPoolReplay(q, &Job{Spec: production}, allocationMachine(&m, []string{"0"}), "ram", need/2, need)
	demand := submit(t, q, production)
	for _, victim := range []int{0, 4, 3, 1} {
		clock.Advance(time.Second)
		if got := assignedJob(pull()); got != nil {
			t.Fatal("production was offered before release", got.ID)
		}
		for i, j := range active {
			if j == nil {
				continue
			}
			stopped := job(t, q, j.ID).State == "stopping"
			if stopped != (i == victim) {
				t.Fatalf("holder %d stopping=%t, want holder %d; demand: %s", i, stopped, victim, job(t, q, demand.ID).Reason)
			}
		}
		victimJob := active[victim]
		if stop := priorityStop(job(t, q, victimJob.ID)); stop == nil || stop.Demand != demand.ID {
			t.Fatal("missing production preemption provenance", stop)
		}
		r := result(victimJob, false)
		r.Interrupted = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if j := job(t, q, victimJob.ID); j.State != "queued" || j.Losses != 0 || j.InfrastructureRetries != 0 {
			t.Fatalf("holder %d did not requeue cleanly: %s (%s)", victim, j.State, j.Reason)
		}
		active[victim] = nil
		m.HostAvailableMemoryGB += holders[victim].freed
		m.MemoryObservation.UsedGB -= holders[victim].freed
	}
	clock.Advance(time.Second)
	run := assignedJob(pull())
	if run == nil || run.ID != demand.ID || run.Attempt != 1 || math.Abs(run.Memory.RAM.ChargeGB-need) > .001 {
		t.Fatalf("production did not start at its original charge: %v, %s", run, job(t, q, demand.ID).Reason)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, active[2].ID, "running")
}
