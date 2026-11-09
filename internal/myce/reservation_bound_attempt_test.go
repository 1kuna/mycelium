package myce

import (
	"strconv"
	"testing"
	"time"
)

// #946: acquiring a reservation between native custody and Started must not
// withdraw its own assignment while another Started command keeps it draining.
func TestReservationAcquisitionPreservesOwnPendingStart(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		m := m3Machine()
		submit(t, q, cpuSpec(1, 1))
		neighbor := assignedJob(machineCheck(t, q, m))
		if neighbor == nil {
			t.Fatal("missing neighboring CPU command")
		}
		if err := q.Started(result(neighbor, false)); err != nil {
			t.Fatal(err)
		}
		bound := submit(t, q, exclusionSpec(m.ID, "1"))
		run := assignedJob(machineCheck(t, q, m, neighbor))
		if run == nil || run.ID != bound.ID {
			t.Fatal("missing bound assignment", run)
		}
		clock.Advance(2 * time.Second)
		ledgerCustody(t, q, run, "owned-custody", "reserved")
		ledgerCustody(t, q, run, "owned-custody", "started")
		if got := job(t, q, bound.ID); got.StartedAt != nil {
			t.Fatal("custody was mistaken for Started")
		}
		clock.Advance(time.Second)
		r, err := q.CreateReservation(reservationRequest("own-pending-start", "host="+m.HostID, bound.ID))
		if err != nil {
			t.Fatal(err)
		}
		if r.State != "draining" || !r.CreatedAt.Equal(clock.Now()) {
			t.Fatal("neighbor should keep the reservation draining", r)
		}
		got := job(t, q, bound.ID)
		if got.State != "running" || got.Attempt != run.Attempt || got.StopAt != nil || got.StartedAt != nil || got.Copies[0].StopCause != "" || got.Losses != 0 {
			t.Fatalf("reservation stopped its own attempt: state=%s attempt=%d stop=%v started=%v cause=%q losses=%d", got.State, got.Attempt, got.StopAt, got.StartedAt, got.Copies[0].StopCause, got.Losses)
		}
		launch := HostLaunch{Machine: run.Machine, Session: run.Session, Kind: "batch", Key: run.ID, Attempt: strconv.Itoa(run.Attempt)}
		if err := q.AuthorizeLaunch(launch); err != nil {
			t.Fatal("reservation revoked its own launch", err)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal("reservation revoked its own Started handoff", err)
		}
		clock.Advance(time.Second)
		for _, directive := range machineCheck(t, q, m, neighbor, run).Attempts {
			if (directive.ID == run.ID || directive.ID == neighbor.ID) && (directive.Stop || directive.Kill) {
				t.Fatal("reservation stopped existing work", directive)
			}
		}
		if got := job(t, q, bound.ID); got.State != "running" || got.Attempt != 1 || got.StartedAt == nil || got.Losses != 0 {
			t.Fatal("bound attempt churned instead of starting", got)
		}

	})

	t.Run("ReservationBoundJobWithConcurrentCPUPreparation", func(t *testing.T) {
		q, clock := fixture(t)
		m := m3Machine()
		m.EnvironmentBuilder = true
		recipe := binaryRecipe(t, "cpu")
		if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"cpu-tools": {"cpu": recipe}}}); err != nil {
			t.Fatal(err)
		}
		machineCheck(t, q, m)
		cpu := cpuSpec(1, 1)
		cpu.Environment.Name = "cpu-tools"
		neighbor := submit(t, q, cpu)
		bound := submit(t, q, exclusionSpec(m.ID, "0"))
		if _, err := q.CreateReservation(reservationRequest("check", "host=machine;devices=0", bound.ID)); err != nil {
			t.Fatal(err)
		}
		// Assignment precedes preparation offers in this very same check-in. A CPU
		// preparation must not turn the GPU check's ready reservation into a drain.
		reply := machineCheck(t, q, m)
		run := assignedJob(reply)
		if run == nil || run.ID != bound.ID || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch.Demand != neighbor.ID {
			t.Fatal("missing check and concurrent CPU preparation", reply)
		}
		reservationState(t, q, "check", "ready")
		launch := HostLaunch{Machine: run.Machine, Session: run.Session, Kind: "batch", Key: run.ID, Attempt: strconv.Itoa(run.Attempt)}
		if err := q.AuthorizeLaunch(launch); err != nil {
			t.Fatal("ready check refused after CPU preparation offer", err)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(6 * time.Second)
		for _, directive := range machineCheck(t, q, m, run).Attempts {
			if directive.ID == run.ID && directive.Stop {
				t.Fatal("check self-stopped", directive)
			}
		}
		if got := job(t, q, run.ID); got.Attempt != 1 || got.InfrastructureRetries != 0 {
			t.Fatal("check retried instead of running once", got)
		}

	})

	t.Run("ReservationOwnPreparationDoesNotSelfStop", func(t *testing.T) {
		q, clock := fixture(t)
		m := m3Machine()
		m.EnvironmentBuilder = true
		recipe := binaryRecipe(t, "fake")
		if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"validation": {"fake": recipe}}}); err != nil {
			t.Fatal(err)
		}
		machineCheck(t, q, m)
		s := exclusionSpec(m.ID, "0")
		s.Environment.Name = "validation"
		bound := submit(t, q, s)
		if _, err := q.CreateReservation(reservationRequest("check", "host=machine;devices=0", bound.ID)); err != nil {
			t.Fatal(err)
		}
		reply := machineCheck(t, q, m)
		if assignedJob(reply) != nil || len(reply.EnvironmentPlan) != 1 {
			t.Fatal("expected preparation before assignment", reply)
		}
		launch := *reply.EnvironmentPlan[0].Launch
		if launch.Demand != bound.ID || len(q.s.PreparationPlans) != 1 {
			t.Fatal("own preparation lost its owner", launch)
		}
		reservationState(t, q, "check", "ready")
		if err := q.AuthorizeLaunch(launch); err != nil {
			t.Fatal("own preparation refused", err)
		}
		clock.Advance(6 * time.Second)
		m.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
		machineCheck(t, q, m)
		if got := job(t, q, bound.ID); got.State != "queued" || got.Attempt != 0 {
			t.Fatal("job churned while preparing", got)
		}
		m.EnvironmentReports[0].State = "ready"
		m.Environments = []string{launch.Key}
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.ID != bound.ID || run.Attempt != 1 {
			t.Fatal("prepared bound job not assigned once", run)
		}
		batch := HostLaunch{Machine: run.Machine, Session: run.Session, Kind: "batch", Key: run.ID, Attempt: "1"}
		if err := q.AuthorizeLaunch(batch); err != nil {
			t.Fatal(err)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		reservationState(t, q, "check", "in_use")

	})
}
