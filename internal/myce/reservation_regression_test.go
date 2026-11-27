package myce

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReservationListFitsWithRetainedHistory(t *testing.T) {
	q, clock := fixture(t)
	now := clock.Now()
	m := m3Machine()
	machineCheck(t, q, m)
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	if _, err := q.CreateReservation(reservationRequest("current", "host=machine;devices=0", bound.ID)); err != nil {
		t.Fatal(err)
	}
	// Let the worker's evidence age on the injected clock so the active list
	// must still project its current draining state.
	clock.Advance(time.Hour)
	const retained = 4096
	if err := q.update(func() error {
		for i := range retained {
			id := fmt.Sprintf("released-%04d", i)
			q.s.Reservations[id] = Reservation{
				ReservationOptions: reservationRequest(id, "host=machine;devices=0", "").ReservationOptions,
				CreatedAt:          now, ExpiresAt: now.Add(2 * time.Hour), ReleasedAt: &now, Revision: 2,
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	budgetHost(q, 256<<10, 0)
	handler := (API{Queue: q, Token: "test"}).Handler()
	read := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}
	if w := read("/api/reservations"); w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "projection_capacity_exceeded") {
		t.Fatalf("unfiltered history should exceed this request budget, got HTTP %d (%d bytes)", w.Code, w.Body.Len())
	}
	for _, test := range []struct{ query, id, state string }{
		{"view=unreleased", "current", "draining"},
		{"id=released-4095", "released-4095", "released"},
	} {
		w := read("/api/reservations?" + test.query)
		var records []Reservation
		if w.Code != http.StatusOK {
			t.Fatalf("%s: HTTP %d: %s", test.query, w.Code, w.Body)
		}
		if err := json.Unmarshal(w.Body.Bytes(), &records); err != nil || len(records) != 1 || records[0].ID != test.id || records[0].State != test.state {
			t.Fatalf("%s: list = %+v, decode error = %v", test.query, records, err)
		}
	}
}

func TestReservationBoundJobWithConcurrentCPUPreparation(t *testing.T) {
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
}

func TestReservationOwnPreparationDoesNotSelfStop(t *testing.T) {
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
}

func TestDeviceReservationDrainingAdmitsCPU(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	demand := submit(t, q, exclusionSpec(m.ID, "0"))
	existing := assignedJob(machineCheck(t, q, m))
	if existing == nil {
		t.Fatal("missing existing GPU work", job(t, q, demand.ID).Reason)
	}
	if err := q.Started(result(existing, false)); err != nil {
		t.Fatal(err)
	}
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	if _, err := q.CreateReservation(reservationRequest("check", "host=machine;devices=0", bound.ID)); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, "check", "draining")
	// The CPU command has a known estimate and needs no reserved device.
	s := cpuSpec(2, 1)
	cpu := submit(t, q, s)
	clock.Advance(time.Second)
	run := assignedJob(machineCheck(t, q, m, existing))
	if run == nil || run.ID != cpu.ID || len(run.AllocatedDevices) != 0 {
		t.Fatal("device drain withheld fitting CPU work", run, job(t, q, cpu.ID).Reason)
	}
	if err := q.AuthorizeLaunch(HostLaunch{Machine: run.Machine, Session: run.Session, Kind: "batch", Key: run.ID, Attempt: "1"}); err != nil {
		t.Fatal("CPU launch refused by device drain", err)
	}
	reservationState(t, q, "check", "draining")
}
