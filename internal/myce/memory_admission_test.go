package myce

import (
	"context"
	"encoding/json"
	"math"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func memoryAdmissionFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 21.3)
	m.HostMemoryBudgetGB = 24
	m.CapabilityProfile.SystemMemoryBytes = 31 << 30
	m.Devices = []MachineDevice{}
	machineCheck(t, q, m)
	return q, clock, m
}

func TestMemoryAdmissionPreservesCancelledOwnerThroughDeadline(t *testing.T) {
	q, clock, m := memoryAdmissionFixture(t)
	holder := submit(t, q, cpuSpec(1, 17))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != holder.ID {
		t.Fatal("missing holder")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 21.3)
	s.Priority = "production"
	s.StartWindow = "10m"
	// Non-resumable work remains resident. A reservation cannot grant its RAM.
	queued := submit(t, q, s)
	if _, err := q.CreateReservation(reservationRequest("validation", "host="+m.HostID, queued.ID)); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(run.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB = 4
	machineCheck(t, q, m)
	wait := q.s.Jobs[queued.ID].LastMemoryAdmission
	if wait == nil {
		t.Fatal("no blocked comparison retained")
	}
	l := wait.Ledger
	if l.ReservedGB != 17 || l.GrowthGB != 17 || l.SignedRoomGB != -13 || l.DebtGB != 13 || l.FreeGB != 0 || l.AllowanceGB != 24 || l.AvailableGB != 4 {
		t.Fatalf("ledger differs from admission: %+v", l)
	}
	if len(l.Owners) != 1 || l.Owners[0].Kind != "attempt" || l.Owners[0].Execution.ID != run.ID || l.Owners[0].Execution.Attempt != run.Attempt || l.Owners[0].State != "stopping" {
		t.Fatalf("missing exact cancelled owner or double-counted receipt: %+v", l.Owners)
	}
	if len(wait.Reservations) != 1 || wait.Reservations[0].State != "draining" || len(wait.Reservations[0].Pending) == 0 {
		t.Fatalf("missing reservation pending state: %+v", wait.Reservations)
	}
	clock.Advance(10 * time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	failed := q.s.Jobs[queued.ID]
	if failed.State != "failed" || failed.LastMemoryAdmission == nil {
		t.Fatal("deadline lost comparison")
	}
	retained, err := json.Marshal(failed.LastMemoryAdmission)
	if err != nil {
		t.Fatal(err)
	}
	r := result(run, false)
	r.ReleaseOnly, r.Interrupted = true, true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	m.HostAvailableMemoryGB = 21.3
	machineCheck(t, q, m)
	after, _ := json.Marshal(q.s.Jobs[queued.ID].LastMemoryAdmission)
	if string(after) != string(retained) {
		t.Fatal("release rewrote frozen failure evidence")
	}
	q = memoryFactsRestart(t, q, clock)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	got, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	public, _ := json.Marshal(got.LastMemoryAdmission)
	if string(public) != string(retained) {
		t.Fatal("restart or terminal observation lost admission evidence")
	}
}

func TestMemoryAdmissionCapturesGrowthWithoutNominalDoubleCharge(t *testing.T) {
	q, clock, m := memoryAdmissionFixture(t)
	first := submit(t, q, cpuSpec(1, 17))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != first.ID {
		t.Fatal("missing holder")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	// Fresh current nonreclaimable use grants only remaining-growth credit.
	report := AttemptReport{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: run.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 12, GrowthGB: 1}}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	queued := submit(t, q, cpuSpec(1, 10))
	a := q.s.Jobs[queued.ID].LastMemoryAdmission
	if a == nil || a.Ledger.ReservedGB != 17 || a.Ledger.GrowthGB != 6 || len(a.Ledger.Owners) != 1 || a.Ledger.Owners[0].GrowthGB != 6 || a.Ledger.FreeGB != 7 {
		t.Fatalf("not canonical remaining growth: %+v", a)
	}
	old := a.CapturedAt
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if !q.s.Jobs[queued.ID].LastMemoryAdmission.CapturedAt.Equal(old) {
		t.Fatal("unchanged comparison rewrote job")
	}
	clock.Advance(hostMemorySampleFreshFor)
	report.CurrentHostMemory.AgeSeconds = hostMemorySampleFreshFor.Seconds()
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	a = q.s.Jobs[queued.ID].LastMemoryAdmission
	if a.Ledger.GrowthGB != 17 || !a.CapturedAt.After(old) {
		t.Fatal("stale sample retained growth credit", a.Ledger)
	}
	// A new episode must have a distinct comparison, not an inherited pointer.
	q.mu.Lock()
	q.requeue(q.s.Jobs[queued.ID], "new episode", false)
	cleared := q.s.Jobs[queued.ID].LastMemoryAdmission == nil
	q.mu.Unlock()
	if !cleared {
		t.Fatal("new episode inherited old evidence")
	}
}

func TestMemoryAdmissionColdDriftAndOOMFloor(t *testing.T) {
	q, clock, m := memoryAdmissionFixture(t)
	// An auto requirement retained from an OOM must never shrink to live room.
	s := cpuSpec(1, 0)
	s.Needs.Memory = "auto"
	j := submit(t, q, s)
	if err := q.update(func() error {
		live := q.touchJob(j.ID)
		live.MemoryOOMFloor = &SizeHint{HostMemoryGB: 22}
		q.maintain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	a := q.s.Jobs[j.ID].LastMemoryAdmission
	if a == nil || a.Envelope.HostGB < 22 || math.Abs(a.DriftReserveGB-exploratoryDriftMarginGB(&m)) > 1e-10 {
		t.Fatalf("lost cold reserve or OOM floor: %+v", a)
	}
	if !a.CapturedAt.Equal(clock.Now()) || !a.WaitingSince.Equal(j.WaitingSince) || a.Session != m.Session || a.Target.OS != "linux" {
		t.Fatal("wrong observation identity", a)
	}
	if !a.Ledger.AvailableKnown || !a.Ledger.ObservedAt.Equal(q.s.Machines[m.ID].LastSeen) {
		t.Fatal("lost observation provenance", a.Ledger)
	}
	before := *a
	if _, err := q.jobSnapshotGeneration(j.ID); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, *q.s.Jobs[j.ID].LastMemoryAdmission) {
		t.Fatal("observation mutated retained evidence")
	}
	// Legacy/restored inventory may lack a usable observation. The current
	// enforced-worker protocol correctly refuses that malformed check-in.
	if err := q.update(func() error {
		q.s.Machines[m.ID].HostMemoryError = "capacity unavailable"
		q.maintain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if a := q.s.Jobs[j.ID].LastMemoryAdmission; a == nil || a.Ledger.AvailableKnown {
		t.Fatal("missing telemetry presented as known available RAM", a)
	}
}

func TestMemoryAdmissionNamesOrphanReleaseAndModelCustody(t *testing.T) {
	q, _, m := memoryAdmissionFixture(t)
	q.models = ModelConfig{Models: []Model{{Name: "resident", MemoryGB: 2, HostMemoryGB: 5}}}
	q.s.ModelSlots = map[string]*ModelSlot{"resident": {Model: "resident", Machine: m.ID, Session: m.Session, Attempt: "engine-1", State: "unloading"}}
	q.s.HostReleases = map[string]HostRelease{"orphan": {Host: m.HostID, Job: "previous", Attempt: 3, Machine: m.ID, Session: "old-session", HostChargeGB: 4}}
	ledger := q.hostMemoryLedger(&m)
	if ledger.ReservedGB != 9 || ledger.GrowthGB != 9 || len(ledger.Owners) != 2 {
		t.Fatal("lost retained custody", ledger)
	}
	model, release := ledger.Owners[0], ledger.Owners[1]
	if model.Kind != "model" || model.Model != "resident" || model.ModelAttempt != "engine-1" || model.State != "unloading" || model.ChargeGB != 5 {
		t.Fatal("lost exact model owner", model)
	}
	if release.Kind != "release" || release.Execution.ID != "previous" || release.Execution.Attempt != 3 || release.Session != "old-session" || release.ChargeGB != 4 {
		t.Fatal("lost orphan release owner", release)
	}
}
