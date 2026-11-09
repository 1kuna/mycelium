package myce

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestHostMemoryProductionReclaimsOtherCard(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		a, b := machine("card0", "fake"), machine("card1", "fake")
		for _, m := range []*Machine{&a, &b} {
			m.HostID = "box"
			m.HostMemoryMeasured = true
			m.HostAvailableMemoryGB = 12
		}
		check(t, q, a, nil)
		check(t, q, b, nil)
		s := spec("experiment")
		s.Needs.HostMemoryGB = 10
		s.AllowedMachines = []string{a.ID}
		submit(t, q, s)
		running := check(t, q, a, nil).Job
		s.Priority = "production"
		s.AllowedMachines = []string{b.ID}
		prod := submit(t, q, s)
		requireState(t, q, running.ID, "stopping")
		if r := check(t, q, b, nil); r.Job != nil {
			t.Fatal("RAM released before stop acknowledgement")
		}
		if r := check(t, q, a, running); !r.Stop {
			t.Fatal("no stop")
		}
		if err := q.Complete(Result{Machine: a.ID, Session: a.Session, Execution: Execution{ID: running.ID, Attempt: running.Attempt}, Interrupted: true}); err != nil {
			t.Fatal(err)
		}
		if r := check(t, q, b, nil); r.Job == nil || r.Job.ID != prod.ID {
			t.Fatal("production not admitted")
		}

	})

	t.Run("M3CrossDeviceRAMVictimAndExactAck", func(t *testing.T) {
		q, _ := fixture(t)
		m := m3Machine()
		m.HostMemoryBudgetGB = 10
		submit(t, q, m3Spec("experiment", "1", 2, 8))
		victim := assignedJob(machineCheck(t, q, m))
		if victim == nil {
			t.Fatal("missing victim")
		}
		demand := submit(t, q, m3Spec("production", "0", 2, 5))
		reply := machineCheck(t, q, m, victim)
		if job(t, q, victim.ID).State != "stopping" || assignedJob(reply) != nil {
			t.Fatal("RAM release must stop neighbor and wait for acknowledgement", reply)
		}
		machineCheck(t, q, m) // Omission is not an acknowledgement.
		requireState(t, q, demand.ID, "queued")
		stale := result(victim, false)
		stale.Session = "stale"
		stale.ReleaseOnly = true
		if err := q.Complete(stale); err != nil {
			t.Fatal(err)
		}
		if got := job(t, q, victim.ID); got.ReleaseConfirmed || got.State != "stopping" {
			t.Fatal("stale acknowledgement discharged custody", got)
		}
		finishInterrupted(t, q, victim)
		a := assignedJob(machineCheck(t, q, m))
		if a == nil || a.ID != demand.ID || !slices.Equal(a.AllocatedDevices, []string{"0"}) {
			t.Fatal("exact ack did not free shared RAM", a)
		}
		if got := job(t, q, victim.ID); got.Losses != 0 || got.InfrastructureRetries != 0 {
			t.Fatal("planned stop charged retry", got)
		}

	})

	t.Run("HostMemoryModelReservationsAndPins", func(t *testing.T) {
		for _, pin := range []bool{false, true} {
			t.Run(fmt.Sprint(pin), func(t *testing.T) {
				q, _ := fixture(t)
				model := Model{Name: "speech", Engine: "fake", Path: "weights", Platforms: []string{"fake"}, MemoryGB: 2, HostMemoryGB: 10}
				if err := q.ConfigureModels(ModelConfig{Models: []Model{model}, Idle: time.Hour, Wait: time.Minute}); err != nil {
					t.Fatal(err)
				}
				a, b := machine("a", "fake"), machine("b", "fake")
				for _, m := range []*Machine{&a, &b} {
					m.HostID = "host"
					m.HostMemoryMeasured = true
					m.HostAvailableMemoryGB = 12
					m.ModelURL = "http://worker"
					m.ModelConfigHash = modelConfigHash(q.models)
				}
				check(t, q, a, nil)
				check(t, q, b, nil)
				if pin {
					if err := q.Pin(ModelPin{Model: "speech", Project: "test", Machine: a.ID}); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := q.addCall(&ModelCall{Model: "speech", Project: "test", Priority: "experiment"}); err != nil {
						t.Fatal(err)
					}
				}
				s := spec("experiment")
				s.Needs.HostMemoryGB = 4
				s.AllowedMachines = []string{b.ID}
				submit(t, q, s)
				if r := check(t, q, b, nil); r.Job != nil {
					t.Fatal("batch ignored server RAM")
				}
			})
		}

	})

	t.Run("HostMemoryModelProductionReclaim", func(t *testing.T) {
		q, _ := fixture(t)
		model := Model{Name: "speech", Engine: "fake", Path: "weights", Platforms: []string{"fake"}, MemoryGB: 2, HostMemoryGB: 10}
		if err := q.ConfigureModels(ModelConfig{Models: []Model{model}, Idle: time.Hour, Wait: time.Minute}); err != nil {
			t.Fatal(err)
		}
		a, b := machine("a", "fake"), machine("b", "fake")
		for _, m := range []*Machine{&a, &b} {
			m.HostID = "host"
			m.HostMemoryMeasured = true
			m.HostAvailableMemoryGB = 12
			m.ModelURL = "http://worker"
			m.ModelConfigHash = modelConfigHash(q.models)
		}
		check(t, q, a, nil)
		check(t, q, b, nil)
		s := spec("experiment")
		s.Needs.HostMemoryGB = 10
		s.AllowedMachines = []string{"a"}
		submit(t, q, s)
		running := check(t, q, a, nil).Job
		if err := q.addCall(&ModelCall{Model: "speech", Project: "test", Priority: "production", Exclude: "a"}); err != nil {
			t.Fatal(err)
		}
		requireState(t, q, running.ID, "stopping")
		snap, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range snap.Jobs {
			if j.Spec.Name == "model call: speech" && !strings.Contains(j.Reason, "RAM") {
				t.Fatalf("model RAM wait hidden: %s", j.Reason)
			}
		}

	})

	t.Run("HostMemoryProductionUsesFreeHostBeforeReclaim", func(t *testing.T) {
		q, _ := fixture(t)
		a, b := machine("a", "fake"), machine("b", "fake")
		for _, m := range []*Machine{&a, &b} {
			m.HostID = m.ID
			m.HostMemoryMeasured = true
			m.HostAvailableMemoryGB = 12
		}
		check(t, q, a, nil)
		check(t, q, b, nil)
		s := spec("experiment")
		s.Needs.HostMemoryGB = 10
		s.AllowedMachines = []string{"a"}
		submit(t, q, s)
		running := check(t, q, a, nil).Job
		s.Priority = "production"
		s.AllowedMachines = nil
		submit(t, q, s)
		requireState(t, q, running.ID, "running")
		if check(t, q, b, nil).Job == nil {
			t.Fatal("free physical host not used")
		}

	})
}
func TestHostMemoryUnifiedPool(t *testing.T) {
	q, _ := fixture(t)
	m := machine("mac", "fake")
	m.FreeMemoryGB = 32
	m.HostID = "mac"
	m.UnifiedMemory = true
	m.CapabilityProfile.MemoryLayout = "unified"
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 12
	check(t, q, m, nil)
	s := spec("experiment")
	s.Needs.MemoryGB = 8
	s.Needs.HostMemoryGB = 5
	j := submit(t, q, s)
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("GPU and CPU allocations exceed common pool")
	}
	if !strings.Contains(job(t, q, j.ID).Reason, "RAM") {
		t.Fatal("missing RAM reason")
	}
}

func TestHostMemoryRestartReconciliation(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "host"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	s.AllowedMachines = []string{"a"}
	submit(t, q, s)
	running := check(t, q, a, nil).Job
	a.Session = "new-session"
	r, err := q.Checkin(Checkin{Machine: a, Fresh: true, PreviousSession: running.Session, period: time.Second})
	if err != nil || r.Job == nil {
		t.Fatalf("%+v %v", r, err)
	}
	if r.Job.Attempt <= running.Attempt {
		t.Fatal("stale attempt reused")
	}
	s.AllowedMachines = []string{"b"}
	submit(t, q, s)
	if check(t, q, b, nil).Job != nil {
		t.Fatal("restart duplicated allowance")
	}
	finish(t, q, r.Job, true)
	if check(t, q, b, nil).Job == nil {
		t.Fatal("reconciled reservation retained twice")
	}
}

func TestHostMemoryLossRetainsReservationUntilExactRelease(t *testing.T) {
	q, c := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	other := machine("other", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "host"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	check(t, q, other, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	s.AllowedMachines = []string{"a"}
	submit(t, q, s)
	running := check(t, q, a, nil).Job
	s.AllowedMachines = []string{"b"}
	second := submit(t, q, s)
	var r Reply
	for range 31 {
		c.Advance(2 * time.Second)
		check(t, q, other, nil)
		r = check(t, q, b, nil)
	}
	if r.Job != nil {
		t.Fatal("loss freed RAM without exact release", r.Job)
	}
	requireState(t, q, running.ID, "queued")
	receipt := result(running, false)
	receipt.ReleaseOnly = true
	if err := q.Complete(receipt); err != nil {
		t.Fatal(err)
	}
	if r = check(t, q, b, nil); r.Job == nil || r.Job.ID != second.ID {
		t.Fatal("exact release did not refund RAM")
	}
}

// Running includes accepted preparation before Started; Docker's declared host
// total already includes its simultaneous main/sidecar ceilings. Both retain
// their whole charge until the exact execution releases.
func TestHostMemoryAllowanceAggregatePreparationsAndSidecars(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, docker := range []bool{false, true} {
			t.Run(fmt.Sprint(docker), func(t *testing.T) {
				q, clock := fixture(t)
				a, b := machine("card-a", "fake"), machine("card-b", "fake")
				for _, m := range []*Machine{&a, &b} {
					if docker {
						m.Capabilities = append(m.Capabilities, "docker")
						m.CapabilityProfile.OS = "linux"
					}
					m.HostID = "host"
					m.HostMemoryMeasured = true
					m.HostAvailableMemoryGB = 64
					m.HostMemoryBudgetGB = 6
					m.MemoryCeilingSlackGB = .25
				}
				check(t, q, a, nil)
				check(t, q, b, nil)
				first := spec("experiment")
				if docker {
					first = dockerGroupJob(t).Spec
				}
				first.Needs.HostMemoryGB = 6
				first.AllowedMachines = []string{a.ID}
				submitted := submit(t, q, first)
				preparing := check(t, q, a, nil).Job
				if preparing != nil {
					t.Fatal("slack exceeded host allowance")
				}
				maintenanceChange(t, q, "drain", false)
				value := 7.0
				if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &value}); err != nil {
					t.Fatal(err)
				}
				for _, m := range []*Machine{&a, &b} {
					m.HostMemoryBudgetGB = 7
				}
				check(t, q, a, nil)
				check(t, q, b, nil)
				maintenanceChange(t, q, "resume", false)
				check(t, q, b, nil)
				preparing = check(t, q, a, nil).Job
				if preparing == nil || preparing.ID != submitted.ID || preparing.StartedAt != nil {
					t.Fatal("missing accepted preparation", preparing)
				}
				second := spec("experiment")
				second.Needs.HostMemoryGB = 1
				second.AllowedMachines = []string{b.ID}
				submit(t, q, second)
				if check(t, q, b, nil).Job != nil {
					t.Fatal("preparation/sidecars double-spent sibling allowance")
				}
				snap, err := q.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range snap.Machines {
					if m.HostMemoryInUseGB != 6.25 {
						t.Fatal("missing whole preparation/sidecar charge", m.HostMemoryInUseGB)
					}
				}
				cancelAck(t, q, preparing.ID)
				receipt := result(preparing, false)
				receipt.ReleasePending = true
				receipt.Interrupted = true
				if err := q.Complete(receipt); err != nil {
					t.Fatal(err)
				}
				for range 3 {
					clock.Advance(24 * time.Hour)
					check(t, q, b, nil)
				}
				snap, err = q.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range snap.Machines {
					if m.HostMemoryInUseGB != 6.25 {
						t.Fatal("unconfirmed release refunded", m.HostMemoryInUseGB)
					}
				}
				receipt.ReleasePending = false
				receipt.ReleaseOnly = true
				if err := q.Complete(receipt); err != nil {
					t.Fatal(err)
				}
				if check(t, q, b, nil).Job == nil {
					t.Fatal("exact release did not refund allowance")
				}
			})
		}

	})

	t.Run("M3UnifiedMemoryDebtAndDeterminism", func(t *testing.T) {
		q, _ := fixture(t)
		// Scheduling fakes must remain usable even when the real I/O budget expires.
		q.nasProbe.timeout = -1
		m := m3Machine()
		m.HostMemoryBudgetGB = 12
		m.Devices[1].UnifiedMemory = true
		j := submit(t, q, m3Spec("experiment", "1", 6, 4))
		reply := machineCheck(t, q, m)
		a := assignedJob(reply)
		if a == nil {
			q.mu.Lock()
			reason, available := q.s.Jobs[j.ID].Reason, q.nasAvailable
			q.mu.Unlock()
			t.Fatalf("first assignment: reply=%+v reason=%q nas_available=%t", reply, reason, available)
		}
		submit(t, q, m3Spec("experiment", "0", 1, 4))
		if b := assignedJob(machineCheck(t, q, m, a)); b != nil {
			t.Fatal("unified allocation charged only dedicated RAM", b)
		}
		snap, _ := q.Snapshot()
		if len(snap.Machines) != 1 || snap.Machines[0].HostMemoryInUseGB != 10 || len(snap.Machines[0].Devices[1].Attempts) != 1 {
			t.Fatal("component/host status", snap.Machines)
		}
		q.mu.Lock()
		demand := &Job{Spec: m3Spec("production", "0", 1, 8)}
		for range 3 {
			target := q.machineTargets(q.s.Machines[m.ID])[1]
			p := q.roomPlan(demand, target, true)
			if p == nil || len(p.jobs) != 1 || p.jobs[0].ID != a.ID {
				q.mu.Unlock()
				t.Fatal("heuristic did not select useful cross-device unified victim", p)
			}
		}
		q.mu.Unlock()

	})

	t.Run("HostMemorySharedWorkers", func(t *testing.T) {
		q, c := fixture(t)
		a, b := machine("card0", "fake"), machine("card1", "fake")
		for _, m := range []*Machine{&a, &b} {
			m.HostID = "box"
			m.HostMemoryMeasured = true
			m.HostAvailableMemoryGB = 12
		}
		check(t, q, a, nil)
		check(t, q, b, nil)
		s := spec("experiment")
		s.Needs.HostMemoryGB = 10
		first := submit(t, q, s)
		c.Advance(time.Second)
		second := submit(t, q, s)
		r := check(t, q, a, nil)
		if r.Job == nil || r.Job.ID != first.ID {
			t.Fatal("first not assigned")
		}
		c.Advance(time.Second)
		if r := check(t, q, b, nil); r.Job != nil {
			t.Fatal("second GPU spent same host RAM")
		}
		if !strings.Contains(job(t, q, second.ID).Reason, "RAM") {
			t.Fatal("missing RAM wait reason")
		}
		finish(t, q, r.Job, true)
		if r := check(t, q, b, nil); r.Job == nil {
			t.Fatal("completed reservation not released")
		}

	})

	t.Run("HostMemoryUnifiedDoesNotChargeGPUtwice", func(t *testing.T) {
		q, _ := fixture(t)
		m := machine("mac", "fake")
		m.FreeMemoryGB = 12
		m.HostID = "mac"
		m.UnifiedMemory = true
		m.CapabilityProfile.MemoryLayout = "unified"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
		check(t, q, m, nil)
		s := spec("experiment")
		s.Needs.MemoryGB = 8
		s.Needs.HostMemoryGB = 2
		submit(t, q, s)
		if r := check(t, q, m, nil); r.Job == nil {
			t.Fatal("8 GPU + 2 CPU should fit 12 GiB unified pool")
		}

	})
}

// A controlled heartbeat changes live headroom without allocating host RAM.
func TestHostMemoryPressureStatusAdmissionAgreement(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		m := machine("pressure-fixture", "fake")
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 12
		check(t, q, m, nil)
		s := spec("experiment")
		s.Needs.HostMemoryGB = 4
		j := submit(t, q, s)
		m.HostAvailableMemoryGB = 3
		if r := check(t, q, m, nil); r.Job != nil {
			t.Fatal("dispatch ignored live pressure")
		}
		snapshot, err := q.snapshot(true)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Machines) != 1 {
			t.Fatal(snapshot.Machines)
		}
		observed := snapshot.Machines[0]
		if observed.HostAvailableMemoryGB != 3 || observed.HostMemoryInUseGB != 0 || observed.FreeHostMemoryGB != 3 {
			t.Fatalf("live/reserved projection: %+v", observed)
		}
		var held *Job
		for _, candidate := range snapshot.Jobs {
			if candidate.ID == j.ID {
				held = candidate
			}
		}
		if held == nil || held.State != "queued" || held.DiagnosticCode != "host_memory_headroom" || !strings.Contains(held.Reason, "live available 3.0 GiB; reserved 0.0 GiB") {
			t.Fatalf("typed pressure status: %+v", held)
		}
		w := Worker{Machine: m, HostMemorySource: func(context.Context) (float64, error) { return 3, nil }}
		if err := w.checkHostRAM(s.Needs.MemoryGB, s.Needs.HostMemoryGB); err == nil {
			t.Fatal("launch ignored pressure")
		}
		m.HostAvailableMemoryGB = 12
		if r := check(t, q, m, nil); r.Job == nil || r.Job.ID != j.ID {
			t.Fatal("recovered headroom did not admit queued job")
		}
		w.HostMemorySource = func(context.Context) (float64, error) { return 12, nil }
		if err := w.checkHostRAM(s.Needs.MemoryGB, s.Needs.HostMemoryGB); err != nil {
			t.Fatal(err)
		}

	})

	t.Run("HostMemoryWorkerRefusesLatePressure", func(t *testing.T) {
		w := Worker{HostMemorySource: func(context.Context) (float64, error) { return 3, nil }, Machine: Machine{}}
		j := Job{Spec: spec("experiment")}
		j.Spec.Needs.HostMemoryGB = 10
		_, err := w.start(nil, j)
		if err == nil || !strings.Contains(err.Error(), "RAM") {
			t.Fatalf("%v", err)
		}
		if e := startFailureOutcome(Result{}, err).ExecutionEvidence; e != nil {
			t.Fatalf("refusal without attempt custody invented execution proof: %+v", e)
		}

	})
}
