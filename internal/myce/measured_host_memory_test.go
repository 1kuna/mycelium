package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

func measuredMemoryMachine() Machine {
	m := machine("measured-worker", "cpu", "host")
	m.HostMemoryBudgetGB, m.HostMemoryBudgetMeasured = 20, true
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 26
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 32, UsedGB: 4, MyceliumGB: 1, ReserveGB: 2, BaselineVersion: 1}
	return m
}

func TestMeasuredHostMemoryLearnsPhysicalIdleOnly(t *testing.T) {
	q, clock := fixture(t)
	m := measuredMemoryMachine()
	policy := HostMemoryBudget{GiB: 20, Measured: true}
	// A busy worker's residual is not an ownership proof, even with huge peaks.
	m.MemoryObservation.UsedGB = 30
	q.recordExternalMemory(&m)
	budget, d := q.measuredHostMemoryBudget(&m, policy)
	if budget.GiB != 20 || d.Source != "authored_fallback" {
		t.Fatal(budget, d)
	}
	m.MemoryObservation.Quiescent, m.MemoryObservation.UsedGB = true, 4
	q.recordExternalMemory(&m)
	clock.Advance(time.Second)
	// Duplicate RSS must not create additional allowance; all idle physical use
	// (including the worker) remains in the baseline.
	m.MemoryObservation.MyceliumGB = 4
	q.recordExternalMemory(&m)
	budget, d = q.measuredHostMemoryBudget(&m, policy)
	if budget.GiB != 26 || d.BaselineMaxGB != 4 || d.History.Windows != 1 || d.History.Samples != 2 {
		t.Fatal(budget, d)
	}
	m.MemoryObservation.Quiescent = false
	q.recordExternalMemory(&m)
	clock.Advance(time.Second)
	m.MemoryObservation.Quiescent, m.MemoryObservation.UsedGB = true, 8
	q.recordExternalMemory(&m)
	budget, d = q.measuredHostMemoryBudget(&m, policy)
	// Default node role: 32 - p50 4 - max(reserve 2, burst 4) = 32 - max 8.
	if budget.GiB != 24 || d.ReserveGB != 4 || d.History.Windows != 2 {
		t.Fatal(budget, d)
	}
	// Expired observations return to the authored bound, never to uncapped.
	clock.Advance(8 * 24 * time.Hour)
	budget, d = q.measuredHostMemoryBudget(&m, policy)
	if budget.GiB != 20 || d.Source != "authored_fallback" {
		t.Fatal(budget, d)
	}
}

func TestMeasuredHostMemoryRequiresWholeHostCustody(t *testing.T) {
	q, _ := fixture(t)
	m := measuredMemoryMachine()
	m.MemoryObservation.Quiescent = true
	if !q.hostMemoryQuiescent(&m) {
		t.Fatal("idle fixture not qualified")
	}
	r := HostRelease{Host: m.HostID, Job: "old-attempt", Machine: m.ID, Session: "old-session", Attempt: 1, HostChargeGB: 1}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	if q.hostMemoryQuiescent(&m) {
		t.Fatal("pending release treated as idle")
	}
	delete(q.s.HostReleases, releaseKey(r))
	peer := m
	peer.ID = "legacy-sibling"
	q.s.Machines[peer.ID] = &peer
	if q.hostMemoryQuiescent(&m) {
		t.Fatal("unreconciled sibling treated as idle")
	}
	peer.Gone = true
	if !q.hostMemoryQuiescent(&m) {
		t.Fatal("gone sibling blocked idle proof")
	}
	m.MemoryObservation.BaselineVersion = 0
	if q.hostMemoryQuiescent(&m) {
		t.Fatal("legacy attribution used for baseline")
	}
}

func TestMeasuredHostMemoryZeroIsExhaustedNotUncapped(t *testing.T) {
	q, _ := fixture(t)
	m := measuredMemoryMachine()
	m.MemoryObservation.Quiescent, m.MemoryObservation.UsedGB = true, 31
	q.recordExternalMemory(&m)
	budget, d := q.measuredHostMemoryBudget(&m, HostMemoryBudget{GiB: 20, Measured: true})
	if budget.GiB != 0 || d.Source != "quiescent_v1" {
		t.Fatal(budget, d)
	}
	m.HostMemoryBudgetGB = budget.GiB
	if err := (Worker{Machine: m}).checkHostMemoryBudget(0, 1); !errors.Is(err, errWorkerMemory) {
		t.Fatal("worker bypassed zero derived allowance", err)
	}
	q.s.Machines[m.ID] = &m
	q.s.HostMaintenance = map[string]HostMaintenance{m.HostID: {MemoryBudget: &HostMemoryBudget{GiB: 20, Measured: true}}}
	q.refreshHostMemory()
	if m.FreeHostMemoryGB != 0 || hostFits(&m, 0, 1) {
		t.Fatal("queue bypassed exhausted allowance", m)
	}
}

func TestMeasuredHostMemoryPolicyAcknowledgement(t *testing.T) {
	q, _ := fixture(t)
	m := measuredMemoryMachine()
	m.HostMemoryBudgetMeasured = false
	check(t, q, m, nil)
	maintenanceChange(t, q, "drain", false)
	on := true
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "budget", MemoryBudgetMeasured: &on}); err != nil {
		t.Fatal(err)
	}
	m.MemoryObservation.Quiescent = true
	reply := check(t, q, m, nil)
	if reply.HostMemoryBudget == nil || !reply.HostMemoryBudget.Measured || reply.HostMemoryBudget.GiB != 26 || !reply.AdmissionBlocked {
		t.Fatal(reply)
	}
	m.HostMemoryBudgetGB, m.HostMemoryBudgetMeasured = reply.HostMemoryBudget.GiB, true
	check(t, q, m, nil)
	maintenanceChange(t, q, "resume", false)
	if stored := q.s.Machines[m.ID]; stored.HostMemoryPolicyMismatch || stored.EffectiveHostMemoryBudgetGB != 26 {
		t.Fatal(stored)
	}
	if q.s.HostMaintenance[m.HostID].MemoryBudget.GiB != 20 {
		t.Fatal("learned value overwrote authored fallback")
	}
}

func TestMeasuredHostMemoryCompactionPreservesIdleProvenance(t *testing.T) {
	q, clock := fixture(t)
	m := measuredMemoryMachine()
	m.MemoryObservation.Quiescent = true
	q.recordExternalMemory(&m)
	clock.Advance(24 * time.Hour)
	m.MemoryObservation.UsedGB = 8
	q.recordExternalMemory(&m)
	before := q.externalMemoryStatus(m.HostID).Quiescent.Week
	compactExternalEvidence(q.s.ExternalMemory[m.HostID], clock.Now(), 1)
	after := q.externalMemoryStatus(m.HostID).Quiescent.Week
	if after.Samples != before.Samples || after.MaxGB != before.MaxGB || after.Windows != before.Windows {
		t.Fatal(before, after)
	}
}

func TestMeasuredHostMemoryResumesAfterModelUnload(t *testing.T) {
	rt, m, _ := runtimeLoadFixture(t, &lifecycleEngine{})
	rt.load(context.Background(), m)
	reports, _ := rt.report()
	if modelReportsQuiescent(reports) {
		t.Fatal("loaded model lost custody")
	}
	rt.sync(nil)
	rt.wg.Wait()
	reports, _ = rt.report()
	if len(reports) != 1 || reports[0].State != "unloaded" || !modelReportsQuiescent(reports) {
		t.Fatal("unloaded receipt prevented idle learning", reports)
	}
	// Failed cleanup still owns memory; a failure receipt after cleanup does not.
	rt, m, _ = runtimeLoadFixture(t, &lifecycleEngine{loadErr: errors.New("load"), unloadErr: errors.New("cleanup")})
	rt.load(context.Background(), m)
	reports, _ = rt.report()
	if modelReportsQuiescent(reports) {
		t.Fatal("failed cleanup treated as idle")
	}
}

func TestMeasuredHostMemorySameBinVariationSurvivesCompaction(t *testing.T) {
	q, clock := fixture(t)
	m := measuredMemoryMachine()
	m.MemoryObservation.Quiescent, m.MemoryObservation.ReserveGB = true, 1
	for i := 0; i < 100; i++ {
		m.MemoryObservation.UsedGB = 4.25
		if i == 99 {
			m.MemoryObservation.UsedGB = 7.75
		}
		q.recordExternalMemory(&m)
		clock.Advance(time.Minute)
	}
	for i := 0; i < 2; i++ {
		budget, d := q.measuredHostMemoryBudget(&m, HostMemoryBudget{GiB: 20, Measured: true})
		if budget.GiB != 24.25 || d.History.P50GB != 4.25 || d.ReserveGB != 3.5 {
			t.Fatal("histogram erased observed variation", budget, d)
		}
		compactExternalEvidence(q.s.ExternalMemory[m.HostID], clock.Now(), 1)
	}
}

func dedicatedProfile(m *Machine, mode string, laptop bool) {
	m.CapabilityProfile.Availability = &Availability{Mode: mode, Available: true, Signals: OwnerSignals{Laptop: laptop}}
}

// MY-45: capacity is live availability minus one measured margin. A node keeps
// idle p50 plus the larger of reserve and burst; the services host keeps its
// persistent p95 instead. Never the burst twice, never below the idle maximum.
// Owner-used, laptop and interactive hosts keep the conservative rule.
func TestMeasuredHostMemoryRoleSingleMargin(t *testing.T) {
	for _, tc := range []struct {
		name      string
		role      string
		mode      string
		laptop    bool
		shared    bool
		reserve   float64
		allowance float64
		source    string
	}{
		// Idle history: p50 2.25, p95 5, max 7.75 GiB on 32 GiB.
		{"node-default-burst", "", "dedicated", false, false, 1, 24.25, "idle_burst"}, // 32-2.25-5.5 = 32-max
		{"node-reserve", "node", "dedicated", false, false, 6, 23.75, "explicit_reserve"},
		{"services-reserve", "services", "dedicated", false, false, 4, 23, "explicit_reserve"}, // 32-5-4
		{"services-burst", "services", "dedicated", false, false, 1, 24.25, "idle_burst"},
		{"daily", "", "daily", false, false, 1, 18.75, ""}, // 32-7.75-max(1,7.75-2.25)
		{"laptop", "", "dedicated", true, false, 1, 18.75, ""},
		{"interactive", "interactive", "dedicated", false, true, 1, 18.75, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := measuredMemoryMachine()
			dedicatedProfile(&m, tc.mode, tc.laptop)
			policy := HostMaintenance{Role: tc.role}
			if tc.shared {
				policy.SharedHost = &SharedHostProfile{InteractiveReserveGB: 1}
			}
			q.s.HostMaintenance = map[string]HostMaintenance{m.HostID: policy}
			m.MemoryObservation.Quiescent, m.MemoryObservation.ReserveGB = true, tc.reserve
			for i := 0; i < 100; i++ {
				m.MemoryObservation.UsedGB = 2.25
				if i >= 88 {
					m.MemoryObservation.UsedGB = 5
				}
				if i == 99 {
					m.MemoryObservation.UsedGB = 7.75
				}
				q.recordExternalMemory(&m)
				clock.Advance(time.Minute)
			}
			budget, d := q.measuredHostMemoryBudget(&m, HostMemoryBudget{GiB: 20, Measured: true})
			if budget.GiB != tc.allowance || d.Dedicated != (tc.source != "") || d.ReserveSource != tc.source {
				t.Fatal(budget, d)
			}
		})
	}
}
