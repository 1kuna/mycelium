package myce

import (
	"strings"
	"testing"
	"time"
)

func unifiedCUDAAdmissionMachine() Machine {
	m := machine("unified", "cuda", "unified-host")
	m.UnifiedMemory, m.HostMemoryMeasured = true, true
	m.CapabilityProfile.MemoryLayout = "unified"
	m.CapabilityProfile.SystemMemoryBytes = 121 << 30
	m.FreeMemoryGB, m.TotalMemoryGB = 96, 96
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 96, 90
	m.MemoryCeilingSlackGB = .125
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 15}
	return m
}

func TestUnifiedCUDAAdmissionPhysicalFloor(t *testing.T) {
	for _, mode := range []string{"unified", "discrete-cuda", "other-unified"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := unifiedCUDAAdmissionMachine()
			if mode == "discrete-cuda" {
				m.UnifiedMemory, m.CapabilityProfile.MemoryLayout = false, "separate"
			}
			if mode == "other-unified" {
				m.GPU = "fake"
			}
			m.HostAvailableMemoryGB = 20
			m.MemoryObservation.UsedGB = 97 // 24 GiB raw free, below the 25 GiB floor.
			s := spec("experiment")
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = 2, 1
			j := submit(t, q, s)
			reply := check(t, q, m, nil)
			if mode != "unified" {
				if reply.Job == nil || reply.Job.ID != j.ID {
					t.Fatal("other host admission changed", reply)
				}
				return
			}
			if reply.Job != nil {
				t.Fatal("CUDA admitted below physical floor")
			}
			if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "physical RAM floor 25.000 GiB") {
				t.Fatal("physical refusal unexplained", reason)
			}
			observation := m.MemoryObservation
			m.MemoryObservation = nil
			if run := check(t, q, m, nil).Job; run != nil {
				t.Fatal("missing physical telemetry admitted CUDA", run)
			}
			m.MemoryObservation = observation
			clock.Advance(time.Second)
			m.MemoryObservation.UsedGB = 89 // 32 - 25 = 7 GiB; charge 3.125 fits.
			if run := check(t, q, m, nil).Job; run == nil || run.ID != j.ID {
				t.Fatal("fresh physical headroom did not restore admission", run)
			}
		})
	}
}

func TestUnifiedCUDAAdmissionProtectsExpectedProduction(t *testing.T) {
	q, clock, source, _, producer, _ := productionAdmissionFixture(t)
	parent := submit(t, q, producer)
	wave := check(t, q, source, nil).Job
	if wave == nil {
		t.Fatal("missing production producer")
	}
	if err := q.Started(result(wave, false)); err != nil {
		t.Fatal(err)
	}
	m := unifiedCUDAAdmissionMachine()
	m.HostAvailableMemoryGB = 90
	m.MemoryObservation.UsedGB = 16 // 105 raw - 25 reserve = 80 usable.
	check(t, q, m, nil)
	prod := spec("production")
	prod.Project = producer.Project
	prod.Needs.MemoryGB, prod.Needs.HostMemoryGB = 24, 8
	prod.AllowedMachines = []string{m.ID}
	prod.After = &PieceDependency{Job: parent.ID, Piece: "item-0", Outputs: map[string]string{"input": "out"}}
	demand := submit(t, q, prod)
	experiment := spec("experiment")
	experiment.Needs.MemoryGB, experiment.Needs.HostMemoryGB = 32, 21
	experiment.AllowedMachines = []string{m.ID}
	j := submit(t, q, experiment)
	clock.Advance(time.Second)
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("large CUDA experiment consumed expected production headroom", r.Job)
	}
	if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "unified CUDA production headroom") {
		t.Fatal("missing production headroom reason", reason)
	}
	if q.held(m.HostID) {
		t.Fatal("forecast changed production takeover")
	}
	productionCandidate := *job(t, q, j.ID)
	productionCandidate.Spec.Priority = "production"
	if reason := q.productionAdmissionReason(&productionCandidate, q.s.Machines[m.ID]); reason != "" {
		t.Fatal("co-tenant guard refused production itself", reason)
	}
	// A smaller co-tenant fits with the production envelope and physical floor.
	small := *job(t, q, j.ID)
	small.Spec.Needs.MemoryGB, small.Spec.Needs.HostMemoryGB = 8, 4
	target := q.s.Machines[m.ID]
	if reason := q.productionAdmissionReason(&small, target); reason != "" {
		t.Fatal("small co-tenant withheld", reason)
	}
	// Rechecking an assigned, unstarted offer must not charge it twice.
	small.State, small.Machine, small.Session = "running", m.ID, m.Session
	copy := *target
	copy.FreeHostMemoryGB = hostNeed(target, 24, 8) // Exactly the production envelope remains.
	if reason := q.productionAdmissionReason(&small, &copy); reason != "" {
		t.Fatal("unstarted offer charged twice", reason)
	}
	// Qualification elsewhere does not authorize bypassing a bound host's
	// work-scoped production parity policy (#401).
	other := unifiedCUDAAdmissionMachine()
	other.ID, other.HostID, other.Session = "other", "other-host", "other-session"
	check(t, q, other, nil)
	q.s.Jobs[demand.ID].Spec.AllowedMachines = []string{m.ID, other.ID}
	q.s.Jobs[demand.ID].ProductionHost = m.HostID
	if reason := q.productionAdmissionReason(job(t, q, j.ID), q.s.Machines[m.ID]); reason == "" {
		t.Fatal("unapproved alternative bypassed production binding")
	}
	q.s.Jobs[demand.ID].ProductionHost = ""
	if reason := q.productionAdmissionReason(job(t, q, j.ID), q.s.Machines[m.ID]); reason != "" {
		t.Fatal("qualified unbound alternative did not remove sole-host protection", reason)
	}
	if _, err := q.ChangeHostMaintenance(other.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	if reason := q.productionAdmissionReason(job(t, q, j.ID), q.s.Machines[m.ID]); reason == "" {
		t.Fatal("unavailable alternative bypassed production headroom")
	}
	clock.Advance(100 * time.Second)
	if reason := q.productionAdmissionReason(job(t, q, j.ID), q.s.Machines[m.ID]); reason != "" {
		t.Fatal("overrunning producer permanently withheld CUDA", reason)
	}
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	if r := check(t, q, m, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("withdrawn production demand still withholds CUDA", r.Job)
	}
}
