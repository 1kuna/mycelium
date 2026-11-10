package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func incidentAllowance(t *testing.T, q *Queue, m *Machine, gb float64) {
	t.Helper()
	// Reports cannot change the queue-owned allowance after registration.
	// Replay the operator update and worker acknowledgement before resuming.
	for _, request := range []HostMaintenanceRequest{{Action: "drain"}, {Action: "budget", MemoryBudgetGB: &gb}} {
		if _, err := q.ChangeHostMaintenance(hostID(m), request); err != nil {
			t.Fatal(err)
		}
	}
	m.HostMemoryBudgetGB = gb
	check(t, q, *m, nil)
	if _, err := q.ChangeHostMaintenance(hostID(m), HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
}

func incidentPeer(t *testing.T, q *Queue, m *Machine, id string) Machine {
	t.Helper()
	peer := cloneMachine(m)
	peer.ID, peer.HostID, peer.Session = id, id, "session-"+id
	// Qualification is host-bound; copying another host's verdict does not
	// make this peer an eligible alternative, even with identical hardware.
	for i, old := range peer.ExtensionReports {
		e, err := q.extensionByDigest(old.Digest)
		if err != nil {
			t.Fatal(err)
		}
		r := newExtensionReport(e, &peer, old.Work)
		r.Setup, r.Qualified, r.CheckID = true, true, "peer-check"
		peer.ExtensionReports[i] = r
	}
	return peer
}

func incidentVariantFixture(t *testing.T, capGB float64) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
	v.Execution.Main.MemoryGB = capGB
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("variant-worker", "variant-host", "cpu", 8, 64)
	m.ExtensionFingerprint = "cpu-v1"
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	s := Spec{Name: "variant-work", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	return q, clock, m, s
}

func TestHealthIncidentInheritedAutoMemoryReplay(t *testing.T) {
	for _, requireTarget := range []bool{false, true} {
		t.Run(fmt.Sprint("require_target=", requireTarget), func(t *testing.T) {
			q, clock, m, s := incidentVariantFixture(t, 0)
			first := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			r := result(&first, true)
			peak := 26.0 // Measured demand itself exceeds the 24 GiB allowance; no universal padding.
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			incidentAllowance(t, q, &m, 24)
			s.RequireTarget = requireTarget
			queued, err := q.Submit(s)
			if err != nil {
				t.Fatal("unexpected admission", err)
			}
			if queued.Spec.Needs.Memory != "" {
				t.Fatal("fixture did not inherit auto from variant")
			}
			if why := q.memoryEvidenceWaitingReason(queued); !strings.HasPrefix(why, memoryEvidenceRecovering+":") {
				t.Fatal("inherited learned sizing lost typed wait", why)
			}
			kind := "learned_envelope_exceeds_allowances"
			var key string
			for k := range q.s.Health.Conditions {
				if strings.HasPrefix(k, "learned-envelope/") {
					key = k
				}
			}
			requireIncident(t, q, kind, key, 1, true)
			incidentAllowance(t, q, &m, 64)
			requireIncident(t, q, kind, key, 1, false)
			clock.Advance(healthAlertCooldown)
			check(t, q, m, nil)
			if _, exists := q.s.Health.Conditions[key]; exists {
				t.Fatal("recovered envelope condition outlived cooldown")
			}
		})
	}
}

func TestHealthIncidentDeclaredMemberCapExcluded(t *testing.T) {
	q, clock, m, s := incidentVariantFixture(t, 32)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&first, true)
	peak := 2.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	incidentAllowance(t, q, &m, 24)
	s.RequireTarget = true
	if _, err := q.Submit(s); err == nil {
		t.Fatal("oversized declared member cap admitted")
	}
	if events := alertsOf(q, "learned_envelope_exceeds_allowances"); len(events) != 0 {
		t.Fatal("declared cap misclassified as learning", events)
	}
}

func TestHealthIncidentHealthyDemandLeavesNoHistory(t *testing.T) {
	q, _, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	for i := range 8 {
		s.Name = fmt.Sprintf("healthy-%d", i)
		j := submit(t, q, s)
		if err := q.Cancel(j.ID); err != nil {
			t.Fatal(err)
		}
		check(t, q, m, nil)
	}
	for key := range q.s.Health.Conditions {
		if strings.HasPrefix(key, "learned-envelope/") {
			t.Fatal("healthy demand retained a condition", key)
		}
	}
}

func TestHealthIncidentOfflineUndersizedPeerDoesNotSuppress(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&first, true)
	peak := 26.0 // Measured demand itself exceeds the 24 GiB allowance; no universal padding.
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	incidentAllowance(t, q, &m, 24)
	peer := incidentPeer(t, q, &m, "offline-peer")
	check(t, q, peer, nil)
	// Reproduce the queue's loss observation without making the fitting
	// worker stale too. Qualification ignores this temporary availability.
	if err := q.update(func() error { q.touchMachine(peer.ID).Gone = true; return nil }); err != nil {
		t.Fatal(err)
	}
	s.Needs.Memory, s.RequireTarget = "auto", true
	queued := submit(t, q, s)
	if why := q.memoryEvidenceWaitingReason(queued); !strings.HasPrefix(why, memoryEvidenceRecovering+":") {
		t.Fatal("oversized learned work lost typed wait", why)
	}
	if assigned := assignedJob(machineCheck(t, q, m)); assigned != nil {
		t.Fatal("oversized learned work assigned", assigned.ID)
	}
	if events := alertsOf(q, "learned_envelope_exceeds_allowances"); len(events) != 1 {
		t.Fatal("offline undersized peer suppressed the incident", events)
	}
}

func TestHealthIncidentLearnedEnvelopeHealthyExclusions(t *testing.T) {
	for _, history := range []string{"cold", "fitting peer", "wrong platform"} {
		t.Run(history, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			if history != "cold" {
				first := startObservation(t, q, m, s)
				clock.Advance(time.Second)
				r := result(&first, true)
				peak := 22.0
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			incidentAllowance(t, q, &m, 24)
			s.Needs.Memory, s.RequireTarget = "auto", true
			if history == "fitting peer" {
				peer := incidentPeer(t, q, &m, "peer")
				peer.HostMemoryBudgetGB, peer.HostAvailableMemoryGB = 64, 64
				peer.CapabilityProfile.SystemMemoryBytes = 64 << 30
				check(t, q, peer, nil)
			}
			if history == "wrong platform" {
				s.Targets = []ExecutionTarget{{OS: "darwin", Arch: "arm64", Platform: "cpu"}}
			}
			_, err := q.Submit(s)
			if (err != nil) != (history == "wrong platform") {
				t.Fatal("unexpected admission result", err)
			}
			if events := alertsOf(q, "learned_envelope_exceeds_allowances"); len(events) != 0 {
				t.Fatal("healthy or ineligible history became a learned sizing incident", events)
			}
		})
	}
}
