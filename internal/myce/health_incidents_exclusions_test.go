package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

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
	for _, refuse := range []bool{false, true} {
		t.Run(fmt.Sprint("refused=", refuse), func(t *testing.T) {
			q, clock, m, s := incidentVariantFixture(t, 0)
			first := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			r := result(&first, true)
			peak := 22.0
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			m.HostMemoryBudgetGB = 24
			check(t, q, m, nil)
			s.RequireTarget = refuse
			queued, err := q.Submit(s)
			if (err != nil) != refuse {
				t.Fatal("unexpected admission", err)
			}
			if !refuse && queued.Spec.Needs.Memory != "" {
				t.Fatal("fixture did not inherit auto from variant")
			}
			kind := "learned_envelope_exceeds_allowances"
			var key string
			for k := range q.s.Health.Conditions {
				if strings.HasPrefix(k, "learned-envelope/") {
					key = k
				}
			}
			requireIncident(t, q, kind, key, 1, true)
			m.HostMemoryBudgetGB = 64
			check(t, q, m, nil)
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
	m.HostMemoryBudgetGB = 24
	check(t, q, m, nil)
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
	peak := 22.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	m.HostMemoryBudgetGB = 24
	check(t, q, m, nil)
	peer := cloneMachine(&m)
	peer.ID, peer.HostID, peer.Session = "offline-peer", "offline-peer", "offline-session"
	check(t, q, peer, nil)
	// Reproduce the queue's loss observation without making the fitting
	// worker stale too. Qualification ignores this temporary availability.
	if err := q.update(func() error { q.s.Machines[peer.ID].Gone = true; return nil }); err != nil {
		t.Fatal(err)
	}
	s.Needs.Memory, s.RequireTarget = "auto", true
	if _, err := q.Submit(s); err == nil {
		t.Fatal("oversized work admitted")
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
			m.HostMemoryBudgetGB = 24
			check(t, q, m, nil)
			s.Needs.Memory, s.RequireTarget = "auto", true
			if history == "fitting peer" {
				peer := cloneMachine(&m)
				peer.ID, peer.HostID, peer.Session = "peer", "peer", "session-peer"
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
