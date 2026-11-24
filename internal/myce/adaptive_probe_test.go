package myce

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdaptiveProbeUnknownHealthRecovers(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	m := machine("health-recovery", "fake")
	w := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test"}, toolReport: &workerToolReport{}}
	m.HostHealth = &HostHealth{Probe: &ProbeObservation{}}
	for _, healthy := range []bool{false, true} {
		if healthy {
			m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}, History: []HostHealthSample{{At: clock.Now()}}}
		}
		reply, err := w.fixtureCheckin(t.Context(), Checkin{Machine: m})
		if err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		stored := q.s.Machines[m.ID]
		leaving := stored.Leaving
		blocked := availabilityBlocked(stored) || q.hostHygieneReason(stored) != ""
		q.mu.Unlock()
		if reply.Leave || leaving {
			t.Fatal("temporary health uncertainty latched permanent leave", healthy)
		}
		if blocked == healthy {
			t.Fatal("health admission failed to withhold or recover", healthy, blocked)
		}
	}
}

func TestAdaptiveProbeQueueDelayCannotRenewEvidence(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	check(t, q, m, nil)
	observation := &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}
	m = profileAvailability(m, Availability{Mode: "daily", Available: true, Signals: idleSignals(), Probe: observation})
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, Probe: observation}
	m.HostHealth = &HostHealth{Probe: observation, History: []HostHealthSample{{At: clock.Now()}}}
	q.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := q.Checkin(Checkin{Machine: m}); done <- err }()
	await(t, func() bool {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		// Contact registration precedes receipt-age capture. Wait until that
		// capture and health preparation exist before advancing delivery time.
		return len(q.healthPreparing[m.ID]) > 0
	})
	clock.Advance(3 * time.Second)
	q.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	stored := q.s.Machines[m.ID]
	if hostAvailabilityFresh(stored, clock.Now()) || machineAvailability(stored).Available || q.sharedPressureFresh(stored) {
		t.Fatal("queue delay renewed expired owner or pressure evidence")
	}
	// Host health now excludes queue service, as required by the reporting-
	// worker incident. The original assertion treated all three lanes as wall
	// age and incorrectly required this fresh-at-send health report to expire.
	if reason := q.hostHygieneReason(stored); reason != "" {
		t.Fatal("queue service invalidated fresh-at-send health", reason)
	}
	// The exception cannot renew evidence that was already stale at send.
	m.HostHealth.AgeSeconds = observation.ValidSeconds
	check(t, q, m, nil)
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
		t.Fatal("queue delivery renewed stale-at-send health", reason)
	}
}
