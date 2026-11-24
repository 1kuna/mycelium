package myce

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdaptiveProbeQueueOwnerAgeAndClockSkew(t *testing.T) {
	q, clock := fixture(t)
	m := machine("owner-age", "fake")
	m = profileAvailability(m, Availability{Mode: "daily", Available: true, Signals: idleSignals(), CheckedAt: clock.Now().Add(24 * time.Hour), AgeSeconds: 1, Probe: &ProbeObservation{PeriodSeconds: 2, CostSeconds: .5, ValidSeconds: 3}})
	check(t, q, m, nil)
	stored := q.s.Machines[m.ID]
	if !hostAvailabilityFresh(stored, clock.Now()) {
		t.Fatal("worker wall-clock skew controlled freshness")
	}
	clock.Advance(2 * time.Second)
	if hostAvailabilityFresh(stored, clock.Now()) {
		t.Fatal("report age was renewed")
	}
	m.CapabilityProfile.Availability.AgeSeconds = 3
	check(t, q, m, nil)
	if machineAvailability(q.s.Machines[m.ID]).Available {
		t.Fatal("stale owner evidence admitted")
	}
}

func TestAdaptiveProbeHealthFreshness(t *testing.T) {
	q, clock := fixture(t)
	m := machine("health-probe", "fake")
	m.HostHealth = &HostHealth{AgeSeconds: 1, Probe: &ProbeObservation{PeriodSeconds: 2, CostSeconds: .1, ValidSeconds: 3}, History: []HostHealthSample{{At: clock.Now()}}}
	check(t, q, m, nil)
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "" {
		t.Fatal(reason)
	}
	clock.Advance(2 * time.Second)
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
		t.Fatal("stale health admitted", reason)
	}
	m.HostHealth.AgeSeconds = 0
	m.HostHealth.History[0].Error = "host_sensor_unavailable"
	check(t, q, m, nil)
	if q.hostHygieneReason(q.s.Machines[m.ID]) != "host_probe_unknown" {
		t.Fatal("failed source claimed recovery")
	}
}

func TestAdaptiveProbeHostRelativePressure(t *testing.T) {
	for _, baseline := range []float64{.05, 50} {
		q, clock := fixture(t)
		m := sharedHostFixture(t, q)
		m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
		// A new worker session establishes its own uncontended reference.
		m.ID = "fresh-baseline"
		m.Session = "fresh-baseline"
		m.HostID = "fresh-baseline"
		q.s.HostMaintenance[m.HostID] = HostMaintenance{SharedHost: m.SharedHost}
		for i := 0; i < 12; i++ {
			clock.Advance(time.Second)
			m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, WakeDelayMS: baseline}
			check(t, q, m, nil)
		}
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, WakeDelayMS: baseline * 8}
		clock.Advance(time.Second)
		check(t, q, m, nil)
		if q.s.Machines[m.ID].SharedHostYieldReason != "shared host owner scheduling latency" {
			t.Fatal("did not yield relative to own host", baseline)
		}
		// A middling value neither clears the latch nor teaches a busy baseline.
		m.SharedHostPressure.WakeDelayMS = baseline * 4
		clock.Advance(time.Second)
		check(t, q, m, nil)
		if q.s.Machines[m.ID].SharedHostYieldReason == "" {
			t.Fatal("busy session became its own baseline")
		}
		m.SharedHostPressure.WakeDelayMS = baseline
		clock.Advance(time.Second)
		check(t, q, m, nil)
		if q.s.Machines[m.ID].SharedHostYieldReason != "" {
			t.Fatal("own baseline did not recover")
		}
	}
}

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

func TestAdaptiveProbeMissingPressurePreservesBaseline(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	for i, wake := range []float64{0, 50, -1, 50, 0} {
		clock.Advance(time.Second)
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, WakeDelayMS: wake}
		if wake < 0 {
			m.SharedHostPressure = nil
		}
		check(t, q, m, nil)
		stored := q.s.Machines[m.ID]
		if i >= 1 && i <= 3 && stored.SharedHostYieldReason != "shared host owner scheduling latency" {
			t.Fatal("missing evidence erased adverse-episode latch", i)
		}
		if stored.SharedHostPressure.Learning.Wake.Mean != 0 {
			t.Fatal("adverse value trained healthy baseline", i)
		}
		if i == 4 && stored.SharedHostYieldReason != "" {
			t.Fatal("fresh healthy evidence did not recover")
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

func TestAdaptiveProbeDedicatedPolicyDoesNotDependOnOwnerSensorAge(t *testing.T) {
	q, clock := fixture(t)
	m := machine("dedicated-owner", "fake")
	m = profileAvailability(m, Availability{Mode: "dedicated", Available: true, Probe: &ProbeObservation{PeriodSeconds: .1, CostSeconds: .001, ValidSeconds: .2}, AgeSeconds: 1})
	check(t, q, m, nil)
	if !machineAvailability(q.s.Machines[m.ID]).Available {
		t.Fatal("dedicated host withdrew work for irrelevant owner-sensor age")
	}
	clock.Advance(time.Second)
	q.maintain()
	if !machineAvailability(q.s.Machines[m.ID]).Available {
		t.Fatal("dedicated host acquired daily-owner freshness policy")
	}
}

func TestAdaptiveProbePressureReportDeliveryAndFrozenExpiry(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	monitor := sharedPressureMonitor{clock: clock}
	monitor.lane.timing = probeTiming{base: 100 * time.Millisecond, cores: 2}
	// Observe the actual control/report gap before enabling the shared profile.
	// Cheap OS sensing does not imply equally cheap report delivery or launch.
	monitor.snapshot(false)
	clock.Advance(400 * time.Millisecond)
	monitor.snapshot(true)
	start := clock.Now()
	clock.Advance(time.Millisecond)
	monitor.current = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1,
		sampledAt: start, Probe: monitor.lane.timing.observe(start, clock.Now(), false)}
	m.SharedHostPressure = monitor.snapshot(true)
	check(t, q, m, nil)
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, cpuSpec(1, 1))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("missing shared-host assignment")
	}
	// The queue's last report must survive the measured delivery path, even
	// though the independent OS lane can complete more probes meanwhile.
	clock.Advance(200 * time.Millisecond)
	if err := q.AuthorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: run.ID, Attempt: "1"}); err != nil {
		t.Fatal("healthy pressure expired before measured launch delivery", err)
	}
	frozen := *monitor.current.Probe
	clock.Advance(2 * time.Second)
	late := monitor.snapshot(true)
	if *late.Probe != frozen || validSharedPressure(late) || q.sharedPressureFresh(q.s.Machines[m.ID]) {
		t.Fatal("slow reporting renewed the old observation")
	}
	// Only a newly completed probe can use the subsequently learned gap.
	start = clock.Now()
	next := monitor.lane.timing.observe(start, start.Add(time.Millisecond), false)
	if next.ValidSeconds <= frozen.ValidSeconds {
		t.Fatal("new evidence did not adapt to the slower report path")
	}
}
