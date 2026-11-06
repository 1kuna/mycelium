package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptiveProbeTinyLargeIdleAndSlow(t *testing.T) {
	for _, cores := range []int{1, 128} {
		t.Run(time.Duration(cores).String(), func(t *testing.T) {
			clock := &fakeClock{now: time.Unix(1000, 0)}
			p := probeTiming{base: time.Millisecond, cores: cores}
			start := clock.Now()
			clock.Advance(10 * time.Millisecond)
			first := p.observe(start, clock.Now(), false)
			clock.Advance(time.Hour)
			start = clock.Now()
			clock.Advance(10 * time.Millisecond)
			idle := p.observe(start, clock.Now(), false)
			if !first.valid() || !idle.valid() || idle.PeriodSeconds <= first.PeriodSeconds {
				t.Fatal("idle scan did not relax", first, idle)
			}
			start = clock.Now()
			clock.Advance(5 * time.Second)
			slow := p.observe(start, clock.Now(), true)
			if slow.CostSeconds != 5 || slow.PeriodSeconds < 5 || slow.ValidSeconds <= idle.ValidSeconds {
				t.Fatal("slow cost not reflected", slow, idle)
			}
		})
	}
	start := time.Unix(1000, 0)
	tiny := probeTiming{base: time.Millisecond, cores: 1}
	large := probeTiming{base: time.Millisecond, cores: 128}
	if tiny.observe(start, start.Add(time.Second), false).PeriodSeconds <= large.observe(start, start.Add(time.Second), false).PeriodSeconds {
		t.Fatal("capacity did not change observation cost budget")
	}
}

func TestAdaptiveProbeWireNegotiationAndDowngrade(t *testing.T) {
	q, clock := fixture(t)
	handler := API{q, "test"}.Handler()
	strict := false
	var reports []Checkin
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var c Checkin
		if err := json.Unmarshal(data, &c); err != nil {
			t.Error(err)
			return
		}
		reports = append(reports, c)
		if strict {
			if hasHostProbeFields(c) || c.ToolInventoryHash != "" {
				rw.WriteHeader(400)
				json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
				return
			}
			json.NewEncoder(rw).Encode(Reply{AttemptProtocol: keyedAttemptProtocol})
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
		handler.ServeHTTP(rw, r)
	}))
	defer server.Close()
	m := machine("probe-wire", "fake")
	observation := &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, Probe: observation}
	m = profileAvailability(m, Availability{Mode: "daily", Available: true, Signals: idleSignals(), AgeSeconds: .25, Probe: observation})
	w := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test"}, toolReport: &workerToolReport{}, hygieneWire: &workerHygieneWire{}}
	send := func() {
		t.Helper()
		if _, err := w.checkin(t.Context(), Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
	}
	send()
	send()
	if hasHostProbeFields(reports[0]) || !hasHostProbeFields(reports[1]) {
		t.Fatal("probe fields were not negotiated")
	}
	if m.SharedHostPressure.Probe != observation || machineAvailability(&m).AgeSeconds != .25 {
		t.Fatal("wire adaptation changed local facts")
	}
	strict = true
	send()
	send()
	if len(reports) != 5 {
		t.Fatal("downgrade did not use one legacy retry", len(reports))
	}
	for _, c := range reports[3:] {
		if hasHostProbeFields(c) || c.Machine.Session != m.Session || c.AttemptProtocol != keyedAttemptProtocol {
			t.Fatal("downgrade changed protocol/custody")
		}
	}
}

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

func TestAdaptiveProbeHealthFreshnessAndLegacyDrain(t *testing.T) {
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
	m.HostHealth.AgeSeconds = 3
	legacy := legacyHostProbeCheckin(Checkin{Machine: m})
	if legacy.Machine.Leaving || !availabilityBlocked(&legacy.Machine) || legacy.Machine.HostHealth.DrainReason != "host_probe_unknown" || legacy.Machine.HostHealth.Probe != nil || m.HostHealth.Probe == nil {
		t.Fatal("downgrade lost unknown-health drain or changed local facts")
	}
	m.HostHealth.AgeSeconds = 0
	m.HostHealth.History[0].Error = "host_sensor_unavailable"
	check(t, q, m, nil)
	if q.hostHygieneReason(q.s.Machines[m.ID]) != "host_probe_unknown" {
		t.Fatal("failed source claimed recovery")
	}
}

func TestAdaptiveProbeStallFailureAndOriginalAge(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	lane := hostProbeLane[int]{clock: clock, timing: probeTiming{base: time.Second, cores: 2}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls atomic.Int32
	entered, release := make(chan struct{}, 1), make(chan struct{})
	source := func(context.Context) (int, error) {
		n := calls.Add(1)
		if n == 2 {
			entered <- struct{}{}
			<-release
		}
		if n == 3 {
			return 0, errors.New("unavailable")
		}
		return int(n), nil
	}
	lane.step(ctx, source)
	await(t, func() bool { return len(lane.pending) == 1 })
	lane.step(ctx, source)
	if !lane.fresh(clock.Now()) {
		t.Fatal("first observation missing")
	}
	original := lane.at
	clock.Advance(time.Second)
	lane.step(ctx, source)
	<-entered
	clock.Advance(20 * time.Second)
	for i := 0; i < 100; i++ {
		lane.step(ctx, source)
	}
	if lane.fresh(clock.Now()) || lane.at != original || calls.Load() != 2 {
		t.Fatal("stall renewed evidence or spawned probes")
	}
	close(release)
	await(t, func() bool { return len(lane.pending) == 1 })
	lane.step(ctx, source)
	if lane.at != original.Add(time.Second) {
		t.Fatal("late completion stamped fresh at consume time")
	}
	clock.Advance(time.Duration(lane.observation.PeriodSeconds * float64(time.Second)))
	lane.step(ctx, source)
	await(t, func() bool { return len(lane.pending) == 1 })
	lane.step(ctx, source)
	if lane.known || lane.fresh(clock.Now()) {
		t.Fatal("failure retained good fact")
	}
}

func TestAdaptiveProbeOwnerSessionLearnsPauses(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	p := availabilityPolicy{clock: clock, daily: true, probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}}
	s := idleSignals()
	s.IdleSeconds = 0
	for i := 0; i < 8; i++ {
		if p.evaluate(s).Available {
			t.Fatal("busy owner admitted")
		}
		clock.Advance(time.Second)
	}
	if p.inputPauses.Mean != 1 {
		t.Fatal("owner pauses not learned", p.inputPauses)
	}
	s.IdleSeconds = .5
	if p.evaluate(s).Available {
		t.Fatal("within-session pause admitted")
	}
	for i := 1; i < 8; i++ {
		clock.Advance(time.Second)
		s.IdleSeconds = float64(i)
		p.evaluate(s)
	}
	if !p.evaluate(s).Available {
		t.Fatal("idle host did not recover")
	}
	s.IdleSeconds = 0
	if p.evaluate(s).Available {
		t.Fatal("owner return did not immediately withdraw")
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

// Nonzero probe cost must not round each next scan up to a control tick and
// repeatedly cancel healthy owner work or remove inventory capacity.
func TestAdaptiveProbeHealthyCyclesKeepEvidence(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	lane := hostProbeLane[OwnerSignals]{clock: clock, timing: probeTiming{base: time.Second, cores: 2}}
	policy := availabilityPolicy{clock: clock, daily: true}
	source := func(context.Context) (OwnerSignals, error) {
		clock.Advance(10 * time.Millisecond)
		return idleSignals(), nil
	}
	for i := 0; i < 8; i++ {
		if i > 0 {
			wait := lane.waitDuration()
			if wait <= 0 || wait >= time.Second {
				t.Fatal("scan did not wake at its actual due time", wait)
			}
			clock.Advance(wait)
			if !lane.fresh(clock.Now()) {
				t.Fatal("healthy observation expired before replacement started", i)
			}
		}
		lane.step(t.Context(), source)
		await(t, func() bool { return len(lane.pending) == 1 })
		if i > 0 && !lane.fresh(clock.Now()) {
			t.Fatal("healthy observation expired while ordinary replacement completed", i)
		}
		lane.step(t.Context(), source)
		policy.probe = lane.observation
		if !lane.fresh(clock.Now()) || !policy.evaluate(lane.value).Available {
			t.Fatal("ordinary cost caused owner availability flapping", i)
		}
	}
}

func TestAdaptiveProbeUnknownHealthNegotiationRecovers(t *testing.T) {
	q, clock := fixture(t)
	handler := API{q, "test"}.Handler()
	legacy := false
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if !legacy {
			handler.ServeHTTP(rw, r)
			return
		}
		data, _ := io.ReadAll(r.Body)
		var c Checkin
		_ = json.Unmarshal(data, &c)
		if c.Machine.HostHealth != nil || hasHostProbeFields(c) || c.ToolInventoryHash != "" {
			rw.WriteHeader(400)
			_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
		recorded := httptest.NewRecorder()
		handler.ServeHTTP(recorded, r)
		var reply Reply
		_ = json.Unmarshal(recorded.Body.Bytes(), &reply)
		reply.HostProbeProtocol, reply.HostHygieneProtocol, reply.ToolInventoryProtocol, reply.HostCollectionProtocol = "", "", "", ""
		rw.WriteHeader(recorded.Code)
		_ = json.NewEncoder(rw).Encode(reply)
	}))
	defer server.Close()
	m := machine("health-recovery", "fake")
	w := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test"}, toolReport: &workerToolReport{}, hygieneWire: &workerHygieneWire{}}
	for _, downgrade := range []bool{false, true} {
		legacy = downgrade
		m.HostHealth = &HostHealth{Probe: &ProbeObservation{}}
		for _, healthy := range []bool{false, true} {
			if healthy {
				m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}, History: []HostHealthSample{{At: clock.Now()}}}
			}
			reply, err := w.checkin(t.Context(), Checkin{Machine: m})
			if err != nil {
				t.Fatal(err)
			}
			stored := q.s.Machines[m.ID]
			if reply.Leave || stored.Leaving {
				t.Fatal("temporary health uncertainty latched permanent leave", downgrade, healthy)
			}
			blocked := availabilityBlocked(stored) || q.hostHygieneReason(stored) != ""
			if blocked == healthy {
				t.Fatal("health admission failed to withhold or recover", downgrade, healthy, blocked)
			}
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
		return len(q.contacts[m.ID].pending) > 0
	})
	clock.Advance(3 * time.Second)
	q.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	stored := q.s.Machines[m.ID]
	if hostAvailabilityFresh(stored, clock.Now()) || machineAvailability(stored).Available || q.sharedPressureFresh(stored) || q.hostHygieneReason(stored) != "host_probe_unknown" {
		t.Fatal("queue delay renewed an expired observation")
	}
}
