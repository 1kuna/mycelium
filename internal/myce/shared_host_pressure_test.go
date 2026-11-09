package myce

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// Synthetic old-release observations replay custody/hysteresis incidents.
const sharedWakeHighMS = 20.0
const sharedWakeLowMS = 0.0
const sharedPSIHigh = 1.0
const sharedPSILow = 0.0

func sharedRunning(t *testing.T, resumable bool) (*Queue, *fakeClock, Machine, *Job) {
	t.Helper()
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	check(t, q, m, nil)
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Resumable = resumable
	s.SubmissionKey = "owner-pressure-logical-job"
	j := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("missing assignment", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	return q, clock, m, run
}

// Owner pressure is temporary occupancy: a pinned require_target job queues and
// waits for hysteresis instead of being refused as unqualified (#605).
func TestSharedHostPressureQueuesRequireTarget(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	check(t, q, m, nil)
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 2}
	check(t, q, m, nil)
	s := cpuSpec(2, 2)
	s.AllowedMachines, s.RequireTarget = []string{m.ID}, true
	j := submit(t, q, s)
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("assigned under owner pressure", r.Job)
	}
	if got := job(t, q, j.ID); got.State != "queued" {
		t.Fatal("pressure did not leave job queued", got.State)
	}
	if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "shared host temporarily unavailable: shared host memory pressure") {
		t.Fatal("pressure wait is not visible", reason)
	}
	resolved, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(resolved.Targets, func(target ResolveTarget) bool {
		return target.Machine == m.ID && target.Eligible
	}) {
		t.Fatal("pressure made resolve unqualified", resolved.Targets)
	}
	report, err := q.Portability(PortabilityRequest{Spec: &s})
	if err != nil {
		t.Fatal(err)
	}
	admissions := []string{}
	for _, target := range report.Targets {
		for _, h := range target.Hosts {
			admissions = append(admissions, h.Admission)
		}
	}
	if !slices.ContainsFunc(admissions, func(a string) bool {
		return strings.HasPrefix(a, "temporarily unavailable: shared host memory pressure")
	}) {
		t.Fatal("portability did not call pressure temporary", admissions)
	}
	docker := s
	docker.Run = RunSpec{Docker: true, Command: []string{"docker", "run", "image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
	var httpErr *HTTPError
	if _, err := q.Submit(docker); !errors.As(err, &httpErr) || httpErr.Code != "no_qualified_target" {
		t.Fatal("structural shared-host refusal accepted under pressure", err)
	}
	tooLarge := s
	tooLarge.Needs.HostMemoryGB = 1000
	if _, err := q.Submit(tooLarge); !errors.As(err, &httpErr) || httpErr.Code != "no_qualified_target" {
		t.Fatal("impossible shared-host capacity accepted under pressure", err)
	}
	clock.Advance(time.Second)
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, WakeDelayMS: (sharedWakeHighMS + sharedWakeLowMS) / 2}
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("assigned before pressure hysteresis cleared", r.Job)
	}
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
	if r := check(t, q, m, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("not assigned after pressure cleared", r.Job)
	}
}

// #684: queue contention is contact, not a failed pressure probe. Both health
// APIs must preserve the same contact evidence in their observation snapshot.
func TestSharedHostPressureSlowCheckinHealthViews(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock, m, _ := sharedRunning(t, false)
		clock.Advance(time.Second)
		settle := q.beginWorkerCheckin(m.ID, m.Session, time.Second, true)
		defer settle(false)
		clock.Advance(20 * time.Second)
		q.mu.Lock()
		q.refreshHostMemory()
		reason := q.s.Machines[m.ID].SharedHostReason
		q.mu.Unlock()
		if reason != "" {
			t.Fatalf("slow check-in expired valid pressure: %s", reason)
		}
		assertSharedPressureHealth(t, q, m.ID, "healthy")
		// The same pending request must not hide missing evidence indefinitely.
		clock.Advance(legacyPressureDeliveryAllowance)
		q.mu.Lock()
		reason = q.sharedPressureReason(q.s.Machines[m.ID])
		q.mu.Unlock()
		if reason == "" {
			t.Fatal("abandoned check-in renewed pressure indefinitely")
		}
		assertSharedPressureHealth(t, q, m.ID, "unhealthy")

	})

	t.Run("SharedHostPressureMissingReportsHealthViews", func(t *testing.T) {
		q, clock, m, _ := sharedRunning(t, false)
		clock.Advance(signalMaxAge)
		q.mu.Lock()
		reason := q.sharedPressureReason(q.s.Machines[m.ID])
		q.mu.Unlock()
		if reason == "" {
			t.Fatal("missing worker kept pressure fresh")
		}
		assertSharedPressureHealth(t, q, m.ID, "unhealthy")

	})

	t.Run("SharedHostPressureFailingProbeHealthViews", func(t *testing.T) {
		q, clock, m, run := sharedRunning(t, false)
		for _, pending := range []bool{true, false} {
			t.Run(fmt.Sprint("pending=", pending), func(t *testing.T) {
				m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
				check(t, q, m, run)
				for age := signalRefresh; age <= signalMaxAge; age += signalRefresh {
					clock.Advance(signalRefresh)
					if pending {
						m.SharedHostPressure.AgeSeconds = age.Seconds()
					} else {
						m.SharedHostPressure = nil
					}
					check(t, q, m, run)
				}
				assertSharedPressureHealth(t, q, m.ID, "unhealthy")
			})
		}

	})

	t.Run("SharedHostPressureOwnerYieldDoesNotConsumeHealthAlert", func(t *testing.T) {
		q, clock, m, run := sharedRunning(t, false)
		clock.Advance(healthAlertCooldown)
		check(t, q, m, run)
		before := len(alertsOf(q, "host_unhealthy_or_stale"))
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 2}
		check(t, q, m, run)
		if len(alertsOf(q, "host_unhealthy_or_stale")) != before {
			t.Fatal("normal owner-pressure yield emitted an infrastructure-failure alert")
		}
		assertSharedPressureHealth(t, q, m.ID, "healthy")
		m.SharedHostPressure = nil
		check(t, q, m, run)
		if len(alertsOf(q, "host_unhealthy_or_stale")) != before+1 {
			t.Fatal("owner yield suppressed a subsequent real pressure-signal failure")
		}
		assertSharedPressureHealth(t, q, m.ID, "unhealthy")

	})
}

func assertSharedPressureHealth(t *testing.T, q *Queue, host, want string) {
	t.Helper()
	direct, err := q.HostMaintenanceStatus(host)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := q.statusSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if direct.State != want || direct.Healthy != (want == "healthy") || fleet.HostStates[host] != want {
		t.Fatalf("health disagreement: host=%s healthy=%t fleet=%s want=%s", direct.State, direct.Healthy, fleet.HostStates[host], want)
	}
}

// Expected owner yield must not consume an infrastructure-failure alert's
// episode/cooldown; otherwise a later missing probe would be suppressed.
