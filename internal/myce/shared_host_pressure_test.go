package myce

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

func TestSharedHostPressureYieldHysteresisAndExactRelease(t *testing.T) {
	for _, sensor := range []string{"darwin memory", "linux PSI", "owner latency"} {
		t.Run(sensor, func(t *testing.T) {
			q, clock, m, run := sharedRunning(t, true)
			p := SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
			switch sensor {
			case "darwin memory":
				p.MemoryLevel = 2
			case "linux PSI":
				p.MemoryPSI = sharedPSIHigh
			default:
				p.WakeDelayMS = sharedWakeHighMS
			}
			m.SharedHostPressure = &p
			clock.Advance(time.Second)
			r := check(t, q, m, run)
			j := job(t, q, run.ID)
			if j.State != "stopping" || priorityStop(j) == nil || priorityStop(j).Cause != sharedHostPressureCause || !r.Stop || r.Kill || r.GraceSeconds != q.preemptionGrace().Seconds() {
				t.Fatalf("missing planned grace: %+v %+v", j, r)
			}
			stopAt := *j.StopAt
			// A middling sample does not clear the latch or renew stop grace.
			p.MemoryLevel = 1
			p.MemoryPSI = (sharedPSIHigh + sharedPSILow) / 2
			p.WakeDelayMS = (sharedWakeHighMS + sharedWakeLowMS) / 2
			clock.Advance(time.Second)
			check(t, q, m, run)
			if j = job(t, q, run.ID); j.StopAt == nil || !j.StopAt.Equal(stopAt) || q.s.Machines[m.ID].SharedHostYieldReason == "" {
				t.Fatal("flapped or renewed grace")
			}
			clock.Advance(q.preemptionGrace())
			r = check(t, q, m, run)
			if !r.Kill || job(t, q, run.ID).State != "stopping" || q.s.Machines[m.ID].CPUInUseCores != 1 {
				t.Fatal("elapsed grace released custody", r)
			}
			finishInterrupted(t, q, run)
			j = job(t, q, run.ID)
			if j.State != "queued" || j.InfrastructureRetries != 0 || j.Losses != 0 || j.Spec.SubmissionKey != "owner-pressure-logical-job" {
				t.Fatal("yield changed logical work/accounting", j)
			}
			if next := check(t, q, m, nil).Job; next != nil {
				t.Fatal("readmitted before recovery")
			}
			p.MemoryPSI = sharedPSILow
			p.WakeDelayMS = sharedWakeLowMS
			r = check(t, q, m, nil)
			if sensor == "owner latency" {
				// The first latency interruption now requires affirmative quiet
				// observations covering this attempt's measured interference span.
				if r.Job != nil {
					t.Fatal("latency cooldown skipped")
				}
				for i := 0; i < 16; i++ {
					clock.Advance(time.Second)
					r = check(t, q, m, nil)
					if r.Job != nil {
						break
					}
				}
			}
			if r.Job == nil || r.Job.ID != run.ID || r.Job.Attempt <= run.Attempt {
				t.Fatal("fresh recovery did not requeue same job", r)
			}
			if err := q.Complete(result(run, true)); !errors.Is(err, ErrStale) {
				t.Fatal("stale attempt accepted", err)
			}
		})
	}
}

func TestSharedHostPressureUnknownAndStale(t *testing.T) {
	q, clock, m, run := sharedRunning(t, false)
	for _, p := range []*SharedHostPressure{nil, {}, {MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, AgeSeconds: signalMaxAge.Seconds()}, {MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, MemoryPSI: -1}} {
		m.SharedHostPressure = p
		check(t, q, m, run)
		if job(t, q, run.ID).State != "running" || q.s.Machines[m.ID].SharedHostReason == "" {
			t.Fatal("unknown became pressure or admitted work")
		}
	}
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
	check(t, q, m, run)
	clock.Advance(signalMaxAge)
	q.mu.Lock()
	q.refreshHostMemory()
	reason := q.s.Machines[m.ID].SharedHostReason
	q.mu.Unlock()
	if reason == "" {
		t.Fatal("cached sample did not expire with injected clock")
	}
	// Queue-owned times and latches cannot be supplied by a worker.
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, ReceivedAt: clock.Now().Add(time.Hour)}
	m.SharedHostYieldReason = "forged pressure"
	check(t, q, m, run)
	if q.s.Machines[m.ID].SharedHostYieldReason != "" || !q.s.Machines[m.ID].SharedHostPressure.ReceivedAt.Equal(clock.Now()) {
		t.Fatal("trusted worker timestamp/latch")
	}
	// Stale evidence must not clear a real pressure latch.
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 4}
	check(t, q, m, run)
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, AgeSeconds: signalMaxAge.Seconds()}
	check(t, q, m, run)
	if q.s.Machines[m.ID].SharedHostYieldReason == "" {
		t.Fatal("stale recovery cleared latch")
	}
}

func TestSharedHostPressureFailureAndCancellationPrecedence(t *testing.T) {
	for _, ending := range []string{"independent failure", "cancellation", "completed before signal"} {
		t.Run(ending, func(t *testing.T) {
			q, _, m, run := sharedRunning(t, false)
			m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 2}
			r := check(t, q, m, run)
			if !r.Kill || r.GraceSeconds != 0 {
				t.Fatal("nonresumable work got checkpoint delay")
			}
			out := result(run, false)
			want := "failed"
			if ending == "cancellation" {
				if err := q.Cancel(run.ID); err != nil {
					t.Fatal(err)
				}
				out.Interrupted = true
				want = "cancelled"
			} else if ending == "completed before signal" {
				out.Success = true
				want = "succeeded"
			}
			if err := q.Complete(out); err != nil {
				t.Fatal(err)
			}
			requireState(t, q, run.ID, want)
		})
	}
}

func TestSharedHostPressureDedicatedHostsUnchanged(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("ordinary", "ordinary", "cpu", 2, 8)
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 4}
	j := submit(t, q, cpuSpec(1, 1))
	r := check(t, q, m, nil)
	if r.Job == nil || r.Job.ID != j.ID || r.Stop || r.Kill {
		t.Fatal("pressure changed dedicated admission", r)
	}
}

func TestSharedHostPressureRetiredWorkerDoesNotStopReplacement(t *testing.T) {
	q, _, m, run := sharedRunning(t, true)
	q.mu.Lock()
	retired := *q.s.Machines[m.ID]
	retired.ID = "retired-worker"
	retired.Gone = true
	retired.SharedHostYieldReason = "shared host memory pressure"
	q.s.Machines[retired.ID] = &retired
	q.mu.Unlock()
	r := check(t, q, m, run)
	if r.Stop || r.Kill || job(t, q, run.ID).State != "running" {
		t.Fatal("retired pressure stopped healthy replacement", r)
	}
}

func TestSharedHostPressurePSIParsing(t *testing.T) {
	for _, value := range []string{"", "some avg10=NaN", "some avg10=101", "full avg10=1.00"} {
		if _, err := sharedMemoryPSI([]byte(value)); err == nil {
			t.Error("unknown PSI accepted", value)
		}
	}
	if n, err := sharedMemoryPSI([]byte("some avg10=1.25 avg60=0.50 total=123\nfull avg10=0.1\n")); err != nil || n != 1.25 {
		t.Fatal(n, err)
	}
}

func TestSharedHostCPUConcurrencyCap(t *testing.T) {
	_, clock, m, run := sharedRunning(t, false)
	m.SharedHostReason = ""
	w := Worker{sharedCPUDev: true, Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 12, nil }}
	next := Job{ID: "next", Spec: cpuSpec(2, 1)}
	if err := normalizeResources(&next.Spec); err != nil {
		t.Fatal(err)
	}
	if err := localAttemptAdmission(w, []Job{*run}, next, nil); err == nil {
		t.Fatal("concurrent declarations exceeded two-core profile")
	}
	next.Spec.Needs.CPUCores = 1
	if err := localAttemptAdmission(w, []Job{*run}, next, nil); err != nil {
		t.Fatal("fitting concurrent work rejected", err)
	}
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
	var httpErr *HTTPError
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

func TestSharedHostPressureMissingReportsHealthViews(t *testing.T) {
	q, clock, m, _ := sharedRunning(t, false)
	clock.Advance(signalMaxAge)
	q.mu.Lock()
	reason := q.sharedPressureReason(q.s.Machines[m.ID])
	q.mu.Unlock()
	if reason == "" {
		t.Fatal("missing worker kept pressure fresh")
	}
	assertSharedPressureHealth(t, q, m.ID, "unhealthy")
}

func TestSharedHostPressureFailingProbeHealthViews(t *testing.T) {
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
}

// The monitor re-reads its injected clock on a real control-period timer.
// synctest makes that timer virtual, so each fake-clock step costs no wall time.
func TestSharedHostPressureRefreshIndependentOfCheckins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &workerFakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
		var failing, blocked atomic.Bool
		entered, release := make(chan struct{}, 1), make(chan struct{})
		monitor := &sharedPressureMonitor{clock: clock, enabled: true, source: func(context.Context) (SharedHostPressure, error) {
			if blocked.Load() {
				entered <- struct{}{}
				<-release
			}
			if failing.Load() {
				return SharedHostPressure{}, errors.New("probe failed")
			}
			return SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}, nil
		}}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); monitor.run(ctx) }()
		defer func() { cancel(); close(release); <-done }()
		// No check-in consumes the probe during twenty seconds of queue delay.
		for i := 0; i < 5; i++ {
			await(t, func() bool { p := monitor.snapshot(true); return p != nil && p.AgeSeconds == 0 })
			if i < 4 {
				clock.Advance(signalRefresh)
			}
		}
		failing.Store(true)
		clock.Advance(signalRefresh)
		await(t, func() bool { return monitor.snapshot(true) == nil })
		failing.Store(false)
		clock.Advance(signalRefresh)
		await(t, func() bool { return validSharedPressure(monitor.snapshot(true)) })
		blocked.Store(true)
		clock.Advance(signalRefresh)
		<-entered
		clock.Advance(signalMaxAge)
		if validSharedPressure(monitor.snapshot(true)) {
			t.Fatal("pending probe renewed old evidence")
		}
	})
}

// Expected owner yield must not consume an infrastructure-failure alert's
// episode/cooldown; otherwise a later missing probe would be suppressed.
func TestSharedHostPressureOwnerYieldDoesNotConsumeHealthAlert(t *testing.T) {
	q, clock, m, run := sharedRunning(t, false)
	clock.Advance(healthAlertCooldown)
	check(t, q, m, run)
	before := firings(q, "host_unhealthy_or_stale")
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 2}
	check(t, q, m, run)
	if firings(q, "host_unhealthy_or_stale") != before {
		t.Fatal("normal owner-pressure yield emitted an infrastructure-failure alert")
	}
	assertSharedPressureHealth(t, q, m.ID, "healthy")
	m.SharedHostPressure = nil
	check(t, q, m, run)
	clock.Advance(hostDegradedSustain(q.s.Machines[m.ID]))
	check(t, q, m, run)
	if firings(q, "host_unhealthy_or_stale") != before+1 {
		t.Fatal("owner yield suppressed a subsequent real pressure-signal failure")
	}
	assertSharedPressureHealth(t, q, m.ID, "unhealthy")
}

// #684 live storm: a fresh measured observation expired between check-ins
// whenever the next report reached the queue later than its frozen validity,
// because validity predicts the worker's next send, not that report's
// delivery. Every queue transaction then raised a host alert that the next
// check-in cleared. Reported samples, missing samples and a silent worker
// must still alert and withhold new work.
func TestSharedHostPressureNextReportDeliveryDoesNotAlert(t *testing.T) {
	q, clock, m, run := sharedRunning(t, false)
	other := machine("other", "fake")
	check(t, q, other, nil)
	// Three completed check-ins of this session each took one second from
	// arrival to completion: a delivery prediction of about 2.1 s.
	for range 3 {
		settle := q.beginWorkerCheckin(m.ID, m.Session, time.Second, true)
		clock.Advance(time.Second)
		settle(true)
	}
	probe := &ProbeObservation{PeriodSeconds: 1, CostSeconds: .01, ValidSeconds: 6.5}
	report := func(age float64) {
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, AgeSeconds: age, Probe: probe}
		check(t, q, m, run)
	}
	withheld := func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.sharedPressureReason(q.s.Machines[m.ID]) != ""
	}
	report(1.5)
	before := firings(q, "host_unhealthy_or_stale")
	// Live shape: a 1.5 s old sample, then a 6 s gap until the next report.
	// Another worker's check-in evaluates health inside that gap.
	clock.Advance(5500 * time.Millisecond)
	check(t, q, other, nil)
	if got := firings(q, "host_unhealthy_or_stale"); got != before {
		t.Fatalf("pending next report raised %d host alerts", got-before)
	}
	if withheld() {
		t.Fatal("pending next report withheld new work as unknown pressure")
	}
	assertSharedPressureHealth(t, q, m.ID, "healthy")
	clock.Advance(500 * time.Millisecond)
	report(.5)

	// A silent worker's pressure expires once the next report is overdue.
	clock.Advance(6500*time.Millisecond + 2200*time.Millisecond)
	check(t, q, other, nil)
	if !withheld() || firings(q, "host_unhealthy_or_stale") != before {
		t.Fatal("overdue pressure report did not withhold at once, or alerted before its evidence window")
	}
	clock.Advance(hostDegradedSustain(q.s.Machines[m.ID]))
	check(t, q, other, nil)
	if !withheld() || firings(q, "host_unhealthy_or_stale") != before+1 {
		t.Fatal("overdue pressure report did not alert and withhold")
	}
	assertSharedPressureHealth(t, q, m.ID, "unhealthy")
	report(.5)
	assertSharedPressureHealth(t, q, m.ID, "healthy")

	// A stopped sensor fails at the report whose sample reached validity, and
	// a failed probe sends none; both withhold immediately and alert once the
	// fault outlasts the host's evidence window.
	for i, age := range []float64{6.5, -1} {
		fault := func() {
			if age < 0 {
				m.SharedHostPressure = nil
				check(t, q, m, run)
			} else {
				report(age)
			}
		}
		fault()
		if !withheld() || firings(q, "host_unhealthy_or_stale") != before+1+i {
			t.Fatalf("pressure fault %d did not withhold at once, or alerted early", i)
		}
		clock.Advance(hostDegradedSustain(q.s.Machines[m.ID]))
		fault()
		if !withheld() || firings(q, "host_unhealthy_or_stale") != before+2+i {
			t.Fatalf("pressure fault %d did not alert and withhold", i)
		}
		assertSharedPressureHealth(t, q, m.ID, "unhealthy")
		report(.5)
	}
}
