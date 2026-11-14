package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func requireIncident(t *testing.T, q *Queue, kind, key string, count int, active bool) {
	t.Helper()
	if got := alertsOf(q, kind); firings(q, kind) != count || q.s.Health.Conditions[key].Active != active {
		t.Fatalf("%s: events=%+v condition=%+v; want count=%d active=%v", kind, got, q.s.Health.Conditions[key], count, active)
	}
}

func TestHealthIncidentCheckpointRootReplay(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "cuda")
	key, kind := "storage/worker", "checkpoint_store_unavailable"
	check(t, q, m, nil)
	// Other root roles and short mount interruptions aren't this incident.
	m.UnavailableRoots = []UnavailableRoot{{Root: "model", Reason: "configured NAS mount missing"}}
	check(t, q, m, nil)
	clock.Advance(DefaultHealthStorageDelay)
	check(t, q, m, nil)
	requireIncident(t, q, kind, key, 0, false)
	m.UnavailableRoots[0].Root = "checkpoint"
	check(t, q, m, nil)
	clock.Advance(DefaultHealthStorageDelay - time.Second)
	check(t, q, m, nil)
	requireIncident(t, q, kind, key, 0, false)
	clock.Advance(time.Second)
	check(t, q, m, nil)
	check(t, q, m, nil)
	requireIncident(t, q, kind, key, 1, true)
	m.UnavailableRoots = nil
	check(t, q, m, nil)
	requireIncident(t, q, kind, key, 1, false)
	clock.Advance(healthAlertCooldown)
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint"}}
	check(t, q, m, nil)
	clock.Advance(DefaultHealthStorageDelay)
	check(t, q, m, nil)
	requireIncident(t, q, kind, key, 2, true)
}

func TestHealthIncidentLaunchRefusalReplay(t *testing.T) {
	for _, operation := range []string{"launch", "start_authorization", "start_handoff"} {
		t.Run(operation, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 4, 16)
			key, kind := "launch-refusals/host", "repeated_launch_refusals"
			complete := func(cause *RuntimeCause, started bool) {
				submit(t, q, cpuSpec(1, 1))
				assigned := check(t, q, m, nil).Job
				if assigned == nil {
					t.Fatal("missing assignment")
				}
				if started {
					if err := q.Started(result(assigned, false)); err != nil {
						t.Fatal(err)
					}
				}
				clock.Advance(time.Second)
				r := result(assigned, cause == nil)
				r.ExecutionCause = cause
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				// Exact replay must never be a second refusal. #1576 keeps
				// prelaunch capacity refusals queued, so their released copy
				// is stale rather than a replay of a terminal completion.
				replayErr := q.Complete(r)
				if cause != nil && cause.Class == launchRefused && !started {
					if !errors.Is(replayErr, ErrStale) {
						t.Fatalf("prelaunch replay: %v; want stale execution", replayErr)
					}
					if got := job(t, q, assigned.ID); got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
						t.Fatal("prelaunch refusal spent the loss budget", got)
					}
				} else if replayErr != nil {
					t.Fatal(replayErr)
				}
			}
			for range 3 {
				complete(nil, true)
				complete(&RuntimeCause{Operation: "start_authorization", Class: "io_error"}, false)
			}
			requireIncident(t, q, kind, key, 0, false)
			cause := &RuntimeCause{Operation: operation, Class: "admission_changed"}
			if operation == "launch" {
				cause.Class, cause.Predicate = launchRefused, "launch_authorization"
			}
			for i := range 3 {
				complete(cause, false)
				want := 0
				if i == 2 {
					want = 1
				}
				requireIncident(t, q, kind, key, want, want == 1)
			}
			clock.Advance(launchRefusalPeriod)
			check(t, q, m, nil)
			requireIncident(t, q, kind, key, 1, false)
		})
	}
}

func TestHealthIncidentStaleFlapReplay(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "cuda")
	key, kind := "stale-flaps/worker", "worker_stale_flapping"
	check(t, q, m, nil)
	freshness := q.machineLossTimeout(m.ID)
	// An ordinary healthy history, then one continuously quiet worker, is quiet.
	for range 3 {
		clock.Advance(freshness / 2)
		check(t, q, m, nil)
	}
	clock.Advance(q.machineLossTimeout(m.ID))
	for range 3 {
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	requireIncident(t, q, kind, key, 0, false)
	for i := range 2 {
		check(t, q, m, nil) // Same session, just like #661.
		clock.Advance(q.machineLossTimeout(m.ID))
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		requireIncident(t, q, kind, key, i, i == 1)
	}
	check(t, q, m, nil)
	clock.Advance(staleFlapPeriod)
	check(t, q, m, nil)
	requireIncident(t, q, kind, key, 1, false)
	// Planned leave doesn't seed the next burst of flaps.
	m.Leaving = true
	check(t, q, m, nil)
	clock.Advance(q.machineLossTimeout(m.ID))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	requireIncident(t, q, kind, key, 1, false)
}

func TestHealthIncidentXPUTeardownReplay(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	for i := range m.Devices {
		m.Devices[i].Platform = "xpu"
	}
	m.Devices[0].pciAddress = "0000:03:00.0"
	w := Worker{Machine: m, Clock: clock, DeviceFaultMatchers: map[string]DeviceFaultMatcher{"xpu": intelKernelFault}}
	w.control = &processControl{clock: clock}
	root := &workerStore{dir: t.TempDir()}
	var lines []KernelLogLine
	w.DeviceFaultSource = func(context.Context, time.Time) (KernelLogSnapshot, error) {
		return KernelLogSnapshot{BootID: "boot", Lines: lines}, nil
	}
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	machineCheck(t, q, w.Machine)
	key, kind := "teardown-faults/machine/xpu:fake-0", "xpu_repeated_teardown_faults"
	requireIncident(t, q, kind, key, 0, false)
	for i := range 3 {
		clock.Advance(10 * time.Minute)
		stop := clock.Now()
		j := Job{ID: fmt.Sprintf("job-%d", i), Attempt: 1, Session: m.Session, AllocatedDevices: []string{"0"}}
		w.control.set(&process{job: j, stopRequested: stop})
		w.control.remove(j)
		clock.Advance(5 * time.Second)
		lines = []KernelLogLine{{ID: fmt.Sprint(i), At: stop.Add(273 * time.Millisecond), Message: "xe 0000:03:00.0: [drm] Fault response: Unsuccessful -ENOENT"}}
		if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		machineCheck(t, q, w.Machine)
		machineCheck(t, q, w.Machine)
		want := 0
		if i == 2 {
			want = 1
		}
		requireIncident(t, q, kind, key, want, want == 1)
	}
	// The escalation provenance survives the worker's actual saved state.
	data, err := os.ReadFile(filepath.Join(root.dir, deviceFaultFile))
	if err != nil {
		t.Fatal(err)
	}
	var saved deviceQuarantine
	if err := json.Unmarshal(data, &saved); err != nil || !saved.Devices[deviceFaultKey(m.Devices[0])].RepeatedTeardown {
		t.Fatal("escalation not durable", err)
	}
	if err := saveWorkerJSON(root.dir, "clear-device-quarantine-0.json", deviceFaultClear{Device: deviceFaultKey(m.Devices[0]), BootID: saved.BootID, Event: saved.Devices[deviceFaultKey(m.Devices[0])].Event}); err != nil {
		t.Fatal(err)
	}
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	machineCheck(t, q, w.Machine)
	requireIncident(t, q, kind, key, 1, false)
}

func TestHealthIncidentReservationReplay(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprint("ready=", ready), func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			machineCheck(t, q, m)
			submit(t, q, exclusionSpec(m.ID, "0"))
			existing := assignedJob(machineCheck(t, q, m))
			if existing == nil {
				t.Fatal("missing conflicting attempt")
			}
			if err := q.Started(result(existing, false)); err != nil {
				t.Fatal(err)
			}
			bound := submit(t, q, exclusionSpec(m.ID, "0"))
			r, err := q.CreateReservation(reservationRequest("validation", "host=machine;devices=0", bound.ID))
			if err != nil {
				t.Fatal(err)
			}
			key, kind := "reservation-never-ready/validation", "reservation_never_ready"
			requireIncident(t, q, kind, key, 0, false)
			if ready {
				finish(t, q, existing, true)
				machineCheck(t, q, m)
				if q.s.Reservations[r.ID].ReadyAt == nil {
					t.Fatal("fixture never became ready")
				}
			}
			clock.Advance(2 * time.Hour)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			want := 1
			if ready {
				want = 0
			}
			requireIncident(t, q, kind, key, want, !ready)
			r = q.s.Reservations[r.ID]
			if _, err := q.ReleaseReservation(r.ID, r.Revision, "validation ended"); err != nil {
				t.Fatal(err)
			}
			requireIncident(t, q, kind, key, want, false)
		})
	}
}

func TestHealthIncidentLearnedEnvelopeReplay(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&first, true)
	peak := 26.0 // Measured need exceeds the 24 GiB allowance without retired percentage padding.
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	s.Needs.Memory, s.RequireTarget = "auto", true
	kind := "learned_envelope_exceeds_allowances"
	if len(alertsOf(q, kind)) != 0 {
		t.Fatal("fitting history alerted")
	}
	incidentAllowance(t, q, &m, 24)
	s.SubmissionKey = "learned-incident"
	queued := submit(t, q, s)
	if why := memoryEvidenceReason(t, q, queued); !strings.HasPrefix(why, memoryEvidenceRecovering+":") {
		t.Fatal("oversized learned submission lost typed wait", why)
	}
	if len(alertsOf(q, kind)) != 1 {
		t.Fatal("waiting learned demand did not alert", q.s.Health)
	}
	var key string
	for k := range q.s.Health.Conditions {
		if strings.HasPrefix(k, "learned-envelope/") {
			key = k
		}
	}
	if key == "" {
		t.Fatal("no retained waiting demand condition")
	}
	if retry := submit(t, q, s); retry.ID != queued.ID {
		t.Fatal("unchanged retry created a new job")
	}
	if assigned := assignedJob(machineCheck(t, q, m)); assigned != nil {
		t.Fatal("oversized learned work assigned", assigned.ID)
	}
	requireIncident(t, q, kind, key, 1, true)
	// A larger allowance recovers automatically without a resubmission.
	incidentAllowance(t, q, &m, 32)
	requireIncident(t, q, kind, key, 1, false)
	if assigned := assignedJob(machineCheck(t, q, m)); assigned == nil || assigned.ID != queued.ID {
		t.Fatal("waiting job did not recover automatically")
	}
	if q.s.Health.Conditions[key].Envelope != nil {
		t.Fatal("recovered probe retained")
	}
	// Cold/exploratory and declared oversize work aren't learned incidents.
	s.Needs.Memory, s.Needs.HostMemoryGB = "", 100
	s.SubmissionKey = "declared-incident"
	if _, err := q.Submit(s); err == nil {
		t.Fatal("declared oversize work admitted")
	}
	if len(alertsOf(q, kind)) != 1 {
		t.Fatal("declared sizing alerted as learning")
	}
}
