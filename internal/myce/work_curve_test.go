package myce

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

func curveFixture(t *testing.T) (*Queue, *workerFakeClock, Machine, Extension, Spec) {
	t.Helper()
	q, initial := fixture(t)
	clock := &workerFakeClock{now: initial.Now()}
	q.clock = clock
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target.Platform = "cpu"
	v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("curves", "curve-host", "cpu", 8, 32)
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
	m.ExtensionFingerprint = "cpu-v1"
	r := newExtensionReport(e, &m, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "curve-check"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	return q, clock, m, e, Spec{Name: "count", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
}
func TestWorkCurveMemoryRecoversAfterQueueRestart(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "same component"
		if changed {
			name = "changed fingerprint"
		}
		t.Run(name, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			s.Needs.Memory = "auto"
			first := startObservation(t, q, m, s)
			clock.Advance(2 * time.Second)
			r := result(&first, true)
			host := 2.0
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			var database string
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
				t.Fatal(err)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(database, q.root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			// Initialization runs a mutation before any worker has re-reported.
			// This also exercises recovery of the flag persisted by old releases.
			for _, point := range reopened.s.WorkObservations {
				if point.StaleReason != "component unavailable" {
					t.Fatalf("expected unreported component: %+v", point)
				}
			}
			if changed {
				m.ExtensionFingerprint = "replacement-backend"
				for i := range m.ExtensionReports {
					m.ExtensionReports[i].Fingerprint = m.ExtensionFingerprint
				}
			}
			check(t, reopened, m, nil)
			for _, point := range reopened.s.WorkObservations {
				want := ""
				if changed {
					want = "driver or backend fingerprint changed"
				}
				if point.StaleReason != want {
					t.Fatalf("fresh component validation: want %q, got %+v", want, point)
				}
			}
			started := startObservation(t, reopened, m, s)
			next := job(t, reopened, started.ID)
			if !changed {
				if next.MemoryEnvelope.Source != "accepted peak + measured uncertainty" || next.Spec.Needs.HostMemoryGB != 2+minimumHostMemoryGB || next.Timing.LastAcceptedSeconds == nil {
					t.Fatalf("restart lost learned memory or timing: envelope=%+v timing=%+v", next.MemoryEnvelope, next.Timing)
				}
			} else if next.MemoryEnvelope.Source != physicalMemoryProbe {
				t.Fatalf("changed backend reused historical peak: %+v", next.MemoryEnvelope)
			}
		})
	}
}

func TestWorkCurveMemorySurvivesBudgetChange(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("fingerprint_changed=%v", changed), func(t *testing.T) {
			q, clock, m, s := unifiedAutoFixture(t, 0)
			m.CapabilityProfile.Accelerators[0].MemoryBytes = 128 << 30
			submit(t, q, s)
			first := assignedJob(machineCheck(t, q, m))
			if first == nil {
				t.Fatal("not assigned")
			}
			if err := q.Started(result(first, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			host, device := 2.0, 3.0
			r := result(first, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			learnedAt := clock.Now()
			// A real queue-owned budget change produces the old release's
			// allowance invalidation. Recover it through startup/check-in.
			if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "disable"}); err != nil {
				t.Fatal(err)
			}
			budget := 96.0
			if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &budget}); err != nil {
				t.Fatal(err)
			}
			m.HostMemoryBudgetGB = budget
			machineCheck(t, q, m)
			if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "resume"}); err != nil {
				t.Fatal(err)
			}
			var database string
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
				t.Fatal(err)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			q, err := Open(database, q.root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			m.HostMemoryBudgetGB = 96
			machineCheck(t, q, m)
			if changed {
				fingerprint := m.Devices[0].ExtensionFingerprint
				m.Devices[0].ExtensionFingerprint = "replacement-backend"
				machineCheck(t, q, m)
				m.Devices[0].ExtensionFingerprint = fingerprint
				machineCheck(t, q, m) // A real change remains sticky after return.
			}
			submit(t, q, s)
			next := assignedJob(machineCheck(t, q, m))
			if next == nil {
				t.Fatalf("post-change attempt not assigned: %+v", q.s.Machines[m.ID])
			}
			want := "accepted peak + measured uncertainty"
			if changed {
				want = "exploratory first run exploratory shared physical headroom; exclusive device"
			}
			if next.MemoryEnvelope.Source != want || !changed && next.MemoryEnvelope.HostGB != 5.125+2*minimumHostMemoryGB {
				t.Fatalf("post-change memory: %+v", next.MemoryEnvelope)
			}
			for _, p := range q.s.WorkObservations {
				if !p.MemoryRecordedAt.Equal(learnedAt) || p.MemoryPeak == nil || *p.MemoryPeak.HostGB != host {
					t.Fatalf("recovery changed learned evidence: %+v", p)
				}
			}
		})
	}
}

func TestWorkCurveFingerprintStalenessAndOOM(t *testing.T) {
	q, _, m, e, s := curveFixture(t)
	// Real job OOM has exact execution custody, forbids the record and learns no duration.
	j := startObservation(t, q, m, s)
	key := *q.s.Jobs[j.ID].Timing.Key
	if err := q.Complete(Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, OutOfMemory: true}); err != nil {
		t.Fatal(err)
	}
	if p := q.s.WorkObservations[identityHash(key)]; !p.Forbidden || p.LastSeconds != 0 {
		t.Fatal("ordinary OOM was not retained", p)
	}
	m.ExtensionFingerprint = "changed-driver"
	check(t, q, m, nil)
	for _, p := range q.s.WorkObservations {
		if !strings.Contains(p.StaleReason, "fingerprint changed") {
			t.Fatalf("no stale fingerprint reason: %+v", p)
		}
	}
	r := newExtensionReport(e, &m, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "new-check"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	job := startObservation(t, q, m, s)
	if got := q.s.Jobs[job.ID].Timing; got.LastAcceptedSeconds != nil {
		t.Fatal("new fingerprint reused a duration", got)
	}
}

// #210: memory-only observations (auto extension checks) carry memory_peak with
// zero legacy usage maxima; status curve lines must show the accepted peak.
func TestWorkCurveSummaryShowsMemoryPeak(t *testing.T) {
	host, device := 3.0, 1.5
	point := WorkObservation{Key: ObservationKey{Work: "check", Host: "host", Component: "cpu", Concurrency: 1}, MemoryPeak: &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}, Samples: 1}
	if line := point.Summary(); !strings.Contains(line, "peak host 3.00 GiB, device 1.50 GiB") || !strings.Contains(line, "last accepted duration unknown") {
		t.Fatalf("memory-only peak not shown: %s", line)
	}
	point.LastSeconds = 2
	if line := point.Summary(); !strings.Contains(line, "last accepted duration 2.00s") {
		t.Fatalf("last accepted duration not shown: %s", line)
	}
}

// A worker upgrade reports physical inventory before its asynchronous backend
// fingerprint probe completes. That heartbeat must not poison retained curves.
func TestWorkCurveMemorySurvivesIncompleteUpgradeInventory(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("backend_changed=%v", changed), func(t *testing.T) {
			q, clock, m, s := unifiedAutoFixture(t, 0)
			queued := submit(t, q, s)
			first := assignedJob(machineCheck(t, q, m))
			if first == nil || first.ID != queued.ID {
				t.Fatal("first attempt not assigned")
			}
			if err := q.Started(result(first, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			host, device := 2.0, 3.0
			r := result(first, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			learnedAt := clock.Now()
			fingerprint := m.Devices[0].ExtensionFingerprint
			previousSession := m.Session
			m.Session = ID()
			m.Devices[0].ExtensionFingerprint = ""
			if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Fresh: true, PreviousSession: previousSession}); err != nil {
				t.Fatal(err)
			}
			for _, point := range q.s.WorkObservations {
				if point.StaleReason != "component unavailable" || !point.MemoryRecordedAt.Equal(learnedAt) {
					t.Fatalf("incomplete inventory discarded learning: %+v", point)
				}
			}
			if changed {
				m.Devices[0].ExtensionFingerprint = "replacement-backend"
				machineCheck(t, q, m)
			}
			m.Devices[0].ExtensionFingerprint = fingerprint
			machineCheck(t, q, m)
			for _, point := range q.s.WorkObservations {
				want := ""
				if changed {
					want = "driver or backend fingerprint changed"
				}
				if point.StaleReason != want || !point.MemoryRecordedAt.Equal(learnedAt) {
					t.Fatalf("returned identity: want %q, got %+v", want, point)
				}
			}
			next := submit(t, q, s)
			assigned := assignedJob(machineCheck(t, q, m))
			if assigned == nil || assigned.ID != next.ID {
				t.Fatal("post-upgrade attempt not assigned")
			}
			want := "accepted peak + measured uncertainty"
			if changed {
				want = "exploratory first run exploratory shared physical headroom; exclusive device"
			}
			if err := q.Started(result(assigned, false)); err != nil {
				t.Fatal(err)
			}
			actual := job(t, q, assigned.ID)
			if actual.MemoryEnvelope.Source != want || !changed && actual.Timing.LastAcceptedSeconds == nil {
				t.Fatalf("post-upgrade learning: envelope=%+v timing=%+v", actual.MemoryEnvelope, actual.Timing)
			}
		})
	}
}

func TestWorkCurveHeartbeatLabelsDoNotRewriteMeasurements(t *testing.T) {
	q, workers := candidateCostFixture(t)
	drainAdmissionWait(t, q)
	reasons := map[string]string{}
	for id, p := range q.s.WorkObservations {
		reasons[id] = p.StaleReason
	}
	for n := range 4 {
		q.clock.(*workerFakeClock).Advance(time.Second)
		m := workers[n%len(workers)]
		_, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", n%len(workers)), Attempt: 1}, Session: m.Session, Started: true}}})
		if err != nil {
			t.Fatal(err)
		}
		drainAdmissionWait(t, q)
		for id, p := range q.s.WorkObservations {
			// The round-robin fixture deliberately ages other reporters past
			// their advertised cadence. Availability can change; identity cannot.
			want := reasons[id]
			if owner := q.s.Machines[p.Key.Host]; owner != nil && owner.Gone {
				want = "component unavailable"
			}
			if p.StaleReason != want {
				t.Fatalf("heartbeat label %s: want %q, got %q", id, want, p.StaleReason)
			}
			var durable WorkObservation
			if err := json.Unmarshal(durableQueueRecord(t, q.db, "work_observations/"+id), &durable); err != nil {
				t.Fatal(err)
			}
			if durable.StaleReason != "" || curveMeasurementDigest(durable) != curveMeasurementDigest(p) {
				t.Fatal("diagnostic label changed accepted measurement")
			}
		}
	}
}

func TestWorkCurvePermanentInvalidationSurvivesRestart(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(2 * time.Second)
	if err := q.Complete(result(&first, true)); err != nil {
		t.Fatal(err)
	}
	changed := cloneMachine(&m)
	changed.ExtensionFingerprint = "replacement-backend"
	check(t, q, changed, nil)
	for _, p := range q.s.WorkObservations {
		if p.StaleReason != "driver or backend fingerprint changed" {
			t.Fatal("missing identity invalidation")
		}
	}
	q = reopenQueue(t, q, clock)
	check(t, q, m, nil)
	for _, p := range q.s.WorkObservations {
		if p.StaleReason != "driver or backend fingerprint changed" {
			t.Fatal("restart forgot permanent invalidation")
		}
	}
}
