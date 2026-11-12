package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// Incident replay: lower-tier CPU jobs hold 17.2 GiB; signed RAM room is
// -0.8 GiB; a qualified serial fan-out needs 11.5 GiB and a healthy XPU.
func TestIncidentHostRAMPreemptionReplay(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		name := "quarantined"
		if healthy {
			name = "healthy"
		}
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			e := compositionExtension(t, false)
			e.Kinds = nil
			v := &e.Variants[0]
			v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "xpu"}
			v.Needs = ResourceNeeds{CPUCores: 4, Memory: "auto"}
			delete(v.Baselines, "kind:sample")
			v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			m := m3Machine()
			m.CapabilityProfile.LogicalCPUs = 16
			m.HostMemoryBudgetGB = 24
			m.HostAvailableMemoryGB = 24
			m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
			m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "xpu", MemoryBytes: 30 << 30}}
			for i := range m.Devices {
				m.Devices[i].Platform = "xpu"
				m.Devices[i].ExtensionFingerprint = "synthetic-xpu"
				m.Devices[i].TotalMemoryGB, m.Devices[i].BudgetGB, m.Devices[i].FreeMemoryGB = 30, 30, 30
				target := m.Devices[i].machine(m)
				report := newExtensionReport(e, &target, "extension:sample")
				report.Setup, report.Qualified, report.CheckID = true, true, "synthetic-check"
				m.Devices[i].ExtensionReports = []ExtensionReport{report}
			}
			machineCheck(t, q, m)
			var runs []*Job
			for i, ram := range []float64{6, 6, 5.2} {
				s := cpuSpec(2, 0)
				s.Project = "tests"
				s.Resumable = false
				s.Needs.HostMemoryGB = ram
				if i == 0 {
					s.Priority = "deadline"
					deadline := clock.Now().Add(time.Hour)
					s.Deadline = &deadline
				}
				low := submit(t, q, s)
				run := assignedJob(machineCheck(t, q, m, runs...))
				if run == nil || run.ID != low.ID {
					t.Fatal("CPU holder missing", i)
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				runs = append(runs, run)
			}
			// A host observation changes after admission; charges stay conservative.
			m.HostAvailableMemoryGB = 16.4
			for i := range m.Devices {
				m.Devices[i].Reason = "device_fault_xe"
				m.Devices[i].FreeMemoryGB = 0
			}
			machineCheck(t, q, m, runs...)
			s := Spec{Name: "production-piece", Project: "consumer", Priority: "production", Extension: extensionKey(e),
				Targets: []ExecutionTarget{v.Target}, Workload: &Workload{InputShape: "small-v1", Units: 10},
				FanOut: &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "piece"}}}}
			parent := submit(t, q, s)
			if err := q.update(func() error {
				target := m.Devices[1].machine(*q.s.Machines[m.ID])
				ordinary := extensionJobs(q.fanOutTemplate(q.s.Jobs[parent.ID]), &target, true)[0]
				probe := *ordinary
				probe.Machine, probe.Session, probe.AllocatedDevices = m.ID, target.Session, []string{"1"}
				key, why := q.workObservationKey(&probe)
				if key == nil {
					return errors.New(why)
				}
				host, device := 11.5-minimumHostMemoryGB, 4.5-minimumHostMemoryGB
				q.putWorkObservation(identityHash(key), WorkObservation{Key: *key, Source: "accepted work", Samples: 19,
					RecordedAt: clock.Now(), MemoryRecordedAt: clock.Now(), MemoryLayout: "separate",
					MemoryConfiguration: memoryConfiguration(probe.Spec.Needs, probe.Spec.Args, probe.Spec.Program),
					MemoryPeak:          &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, DeviceGB: &device, Events: &MemoryEventCounts{}},
					MemoryRange:         &MemoryPeakRange{Samples: 19, MinGB: host, MaxGB: host}})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if healthy {
				for i := range m.Devices {
					m.Devices[i].Reason = ""
					m.Devices[i].FreeMemoryGB = 30
				}
			}
			clock.Advance(time.Second)
			reply := machineCheck(t, q, m, runs...)
			q.mu.Lock()
			owner := q.s.Machines[m.ID]
			charged, debt := owner.HostMemoryInUseGB, owner.hostReservationDebtGB
			q.mu.Unlock()
			if math.Abs(charged-17.2) > 1e-6 || math.Abs(debt-.8) > 1e-6 {
				t.Fatalf("wrong replay ledger: charged=%g debt=%g", charged, debt)
			}
			for _, run := range runs {
				current := job(t, q, run.ID)
				stop := priorityStop(current)
				if healthy {
					if current.State != "stopping" || stop == nil || stop.Demand != parent.ID {
						t.Fatalf("healthy XPU did not preempt tier %s: state=%s stop=%+v parent=%s", run.Spec.Priority, current.State, stop, job(t, q, parent.ID).Reason)
					}
				} else if current.State != "running" || stop != nil {
					t.Fatalf("quarantine stopped useful CPU work: %+v", current)
				}
			}
			if assignedJob(reply) != nil {
				t.Fatal("admitted before exact release")
			}
			observed := admissionWaitAPI(t, q, parent.ID)
			if observed.Latest == nil || len(observed.Latest.Targets) == 0 || observed.Latest.NextPiece != 0 || observed.Latest.Attempt != 0 {
				t.Fatalf("missing pre-child parent evidence: %+v", observed)
			}
			planned := false
			retainedWithoutNewStop := false
			for _, target := range observed.Latest.Targets {
				if target.Device != "0" && target.Device != "1" {
					continue
				}
				if !slices.Contains(target.Predicates, "host_memory") || math.Abs(target.HostDebtGB-.8) > 1e-6 || math.Abs(target.RequiredHostGB-11.5) > 1e-6 {
					t.Fatalf("lost simultaneous host predicate/vector: %+v", target)
				}
				if healthy {
					planned = planned || target.Preemption.Planned || target.ReleaseCommitment
					if target.Preemption.Source == "planner" && (target.LastPlannerDecision == nil || target.Preemption != target.LastPlannerDecision.Preemption) {
						t.Fatalf("retained releases rewrote the recorded planner decision: %+v", target)
					}
					retainedWithoutNewStop = retainedWithoutNewStop || target.ReleaseCommitment && target.Preemption.Reason == "no_new_stop_needed" && !target.Preemption.Planned
					if target.Preemption.Planned && (target.LastPlannerDecision == nil || !target.LastPlannerDecision.Preemption.Considered || target.LastPlannerDecision.Preemption.Source != "planner") {
						t.Fatalf("lost real selected decision: %+v", target)
					}
					if target.Preemption.Considered && target.Preemption.Source != "planner" {
						t.Fatalf("projection claimed a decision: %+v", target)
					}
				} else if target.DeviceFault != "device_fault_xe" || !slices.Contains(target.Predicates, "gpu_memory_telemetry") || target.Preemption.Considered || target.Preemption.Planned || target.ReleaseCommitment {
					t.Fatalf("quarantine was not an independent blocker: %+v", target)
				}
				for _, run := range runs {
					i := slices.IndexFunc(target.Holders, func(h AdmissionWaitHolder) bool { return h.Job == run.ID && h.Attempt == run.Attempt })
					if i < 0 {
						t.Fatalf("missing exact holder %s: %+v", run.ID, target.Holders)
					}
					h := target.Holders[i]
					if h.Tier == nil || h.Tier.Tier != run.Spec.Priority || h.ChargedHostGB == nil || math.Abs(*h.ChargedHostGB-run.Spec.Needs.HostMemoryGB) > 1e-6 || h.EnforcementBasis != "started_worker_contract_not_direct_read" || h.SampleFresh || h.ForecastHostGB != nil {
						t.Fatalf("wrong holder policy, charge, enforcement or unknown sample: %+v", h)
					}
				}
			}
			if healthy && !planned {
				t.Fatal("selected releases absent from status")
			}
			if healthy {
				captured := false
				for _, target := range observed.Latest.Targets {
					d := target.AdmissionDecision
					captured = captured || d != nil && d.Outcome == AdmissionRejected && d.Stage == AdmissionPlacement && d.Predicate == predicateReleasePending
				}
				if !captured {
					t.Fatalf("missing actual placement rejection while selected releases remain pending: %+v", observed.Latest.Targets)
				}
			}
			if healthy && !retainedWithoutNewStop {
				t.Fatal("release wait lost the planner's no-new-stop decision")
			}
			if !healthy {
				// An arbitrary worker diagnostic suffix must never enter the wire
				// enum or escape redaction through this new nested payload.
				for i := range m.Devices {
					m.Devices[i].Reason = "device_fault_xe: test-secret-marker /private/test-marker"
				}
				machineCheck(t, q, m, runs...)
				safe := admissionWaitAPI(t, q, parent.ID)
				for _, target := range safe.Latest.Targets {
					if target.DeviceFault != "" {
						t.Fatal("unrecognized fault suffix was accepted as typed evidence")
					}
				}
				t.Log("quarantined devices: all three CPU holders preserved; no priority_stop")
				return
			}
			for _, run := range runs {
				finishInterrupted(t, q, run)
			}
			assertReleased := func() *AdmissionWaitSnapshot {
				t.Helper()
				latest := admissionWaitAPI(t, q, parent.ID).Latest
				if latest == nil || latest.ClosedAt != nil || len(latest.Targets) == 0 {
					t.Fatalf("missing open wait after release acknowledgement: %+v", latest)
				}
				for _, target := range latest.Targets {
					if target.ReleaseCommitment || len(target.Holders) != 0 {
						t.Fatalf("acknowledged releases still reported as outstanding: %+v", target)
					}
				}
				return latest
			}
			// A retained placement survives acknowledgements until assignment,
			// but its selected releases no longer constitute outstanding custody.
			assertReleased()
			m.HostAvailableMemoryGB = 1
			if assignedJob(machineCheck(t, q, m)) != nil {
				t.Fatal("production admitted under unrelated RAM pressure")
			}
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			ramBlocked := false
			for _, target := range assertReleased().Targets {
				// The replay measures serial targets; combined-device projections
				// can instead be blocked on an unknown memory envelope.
				if target.Device != "0" && target.Device != "1" {
					continue
				}
				ramBlocked = true
				if !slices.Contains(target.Predicates, "host_memory") {
					t.Fatalf("lost RAM blocker after release acknowledgement: %+v", target)
				}
			}
			if !ramBlocked {
				t.Fatal("missing measured serial targets under RAM pressure")
			}
			m.HostAvailableMemoryGB = 16.4
			clock.Advance(time.Second)
			next := assignedJob(machineCheck(t, q, m))
			if next == nil || next.Batch == nil || next.Batch.Parent != parent.ID {
				t.Fatalf("production not admitted after exact releases: %s", job(t, q, parent.ID).Reason)
			}
			closed := admissionWaitAPI(t, q, parent.ID)
			if closed.Latest.ClosedAt == nil || closed.Latest.ClosedReason != "wave_admitted" {
				t.Fatalf("parent wait did not close on admission: %+v", closed)
			}
			if !reflect.DeepEqual(observed.First, closed.First) {
				t.Fatal("admission mutated the first observation")
			}
			t.Log("healthy devices: deadline and both experiment CPU holders preempted; production admitted after exact release")
		})
	}
}

// Exercise both supported HTTP observations, including their redaction and
// immutable committed-generation paths. Read operations must not capture a new
// scheduler decision or mutate retained evidence.
func admissionWaitAPI(t *testing.T, q *Queue, id string) *AdmissionWaitEvidence {
	t.Helper()
	drainAdmissionWait(t, q)
	before, _ := json.Marshal(q.s.Jobs[id].AdmissionWait)
	handler := API{Queue: q, Token: "fixture"}.Handler()
	var found *AdmissionWaitEvidence
	for _, path := range []string{"/api/jobs/" + id, "/api/status?detail=full"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer fixture")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "test-secret-marker") || strings.Contains(w.Body.String(), "/private/test-marker") {
			t.Fatalf("private worker/payload text leaked through %s", path)
		}
		var got Job
		if strings.HasPrefix(path, "/api/jobs/") {
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
		} else {
			var snap Snapshot
			if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil {
				t.Fatal(err)
			}
			index := slices.IndexFunc(snap.Jobs, func(j *Job) bool { return j.ID == id })
			if index < 0 {
				t.Fatal("job missing from status", id)
			}
			got = *snap.Jobs[index]
		}
		if got.AdmissionWait == nil {
			t.Fatal("missing admission_wait", path)
		}
		if found != nil && !reflect.DeepEqual(found, got.AdmissionWait) {
			t.Fatalf("job/status evidence differs: %+v %+v", found, got.AdmissionWait)
		}
		found = got.AdmissionWait
	}
	after, _ := json.Marshal(q.s.Jobs[id].AdmissionWait)
	if !bytes.Equal(before, after) {
		t.Fatal("status read mutated durable evidence")
	}
	return found
}

// Test queues publish evidence synchronously (admission_wait_mode_test.go), so
// this is a no-op for them. Tests of the asynchronous builder flush committed
// pending captures with it; never advance scheduling time to wait for a
// goroutine, and never make a status read perform capture in production.
func drainAdmissionWait(t testing.TB, q *Queue) {
	t.Helper()
	b := &q.admissionWaitBuild
	for {
		b.mu.Lock()
		running, done, pending := b.running, b.done, b.pending
		if !running {
			b.next = time.Time{}
		}
		b.mu.Unlock()
		if running {
			select {
			case <-done:
			case <-t.Context().Done():
				t.Fatal("admission wait build did not finish")
			}
		} else if pending != nil {
			q.enqueueAdmissionWait(pending)
		} else {
			return
		}
	}
}

func TestAdmissionWaitLifecycleAndRestart(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	s.Run.Command = []string{"echo", "test-secret-marker", "/private/test-marker"}
	parent := submit(t, q, s)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	first := admissionWaitAPI(t, q, parent.ID)
	detached := admissionWaitAPI(t, q, parent.ID)
	detached.First.Predicate = "caller mutation"
	if admissionWaitAPI(t, q, parent.ID).First.Predicate == "caller mutation" {
		t.Fatal("HTTP reply aliases retained state")
	}
	if first.Latest.Predicate != "no_qualified_target" {
		t.Fatalf("unexpected waiting state: %+v", first)
	}
	clock.Advance(time.Second)
	paused, err := q.PauseJob(parent.ID, PauseJobRequest{Reason: "owner pause"})
	if err != nil {
		t.Fatal(err)
	}
	if paused.AdmissionWait.Latest.ClosedAt == nil || !paused.AdmissionWait.Latest.ClosedAt.Equal(clock.Now()) {
		t.Fatal("pause reply did not close evidence")
	}
	q = reopenVariants(t, q, clock)
	if got := admissionWaitAPI(t, q, parent.ID); got.Latest.ClosedReason != "paused" || !reflect.DeepEqual(first.First, got.First) {
		t.Fatalf("restart lost closed evidence: %+v", got)
	}
	clock.Advance(time.Second)
	if _, err = q.ResumeJob(parent.ID, paused.PauseRevision); err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, parent.ID); got.Latest.ClosedAt != nil || !got.Latest.At.Equal(clock.Now()) {
		t.Fatal("resume did not reopen wait")
	}
	clock.Advance(time.Second)
	if err = q.Cancel(parent.ID); err != nil {
		t.Fatal(err)
	}
	cancelled := admissionWaitAPI(t, q, parent.ID)
	if cancelled.Latest.ClosedReason != "cancelled" || cancelled.Latest.ClosedAt == nil {
		t.Fatal("cancel did not close wait")
	}
	// Cancellation's terminal job is no longer in the live-job index.
	q = reopenVariants(t, q, clock)
	if got := admissionWaitAPI(t, q, parent.ID); !reflect.DeepEqual(got, cancelled) {
		t.Fatal("terminal restart lost evidence")
	}

	next := submit(t, q, s)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	// A zero-duration queued episode may legitimately have unknown evidence
	// when assignment overtakes background capture. This fixture needs a
	// captured wait to verify its later closure, so observe it before assigning.
	waiting := admissionWaitAPI(t, q, next.ID)
	if waiting.Latest == nil || waiting.Latest.Predicate != "no_qualified_target" || waiting.Latest.ClosedAt != nil {
		t.Fatalf("missing pre-assignment wait: %+v", waiting)
	}
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != next.ID {
		t.Fatal("production not assigned")
	}
	if err = q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	started := admissionWaitAPI(t, q, next.ID)
	if started.Latest == nil || started.Latest.ClosedAt == nil || started.Latest.ClosedReason != "assigned" || !reflect.DeepEqual(waiting.First, started.First) {
		t.Fatal("start did not retain closed wait")
	}
	// Terminal failure must close even without an intervening status read.
	expiry := s
	until := clock.Now().Add(time.Second)
	expiry.GiveUpAt = &until
	expired := submit(t, q, expiry)
	drainAdmissionWait(t, q)
	clock.Advance(time.Second)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, expired.ID); got.Latest.ClosedReason != "failed" || got.Latest.ClosedAt == nil {
		t.Fatal("terminal failure left admission open")
	}
	// Failed transactions cannot publish speculative wait snapshots.
	before, _ := json.Marshal(q.s.Jobs[next.ID].AdmissionWait)
	abort := errors.New("abort transaction")
	if err = q.update(func() error { j := q.touchJob(next.ID); j.State = "queued"; q.maintain(); return abort }); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	after, _ := json.Marshal(admissionWaitAPI(t, q, next.ID))
	if !bytes.Equal(before, after) {
		t.Fatal("rollback published speculative evidence")
	}
}

// Distinct transitions consume measured bytes, not a deployment-sized sample cap.
func TestAdmissionWaitRetentionUsesMeasuredBudget(t *testing.T) {
	q, clock := fixture(t)
	_ = q
	first := &AdmissionWaitSnapshot{At: clock.Now(), Predicate: "dependency"}
	evidence := retainAdmissionWait(nil, first, 0, true)
	clock.Advance(time.Second)
	second := &AdmissionWaitSnapshot{At: clock.Now(), Predicate: "admission"}
	evidence = retainAdmissionWait(evidence, second, 0, true)
	clock.Advance(time.Second)
	third := &AdmissionWaitSnapshot{At: clock.Now(), Predicate: "no_qualified_target"}
	evidence = retainAdmissionWait(evidence, third, 0, true)
	if evidence.First != first || evidence.Latest != third || len(evidence.Transitions) != 0 || evidence.Dropped != 1 || !evidence.BudgetUnknown {
		t.Fatalf("unknown capacity must preserve endpoints and declare lost coverage: %+v", evidence)
	}
	// A measured grant fits this one actual encoded transition; no fixed row count.
	bytes, _ := json.Marshal([]*AdmissionWaitSnapshot{second})
	evidence = retainAdmissionWait(&AdmissionWaitEvidence{First: first, Latest: second}, third, uint64(len(bytes)), false)
	if len(evidence.Transitions) != 1 || evidence.BudgetUnknown {
		t.Fatalf("measured capacity failed to retain transition: %+v", evidence)
	}
	// Sample aging updates latest values without creating a membership transition.
	aged := *third
	aged.At = aged.At.Add(time.Second)
	if !sameAdmissionWait(third, &aged) {
		t.Fatal("clock age manufactured a new blocker transition")
	}
}

func TestAdmissionWaitPieceAndRetry(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 2)
	s.Priority = "production"
	parent := submit(t, q, s)
	observed := admissionWaitAPI(t, q, parent.ID)
	target := observed.Latest.Targets[0]
	if target.Concurrency != 1 || target.RequiredCores != 1 || target.RequiredHostGB < 1 || target.Resolution != "planner_candidate" || target.LastPlannerDecision == nil || target.LastPlannerDecision.Concurrency != 1 {
		t.Fatalf("status lost selected piece vector: %+v", target)
	}
	run := check(t, q, m, nil).Job
	if run == nil || run.Batch == nil || run.Batch.Concurrency != 1 {
		t.Fatal("missing piece")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	// A worker loss is retained as an exact old execution until release; the
	// waiting retry keeps its piece and resource vector.
	if err := q.update(func() error { q.retry(q.touchJob(run.ID), "worker lost"); q.maintain(); return nil }); err != nil {
		t.Fatal(err)
	}
	waiting := admissionWaitAPI(t, q, run.ID)
	if waiting.Latest.Attempt != run.Attempt {
		t.Fatal("retry evidence lost attempt identity")
	}
	if len(waiting.Latest.Targets) == 0 {
		t.Fatal("missing retry piece target")
	}
	target = waiting.Latest.Targets[0]
	if target.RequiredCores != 1 || target.RequiredHostGB < 1 {
		t.Fatalf("retry lost its resource vector: %+v", target)
	}
	// Supported exact release retires old custody and permits the same piece.
	ack := result(run, false)
	ack.ReleaseOnly = true
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	retry := check(t, q, m, nil).Job
	if retry == nil || retry.ID != run.ID || retry.Attempt != run.Attempt+1 || retry.Batch.Concurrency != 1 {
		t.Fatalf("piece retry did not resume: %+v", retry)
	}
	if got := admissionWaitAPI(t, q, retry.ID); got.Latest.ClosedAt == nil {
		t.Fatal("retry wait did not close")
	}
	if !reflect.DeepEqual(observed.First, admissionWaitAPI(t, q, parent.ID).First) {
		t.Fatal("child retry rewrote parent's first snapshot")
	}
}

func TestAdmissionWaitPublicationFencesLifecycle(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	j := submit(t, q, s)
	// Capture an actual committed wait, then let a competing lifecycle commit
	// win before its off-lock evidence is ready to publish.
	drainAdmissionWait(t, q)
	q.mu.Lock()
	capture := q.freezeAdmissionWait()
	q.mu.Unlock()
	if capture == nil {
		t.Fatal("missing committed capture")
	}
	stale := admissionWaitAPI(t, q, j.ID)
	clock.Advance(time.Second)
	if _, err := q.PauseJob(j.ID, PauseJobRequest{Reason: "owner pause"}); err != nil {
		t.Fatal(err)
	}
	closed := admissionWaitAPI(t, q, j.ID)
	if err := q.commitAdmissionWait(capture, map[string]*AdmissionWaitEvidence{j.ID: stale}); err != nil {
		t.Fatal(err)
	}
	got := admissionWaitAPI(t, q, j.ID)
	if !reflect.DeepEqual(got.Latest, closed.Latest) || got.CaptureGaps != closed.CaptureGaps+1 || got.DeferredAt == nil {
		t.Fatal("stale off-lock publication overwrote lifecycle or hid its gap")
	}
}

func TestAdmissionWaitCaptureYieldsAndRetainedHistoryShrinks(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	j := submit(t, q, s)
	drainAdmissionWait(t, q)
	// Learn an ordinary-control service quantum, then make observational work
	// consume it. Queue time is independent and never advances here.
	limit := newObservationLimit(clock, q)
	limit.pressure.mu.Lock()
	limit.pressure.baseline = 1
	limit.pressure.samples = 1
	limit.pressure.mu.Unlock()
	measurement := &fakeClock{now: clock.Now()}
	q.holdClock = coldProbeAdvancingClock{measurement}
	before := clock.Now()
	if err := q.update(func() error {
		// Changed demand requires a new explanation; an unchanged wait should
		// skip the optional build entirely, even when the control path is busy.
		q.touchJob(j.ID).Spec.Needs.CPUCores++
		q.admissionWaitPending = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	drainAdmissionWait(t, q)
	q.holdClock = nil
	gaps := admissionWaitAPI(t, q, j.ID)
	if gaps.CaptureGaps == 0 || gaps.DeferredAt == nil || !clock.Now().Equal(before) {
		t.Fatal("expensive capture did not yield explicitly or changed queue time")
	}
	limit.pressure.mu.Lock()
	limit.pressure.baseline = 0
	limit.pressure.samples = 0
	limit.pressure.mu.Unlock()
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, j.ID); got.DeferredAt != nil || got.CaptureGaps != gaps.CaptureGaps {
		t.Fatal("fresh capture lost coverage gap")
	}
	// Use a measured grant large enough for one known transition, then close the
	// job. The general retention path must shed that history under later pressure.
	first := &AdmissionWaitSnapshot{At: clock.Now(), Predicate: "dependency"}
	middle := &AdmissionWaitSnapshot{At: clock.Now().Add(time.Second), Predicate: "admission"}
	latest := &AdmissionWaitSnapshot{At: clock.Now().Add(2 * time.Second), Predicate: "no_qualified_target"}
	encoded, _ := json.Marshal([]*AdmissionWaitSnapshot{middle})
	evidence := retainAdmissionWait(&AdmissionWaitEvidence{First: first, Latest: middle}, latest, uint64(len(encoded)), false)
	if evidence.HistoryBytes != uint64(len(encoded)) || len(evidence.Transitions) != 1 {
		t.Fatal("optional history grant lost byte accounting")
	}
	clock.Advance(2 * time.Second)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { q.touchJob(j.ID).AdmissionWait = evidence; return nil }); err != nil {
		t.Fatal(err)
	}
	closed := admissionWaitAPI(t, q, j.ID)
	q.evidenceMaintenance.capacity = func(context.Context) (RetentionCapacity, error) {
		return RetentionCapacity{MemoryAvailableBytes: 1, DiskAvailableBytes: 1}, nil
	}
	drainEvidence(t, q)
	shrunk := admissionWaitAPI(t, q, j.ID)
	if len(shrunk.Transitions) != 0 || shrunk.HistoryBytes != 0 || shrunk.Dropped != closed.Dropped+1 || !reflect.DeepEqual(shrunk.First, closed.First) || !reflect.DeepEqual(shrunk.Latest, closed.Latest) {
		t.Fatalf("pressure lost endpoints or failed to shed optional history: %+v", shrunk)
	}
}

// #884: a healthy pinned worker really considers a deadline job at attempt 0.
// Tools fail before network isolation and CPU capacity; projections must never
// claim that either later predicate was the captured rejection.
func TestAdmissionWaitDeadlineCheckinDecision(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
	m.Capabilities = []string{"native_isolation_seatbelt", "native_network"}
	m.ToolInventory = map[string][]string{"": {"missing-tool"}}
	check(t, q, m, nil)
	s := cpuSpec(2, 1)
	s.Priority = "deadline"
	deadline := clock.Now().Add(24 * time.Hour)
	s.Deadline = &deadline
	s.AllowedMachines = []string{m.ID}
	s.Needs.Tools, s.Needs.Network = []string{"missing-tool"}, true
	s.Isolation = "sandboxed"
	s.Run.Command = []string{"echo", "test-secret-marker", "/private/test-marker"}
	j := submit(t, q, s)
	m.ToolInventory = nil
	m.Capabilities = []string{"native_isolation_seatbelt"}
	clock.Advance(3 * time.Hour)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("ineligible deadline assigned")
	}
	if got := job(t, q, j.ID); got.Attempt != 0 || got.State != "queued" {
		t.Fatalf("unexpected attempt: %+v", got)
	}
	evidence := admissionWaitAPI(t, q, j.ID)
	assertDecision := func(e *AdmissionWaitEvidence) {
		t.Helper()
		if e.Latest == nil || len(e.Latest.Targets) != 1 {
			t.Fatalf("missing decision target: %+v", e)
		}
		target := e.Latest.Targets[0]
		d := target.AdmissionDecision
		if d == nil || d.Outcome != AdmissionRejected || d.Stage != AdmissionEligibility || d.Predicate != predicateTools || d.CheckinMachine != m.ID || d.CheckinSession != m.Session || !d.At.Equal(clock.Now()) || d.OpportunitySeconds <= 0 {
			t.Fatalf("wrong actual rejection: %+v", d)
		}
	}
	assertDecision(evidence)
	// HTTP buffers and subsequent captures cannot mutate the previously read fact.
	evidence.Latest.Targets[0].AdmissionDecision.Predicate = "caller mutation"
	assertDecision(admissionWaitAPI(t, q, j.ID))
	before := admissionWaitAPI(t, q, j.ID)
	q = reopenVariants(t, q, clock)
	if got := admissionWaitAPI(t, q, j.ID); !reflect.DeepEqual(got, before) {
		t.Fatal("restart lost actual decision")
	}
	// Resume/retry may reuse diagnostic evidence, but only the next real check-in
	// can record a new decision. Closed snapshots preserve their historical fact.
	paused, err := q.PauseJob(j.ID, PauseJobRequest{Reason: "owner pause"})
	if err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, j.ID); got.Latest.ClosedReason != "paused" || got.Latest.ClosedAt == nil {
		t.Fatal("pause left wait open")
	}
	if _, err = q.ResumeJob(j.ID, paused.PauseRevision); err != nil {
		t.Fatal(err)
	}
	if err = q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, j.ID); got.Latest.ClosedReason != "cancelled" || got.Latest.ClosedAt == nil {
		t.Fatal("cancel left wait open")
	}
}

func TestAdmissionWaitExperimentSelectionAndLifecycle(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	check(t, q, m, nil)
	holder := submit(t, q, cpuSpec(1, 1))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != holder.ID {
		t.Fatal("missing holder")
	}
	s := cpuSpec(1, 1)
	s.Needs.Tools = []string{"missing-tool"}
	j := submit(t, q, s)
	clock.Advance(time.Second)
	check(t, q, m, run)
	skipped := admissionWaitAPI(t, q, j.ID)
	d := skipped.Latest.Targets[0].AdmissionDecision
	if d == nil || d.Outcome != AdmissionNotConsidered || d.Stage != AdmissionCheckin || d.Predicate != predicateAttemptReports {
		t.Fatalf("invented consideration: %+v", d)
	}
	finish(t, q, run, true)
	clock.Advance(time.Second)
	check(t, q, m, nil)
	blocked := admissionWaitAPI(t, q, j.ID)
	d = blocked.Latest.Targets[0].AdmissionDecision
	if d == nil || d.Outcome != AdmissionRejected || d.Predicate != predicateTools {
		t.Fatalf("experiment rejection missing: %+v", d)
	}
	// Repairing reported eligibility admits the same job and closes its wait.
	m.ToolInventory = map[string][]string{"": {"missing-tool"}}
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("eligible experiment not assigned")
	}
	started := admissionWaitAPI(t, q, j.ID)
	if started.Latest.ClosedReason != "assigned" || started.Latest.ClosedAt == nil {
		t.Fatal("assignment left wait open")
	}
	finish(t, q, assigned, true)
	if got := admissionWaitAPI(t, q, j.ID); got.Latest.ClosedAt == nil {
		t.Fatal("terminal job left wait open")
	}
	// An unstarted wait can also terminate without ever assigning an attempt.
	expiring := s
	until := clock.Now().Add(2 * time.Second)
	expiring.GiveUpAt = &until
	next := submit(t, q, expiring)
	m.ToolInventory = nil
	clock.Advance(time.Second)
	check(t, q, m, nil)
	admissionWaitAPI(t, q, next.ID)
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := admissionWaitAPI(t, q, next.ID); got.Latest.ClosedReason != "failed" || got.Latest.ClosedAt == nil {
		t.Fatal("terminal failure left wait open")
	}
}
