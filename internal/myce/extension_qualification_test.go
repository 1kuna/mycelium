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

func qualificationFixture(t *testing.T) (*Queue, Worker, Extension, *Job, EnvironmentRecipe, []ExtensionQualificationPlan) {
	t.Helper()
	e := compositionExtension(t, false)
	q, w := prepareTestWorker(t, e)
	w.Machine.Device = "0"
	w.Machine.HostMemoryMeasured, w.Machine.HostAvailableMemoryGB = true, 32
	demand := submit(t, q, Spec{Name: "demand", Project: "test", Extension: extensionKey(e)})
	cold := check(t, q, w.Machine, nil)
	if cold.Job != nil || len(cold.ExtensionQualificationPlan) != 0 || len(cold.EnvironmentPlan) != 1 {
		t.Fatal("cold demand bypassed preparation", cold)
	}
	prepare := cold.EnvironmentPlan[0]
	if err := w.prepareRegisteredExtension(context.Background(), prepare); err != nil {
		t.Fatal(err)
	}
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	ready := check(t, q, w.Machine, nil)
	if ready.Job != nil || len(ready.ExtensionQualificationPlan) != len(extensionWorks(e)) {
		t.Fatal("missing work-type qualification offers", ready)
	}
	return q, w, e, demand, prepare, ready.ExtensionQualificationPlan
}

func qualificationJob(t *testing.T, q *Queue, key string) *Job {
	t.Helper()
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range snapshot.Jobs {
		if j.Spec.SubmissionKey == key {
			return j
		}
	}
	t.Fatal("missing internal check", key)
	return nil
}

func reconcileQualification(t *testing.T, w Worker, plan ExtensionQualificationPlan) {
	t.Helper()
	if err := w.reconcileExtensionQualification(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
}

func assertNativeCheckFinding(t *testing.T, text, checkID string) {
	t.Helper()
	for _, part := range []string{checkID, "attempt 1", "native_stage_failed", "ModuleNotFoundError", "example.stage"} {
		if !strings.Contains(text, part) {
			t.Fatalf("qualification finding missing %q: %s", part, text)
		}
	}
	for _, part := range []string{"private-value", "Traceback", "/private/source.py", "bad/path"} {
		if strings.Contains(text, part) {
			t.Fatalf("qualification finding exposed %q: %s", part, text)
		}
	}
}

func TestExtensionFirstDemandQualification(t *testing.T) {
	t.Run("cold adoption accepted comparison and warm evidence", func(t *testing.T) {
		q, w, e, demand, prepare, plans := qualificationFixture(t)
		for _, plan := range plans {
			reconcileQualification(t, w, plan)
			first := qualificationJob(t, q, plan.Report.CheckID)
			if plan.Report.Work == demand.Spec.workType() {
				got, err := q.jobSnapshot(demand.ID)
				if err != nil || got.Hold == nil || !strings.Contains(got.Hold.Reason, "its native check "+ShortID(first.ID)) ||
					!strings.Contains(got.Hold.Reason, "is queued") || !strings.Contains(got.Hold.Reason, "queued native check job") {
					t.Fatalf("status missed the submitted check: %+v (%v)", got.Hold, err)
				}
			}
			// Repeated demand and preparation retain the persisted invocation.
			reconcileQualification(t, w, plan)
			if err := w.prepareRegisteredExtension(context.Background(), prepare); err != nil {
				t.Fatal(err)
			}
			if replay := qualificationJob(t, q, plan.Report.CheckID); replay.ID != first.ID {
				t.Fatal("repeated offer created another invocation")
			}
			w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
			assigned := check(t, q, w.Machine, nil).Job
			if assigned == nil || assigned.ID != first.ID || assigned.Spec.ExtensionCheck == nil {
				t.Fatalf("exact check not assigned: assignment=%+v check=%+v machine=%+v", assigned, qualificationJob(t, q, plan.Report.CheckID), q.s.Machines[w.Machine.ID])
			}
			if err := q.Started(result(assigned, false)); err != nil {
				t.Fatal(err)
			}
			scratch := assigned.Spec.ExtensionCheck.Directory
			if err := os.WriteFile(filepath.Join(scratch, "actual"), []byte("[99]"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := w.validateExtensionCheckOutput(*assigned); err == nil {
				t.Fatal("ordinary runner comparison accepted incorrect output")
			}
			if err := os.WriteFile(filepath.Join(scratch, "actual"), e.Files["baseline/output"], 0600); err != nil {
				t.Fatal(err)
			}
			if err := w.validateExtensionCheckOutput(*assigned); err != nil {
				t.Fatal(err)
			}
			q.clock.(*fakeClock).Advance(time.Second)
			finish(t, q, assigned, true)
		}
		for _, plan := range plans {
			reconcileQualification(t, w, plan)
			scratch := filepath.Join(extensionCache(w.ExtensionRoot, e, "fake"), plan.Report.CheckID)
			if _, err := os.Stat(scratch); !os.IsNotExist(err) {
				t.Fatal("released scratch retained", err)
			}
		}
		w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
		if qualifiedExtension(demand, &w.Machine) == nil {
			t.Fatal("accepted check did not qualify useful work")
		}
		warm := check(t, q, w.Machine, nil)
		if warm.Job == nil || warm.Job.ID != demand.ID || len(warm.ExtensionQualificationPlan) != 0 {
			t.Fatal("warm demand failed to reuse evidence", warm)
		}
		finish(t, q, warm.Job, true)
		submit(t, q, Spec{Name: "warm-repeat", Project: "test", Extension: extensionKey(e)})
		if repeat := check(t, q, w.Machine, nil); repeat.Job == nil || repeat.Job.Spec.ExtensionCheck != nil || len(repeat.ExtensionQualificationPlan) != 0 {
			t.Fatal("warm repeat scheduled another native check", repeat)
		}
		changed := w.Machine
		changed.ExtensionFingerprint = "new-device-fingerprint"
		if qualifiedExtension(demand, &changed) != nil {
			t.Fatal("warm evidence survived a changed device identity")
		}
	})

	t.Run("reconnect and persisted queue adopt pending check", func(t *testing.T) {
		q, w, _, _, _, plans := qualificationFixture(t)
		plan := plans[0]
		reconcileQualification(t, w, plan)
		first := qualificationJob(t, q, plan.Report.CheckID)
		previous := w.Machine.Session
		w.Machine.Session = "reconnected"
		w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
		if _, err := q.Checkin(Checkin{Machine: w.Machine, Fresh: true, PreviousSession: previous}); err != nil {
			t.Fatal(err)
		}
		reconcileQualification(t, w, plan)
		if replay := qualificationJob(t, q, plan.Report.CheckID); replay.ID != first.ID {
			t.Fatal("reconnect duplicated check")
		}
		var marker []byte
		if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&marker); err != nil {
			t.Fatal(err)
		}
		data, err := readQueueState(q.db, marker)
		if err != nil {
			t.Fatal(err)
		}
		var restored state
		if err := json.Unmarshal(data, &restored); err != nil {
			t.Fatal(err)
		}
		record := restored.Submissions[submissionDigest("extension-check", plan.Report.CheckID)]
		if record == nil || record.JobID != first.ID || automaticExtensionCheck(restored.Jobs[first.ID]) == nil {
			t.Fatal("SQLite lost invocation identity")
		}
	})

	for _, outcome := range []string{"failed", "prelaunch refusal", "prelaunch resource limit", "preempted during startup", "unproven refusal", "stale refusal", "cancelled", "stale", "maintenance", "demand cancelled", "identity changed"} {
		t.Run(outcome, func(t *testing.T) {
			q, w, e, demand, _, plans := qualificationFixture(t)
			plan := plans[0]
			for _, p := range plans {
				if p.Report.Work == demand.Spec.workType() {
					plan = p // the useful job's own work, so its outcome decides that job
				}
			}
			reconcileQualification(t, w, plan)
			w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
			assigned := check(t, q, w.Machine, nil).Job
			if assigned == nil || assigned.Spec.ExtensionCheck == nil {
				t.Fatalf("check not admitted: check=%+v machine=%+v", qualificationJob(t, q, plan.Report.CheckID), q.s.Machines[w.Machine.ID])
			}
			clock := q.clock.(*fakeClock)
			clock.Advance(time.Second)
			retryable := outcome == "prelaunch refusal" || outcome == "prelaunch resource limit" || outcome == "preempted during startup"
			switch outcome {
			case "failed":
				if err := q.Started(result(assigned, false)); err != nil {
					t.Fatal(err)
				}
				for _, text := range []string{
					"{\"code\":\"native_stage_failed\",\"exception_type\":\"ModuleNotFoundError\",",
					"\"missing_module\":\"example.stage\",\"stderr\":\"private-value\"}\nTraceback: /private/source.py\n",
					"{\"code\":\"bad/path\",\"missing_module\":\"ignored\"}\n",
				} {
					if err := q.AppendLog(LogChunk{Result: result(assigned, false), Text: text}); err != nil {
						t.Fatal(err)
					}
				}
				finish(t, q, assigned, false)
			case "prelaunch refusal", "unproven refusal", "stale refusal":
				r := result(assigned, false)
				r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused, Predicate: "launch_authorization", PrelaunchRefused: true}
				if outcome != "unproven refusal" {
					r.ExecutionEvidence = unstartedExecution(assigned.Attempt)
					if outcome == "stale refusal" {
						r.ExecutionEvidence.Attempt++
					}
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			case "prelaunch resource limit":
				r := result(assigned, false)
				r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit"}
				r.ExecutionEvidence = unstartedExecution(assigned.Attempt)
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			case "preempted during startup":
				for attempt := 1; attempt <= 16; attempt++ {
					if assigned.ID != qualificationJob(t, q, plan.Report.CheckID).ID || assigned.Attempt != attempt {
						t.Fatalf("capacity retry changed the check: %s attempt %d", assigned.ID, assigned.Attempt)
					}
					r := result(assigned, false)
					r.Interrupted = true
					r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity"}
					r.ExecutionEvidence = unstartedExecution(attempt)
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					got := job(t, q, assigned.ID)
					last := got.AttemptHistory[len(got.AttemptHistory)-1]
					if got.State != "queued" || last.Reason != launchCapacityWaitReason || got.Losses != 0 || !got.ReleaseConfirmed {
						t.Fatalf("capacity refusal outcome: %s, %s, losses %d, released %t", got.State, last.Reason, got.Losses, got.ReleaseConfirmed)
					}
					clock.Advance(time.Second)
					w.Machine.HostAvailableMemoryGB += 0.125
					assigned = check(t, q, w.Machine, nil).Job
					if assigned == nil {
						t.Fatal("check not reassigned after new capacity evidence")
					}
				}
				if err := q.update(func() error {
					production := &Job{ID: "production-demand", Spec: Spec{Priority: "production"}}
					q.stopBatchFor(q.s.Jobs[assigned.ID], production)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				r := result(assigned, false)
				r.ReleaseOnly = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, assigned.ID)
				last := got.AttemptHistory[len(got.AttemptHistory)-1]
				if got.Attempt != 17 || !got.ReleaseConfirmed || got.StartedAt != nil || last.PriorityStop == nil || !last.Interrupted || last.ExecutionCause != nil {
					t.Fatalf("expected exact release after a priority stop before Started: %+v", last)
				}
			case "cancelled":
				if err := q.Cancel(assigned.ID); err != nil {
					t.Fatal(err)
				}
				finish(t, q, assigned, true)
				reconcileQualification(t, w, plan)
				w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
				for _, offered := range check(t, q, w.Machine, nil).ExtensionQualificationPlan {
					if offered.Report.Work == plan.Report.Work {
						t.Fatal("user-cancelled check was replaced", offered)
					}
				}
				if got := job(t, q, assigned.ID); got.State != "cancelled" || !got.ReleaseConfirmed {
					t.Fatal("user cancellation did not remain terminal", got.State, got.Reason)
				}
			case "stale":
				stale := result(assigned, true)
				stale.Attempt++
				if err := q.Complete(stale); !errors.Is(err, ErrStale) {
					t.Fatal("stale completion accepted", err)
				}
			case "maintenance":
				if _, err := q.ChangeHostMaintenance(hostID(&w.Machine), HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
					t.Fatal(err)
				}
				if err := q.AuthorizeLaunch(HostLaunch{Machine: assigned.Machine, Session: assigned.Session, Kind: "batch", Key: assigned.ID, Attempt: "1"}); !errors.Is(err, ErrStale) {
					t.Fatal("maintenance allowed check launch", err)
				}
				finish(t, q, assigned, true)
			case "demand cancelled":
				if err := q.Cancel(demand.ID); err != nil {
					t.Fatal(err)
				}
				if !cancelled(q.s.Jobs[assigned.ID]) {
					t.Fatal("demand cancellation did not fence its in-flight check")
				}
				finish(t, q, assigned, true)
			case "identity changed":
				w.Machine.ExtensionFingerprint = "changed-native-identity"
				check(t, q, w.Machine, assigned)
				finish(t, q, assigned, true)
			}
			// Even a forged success report cannot qualify without an accepted
			// successful invocation for this exact native identity.
			forged := plan.Report
			forged.Qualified, forged.CheckState = true, "succeeded"
			incoming := w.Machine
			incoming.ExtensionReports = []ExtensionReport{forged}
			check(t, q, incoming, assigned)
			if qualifiedExtension(demand, q.s.Machines[incoming.ID]) != nil {
				t.Fatal("unsuccessful or obsolete check granted qualification")
			}
			if outcome == "failed" || retryable || outcome == "unproven refusal" || outcome == "stale refusal" {
				reconcileQualification(t, w, plan)
				before := qualificationJob(t, q, plan.Report.CheckID)
				reconcileQualification(t, w, plan)
				if after := qualificationJob(t, q, plan.Report.CheckID); after.ID != before.ID || after.Attempt != before.Attempt || !terminal(after.State) {
					t.Fatal("negative cache retried failed check")
				}
				local := LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
				for _, report := range local {
					if report.CheckID == plan.Report.CheckID && (report.Qualified || !terminal(report.CheckState)) {
						t.Fatal("failure was not visible", report)
					}
				}
				w.Machine.ExtensionReports = local
				if retryable || outcome == "failed" {
					// Receipt, aggregate status and read-only resolve use the
					// same identity-matched evidence as the real planner.
					check(t, q, w.Machine, nil)
					report, err := q.Resolve(ResolveRequest{Spec: Spec{Name: demand.Spec.Name, Project: demand.Spec.Project, Extension: extensionKey(e)}})
					if err != nil {
						t.Fatal(err)
					}
					action := "read `myce logs " + assigned.ID + "`, then fix the implementation or check, register a new version, and resubmit with a new key"
					source := nativeFailureSource
					if retryable {
						action, source = "wait for the automatic retry", "accepted release and exact-attempt execution evidence"
						if report.Hold == nil || report.Hold.Kind != "native_qualification" || !strings.Contains(report.Hold.Reason, "qualification remains pending") {
							t.Fatalf("resolve lost the retryable infrastructure evidence: %+v", report.Hold)
						}
					}
					assertCompatibilityAction(t, q, demand.ID, report, action, source)
					if outcome == "failed" {
						assertNativeCheckFinding(t, report.Hold.Reason, assigned.ID)
					}
				}
				var successor *ExtensionQualificationPlan
				for _, offered := range check(t, q, w.Machine, nil).ExtensionQualificationPlan {
					if offered.Report.CheckID == plan.Report.CheckID {
						t.Fatal("terminal failure was offered for automatic retry")
					}
					if offered.Report.Work == plan.Report.Work {
						successor = &offered
					}
				}
				if outcome == "failed" || outcome == "unproven refusal" || outcome == "stale refusal" {
					// A failed verdict is the consumer's to fix: the useful job
					// fails at the next tick with that one action, not a wait.
					if successor != nil {
						t.Fatal("failed verdict was rerun automatically", successor.Report.CheckID)
					}
					// A fan-out parent with no piece started is the same wait.
					parent := submit(t, q, Spec{Name: "fanned", Project: "test", Extension: extensionKey(e), Workload: &Workload{InputShape: "tiny", Units: 1},
						FanOut: &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "first"}, {Key: "second"}}}})
					if err := q.Tick(); err != nil {
						t.Fatal(err)
					}
					for _, id := range []string{demand.ID, parent.ID} {
						if got := job(t, q, id); got.State != "failed" || got.DiagnosticCode != deadEndWait || !strings.Contains(got.Reason, "native check") || !strings.Contains(got.Reason, "action: ") {
							t.Fatal("failed check left its useful job waiting", got.Spec.Name, got.State, got.Reason)
						}
						got, err := w.Client.GetJob(context.Background(), id)
						if err != nil || got.Finding == nil || !strings.Contains(got.Finding.Action, "myce logs "+assigned.ID) || !strings.Contains(got.Finding.What, nativeFailureSource) {
							t.Fatalf("terminal status lost its qualification action: %+v (%v)", got.Finding, err)
						}
						if outcome == "failed" {
							assertNativeCheckFinding(t, JobReason(got), assigned.ID)
							if len(got.NativeCheckFailures) != 1 || got.NativeCheckFailures[0] != (NativeCheckFailure{JobID: assigned.ID, Attempt: assigned.Attempt, Code: "native_stage_failed", ExceptionType: "ModuleNotFoundError", MissingModule: "example.stage"}) {
								t.Fatalf("GET lost typed qualification evidence: %+v", got.NativeCheckFailures)
							}
							states, err := w.Client.WaitBatch(context.Background(), []string{id}, nil, 0)
							if err != nil || len(states.Jobs) != 1 || states.Jobs[0].State != "failed" {
								t.Fatalf("wait missed failed job: %+v (%v)", states, err)
							}
							assertNativeCheckFinding(t, states.Jobs[0].Finding.Text(), assigned.ID)
						}
					}
				} else {
					// A released check without a verdict gets a successor key.
					if successor == nil || successor.Report.CheckID != automaticExtensionRetryCheckID(w.Machine.ID, successor.Report, plan.Report.CheckID) {
						t.Fatal("check without a native verdict was not rerun", successor)
					}
					reconcileQualification(t, w, *successor)
					rerun := qualificationJob(t, q, successor.Report.CheckID)
					if rerun.ID == before.ID || terminal(rerun.State) {
						t.Fatal("successor reused the previous invocation", rerun.State)
					}
					forged := successor.Report
					forged.CheckID = automaticExtensionRetryCheckID(w.Machine.ID, forged, "never-issued")
					if q.automaticCheckKeyValid(w.Machine.ID, forged) {
						t.Fatal("successor key accepted without an accepted predecessor")
					}
					if err := q.Tick(); err != nil {
						t.Fatal(err)
					}
					if got := job(t, q, demand.ID); got.State != "queued" {
						t.Fatal("pending rerun ended its useful job", got.State, got.Reason)
					}
				}
			}
			if outcome == "identity changed" {
				fresh := newExtensionReport(e, &w.Machine, plan.Report.Work)
				if automaticExtensionCheckID(w.Machine.ID, fresh) == plan.Report.CheckID {
					t.Fatal("changed identity reused old check key")
				}
			}
		})
	}
}

func TestExtensionQualificationWithdrawalDuringStartup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stopCause string
		retry     bool
	}{
		{name: "device reservation", stopCause: reservationStopCause, retry: true},
		{name: "host drain", stopCause: maintenanceStopCause, retry: true},
		{name: "release only before custody", stopCause: maintenanceStopCause, retry: true},
		{name: "release only after custody", stopCause: maintenanceStopCause},
		{name: "independent sidecar failure during drain", stopCause: maintenanceStopCause},
		{name: "main authorized before maintenance", stopCause: maintenanceStopCause},
		{name: "user cancel during drain", stopCause: maintenanceStopCause},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := compositionExtension(t, false)
			e.Variants[0].Execution.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"helper"}, MemoryGB: 0.05}}
			e = freezeComposition(t, e)
			q, w := prepareTestWorker(t, e)
			clock := q.clock.(*fakeClock)
			dev := m3Machine().Devices[0]
			dev.ExtensionFingerprint = w.extensionFingerprint
			w.Machine.Devices = []MachineDevice{dev}
			w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return []MachineDevice{dev}, nil }
			checkin := func() Reply {
				target := dev.machine(w.Machine)
				w.Machine.Devices[0].ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &target)
				return machineCheck(t, q, w.Machine)
			}
			demand := submit(t, q, Spec{Name: "demand", Project: "test", SubmissionKey: "useful-work", Extension: extensionKey(e)})
			cold := checkin()
			if len(cold.EnvironmentPlan) != 1 {
				t.Fatal("missing preparation offer", cold.EnvironmentPlan)
			}
			if err := w.prepareRegisteredExtension(context.Background(), cold.EnvironmentPlan[0]); err != nil {
				t.Fatal(err)
			}
			var plan ExtensionQualificationPlan
			for _, p := range checkin().ExtensionQualificationPlan {
				if p.Report.Work == demand.Spec.workType() {
					plan = p
					break
				}
			}
			if plan.Machine == "" {
				t.Fatal("missing qualification offer")
			}
			reconcileQualification(t, w, plan)
			assigned := assignedJob(checkin())
			if assigned == nil || assigned.Spec.ExtensionCheck == nil {
				t.Fatal("missing assigned check")
			}
			target := dev.machine(w.Machine)
			spec := assigned.Spec
			if err := e.lower(&spec, &target); err != nil || len(spec.Run.Sidecars) != 1 {
				t.Fatalf("check did not lower to a sidecar group: %v", err)
			}
			r := result(assigned, false)
			custody := NativeCustodyReport{Execution: r.Execution, Machine: r.Machine, Session: r.Session, ProcessID: "check-process", CustodyID: "check-custody"}
			if tc.name != "release only before custody" {
				for _, phase := range []string{"reserved", "started"} {
					custody.Phase = phase
					if err := q.NativeCustody(custody); err != nil {
						t.Fatal(err)
					}
				}
			}
			clock.Advance(time.Second)
			if tc.name == "main authorized before maintenance" {
				if err := q.Started(r); err != nil {
					t.Fatal(err)
				}
			}
			host := hostID(&w.Machine)
			var reservation Reservation
			if tc.stopCause == reservationStopCause {
				bound := submit(t, q, Spec{Name: "owner's validation", Project: "test", Extension: extensionKey(e), ExtensionDigest: e.Digest,
					AllowedMachines: []string{w.Machine.ID}, Needs: ResourceNeeds{GPU: &GPUSelection{Devices: []string{dev.ID}}},
					ExtensionCheck: &ExtensionCheckRequest{Work: plan.Report.Work, Directory: t.TempDir()}})
				var err error
				reservation, err = q.CreateReservation(reservationRequest("validation", "host="+host+";devices="+dev.ID, bound.ID))
				if err != nil || reservation.State != "draining" {
					t.Fatalf("reservation did not wait for custody: %s, %v", reservation.State, err)
				}
			} else {
				req := HostMaintenanceRequest{Action: "drain", DrainOwner: "test"}
				if tc.name == "main authorized before maintenance" {
					req = HostMaintenanceRequest{Action: "disable", Stop: true}
				}
				if _, err := q.ChangeHostMaintenance(host, req); err != nil {
					t.Fatal(err)
				}
			}
			stopping := job(t, q, assigned.ID)
			if c := maintenanceStoppedCopy(stopping); stopping.State != "stopping" || c == nil || c.StopCause != tc.stopCause {
				t.Fatal("withdrawal lost its exact stop provenance", stopping.State, stopping.Copies)
			}
			if err := q.Started(r); !errors.Is(err, ErrStale) {
				t.Fatal("withdrawal did not fence Started", err)
			}
			if tc.name == "user cancel during drain" {
				if err := q.Cancel(assigned.ID); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			if strings.HasPrefix(tc.name, "release only") {
				r.ReleaseOnly, r.Error = true, ""
			} else {
				r.ExecutionEvidence = unstartedExecution(assigned.Attempt)
				r.ExecutionEvidence.Sidecars = executionMayHaveRun
				r.ExecutionCause = &RuntimeCause{Operation: "command", Class: "cancelled"}
				r.Interrupted = tc.name != "independent sidecar failure during drain"
				if tc.name == "main authorized before maintenance" {
					r.ExecutionEvidence.Main = executionMayHaveRun
				}
			}
			if tc.stopCause == reservationStopCause {
				r.ReleasePending = true
				stale := r
				stale.Attempt++
				if err := q.Complete(stale); !errors.Is(err, ErrStale) {
					t.Fatal("stale result was accepted", err)
				}
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if r.ReleasePending {
				next := plan.Report
				next.CheckID = automaticExtensionRetryCheckID(w.Machine.ID, next, plan.Report.CheckID)
				if got := job(t, q, assigned.ID); got.ReleaseConfirmed || automaticCheckNoVerdict(got) {
					t.Fatal("check could be retried while its release was pending")
				}
				q.mu.Lock()
				allowed := q.automaticCheckKeyValid(w.Machine.ID, next)
				q.mu.Unlock()
				if allowed {
					t.Fatal("accepted a successor before exact release")
				}
				stale := r
				stale.Session, stale.ReleasePending = "stale-session", false
				if err := q.Complete(stale); !errors.Is(err, ErrStale) {
					t.Fatal("stale session released current custody", err)
				}
				reservationState(t, q, reservation.ID, "draining")
				ack := result(assigned, false)
				ack.ReleaseOnly = true
				if err := q.Complete(ack); err != nil {
					t.Fatal(err)
				}
			}
			got := job(t, q, assigned.ID)
			wantState := "failed"
			if tc.name == "user cancel during drain" {
				wantState = "cancelled"
			}
			if got.State != wantState || !got.ReleaseConfirmed || automaticCheckNoVerdict(got) != tc.retry {
				t.Fatalf("unexpected check outcome: %s, released=%t, no verdict=%t, history=%+v", got.State, got.ReleaseConfirmed, automaticCheckNoVerdict(got), got.AttemptHistory)
			}
			if !r.ReleaseOnly && !r.Interrupted && len(got.NativeCheckFailures) != 1 {
				t.Fatal("racing maintenance dropped the independent check failure")
			}
			reconcileQualification(t, w, plan)
			if reservation.ID != "" {
				if _, err := q.ReleaseReservation(reservation.ID, reservation.Revision, "validation finished"); err != nil {
					t.Fatal(err)
				}
				if err := q.Cancel(reservation.Job); err != nil {
					t.Fatal(err)
				}
			} else if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "resume"}); err != nil {
				t.Fatal(err)
			}
			var successor *ExtensionQualificationPlan
			for _, p := range checkin().ExtensionQualificationPlan {
				if p.Report.Work == plan.Report.Work {
					successor = &p
				}
			}
			if !tc.retry {
				if successor != nil {
					t.Fatal("terminal check was replaced", successor.Report.CheckID)
				}
				return
			}
			parent, err := q.jobSnapshot(demand.ID)
			if err != nil {
				t.Fatal(err)
			}
			if parent.State != "queued" || parent.Attempt != 0 || parent.Spec.SubmissionKey != demand.Spec.SubmissionKey ||
				parent.Hold == nil || parent.Hold.Kind != "native_qualification" {
				t.Fatalf("parent lost identity or healthy qualification wait: %s attempt %d, key %s, hold %+v", parent.State, parent.Attempt, parent.Spec.SubmissionKey, parent.Hold)
			}
			wantKey := automaticExtensionRetryCheckID(w.Machine.ID, plan.Report, plan.Report.CheckID)
			if successor == nil || successor.Report.CheckID != wantKey {
				t.Fatalf("expected same-identity successor %s, got %+v", wantKey, successor)
			}
			reconcileQualification(t, w, *successor)
			retry := assignedJob(checkin())
			if retry == nil || retry.ID == assigned.ID || retry.Spec.SubmissionKey != wantKey {
				t.Fatal("successor did not get an independent assignment", retry)
			}
			if err := q.Started(result(retry, false)); err != nil {
				t.Fatal(err)
			}
			finish(t, q, retry, true)
			reconcileQualification(t, w, *successor)
			if useful := assignedJob(checkin()); useful == nil || useful.ID != demand.ID || useful.Spec.SubmissionKey != demand.Spec.SubmissionKey {
				t.Fatal("successful retry did not unblock original job", useful)
			}
		})
	}
}

// A probed software requirement is native qualification, not a host pin or
// executable-name inventory. A failed host must not poison another host's proof.
func TestExtensionCapabilityQualificationPlacement(t *testing.T) {
	q, w, e, demand, _, plans := qualificationFixture(t)
	var plan ExtensionQualificationPlan
	for _, p := range plans {
		if p.Report.Work == demand.Spec.workType() {
			plan = p
		}
	}
	healthy := cloneMachine(&w.Machine)
	healthy.ID, healthy.HostID, healthy.Session = "capable", "capable-host", "capable-session"
	healthy.ExtensionFingerprint = "capable-native-fingerprint"
	healthy.ExtensionReports = nil
	for _, work := range extensionWorks(e) {
		r := newExtensionReport(e, &healthy, work)
		r.Setup, r.Qualified, r.CheckID = true, true, "fixture-native-comparison"
		healthy.ExtensionReports = append(healthy.ExtensionReports, r)
	}
	// Keep a second compatible host known before the failed verdict, so the
	// useful job remains runnable there instead of becoming a global dead end.
	if _, err := q.Checkin(Checkin{Machine: healthy, Recovering: true}); err != nil {
		t.Fatal(err)
	}
	reconcileQualification(t, w, plan)
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	assigned := check(t, q, w.Machine, nil).Job
	if assigned == nil || assigned.Spec.ExtensionCheck == nil {
		t.Fatal("probe not admitted before useful work", assigned)
	}
	finish(t, q, assigned, false)
	reconcileQualification(t, w, plan)
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	refused := check(t, q, w.Machine, nil)
	if refused.Job != nil && refused.Job.ID == demand.ID || qualifiedExtension(demand, q.s.Machines[w.Machine.ID]) != nil {
		t.Fatal("failed capability host received useful work")
	}
	for _, offered := range refused.ExtensionQualificationPlan {
		if offered.Report.Work == plan.Report.Work {
			t.Fatal("negative capability cache retried unchanged probe")
		}
	}
	placed := check(t, q, healthy, nil)
	if placed.Job == nil || placed.Job.ID != demand.ID || len(placed.Job.Spec.AllowedMachines) != 0 || len(placed.ExtensionQualificationPlan) != 0 {
		t.Fatal("unrestricted job did not use the qualified host", placed)
	}
	changed := cloneMachine(&healthy)
	changed.ExtensionFingerprint = "changed-native-fingerprint"
	if qualifiedExtension(demand, &changed) != nil {
		t.Fatal("capability cache survived a host identity change")
	}
	changed = cloneMachine(&healthy)
	for i := range changed.ExtensionReports {
		changed.ExtensionReports[i].EnvironmentIdentity = "changed-environment"
	}
	if qualifiedExtension(demand, &changed) != nil {
		t.Fatal("capability cache survived an environment change")
	}
}

func TestExtensionQualificationSuspectRetainsDemand(t *testing.T) {
	q, w, _, demand, _, plans := qualificationFixture(t)
	reconcileQualification(t, w, plans[0])
	checkID := qualificationJob(t, q, plans[0].Report.CheckID).ID
	clock := q.clock.(*fakeClock)
	clock.Advance(q.workerArrivalAllowance(q.s.Machines[w.Machine.ID]))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	m := q.s.Machines[w.Machine.ID]
	if !q.workerSuspect(m, clock.Now()) || m.Gone {
		t.Fatal("fixture did not preserve suspect custody")
	}
	if got := qualificationJob(t, q, plans[0].Report.CheckID); got.State != "queued" && got.State != "waiting" {
		t.Fatal("temporary suspicion destroyed useful qualification", got.State)
	}
	if len(q.extensionQualificationPlan(m)) != 0 {
		t.Fatal("suspect worker received new qualification offers")
	}
	for _, id := range []string{demand.ID, checkID} {
		if j := q.s.Jobs[id]; j.Losses != 0 || j.InfrastructureRetries != 0 || j.Attempt != 0 {
			t.Fatal("suspicion charged or assigned work", j)
		}
	}
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	if got := check(t, q, w.Machine, nil).Job; got == nil || got.ID != checkID {
		t.Fatal("fresh contact did not resume the original check", got)
	}
}

func TestExtensionQualificationMixedBacklog(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}
	m := machine("worker", "fake")
	m.ExtensionFingerprint = "synthetic-native-identity"
	m.LastSeen, m.DiskReceivedAt = clock.Now(), clock.Now()
	m.DiskVolumes = []DiskVolume{{ID: "local", Roots: []string{"data"}, TotalBytes: 1 << 30, FreeBytes: 512 << 20, SampledAt: clock.Now()}}
	e := fakeExtension(t)
	other := fakeExtension(t)
	other.Files["extension.yaml"] = []byte(strings.ReplaceAll(strings.ReplaceAll(string(other.Files["extension.yaml"]), "version: '1'", "version: '2'"), "resumable: true", "resumable: false"))
	var err error
	other, err = extensionFromFiles(other.Files)
	if err != nil {
		t.Fatal(err)
	}
	for _, extension := range []Extension{e, other} {
		for _, work := range extensionWorks(extension) {
			r := newExtensionReport(extension, &m, work)
			r.Setup = true
			m.ExtensionReports = append(m.ExtensionReports, r)
		}
	}
	job := func(id string, extensions ...Extension) *Job {
		return &Job{ID: id, State: "queued", Spec: Spec{Project: "test", Kind: "sample"}, Extensions: extensions}
	}
	host, disk, device := job("host", e), job("disk", e), job("device", e)
	host.Spec.AllowedMachines = []string{"other-worker"}
	disk.Spec.Disk = &DiskNeeds{InputBytes: 1 << 30}
	device.Spec.Needs.GPU = &GPUSelection{Devices: []string{"other-device"}}
	resume := job("resume", other)
	resume.Spec.Resumable = true
	useful := job("useful", other)
	plain := &Job{ID: "plain", State: "queued", Spec: spec("experiment")}
	q := &Queue{clock: clock, s: state{Jobs: map[string]*Job{}, Extensions: map[string]Extension{extensionKey(e): e, extensionKey(other): other}, Machines: map[string]*Machine{m.ID: &m}}}
	// This planning-only fixture bypasses check-in; supply the current worker's
	// advertised cadence as an accepted report would before offering work.
	q.acceptContactPeriod(&m, time.Second, nil)
	for _, j := range []*Job{host, disk, device, resume, useful, plain} {
		q.s.Jobs[j.ID] = j
	}
	assertOffers := func(want ...Extension) {
		t.Helper()
		expected := map[string]bool{}
		for _, extension := range want {
			for _, work := range extensionWorks(extension) {
				expected[extension.Digest+"/"+work] = true
			}
		}
		for _, plan := range q.extensionQualificationPlan(&m) {
			key := plan.Report.Digest + "/" + plan.Report.Work
			if !expected[key] || plan.Machine != m.ID {
				t.Fatalf("unexpected qualification offer: %+v", plan)
			}
			delete(expected, key)
		}
		if len(expected) != 0 {
			t.Fatal("missing qualification offers", expected)
		}
	}
	// Ineligible jobs must not lend their eligibility to another frozen digest.
	assertOffers(other)
	host.Spec.AllowedMachines = []string{m.ID}
	assertOffers(e, other)
	useful.State = "cancelled"
	assertOffers(e) // The resumable job cannot keep the non-resumable check alive.
	for i := range m.ExtensionReports {
		m.ExtensionReports[i].Qualified = true
	}
	assertOffers() // A new pass immediately observes newly accepted qualification.
}

func qualificationStorageFixture(t testing.TB) (*Queue, *Machine, Extension, *Job, StorageObservation) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)}
	e := compositionExtension(t, false)
	m := machine("worker", "fake")
	target := e.Variants[0].Target
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = target.OS, target.Arch
	m.ExtensionFingerprint = "qualified-target"
	m.DiskReceivedAt = clock.Now()
	m.DiskVolumes = []DiskVolume{{ID: "data", Roots: []string{"data"}, TotalBytes: 1 << 30, FreeBytes: 512 << 20, SampledAt: clock.Now()}}
	report := newExtensionReport(e, &m, "extension:"+e.Name)
	report.Setup, report.Qualified, report.CheckID = true, true, "accepted-check"
	m.ExtensionReports = []ExtensionReport{report}
	j := &Job{ID: "item", State: "queued", Spec: Spec{Project: "test", Extension: extensionKey(e)}, Extensions: []Extension{e}}
	q := &Queue{clock: clock, s: state{Jobs: map[string]*Job{j.ID: j}, Machines: map[string]*Machine{m.ID: &m}, StorageObservations: map[string]StorageObservation{}}}
	candidates := extensionJobs(j, &m, false)
	if len(candidates) != 1 {
		t.Fatalf("expected one eligible extension variant, got %d", len(candidates))
	}
	key := q.storageKey(candidates[0], &m)
	if key == nil {
		t.Fatal("fixture has no qualified storage identity")
	}
	scratch := StorageRange{Samples: 2, Min: 1 << 20, Peak: 1 << 20}
	observation := StorageObservation{Key: *key, Family: storageFamily(j.Spec.Project, *key), RecordedAt: clock.Now(), AllocationUnit: 1, Managed: scratch, Scratch: scratch}
	return q, &m, e, j, observation
}

func TestExtensionQualificationDemandRefreshesStorageFamily(t *testing.T) {
	q, m, e, _, observation := qualificationStorageFixture(t)
	q.s.StorageObservations["prior-items"] = observation
	if !q.extensionQualificationDemand(e, m) {
		t.Fatal("a fitting storage family blocked qualification demand")
	}
	observation.Scratch.Peak = 1 << 30
	q.s.StorageObservations["prior-items"] = observation
	if q.extensionQualificationDemand(e, m) {
		t.Fatal("qualification used stale storage evidence after the family outgrew the target")
	}
}

func BenchmarkExtensionQualificationStorageBacklog(b *testing.B) {
	q, m, e, template, observation := qualificationStorageFixture(b)
	q.s.StorageObservations["prior-items"] = observation
	for i := range 2000 {
		id := fmt.Sprintf("retained-%03d", i)
		unrelated := observation
		unrelated.Family = id
		q.s.StorageObservations[id] = unrelated
	}
	for i := range 250 {
		id := fmt.Sprintf("queued-%03d", i)
		j := *template
		j.ID, j.Spec.Args = id, []string{id}
		q.s.Jobs[id] = &j
	}
	b.ReportAllocs()
	for b.Loop() {
		if !q.extensionQualificationDemands(m, nil)[e.Digest] {
			b.Fatal("missing qualification demand")
		}
	}
}

// #459's post-restart queue had 149 registered extensions and 194 queued
// plain commands, each with three shared inputs (608, 637 and 4063 bytes).
// Qualification demand used to rehash those inputs for every catalogue entry
// on every target, despite none of these commands using an extension. Keep
// this fixture synthetic: no deployment identities, paths or payloads.
func BenchmarkExtensionQualificationPlainBacklog(b *testing.B) {
	clock := &fakeClock{now: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}
	m := machine("worker", "fake")
	m.LastSeen, m.DiskReceivedAt = clock.Now(), clock.Now()
	m.DiskVolumes = []DiskVolume{{ID: "local", Roots: []string{"data", "file-cache", "checkpoint"}, TotalBytes: 1 << 40, FreeBytes: 1 << 39, SampledAt: clock.Now()}}
	for i := range 48 {
		m.CachedSharedInputs = append(m.CachedSharedInputs, fmt.Sprintf("%064x", i))
	}
	q := &Queue{clock: clock, s: state{Jobs: map[string]*Job{}, Extensions: map[string]Extension{}, Machines: map[string]*Machine{m.ID: &m}}}
	for i := range 149 {
		id := fmt.Sprint(i)
		q.s.Extensions[id] = Extension{Name: "extension-" + id, Version: "1", Digest: fmt.Sprintf("%064x", i), Resumable: true}
	}
	for i := range 194 {
		id := fmt.Sprint(i)
		s := spec("experiment")
		s.SharedInputs = map[string]SharedInput{}
		for k, size := range []int64{608, 637, 4063} {
			s.SharedInputs[fmt.Sprint(k)] = SharedInput{SHA256: strings.Repeat(fmt.Sprint(k+1), 64), SizeBytes: size}
		}
		q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
	}
	b.ReportAllocs()
	for b.Loop() {
		if plans := q.extensionQualificationPlan(&m); len(plans) != 0 {
			b.Fatal("plain commands manufactured qualification demand", plans)
		}
	}
}

// The live gap-3 job: its base check was cancelled, the host stayed busy, the
// worker restarted while the successor check was queued, and a cancelled
// successor was never retried because the queue re-offered the base key. The
// job waited 114 hours on a check the queue said it ran itself. Each step must
// now keep the check moving, and the check starts once the host has room.
func TestExtensionQualificationProgressesThroughRestartAndBusyHost(t *testing.T) {
	q, w, _, demand, _, plans := qualificationFixture(t)
	clock := q.clock.(*fakeClock)
	plan := plans[0]
	for _, p := range plans {
		if p.Report.Work == demand.Spec.workType() {
			plan = p
		}
	}
	reconcileQualification(t, w, plan)
	base := qualificationJob(t, q, plan.Report.CheckID)
	retire := func(id string) {
		t.Helper()
		if err := q.update(func() error {
			q.cancelJob(q.s.Jobs[id]) // Queue retirement when native demand disappears.
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	retire(base.ID)
	// Another project's job fills the host's one slot.
	w.Machine.MaxJobs = 1
	submit(t, q, Spec{Name: "busy", Project: "other", Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{MemoryGB: 1, Platform: Platform{Any: true}}})
	busy := check(t, q, w.Machine, nil).Job
	if busy == nil || busy.Spec.Name != "busy" {
		t.Fatal("fixture did not fill the host", busy)
	}
	if err := q.Started(result(busy, false)); err != nil {
		t.Fatal(err)
	}
	pendingCheck := func(running *Job) *Job {
		t.Helper()
		for range 3 { // bounded: each offer is one check-in
			w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
			reply := check(t, q, w.Machine, running)
			if reply.Job != nil && reply.Job.Spec.ExtensionCheck != nil && reply.Job.Spec.ExtensionCheck.Work == plan.Report.Work {
				return reply.Job
			}
			for _, p := range reply.ExtensionQualificationPlan {
				reconcileQualification(t, w, p)
			}
			clock.Advance(time.Second)
		}
		s, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range s.Jobs {
			if c := j.Spec.ExtensionCheck; c != nil && c.Work == plan.Report.Work && !terminal(j.State) {
				return j
			}
		}
		t.Fatal("the queue did not resubmit its own check")
		return nil
	}
	successor := pendingCheck(busy)
	if successor.State != "queued" {
		t.Fatal("check on a busy host did not wait in the queue", successor.State, successor.Reason)
	}
	// Status names the check that exists and what it waits on, not a promise.
	if got, err := q.jobSnapshot(demand.ID); err != nil || got.Hold == nil || got.Hold.Kind != "native_qualification" || !strings.Contains(got.Hold.Reason, "native check "+ShortID(successor.ID)) {
		t.Fatalf("hold does not name the pending check: %v %+v", err, got.Hold)
	}
	// The worker restarts; its first check-in has no extension identity yet.
	previous := w.Machine.Session
	restarted := w.Machine
	restarted.Session, restarted.ExtensionFingerprint, restarted.ExtensionReports = "restarted", "", nil
	if _, err := q.Checkin(Checkin{Machine: restarted, Fresh: true, PreviousSession: previous, period: time.Second}); err != nil {
		t.Fatal(err)
	}
	w.Machine.Session = "restarted"
	if got := job(t, q, successor.ID); cancelled(got) || terminal(got.State) {
		t.Fatal("restart before identity report cancelled the queue's check", got.State, got.Reason)
	}
	// Any other loss of the successor is retried under the next key in its chain.
	retire(successor.ID)
	retried := pendingCheck(nil)
	if retried.ID == successor.ID {
		t.Fatal("cancelled successor was not retried")
	}
	// The restart requeued the other job; it ran again and now finishes.
	if err := q.Complete(result(job(t, q, busy.ID), true)); err != nil {
		t.Fatal(err)
	}
	// Bounded: other queued work may take the freed card first and finish.
	for range 6 {
		w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
		assigned := check(t, q, w.Machine, nil).Job
		if assigned != nil && assigned.ID == retried.ID {
			return
		}
		if assigned != nil {
			clock.Advance(time.Second)
			finish(t, q, assigned, true)
		}
		clock.Advance(time.Second)
	}
	got := job(t, q, retried.ID)
	t.Fatal("check did not start once the host had room", got.State, got.Reason)
}

func TestExtensionQualificationNoCapableHostFailsFast(t *testing.T) {
	for _, missing := range []string{"target", "check", "baseline", "capacity"} {
		t.Run(missing, func(t *testing.T) {
			e := compositionExtension(t, false)
			switch missing {
			case "target":
				e.Variants[0].Target.OS = "windows"
			case "check":
				e.Variants[0].Check, e.Variants[0].Baselines = nil, nil
			case "baseline":
				e.Variants[0].Baselines = nil
			case "capacity":
				e.Variants[0].Needs.HostMemoryGB = 1e6
			}
			e = freezeComposition(t, e)
			q, w := prepareTestWorker(t, e)
			j := submit(t, q, Spec{Name: "demand", Project: "test", Extension: extensionKey(e)})
			check(t, q, w.Machine, nil)
			q.clock.(*fakeClock).Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			got, err := q.jobSnapshot(j.ID)
			if err != nil || got.State != "failed" || got.Attempt != 0 || got.Finding == nil || got.Finding.Code != deadEndWait || strings.Count(JobReason(got), "Next:") != 1 {
				t.Fatalf("missing immediate check finding: state=%s reason=%s err=%v", got.State, got.Reason, err)
			}
		})
	}
}
