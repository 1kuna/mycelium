package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestHostRestDevExpectations(t *testing.T) {
	r, c := envelopeFixture(t)
	root := filepath.Join(filepath.Dir(c.Config), "dev-state")
	c.HostStateDir = filepath.Join(root, "dev-host")
	d, err := myce.StartDev(myce.DevConfig{HostStateDir: c.HostStateDir, HostID: c.HostID, Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "fixture-token", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	t.Setenv("MYCE_TOKEN", "fixture-token")
	c.Queue, c.Name, c.RequiredWorkers = d.URL, "fake-1", []string{"fake-1"}
	c.Credentials = filepath.Join(root, "fixture-credentials.json")
	if err := os.WriteFile(c.Credentials, []byte("MYCE_TOKEN=fixture-token\nMYCE_QUEUE="+c.Queue+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(c)
	if err := os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	observe := func(ctx context.Context, v myce.HostInvocation) (myce.HostExclusionReport, error) {
		s, err := myce.ReadHostIntent(v.StateDir)
		return envelopeHardware(ctx, s), err
	}
	invoke := func(flags ...string) (myce.HostEnvelope, error) {
		t.Helper()
		args := append([]string{flags[0], "--local", "--config", c.Config, "--json"}, flags[1:]...)
		var out bytes.Buffer
		err := hostEnvelopeCommandWithObservation(args, myce.MachineProbe{OS: "fixture"}, &out, r, nil, hostMutationExecutor(myce.MachineProbe{OS: "fixture"}).Execute, observe)
		if bytes.Contains(out.Bytes(), []byte("fixture-token")) {
			t.Fatal("credential in envelope")
		}
		var e myce.HostEnvelope
		if decodeErr := json.Unmarshal(out.Bytes(), &e); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if e.HostRestVerdict == nil || e.Verdict != "UNKNOWN" {
			validateEnvelopeOutput(t, out.Bytes())
		}
		return e, err
	}
	wait := func(pred func() bool) {
		t.Helper()
		for !pred() {
			if err := waitPause(ctx, 10*time.Millisecond); err != nil {
				t.Fatal("dev condition deadline", err)
			}
		}
	}
	wait(func() bool {
		e, err := invoke("status", "--expect", "ready", "--verdict")
		return err == nil && e.Verdict == "PASS"
	})
	// Use the production executable for ready too; its ordinary unprivileged GPU
	// observation is UNKNOWN, which must not withhold worker readiness.
	binary := filepath.Join(root, "myce")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	command := exec.CommandContext(ctx, binary, "host", "status", "--local", "--config", c.Config, "--expect", "ready", "--verdict", "--json")
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root, "MYCE_TOKEN=fixture-token"}
	output, err := command.Output()
	if err != nil {
		t.Fatal("executable ready", err)
	}
	ready := validateEnvelopeOutput(t, output)
	if ready.HostRestVerdict == nil || ready.Verdict != "PASS" {
		t.Fatal(ready)
	}
	// A real plain command runs through the API/SQLite/worker, with a visible
	// effect. Current work does not make Ready require empty cgroups.
	marker := filepath.Join(root, "command-started")
	spec := myce.Spec{Name: "fixture-work", Project: "test", Priority: "experiment"}
	spec.Needs.MemoryGB = 1
	spec.Needs.Platform.Any = true
	spec.Run.Command = []string{"sh", "-c", `test -z "$MYCE_TOKEN" || exit 99; printf started > "$1"; sleep 10`, "fixture", marker}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	var job myce.Job
	if err := d.Client().Call(ctx, "POST", "/api/jobs", spec, &job); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { _, err := os.Stat(marker); return err == nil })
	active, err := invoke("status", "--expect", "ready", "--verdict")
	if err != nil || active.Verdict != "PASS" || active.Counts.Active == nil || *active.Counts.Active != 1 {
		t.Fatalf("active ready %+v error %v", active, err)
	}
	report, err := (myce.HostControl{Dir: c.HostStateDir, Clock: myce.RealClock{}, FreshClose: true, Service: envelopeService{}, Observe: envelopeHardware}).Disable(ctx, "fixture close", 0)
	if err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	var closed myce.HostEnvelope
	wait(func() bool {
		closed, err = invoke("status", "--expect", "closed", "--verdict")
		return err == nil && closed.Verdict == "PASS"
	})
	if closed.QueueReachable == nil || !*closed.QueueReachable || closed.Counts.Unsettled == nil || *closed.Counts.Unsettled != 0 {
		t.Fatal("closed did not settle", closed)
	}
	// Close requeues the interrupted experiment. Cancel it before submitting the
	// separate queued-work case so its count and later effect are unambiguous.
	if err := d.Client().Call(ctx, "POST", "/api/jobs/"+job.ID+"/cancel", nil, nil); err != nil {
		t.Fatal(err)
	}
	// Nonempty fenced work is permitted. It cannot create a late assignment.
	spec.Run.Command = []string{"sh", "-c", `printf late > "$1"`, "fixture", filepath.Join(root, "late")}
	if err := d.Client().Call(ctx, "POST", "/api/jobs", spec, &job); err != nil {
		t.Fatal(err)
	}
	closed, err = invoke("status", "--expect", "closed", "--verdict")
	if err != nil || closed.Verdict != "PASS" || closed.Counts.FencedQueued == nil || *closed.Counts.FencedQueued != 1 || !slices.Contains(closed.Reasons, "queued_work_fenced") {
		t.Fatal(closed, err)
	}
	wrong, err := invoke("status", "--expect", "ready", "--verdict")
	if exitCode(err) == 0 || wrong.Verdict != "FAIL" {
		t.Fatal("closed ready accepted", wrong, err)
	}
	if err := d.Client().Call(ctx, "POST", "/api/jobs/"+job.ID+"/cancel", nil, nil); err != nil {
		t.Fatal(err)
	}
	// Reopen with the real production mutation executor; the worker restarts its
	// registered session and acknowledges the new open revision through the API.
	opened, err := invoke("open", "--expect-revision", strconv.FormatUint(*closed.Desired.Revision, 10))
	if err != nil || opened.Accepted == nil {
		t.Fatal(opened, err)
	}
	wait(func() bool {
		e, err := invoke("status", "--expect", "ready", "--verdict")
		return err == nil && e.Verdict == "PASS"
	})
	if _, err := os.Stat(filepath.Join(root, "late")); !os.IsNotExist(err) {
		t.Fatal("closed queued command executed", err)
	}
	report, err = (myce.HostControl{Dir: c.HostStateDir, Clock: myce.RealClock{}, FreshClose: true, Service: envelopeService{}, Observe: envelopeHardware}).Disable(ctx, "rest probe finally", 0)
	if err != nil || !report.Confirmed {
		t.Fatal("final close", report, err)
	}
	wait(func() bool {
		e, err := invoke("status", "--expect", "closed", "--verdict")
		return err == nil && e.Verdict == "PASS"
	})
}

func TestHostRestCommandErrorsAndDeadline(t *testing.T) {
	r, c := envelopeFixture(t)
	for _, args := range [][]string{{"status", "--verdict"}, {"close", "--verdict"}, {"status", "--expect", "other", "--verdict"}} {
		e, err := invokeEnvelope(t, r, c, args...)
		if exitCode(err) != 2 || e.HostRestVerdict != nil {
			t.Fatal(e, err)
		}
	}
	for _, expect := range []string{"closed", "ready"} {
		t.Run(expect, func(t *testing.T) {
			clock := &waitClock{now: time.Now().UTC()}
			start := clock.Now()
			var out bytes.Buffer
			err := hostEnvelopeCommandWithTiming([]string{"status", "--local", "--config", c.Config, "--expect", expect, "--verdict", "--wait", "--timeout", "80ms", "--json"}, myce.MachineProbe{}, &out, r, nil, nil, nil, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
			e := validateEnvelopeOutput(t, out.Bytes())
			if !errors.Is(err, myce.ErrHostDeadline) || e.Verdict == "PASS" || !slices.Contains(e.Reasons, "timeout") || !clock.Now().Equal(start.Add(80*time.Millisecond)) {
				t.Fatal(e, err)
			}
		})
	}
	// Passive diagnostics do not create an operational holds_detail/capdiff/cooldown
	// dependency. The generic fleet status and eligible-host redaction stay intact.
	var out bytes.Buffer
	if err := hostEnvelopeCommand([]string{"status", "--local", "--config", c.Config}, myce.MachineProbe{}, &out, r, nil); err != nil || !strings.Contains(out.String(), "Host closed:") {
		t.Fatal(out.String(), err)
	}
}

type restEvaluationClock struct {
	at   time.Time
	step time.Duration
}

func (c *restEvaluationClock) Now() time.Time {
	at := c.at
	c.at = c.at.Add(c.step)
	return at
}

func TestHostRestVerdictAndExitUseOneInstant(t *testing.T) {
	for _, expect := range []string{"closed", "ready"} {
		for _, jsonOutput := range []bool{false, true} {
			t.Run(expect+"/json="+strconv.FormatBool(jsonOutput), func(t *testing.T) {
				r, _, c := localIdentityFixture(t)
				clock := &restEvaluationClock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
				at := clock.at.Add(-myce.HostObservationWindow + time.Millisecond)
				revision := myce.HostRevision{Revision: 1, Operation: "fixture-operation"}
				host, boot, state := "fixture-host", "fixture-boot", "closed"
				if expect == "ready" {
					state = "open"
				}
				zero, yes := 0, true
				e := myce.UnknownHostEnvelope("status", clock)
				e.Host, e.Boot, e.Operation = &host, &boot, &revision.Operation
				e.Desired = myce.HostEnvelopeState{State: &state, Revision: &revision.Revision}
				e.Observed = e.Desired
				e.QueueRevision, e.Ready = &revision.Revision, &yes
				e.Settlement = "confirmed"
				e.LastClose = &myce.HostClose{HostRevision: revision, Settlement: "confirmed", SettledAt: &at}
				for key := range e.Sources {
					e.Sources[key] = &at
				}
				e.Sources["required_workers"] = &at
				e.RestEvidence = myce.HostRestEvidence{QueueReachable: &yes, QueueAt: &at, UnfencedQueued: &zero}
				e.Counts = myce.HostEnvelopeCounts{Active: &zero, Preparing: &zero, Stopping: &zero, Unsettled: &zero, Queued: &zero, FencedQueued: &zero, UnresolvedPlacement: &zero}
				launch := func(_ context.Context, s myce.HostChildLaunch) ([]byte, error) {
					result := myce.HostMutationResult{HostRevision: revision}
					switch s.Request.Operation {
					case "status":
						result.Envelope = &e
					case "resolve":
						// Collection has finished. Advancing reads expose any later
						// reevaluation across the evidence freshness boundary.
						clock.step = 500 * time.Microsecond
						if expect == "closed" {
							clock.step = 300 * time.Microsecond
						}
					default:
						t.Fatal("unexpected child operation", s.Request.Operation)
					}
					return json.Marshal(struct {
						Version   int                     `json:"version"`
						Operation string                  `json:"operation"`
						Result    myce.HostMutationResult `json:"result"`
					}{1, s.Request.Operation, result})
				}
				observe := func(context.Context, myce.HostInvocation) (myce.HostExclusionReport, error) {
					return myce.HostExclusionReport{HostRevision: revision, Boot: boot, At: at, Verdict: "PASS", RestartSafe: &yes, Units: []myce.ExclusionUnit{{Unit: "myce-worker.service", Populated: new(bool)}}, RunningContainers: &zero, GPUHolders: &zero, PendingOperations: &zero, Sources: e.Sources}, nil
				}
				args := []string{"status", "--local", "--worker-user", "worker", "--config", c.Config, "--expect", expect, "--verdict"}
				if jsonOutput {
					args = append(args, "--json")
				}
				var out bytes.Buffer
				err := hostEnvelopeCommandWithTiming(args, myce.MachineProbe{}, &out, r, launch, nil, observe, clock, func(context.Context, time.Duration) error { t.Fatal("unexpected pause"); return nil })
				if jsonOutput {
					got := validateEnvelopeOutput(t, out.Bytes())
					if got.Verdict != "PASS" || exitCode(err) != 0 {
						t.Fatalf("printed verdict=%s exit=%d error=%v", got.Verdict, exitCode(err), err)
					}
				} else if !strings.Contains(out.String(), expect+": PASS (") || exitCode(err) != 0 {
					t.Fatalf("output=%q exit=%d error=%v", out.String(), exitCode(err), err)
				}
			})
		}
	}
}

func TestHostRestCollectionErrorMatrix(t *testing.T) {
	steps := []string{"identity_resolve", "status", "status_missing", "exclusion", "final_resolve", "final_fence", "final_deadline", "wait_pause"}
	for _, step := range steps {
		for _, expect := range []string{"closed", "ready"} {
			for _, jsonOutput := range []bool{false, true} {
				t.Run(step+"/"+expect+"/json="+strconv.FormatBool(jsonOutput), func(t *testing.T) {
					r, _, c := localIdentityFixture(t)
					clock := &waitClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
					at := clock.Now()
					revision := myce.HostRevision{Revision: 1, Operation: "fixture-operation"}
					host, boot, state := "fixture-host", "fixture-boot", "closed"
					if expect == "ready" {
						state = "open"
					}
					zero, yes := 0, true
					e := myce.UnknownHostEnvelope("status", clock)
					e.Host, e.Boot, e.Operation = &host, &boot, &revision.Operation
					e.Desired = myce.HostEnvelopeState{State: &state, Revision: &revision.Revision}
					e.Observed = e.Desired
					e.QueueRevision, e.Ready = &revision.Revision, &yes
					e.Settlement = "confirmed"
					e.LastClose = &myce.HostClose{HostRevision: revision, Settlement: "confirmed", SettledAt: &at}
					for key := range e.Sources {
						e.Sources[key] = &at
					}
					e.Sources["required_workers"] = &at
					e.RestEvidence = myce.HostRestEvidence{QueueReachable: &yes, QueueAt: &at, UnfencedQueued: &zero}
					e.Counts = myce.HostEnvelopeCounts{Active: &zero, Preparing: &zero, Stopping: &zero, Unsettled: &zero, Queued: &zero, FencedQueued: &zero, UnresolvedPlacement: &zero}
					injected := false
					if step == "identity_resolve" {
						r.Lookup = func(string) (myce.WorkerAccount, error) {
							injected = true
							return myce.WorkerAccount{}, myce.ErrHostIdentity
						}
					}
					if step == "wait_pause" {
						// A fresh proven violation must still become UNKNOWN when a later
						// collection step errors, rather than preserving an earlier verdict.
						one, no := 1, false
						e.Counts.Active, e.Ready = &one, &no
						e.RestEvidence.WorkerHealth = []myce.HostRestWorkerHealth{{At: &at, MaxAge: myce.HostObservationWindow, Value: &no}}
					}
					launch := func(_ context.Context, s myce.HostChildLaunch) ([]byte, error) {
						result := myce.HostMutationResult{HostRevision: revision}
						switch s.Request.Operation {
						case "status":
							if step == "status" {
								injected = true
								return nil, myce.ErrHostChild
							}
							if step == "status_missing" {
								injected = true
							} else {
								result.Envelope = &e
							}
						case "resolve":
							if step == "final_resolve" {
								injected = true
								return nil, myce.ErrHostChild
							}
							if step == "final_fence" {
								injected = true
								result.Revision++
							}
							if step == "final_deadline" {
								injected = true
								clock.advance(30 * time.Second)
							}
						default:
							t.Fatal("unexpected operation", s.Request.Operation)
						}
						return json.Marshal(struct {
							Version   int                     `json:"version"`
							Operation string                  `json:"operation"`
							Result    myce.HostMutationResult `json:"result"`
						}{1, s.Request.Operation, result})
					}
					observe := func(context.Context, myce.HostInvocation) (myce.HostExclusionReport, error) {
						if step == "exclusion" {
							injected = true
							return myce.HostExclusionReport{}, myce.ErrHostChild
						}
						return myce.HostExclusionReport{HostRevision: revision, Boot: boot, At: at, Verdict: "PASS", RestartSafe: &yes, Units: []myce.ExclusionUnit{{Unit: "myce-worker.service", Populated: new(bool)}}, RunningContainers: &zero, GPUHolders: &zero, PendingOperations: &zero, Sources: e.Sources}, nil
					}
					pause := func(context.Context, time.Duration) error {
						if step != "wait_pause" {
							t.Fatal("unexpected pause")
						}
						injected = true
						return myce.ErrHostDeadline
					}
					args := []string{"status", "--local", "--worker-user", "worker", "--config", c.Config, "--expect", expect, "--verdict"}
					if step == "wait_pause" {
						args = append(args, "--wait")
					}
					if jsonOutput {
						args = append(args, "--json")
					}
					var out bytes.Buffer
					err := hostEnvelopeCommandWithTiming(args, myce.MachineProbe{}, &out, r, launch, nil, observe, clock, pause)
					if !injected {
						t.Fatal("error injection was not reached")
					}
					if exitCode(err) == 0 {
						t.Fatal("collection error exited zero", out.String())
					}
					if jsonOutput {
						got := validateEnvelopeOutput(t, out.Bytes())
						if got.Verdict != "UNKNOWN" || !slices.Contains(got.Reasons, "collection_failed") {
							t.Fatalf("got %+v exit=%d", got.HostRestVerdict, exitCode(err))
						}
					} else if !strings.Contains(out.String(), expect+": UNKNOWN (") || !strings.Contains(out.String(), "collection_failed") {
						t.Fatalf("output=%q exit=%d", out.String(), exitCode(err))
					}
				})
			}
		}
	}
}
