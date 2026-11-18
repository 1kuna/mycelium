package myce

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
)

// Exercise the installed helper entry, not an already initialized Worker.Run.
// Only native manager membership, host sensors and GPUs are synthetic; queue
// authorization, final custodian gate, command execution and receipts are real.
func TestCustodianGPUStarted(t *testing.T) {
	for _, device := range []string{"0", "1"} {
		t.Run(device, func(t *testing.T) { testCustodianStarted(t, device, "command") })
	}
}

func TestCustodianExtensionStarted(t *testing.T) {
	for _, device := range []string{"0", "1"} {
		t.Run(device, func(t *testing.T) { testCustodianStarted(t, device, "extension") })
	}
}

func TestCustodianExtensionRefusalDiagnostic(t *testing.T) {
	for _, kind := range []string{"missing", "qualification"} {
		t.Run(kind, func(t *testing.T) { testCustodianStarted(t, "0", kind) })
	}
}

// Prepare the parent's frozen extension, environment and exact device report.
// The helper is deliberately given neither ExtensionRoot nor EnvironmentRoot.
func custodianExtensionFixture(t *testing.T, q *Queue, clock Clock, w Worker, m Machine, device, proof string) (Extension, Machine) {
	t.Helper()
	e := compositionExtension(t, true)
	v := &e.Variants[0]
	v.Execution.Main.Command = []string{"sh", "-c", `test -z "${MYCE_TOKEN+x}" || exit 97; test "$(cat "$MYCE_ENVIRONMENT_DIR/bin/proof")" = custodian-environment || exit 98; printf '%s' "$MYCE_DEVICES" > "$PROOF_DIR/device"; while [ ! -f "$PROOF_DIR/finish" ]; do sleep 0.01; done`}
	v.Execution.Main.Env = map[string]string{"PROOF_DIR": proof}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	roots, err := w.StateRoots()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(roots[0], "extensions")
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	cache := EnvironmentCache{Root: filepath.Join(roots[0], "environment-cache"), Limit: w.EnvironmentLimit, Clock: clock,
		Run: func(_ context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
			if err := os.MkdirAll(filepath.Join(dir, "bin"), 0700); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(dir, "bin", "proof"), []byte("custodian-environment"), 0600)
		},
	}
	r := e.Variants[0].Environments["main"]
	_, release, err := cache.Acquire(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	release()
	m.Environments = append(m.Environments, r.Key)
	for i := range m.Devices {
		d := &m.Devices[i]
		d.ExtensionFingerprint, err = ExtensionFingerprint(context.Background(), "fake", d.Selector)
		if err != nil {
			t.Fatal(err)
		}
		if d.ID != device {
			continue
		}
		bound := d.machine(m)
		report := newExtensionReport(e, &bound, "kind:sample")
		report.Setup, report.Qualified, report.CheckID, report.Reason = true, true, "fixture-check", ""
		d.ExtensionReports = []ExtensionReport{report}
		if err := writeExtensionReports(root, e, "fake", d.ExtensionReports); err != nil {
			t.Fatal(err)
		}
	}
	return e, m
}

func testCustodianStarted(t *testing.T, device, kind string) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := recoveryWorker(t)
	m := m3Machine()
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
	spec := spec("experiment")
	spec.Resumable = false
	spec.Needs.MemoryGB, spec.Needs.HostMemoryGB = 0.125, 0.25
	spec.Needs.GPU = &GPUSelection{Devices: []string{device}}
	proof := t.TempDir()
	spec.Run.Env = map[string]string{"PROOF_DIR": proof}
	spec.Run.Command = []string{"sh", "-c", `test -z "${MYCE_TOKEN+x}" || exit 97; test "$(cat "$MYCE_ENVIRONMENT_DIR/assets/proof")" = custodian-asset || exit 98; printf '%s' "$MYCE_DEVICES" > "$PROOF_DIR/device"; while [ ! -f "$PROOF_DIR/finish" ]; do sleep 0.01; done`}
	var extension Extension
	if kind != "command" {
		extension, m = custodianExtensionFixture(t, q, clock, w, m, device, proof)
		spec.Extension, spec.Kind, spec.Run = extensionKey(extension), "sample", RunSpec{}
		spec.Needs = ResourceNeeds{} // Frozen extension owns reservations; only this card is qualified.
	}
	pending := submit(t, q, spec)
	assigned := assignedJob(machineCheck(t, q, m))
	if assigned == nil || assigned.ID != pending.ID || !slices.Equal(assigned.AllocatedDevices, []string{device}) {
		t.Fatal("missing pinned GPU assignment", assigned)
	}
	// The native extension may write only its work/checkpoint roots. Keep the
	// handshake in that declared writable root, including under Linux Landlock.
	assigned.Checkpoint = proof
	w.Machine, w.Clock, w.Client = m, clock, Client{URL: server.URL, Token: "test", Clock: clock}
	// Worker.Run also installs the asset transport. Exercise that skipped
	// dependency through the detached entry rather than a primed cache.
	if kind == "command" {
		_, asset := retentionUpload(t, w.Client, "custodian-asset")
		assigned.EnvironmentRecipes = map[string]EnvironmentRecipe{"fake": {Key: asset.SHA256, Assets: FileManifest{"proof": asset}}}
	}
	w.Custodian, w.ServiceConfig = ID(), &ServiceConfig{}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	if runtime.GOOS == "linux" {
		w.memoryCeilings = fakeCeilings(t)
	} else {
		w.processMemory = workerDevProcessMemory()
	}
	w.custodianIsolation = func(string) (string, error) { return "", nil }
	roots, err := w.StateRoots()
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareStateRoots(roots...); err != nil {
		t.Fatal(err)
	}
	for _, root := range roots {
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if kind == "missing" || kind == "qualification" {
		path := filepath.Join(extensionFolder(filepath.Join(roots[0], "extensions"), extension), "extension.yaml")
		if kind == "qualification" {
			path = extensionRecords(filepath.Join(roots[0], "extensions"), extension, "fake")
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	dir := custodianDir(roots[0])
	req := custodianRequest{Protocol: custodianProtocol, ID: w.Custodian, Machine: m, State: workerState{Job: assigned, Session: assigned.Session, Identity: ID()}}
	if err := saveWorkerJSON(dir, custodianName(w.Custodian, "request"), req); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.runCustodian(ctx) }()
	defer func() { cancel(); <-done }()
	g := &attemptCustodian{dir: dir, id: w.Custodian}
	parent := Worker{hostChargesExcept: func(attemptKey) (float64, float64) { return 0, 0 }}
	var snapshot custodianSnapshot
	await(t, func() bool {
		parent.serveCustodianGate(ctx, g, *assigned)
		return readCustodianJSON(dir, custodianName(w.Custodian, "snapshot"), &snapshot) == nil && (snapshot.Started || snapshot.Finished)
	})
	if kind == "missing" || kind == "qualification" {
		r := snapshot.State.Result
		if snapshot.Started || !snapshot.Finished || r == nil || !r.ExtensionUnavailable || r.ExecutionCause == nil || r.ExecutionCause.Class != environmentFailed || r.ExecutionCause.Predicate != "environment_prepare" || r.ExecutionEvidence == nil || r.ExecutionEvidence.Main != executionNotStarted || r.ExecutionEvidence.Sidecars != executionNotStarted {
			t.Fatalf("extension refusal lost its boundary or diagnostic: %+v", r)
		}
		f := terminalJobFinding(&Job{State: "failed", Attempt: assigned.Attempt, ExecutionCause: r.ExecutionCause, ExecutionEvidence: r.ExecutionEvidence, Reason: "execution failed"})
		if f == nil || f.Code != "launch_environment_prepare" {
			t.Fatalf("extension refusal finding: %+v", f)
		}
		t.Logf("%s: real custodian refused before Started with %s/%s (%s)", kind, r.ExecutionCause.Class, r.ExecutionCause.Predicate, runtime.GOOS)
		return
	}
	if !snapshot.Started || job(t, q, pending.ID).StartedAt == nil {
		t.Fatal("GPU helper did not reach Started", snapshot.State.Result)
	}
	path := filepath.Join(proof, "device")
	await(t, func() bool { data, err := os.ReadFile(path); return err == nil && string(data) == device })
	if data, err := os.ReadFile(path); err != nil || string(data) != device {
		t.Fatalf("selected device: %q, %v", data, err)
	}
	if err := os.WriteFile(filepath.Join(proof, "finish"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		return readCustodianJSON(dir, custodianName(w.Custodian, "snapshot"), &snapshot) == nil && snapshot.Finished
	})
	if snapshot.State.Result == nil || !snapshot.State.Result.Success {
		t.Fatal("GPU helper did not complete", snapshot.State.Result)
	}
	t.Logf("%s GPU %s: real custodian Started, queue started_at=%s, successful completion (%s)", kind, device, job(t, q, pending.ID).StartedAt.Format(time.RFC3339Nano), runtime.GOOS)
}

func TestCustodianRejectsUnprovedIdentity(t *testing.T) {
	j := Job{ID: ID(), Attempt: 2, Machine: "worker", Session: "session"}
	id, dir := ID(), t.TempDir()
	birth, err := processBirth(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	base := custodianSnapshot{Protocol: custodianProtocol, ID: id, PID: os.Getpid(), Birth: birth, State: workerState{Job: &j, Session: j.Session}}
	for _, kind := range []string{"reused-pid", "wrong-attempt", "wrong-session", "wrong-manager", "wrong-result"} {
		t.Run(kind, func(t *testing.T) {
			v := base
			jcopy := j
			v.State.Job = &jcopy
			switch kind {
			case "reused-pid":
				v.Birth = "another-process-birth"
			case "wrong-attempt":
				jcopy.Attempt++
			case "wrong-session":
				v.State.Session = "other"
			case "wrong-result":
				v.Finished = true
				v.State.Result = &Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt + 1}}
			}
			if err := saveWorkerJSON(dir, custodianName(id, "snapshot"), v); err != nil {
				t.Fatal(err)
			}
			g := &attemptCustodian{dir: dir, id: id}
			if err := g.read(j); err == nil || g.observed {
				t.Fatal("unproved custody adopted", kind)
			}
		})
	}
	// A finished exact result needs no live PID, so restart replay remains valid.
	base.Finished = true
	base.State.Result = &Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	if err := saveWorkerJSON(dir, custodianName(id, "snapshot"), base); err != nil {
		t.Fatal(err)
	}
	g := &attemptCustodian{dir: dir, id: id}
	if err := g.read(j); err != nil {
		t.Fatal(err)
	}
}

func TestCustodianStopDeadlineSurvivesAdoption(t *testing.T) {
	dir := t.TempDir()
	id := ID()
	j := Job{ID: ID(), Attempt: 1, Machine: "worker", Session: "session"}
	r := Result{Machine: j.Machine, Session: j.Session, Execution: runExecution(&j)}
	snapshot := custodianSnapshot{Protocol: custodianProtocol, ID: id, Finished: true, State: workerState{Job: &j, Session: j.Session, Result: &r}}
	custody := custodianDir(dir)
	if err := os.Mkdir(custody, 0700); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(custody, custodianName(id, "snapshot"), snapshot); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Second)
	c := custodianControl{ID: id, Execution: runExecution(&j), Session: j.Session, StopAt: &deadline, UserCancel: true}
	if err := saveWorkerJSON(custody, custodianName(id, "control"), c); err != nil {
		t.Fatal(err)
	}
	s := &workerStore{dir: dir, state: workerState{Job: &j, Session: j.Session, Custodian: id}}
	p, err := (Worker{}).adoptCustodian(s)
	if err != nil {
		t.Fatal(err)
	}
	defer p.custodian.detach()
	p.custodian.stopLocked(p, now, time.Minute, false)
	var saved custodianControl
	if err := readCustodianJSON(custody, custodianName(id, "control"), &saved); err != nil {
		t.Fatal(err)
	}
	if !saved.StopAt.Equal(deadline) || !saved.UserCancel {
		t.Fatal("adoption renewed deadline or lost cancel provenance", saved)
	}
}

func TestCustodianFinalGateRequiresExactRequest(t *testing.T) {
	dir := t.TempDir()
	g := &attemptCustodian{dir: dir, id: ID()}
	j := Job{ID: ID(), Attempt: 2, Machine: "worker", Session: "session"}
	launch := HostLaunch{Machine: j.Machine, Session: j.Session, Kind: "batch", Key: j.ID, Attempt: "2"}
	calls := 0
	w := Worker{batchAdmission: func(context.Context, HostLaunch) error { calls++; return errors.New("aggregate host memory capacity") }}
	// A stale receipt cannot authorize a later final gate; the aggregate ledger
	// evaluates the new nonce and returns the existing refusal to the custodian.
	old := custodianGateReceipt{Nonce: "old"}
	if err := saveWorkerJSON(dir, custodianName(g.id, "gate-receipt"), old); err != nil {
		t.Fatal(err)
	}
	request := custodianGate{Nonce: "new", Launch: launch}
	request.Launch.Session = "other"
	if err := saveWorkerJSON(dir, custodianName(g.id, "gate"), request); err != nil {
		t.Fatal(err)
	}
	w.serveCustodianGate(context.Background(), g, j)
	if calls != 0 {
		t.Fatal("wrong execution reached admission")
	}
	request.Launch = launch
	if err := saveWorkerJSON(dir, custodianName(g.id, "gate"), request); err != nil {
		t.Fatal(err)
	}
	w.serveCustodianGate(context.Background(), g, j)
	w.serveCustodianGate(context.Background(), g, j)
	var receipt custodianGateReceipt
	if err := readCustodianJSON(dir, custodianName(g.id, "gate-receipt"), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Nonce != "new" || receipt.Error != "aggregate host memory capacity" || calls != 1 {
		t.Fatal("gate bypassed aggregate admission or replayed", receipt, calls)
	}
}

func TestCustodianAccountingIncludesWorkerAndDetachedSiblings(t *testing.T) {
	dir := t.TempDir()
	worker, jobs := filepath.Join(dir, "worker.service"), filepath.Join(dir, "jobs.slice")
	for root, pid := range map[string]string{worker: "41", jobs: "42"} {
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "memory.stat"), []byte("anon 1073741824\nkernel 0\nshmem 0\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "cgroup.procs"), []byte(pid), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a := &memoryAttempt{fs: osMemoryFS{}, path: filepath.Join(jobs, "scope", "attempt"), accountingRoots: []string{worker, jobs}}
	if gb, ok := a.accountingResidentGB(); !ok || gb != 2 {
		t.Fatal("detached sibling RAM omitted", gb, ok)
	}
	ids := map[int]bool{}
	if err := a.accountingProcessIDs(ids); err != nil {
		t.Fatal(err)
	}
	if !ids[41] || !ids[42] {
		t.Fatal("worker engines or detached jobs misclassified as outside processes", ids)
	}
	if err := os.Remove(filepath.Join(worker, "memory.stat")); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.accountingResidentGB(); ok {
		t.Fatal("incomplete sibling attribution reported complete")
	}
}

func TestCustodianSharedHostKeepsAggregateCPUQuota(t *testing.T) {
	w := Worker{Machine: Machine{SharedHost: &SharedHostProfile{}}}
	if runtime.GOOS == "linux" && w.detachedAttemptSupported() {
		t.Fatal("detached jobs split worker/engine aggregate CPU cap")
	}
	if runtime.GOOS == "darwin" && !w.detachedAttemptSupported() {
		t.Fatal("cooperative Darwin policy unnecessarily drained")
	}
}

// A failed detached environment build may have no payload PID but still own a
// builder. Its receipt must reach the recovery path; an in-process writer check
// alone must never certify release of that unobserved helper.
func TestCustodianPreparationFailureRetainsHelperCustody(t *testing.T) {
	j := Job{ID: ID(), Attempt: 1, Machine: "worker", Session: "session"}
	g := &attemptCustodian{last: custodianSnapshot{State: workerState{Job: &j, Session: j.Session, InputWriterPending: true, Preparation: "preparation-unconfirmed.json"}}}
	s := &workerStore{dir: t.TempDir(), state: workerState{Job: &j, Session: j.Session}}
	g.updateStore(s)
	if err := s.cleanupOrphan(&fakeClock{now: time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)}); err == nil {
		t.Fatal("missing helper receipt incorrectly proved release")
	}
}

// The service entry point can fail before runCustodian publishes its first
// snapshot (#1709). A reaped pre-snapshot launch must become an exact finished
// refusal, so the ordinary adoption/completion path releases even a cancelled
// assignment. Missing observation alone must never manufacture that proof.
func TestCustodianStartupFailureReleasesCancelledAssignment(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	submit(t, q, spec("experiment"))
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("no assignment")
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	dir, id := t.TempDir(), ID()
	custody := custodianDir(dir)
	if err := os.Mkdir(custody, 0700); err != nil {
		t.Fatal(err)
	}
	req := custodianRequest{Protocol: custodianProtocol, ID: id, Machine: m, State: workerState{Job: j, Session: j.Session}}
	if err := saveWorkerJSON(custody, custodianName(id, "request"), req); err != nil {
		t.Fatal(err)
	}
	store := &workerStore{dir: dir, state: req.State}
	store.state.Custodian = id
	w := Worker{Clock: clock}
	if _, err := w.adoptCustodian(store); err == nil {
		t.Fatal("missing snapshot released uncertain custody")
	}
	if err := RecordCustodianStartupFailure(dir, id); err != nil {
		t.Fatal(err)
	}
	if err := waitCustodian(t.Context(), custody, id); err != nil {
		t.Fatal(err)
	}
	p, err := w.adoptCustodian(store)
	if err != nil {
		t.Fatal(err)
	}
	defer p.custodian.detach()
	select {
	case f := <-p.done:
		r := f.result
		if r.ReleasePending || r.ExecutionCause == nil || !r.ExecutionCause.PrelaunchRefused || r.ExecutionEvidence == nil || r.ExecutionEvidence.Main != executionNotStarted || r.Error != errCustodianStartup.Error() {
			t.Fatalf("startup failure lost refusal/release evidence: %+v", r)
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, j.ID)
		if got.State != "cancelled" || !got.ReleaseConfirmed || got.CommandStarted != nil && *got.CommandStarted {
			t.Fatalf("cancel remained pending: %+v", got)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("finished startup refusal did not release promptly")
	}
}

func TestCustodianStartupFailurePreservesExistingCustody(t *testing.T) {
	for _, kind := range []string{"live", "finished", "corrupt", "wrong-request"} {
		t.Run(kind, func(t *testing.T) {
			dir, id := t.TempDir(), ID()
			custody := custodianDir(dir)
			if err := os.Mkdir(custody, 0700); err != nil {
				t.Fatal(err)
			}
			j := Job{ID: ID(), Attempt: 1, Machine: "worker", Session: "session"}
			req := custodianRequest{Protocol: custodianProtocol, ID: id, Machine: Machine{ID: j.Machine}, State: workerState{Job: &j, Session: j.Session}}
			if kind == "wrong-request" {
				req.ID = ID()
			}
			if err := saveWorkerJSON(custody, custodianName(id, "request"), req); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(custody, custodianName(id, "snapshot"))
			var before []byte
			if kind != "wrong-request" {
				before = []byte(`{"finished":false}`)
				if kind == "finished" {
					before = []byte(`{"finished":true}`)
				}
				if kind == "corrupt" {
					before = []byte(`{`)
				}
				if err := os.WriteFile(path, before, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := RecordCustodianStartupFailure(dir, id); err == nil {
				t.Fatal("uncertain custody overwritten")
			}
			after, err := os.ReadFile(path)
			if kind == "wrong-request" {
				if !os.IsNotExist(err) {
					t.Fatal("wrong identity published", err)
				}
			} else if string(after) != string(before) {
				t.Fatal("snapshot changed")
			}
		})
	}
}

func TestCustodianUnadoptedCancellationPersistsControl(t *testing.T) {
	dir, id := t.TempDir(), ID()
	if err := os.Mkdir(custodianDir(dir), 0700); err != nil {
		t.Fatal(err)
	}
	j := Job{ID: ID(), Attempt: 1, Machine: "worker", Session: "session"}
	s := &workerStore{dir: dir, state: workerState{Job: &j, Session: j.Session, Custodian: id}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	clock := &workerFakeClock{now: time.Unix(100, 0)}
	if err := persistUnadoptedCustodianStop(s, j, AttemptDirective{Stop: true, Kill: true, UserCancel: true}, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := (Worker{Clock: clock}).startCustodian(ctx, s, j); !errors.Is(err, errWorkerCheckpoint) {
		t.Fatal(err)
	}
	var c custodianControl
	if err := readCustodianJSON(custodianDir(dir), custodianName(id, "control"), &c); err != nil {
		t.Fatal(err)
	}
	if c.ID != id || c.Execution != runExecution(&j) || c.Session != j.Session || c.StopAt == nil || !c.StopAt.Equal(clock.Now()) || !c.Kill || !c.UserCancel {
		t.Fatalf("cancellation lost: %+v", c)
	}
}

func TestCustodianWorkerReplacementDoesNotStopUnadoptedAttempt(t *testing.T) {
	dir, id := t.TempDir(), ID()
	if err := os.Mkdir(custodianDir(dir), 0700); err != nil {
		t.Fatal(err)
	}
	j := Job{ID: ID(), Attempt: 1, Machine: "worker", Session: "session"}
	s := &workerStore{dir: dir, state: workerState{Job: &j, Session: j.Session, Custodian: id}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _ = (Worker{}).startCustodian(ctx, s, j)
	if _, err := os.Stat(filepath.Join(custodianDir(dir), custodianName(id, "control"))); !os.IsNotExist(err) {
		t.Fatal("worker context cancellation stopped detached work", err)
	}
}
