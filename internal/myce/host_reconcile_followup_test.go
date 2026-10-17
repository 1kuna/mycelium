package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHostReconcileSessionDischargeWithoutAssignmentFile(t *testing.T) {
	for _, unregistered := range []bool{false, true} {
		t.Run(stringBool(unregistered), func(t *testing.T) {
			q, clock := fixture(t)
			dir, spool := t.TempDir(), t.TempDir()
			s := testHostOpen(t, dir, clock)
			m := machine("registered", "fake")
			m.HostID = s.Host
			check(t, q, m, nil)
			small := spec("experiment")
			small.Needs.MemoryGB = 1
			j := submit(t, q, small)
			j = check(t, q, m, nil).Job
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			if err := AdoptHostWorker(dir, HostWorker{Host: s.Host, State: spool, Name: m.ID, Session: m.Session, Queue: server.URL}); err != nil {
				t.Fatal(err)
			}
			// The queue committed this assignment, but the worker never wrote it.
			// A historical native result is diagnostic, not current custody.
			if err := saveWorkerJSON(spool, "historical.result.json", Result{Machine: m.ID, Session: "old", Execution: Execution{ID: "unknown", Attempt: 1}}); err != nil {
				t.Fatal(err)
			}
			if unregistered {
				other := machine("unregistered", "fake")
				other.HostID = s.Host
				check(t, q, other, nil)
				otherJob := spec("experiment")
				otherJob.Needs.MemoryGB = 1
				otherJob.AllowedMachines = []string{other.ID}
				submit(t, q, otherJob)
				if reply := check(t, q, other, nil); reply.Job == nil {
					t.Fatal("missing unregistered-session fixture")
				}
			}
			if report, err := (HostControl{Dir: dir, Clock: clock, Service: &fakeHostService{}}).Disable(context.Background(), "planned", 0); err != nil || !report.Confirmed {
				t.Fatal(report, err)
			}
			if err := ReconcileHostLocal(context.Background(), dir, clock, func(endpoint string) (Client, error) { return Client{URL: endpoint, Token: "test"}, nil }); err != nil {
				t.Fatal(err)
			}
			after, err := ReadHostIntent(dir)
			if err != nil || !slices.Contains(after.Holds, "diagnostic:historical_result") || job(t, q, j.ID).State != "queued" || job(t, q, j.ID).Losses != 0 {
				t.Fatal(after, err)
			}
			if unregistered {
				if after.HostSettlement() == "confirmed" || len(after.Pending) != 1 || after.Pending[0].Result.Machine != "unregistered" {
					t.Fatal("unregistered session discharged", after)
				}
			} else if after.HostSettlement() != "confirmed" || len(after.Pending) != 0 {
				t.Fatal("lost assignment or historical file blocked session discharge", after)
			}
		})
	}
}

func TestHostReconcileUnknownCurrentCustodyIsDiagnostic(t *testing.T) {
	q, clock := fixture(t)
	dir, spool := t.TempDir(), t.TempDir()
	s := testHostOpen(t, dir, clock)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	if err := AdoptHostWorker(dir, HostWorker{Host: s.Host, Name: "unknown-machine", Session: "session", State: spool, Queue: server.URL}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(spool, "state.json", workerState{Session: "session", Job: &Job{ID: "unknown-job", Attempt: 1}}); err != nil {
		t.Fatal(err)
	}
	if report, err := (HostControl{Dir: dir, Clock: clock, Service: &fakeHostService{}}).Disable(context.Background(), "close", 0); err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	if err := ReconcileHostLocal(context.Background(), dir, clock, func(endpoint string) (Client, error) { return Client{URL: endpoint, Token: "test"}, nil }); err != nil {
		t.Fatal(err)
	}
	after, err := ReadHostIntent(dir)
	if err != nil || after.HostSettlement() != "confirmed" || len(after.Pending) != 0 || !slices.Contains(after.Holds, "diagnostic:unmatched_local_custody") {
		t.Fatal(after, err)
	}
}

func TestHostReconcileLocalCorruptionDoesNotBlockForeignCheckin(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	s := testHostOpen(t, dir, clock, "local-host")
	if err := q.ConfigureLocalHost(dir, "local-queue"); err != nil {
		t.Fatal(err)
	}
	local := machine("local", "fake")
	local.HostID = s.Host
	check(t, q, local, nil)
	foreign := machine("foreign", "fake")
	foreign.HostID = "foreign-host"
	check(t, q, foreign, nil)
	spec := spec("experiment")
	spec.AllowedMachines = []string{foreign.ID}
	submit(t, q, spec)
	j := check(t, q, foreign, nil).Job
	if j == nil {
		t.Fatal("foreign assignment missing")
	}
	if err := os.WriteFile(filepath.Join(dir, hostStateFile), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if reply, err := q.Checkin(Checkin{Machine: foreign, Running: &Execution{ID: j.ID, Attempt: j.Attempt}, Started: true}); err != nil || reply.Leave {
		t.Fatal("foreign check-in coupled to local state", reply, err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal("local read failure would terminate queue Run", err)
	}
	if reply, err := q.Checkin(Checkin{Machine: local}); err != nil || reply.Job != nil || !reply.AdmissionBlocked {
		t.Fatal("corrupt local state admitted local work", reply, err)
	}
	if got := job(t, q, j.ID); got.State != "running" || got.Losses != 0 {
		t.Fatal("foreign execution changed", got)
	}
}

func TestHostReconcileCommittedSubmitIgnoresAckWriteFailure(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	s := testHostOpen(t, dir, clock)
	if err := q.ConfigureLocalHost(dir, "queue"); err != nil {
		t.Fatal(err)
	}
	// Existing intake.lock remains writable; the directory refuses atomic replace.
	if err := mutateHostIntent(dir, s.HostRevision, func(*HostIntent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
	spec := spec("experiment")
	spec.SubmissionKey = "committed-submit"
	j, err := q.Submit(spec)
	if err != nil || j == nil {
		t.Fatal("committed submit returned local acknowledgement error", j, err)
	}
	if got := job(t, q, j.ID); got.ID != j.ID {
		t.Fatal(got)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	after, err := ReadHostIntent(dir)
	if err != nil || after.QueueAcks["queue"].HostRevision != s.HostRevision {
		t.Fatal("Tick did not retry acknowledgement", after, err)
	}
	if retry, err := q.Submit(spec); err != nil || retry.ID != j.ID {
		t.Fatal("ack failure duplicated submission", retry, err)
	}
}

func TestHostReconcileModelAdmissionLearnsLocalClose(t *testing.T) {
	for _, pin := range []bool{false, true} {
		t.Run(stringBool(pin), func(t *testing.T) {
			q, clock, m := modelFixture(t)
			dir := t.TempDir()
			s := testHostOpen(t, dir, clock, hostID(&m))
			if err := q.ConfigureLocalHost(dir, "queue"); err != nil {
				t.Fatal(err)
			}
			if _, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed"}); err != nil {
				t.Fatal(err)
			}
			if pin {
				if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			} else {
				addModelCall(t, q, "experiment")
			}
			if q.hostClosedReason(s.Host) == "" || len(q.s.ModelSlots) != 0 {
				t.Fatal("model admission bypassed local close", q.s.HostIntents[s.Host], q.s.ModelSlots)
			}
		})
	}
}

func TestHostReconcileRejectionKeepsRunningWorkerAlive(t *testing.T) {
	q, _ := fixture(t)
	localDir := t.TempDir()
	testHostOpen(t, localDir, RealClock{}, "local-host")
	if err := q.ConfigureLocalHost(localDir, "local-queue"); err != nil {
		t.Fatal(err)
	}
	remoteDir := readyTestHost(t, t.TempDir(), "remote-host")
	var reject atomic.Bool
	var rejected atomic.Int32
	api := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reject.Load() && strings.HasSuffix(r.URL.Path, "/reconcile") {
			rejected.Add(1)
			respond(w, nil, &hostReconcileError{cause: ErrHostRevision, code: "host_reconcile_rejected"})
			return
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	finish := filepath.Join(t.TempDir(), "finish")
	spec := spec("experiment")
	spec.Run.Command = []string{"/bin/sh", "-c", `echo ready; while [ ! -f "$FINISH" ]; do sleep 0.02; done`}
	spec.Run.Env = map[string]string{"FINISH": finish}
	j := submit(t, q, spec)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := Worker{Machine: machine("remote", "fake"), HostStateDir: remoteDir, StateDir: t.TempDir(), Clock: RealClock{}, Interval: 5 * time.Millisecond, Client: Client{URL: server.URL, Token: "test"}}
	go func() { done <- w.Run(ctx) }()
	defer func() {
		reject.Store(false)
		_ = os.WriteFile(finish, nil, 0600)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("worker did not stop")
		}
	}()
	await(t, func() bool { logs, _ := q.Logs(j.ID, 0); return len(logs) > 0 })
	// A co-located host closes while a different host's command is running.
	if _, err := changeHostIntent(localDir, RealClock{}, hostIntentChange{Desired: "closed"}); err != nil {
		t.Fatal(err)
	}
	reject.Store(true)
	await(t, func() bool { return rejected.Load() >= 2 })
	select {
	case err := <-done:
		t.Fatal("host reconcile rejection terminated running worker", err)
	default:
	}
	if got := job(t, q, j.ID); got.State != "running" || got.Losses != 0 {
		t.Fatal(got)
	}
	reject.Store(false)
	if err := os.WriteFile(finish, nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return job(t, q, j.ID).State == "succeeded" })
}

func TestHostReconcileRevisionAheadRecovery(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	s := testHostOpen(t, dir, clock)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ahead := closeExchange(s.Host, clock)
	ahead.Revision = s.Revision + 7
	if _, err := q.ReconcileHost(ahead); err != nil {
		t.Fatal(err)
	}
	client := Client{URL: server.URL, Token: "test"}
	err := reconcileWorkerHost(context.Background(), dir, clock, client)
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Status != 409 || typed.Code != "host_revision_behind_queue" || typed.QueueRevision != ahead.HostRevision {
		t.Fatal(err)
	}
	observed, err := ReadHostIntent(dir)
	if err != nil || hostStatusCode(observed) != "host_revision_behind_queue" {
		t.Fatal(observed, err)
	}
	closed, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed"})
	if err != nil || closed.Revision != ahead.Revision+1 {
		t.Fatal(closed, err)
	}
	if err = reconcileWorkerHost(context.Background(), dir, clock, client); err != nil {
		t.Fatal(err)
	}
	recovered, err := ReadHostIntent(dir)
	if err != nil || hostStatusCode(recovered) != "" || recovered.HostSettlement() != "pending" {
		t.Fatal("recovery bypassed fresh exclusion", recovered, err)
	}
}

func TestHostReconcileThirdLossRefundRevivesHistoricalCopy(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(stringBool(cancel), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("old-worker", "fake")
			m.HostID = "old-host"
			check(t, q, m, nil)
			j := submit(t, q, spec("experiment"))
			j = check(t, q, m, nil).Job
			q.mu.Lock()
			saved := q.s.Jobs[j.ID]
			saved.Losses, saved.State, saved.Reason = 3, "failed", "failed after 3 infrastructure losses: worker quiet"
			saved.Copies[0].LossCharged = true
			saved.Machine, saved.Session, saved.Attempt = "new-worker", "new-session", 3
			if cancel {
				now := clock.Now()
				saved.CancelledAt = &now
			}
			q.mu.Unlock()
			clock.Advance(time.Hour)
			request := closeExchange(m.HostID, clock)
			request.Sessions = []HostSession{{Machine: m.ID, Session: m.Session}}
			if _, err := q.ReconcileHost(request); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.Losses != 2 || !cancel && (got.State != "queued" || !got.RequeuedByOperator) || cancel && got.State != "failed" {
				t.Fatal(got)
			}
		})
	}
}

func TestHostReconcileConfirmedCleanupAndContentionAreIdempotent(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	testHostOpen(t, dir, clock)
	var stops int
	h := HostControl{Dir: dir, Clock: clock, Service: reconcileStopHook{stop: func() { stops++ }}}
	first, err := h.Disable(context.Background(), "close", 0)
	if err != nil || !first.Confirmed {
		t.Fatal(first, err)
	}
	before, _ := os.Stat(filepath.Join(dir, hostStateFile))
	if again, err := h.Disable(context.Background(), "close", 0); err != nil || !again.Confirmed || stops != 1 {
		t.Fatal(again, err, stops)
	}
	after, _ := os.Stat(filepath.Join(dir, hostStateFile))
	if !os.SameFile(before, after) {
		t.Fatal("unchanged confirmed close rewrote host state")
	}
	unlock, err := hostOperationLock(dir, "reconcile.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if again, err := h.Disable(context.Background(), "close", 0); err != nil || again.HostRevision != first.HostRevision || !again.Confirmed {
		t.Fatal("repeated accepted close failed under contention", again, err)
	}
}

func TestHostReconcileRepeatedCloseDuringStopReturnsAccepted(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	testHostOpen(t, dir, clock)
	stopping, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	h := HostControl{Dir: dir, Clock: clock, Service: reconcileStopHook{stop: func() { close(stopping); <-release }}}
	done := make(chan error, 1)
	go func() { _, err := h.Disable(context.Background(), "close", 0); done <- err }()
	select {
	case <-stopping:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not reach stop")
	}
	accepted, err := ReadHostIntent(dir)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := h.Disable(context.Background(), "close", 0)
	if err != nil || repeated.HostRevision != accepted.HostRevision || repeated.Confirmed {
		t.Fatal("in-progress close did not return its accepted revision", repeated, err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestHostReconcileDockerClearanceWaitsForCleanupLock(t *testing.T) {
	hostDir, spool := t.TempDir(), t.TempDir()
	w := Worker{StateDir: spool}
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	j := dockerJob()
	s.state.Job, s.state.Identity = &j, "owned"
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	s.close()
	if err := saveWorkerJSON(spool, "owned.docker.json", dockerExecution{Identity: "owned", Endpoint: "unix:///fake", Pending: true}); err != nil {
		t.Fatal(err)
	}
	unlock, err := hostOperationLock(hostDir, "reconcile.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() { once.Do(unlock) }
	defer release()
	done := make(chan error, 1)
	go func() { done <- ConfirmDockerGone(hostDir, spool, "owned", true) }()
	select {
	case err := <-done:
		t.Fatal("clearance returned before cleanup released its lock", err)
	case <-time.After(30 * time.Millisecond):
	}
	release()
	if err := <-done; err != nil {
		t.Fatal("clearance failed after brief contention", err)
	}
}

func TestHostReconcileHeartbeatDoesNotRewriteState(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	testHostOpen(t, dir, clock)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	if err := reconcileWorkerHost(context.Background(), dir, clock, client); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(filepath.Join(dir, hostStateFile))
	if err := reconcileWorkerHost(context.Background(), dir, clock, client); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(filepath.Join(dir, hostStateFile))
	if !os.SameFile(before, after) {
		t.Fatal("heartbeat rewrote unchanged host state")
	}
	if err := ReconcileHostLocal(context.Background(), dir, clock, func(string) (Client, error) { t.Fatal("settled open timer contacted queue"); return Client{}, nil }); err != nil {
		t.Fatal(err)
	}
}

func TestHostReconcileFirstOpenPreservesIndependentLeave(t *testing.T) {
	q, clock := fixture(t)
	m := machine("leaving", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	q.s.Machines[m.ID].Leaving = true
	request := closeExchange(m.HostID, clock)
	if _, err := q.ReconcileHost(request); err != nil {
		t.Fatal(err)
	}
	request.LastClose = &HostClose{HostRevision: request.HostRevision, Settlement: "confirmed"}
	request.Revision++
	request.Operation, request.Desired, request.Request = ID(), "open", ID()
	if _, err := q.ReconcileHost(request); err != nil || !q.s.Machines[m.ID].Leaving {
		t.Fatal("first open revoked independent leave", err)
	}
}

func TestHostReconcileCurrentCustodyAndMissingSpool(t *testing.T) {
	dir := t.TempDir()
	w := HostWorker{Name: "one", State: dir, Queue: "queue"}
	j := &Job{ID: "current", Attempt: 2}
	r := Result{Machine: w.Name, Session: "session", Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	for name, value := range map[string]any{"state.json": workerState{Session: r.Session, Identity: "current", Job: j}, "current.result.json": r, "old.result.json": Result{}, "old.docker.json": dockerExecution{Released: true, Owner: &r}} {
		if err := saveWorkerJSON(dir, name, value); err != nil {
			t.Fatal(err)
		}
	}
	custody, results, holds, err := hostCustody([]HostWorker{w})
	if err != nil || len(custody) != 1 || !reflect.DeepEqual(results, []Result{r}) || !slices.Contains(holds, "diagnostic:historical_result") {
		t.Fatal(custody, results, holds, err)
	}
	w.State = filepath.Join(dir, "missing")
	_, _, holds, err = hostCustody([]HostWorker{w})
	if err != nil || !slices.Contains(holds, "missing_worker_spool") {
		t.Fatal(holds, err)
	}
}

func TestHostReconcileConflictResponseHasTypedCode(t *testing.T) {
	q, clock := fixture(t)
	request := closeExchange("host", clock)
	if _, err := q.ReconcileHost(request); err != nil {
		t.Fatal(err)
	}
	request.Operation = ID()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	var ack HostAcknowledgement
	err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/hosts/host/reconcile", request, &ack)
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Status != 409 || typed.Code != "host_reconcile_rejected" {
		t.Fatal(err)
	}
	b, _ := json.Marshal(typed)
	if strings.Contains(string(b), "stale execution") {
		t.Fatal("host rejection confused with a stale execution")
	}
}

func TestHostReconcileProductionResultAndDischargeAgree(t *testing.T) {
	for _, resultPath := range []bool{false, true} {
		t.Run(stringBool(resultPath), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("production", "fake")
			m.HostID = "host"
			check(t, q, m, nil)
			j := submit(t, q, spec("production"))
			j = check(t, q, m, nil).Job
			if err := q.Started(result(j, false)); err != nil {
				t.Fatal(err)
			}
			request := closeExchange(m.HostID, clock)
			if _, err := q.ReconcileHost(request); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Minute)
			if resultPath {
				r := result(j, false)
				r.Interrupted = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			} else {
				request.Sessions = []HostSession{{Machine: m.ID, Session: m.Session}}
				if _, err := q.ReconcileHost(request); err != nil {
					t.Fatal(err)
				}
			}
			got := job(t, q, j.ID)
			if got.State != "failed" || got.Reason != "host closed" || got.FinishedAt == nil || !got.FinishedAt.Equal(clock.Now()) || got.Losses != 0 {
				t.Fatal(got)
			}
		})
	}
}

func TestHostReconcileRechecksRevisionBeforeStop(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	testHostOpen(t, dir, clock)
	stops := 0
	h := HostControl{Dir: dir, Clock: clock, Service: reconcileStopHook{stop: func() { stops++ }}}
	h.Prepare = func() error {
		testConfirmClose(t, dir)
		s, err := ReadHostIntent(dir)
		if err != nil {
			return err
		}
		// Simulate a caller predating S08's shared open lock, after discovery.
		_, err = changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: s.HostRevision})
		return err
	}
	if _, err := h.Disable(context.Background(), "close", 0); !errors.Is(err, ErrHostRevision) || stops != 0 {
		t.Fatal("stale close stopped a reopened service", err, stops)
	}
}

func TestHostReconcileDockerHoldClearsAfterQueueRestart(t *testing.T) {
	w, store, clock, daemon := dockerFixture(t)
	q, _ := fixture(t)
	q.clock = clock
	s, err := ReadHostIntent(w.HostStateDir)
	if err != nil {
		t.Fatal(err)
	}
	m := w.Machine
	m.HostID = s.Host
	check(t, q, m, nil)
	j := submit(t, q, dockerJob().Spec)
	j = check(t, q, m, nil).Job
	store.state.Identity = "owned"
	store.state.Session = m.Session
	store.state.Job = j
	if err = store.save(); err != nil {
		t.Fatal(err)
	}
	owner := result(j, false)
	if err = saveWorkerJSON(store.dir, "owned.docker.json", dockerExecution{Identity: "owned", Endpoint: "unix:///fake", Container: "id1", Owner: &owner}); err != nil {
		t.Fatal(err)
	}
	store.close()
	for name, value := range map[string]string{"created": "id1", "label": "owned", "running": "yes", "mode": "down"} {
		if err = os.WriteFile(filepath.Join(daemon, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var apiMu sync.RWMutex
	active := q
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiMu.RLock()
		defer apiMu.RUnlock()
		API{Queue: active, Token: "test"}.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()
	if err = AdoptHostWorker(w.HostStateDir, HostWorker{State: store.dir, Host: s.Host, Name: m.ID, Session: m.Session, Queue: server.URL}); err != nil {
		t.Fatal(err)
	}
	cancelAck(t, q, j.ID)
	pending := owner
	pending.ReleasePending = true
	if err = q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Dir: w.HostStateDir, Clock: clock, Service: &fakeHostService{}, Docker: true, Timeout: 150 * time.Millisecond}
	if report, err := h.Disable(context.Background(), "close", 0); err == nil || report.Confirmed {
		t.Fatal(report, err)
	}
	waitHostCleanup(t, w.HostStateDir)
	client := func(endpoint string) (Client, error) { return Client{URL: endpoint, Token: "test"}, nil }
	if err = ReconcileHostLocal(context.Background(), w.HostStateDir, clock, client); err != nil {
		t.Fatal(err)
	}
	if ackRelease(cancelAck(t, q, j.ID)) != "pending" {
		t.Fatal("failed exclusion released Docker hold")
	}
	var db string
	if err = q.db.QueryRow("PRAGMA database_list").Scan(new(int), new(string), &db); err != nil {
		t.Fatal(err)
	}
	apiMu.Lock()
	if err = q.Close(); err != nil {
		apiMu.Unlock()
		t.Fatal(err)
	}
	active, err = Open(db, q.root, clock)
	apiMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	if ackRelease(cancelAck(t, active, j.ID)) != "pending" {
		t.Fatal("restart dropped Docker hold")
	}
	if err = os.WriteFile(filepath.Join(daemon, "mode"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	h.Timeout = 5 * time.Second
	if report, err := h.Disable(context.Background(), "close", 0); err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	if err = ReconcileHostLocal(context.Background(), w.HostStateDir, clock, client); err != nil {
		t.Fatal(err)
	}
	after, err := ReadHostIntent(w.HostStateDir)
	if err != nil || after.HostSettlement() != "confirmed" || ackRelease(cancelAck(t, active, j.ID)) != "confirmed" || len(active.hostObligations(s.Host)) != 0 {
		t.Fatal(after, err)
	}
	b, err := os.ReadFile(filepath.Join(store.dir, "owned.docker.json"))
	var custody dockerExecution
	if err != nil || json.Unmarshal(b, &custody) != nil || !custody.Released {
		t.Fatal("real Disable did not persist Docker release", err)
	}
}

func TestHostReconcileTransientErrorPreservesConfirmedClose(t *testing.T) {
	_, v := hostIdentityFixture(t)
	clock := &fakeClock{now: time.Unix(1000, 0)}
	h := HostControl{Dir: v.StateDir, Clock: clock, Service: &fakeHostService{}}
	if report, err := h.Disable(context.Background(), "close", 0); err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	testConfirmClose(t, v.StateDir)
	before, err := ReadHostIntent(v.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	x := HostMutationExecutor{Clock: clock, Probe: MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("confirmed exclusion stopped again")
		return nil, nil
	}}, Reconcile: func(context.Context, HostInvocation) error { return errors.New("queue temporarily down") }}
	v.Privileged = false
	if _, err = x.Execute(context.Background(), v, HostMutation{Operation: "reconcile"}); err == nil {
		t.Fatal("transient error hidden")
	}
	after, err := ReadHostIntent(v.StateDir)
	if err != nil || !reflect.DeepEqual(before.LastClose, after.LastClose) || !reflect.DeepEqual(before.LocalExclusion, after.LocalExclusion) || !slices.Contains(after.Errors, "host_reconcile_pending") {
		t.Fatal(after, err)
	}
}

func TestHostReconcileQueueInstallHasNoTimer(t *testing.T) {
	home := t.TempDir()
	nas := filepath.Join(home, "nas")
	if err := os.Mkdir(nas, 0700); err != nil {
		t.Fatal(err)
	}
	c, err := PlanInstall("queue", home, "", nas, "http://queue:7331", "queue", Detection{OS: "linux", Arch: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(home, "binary")
	if err = os.WriteFile(binary, []byte("test binary"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = Install(context.Background(), c, binary, "test", func(_ context.Context, _ string, args ...string) ([]byte, error) {
		for _, arg := range args {
			if strings.Contains(arg, "host-reconcile") {
				t.Fatal("queue-only install activated timer", args)
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(filepath.Dir(c.Service), "myce-host-reconcile.timer")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("queue-only install wrote timer", err)
	}
}
