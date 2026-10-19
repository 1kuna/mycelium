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
	"testing"
	"time"
)

// Placement tests use the real exchange, with no legacy host-control methods.
func closeQueueHost(q *Queue, host, reason string, results []Result) error {
	old := q.s.HostIntents[host]
	revision := old.HostRevision
	if old.Desired != "closed" {
		revision = HostRevision{Revision: old.Revision + 1, Operation: ID()}
	}
	accepted := q.clock.Now()
	if old.Desired == "closed" {
		accepted = old.AcceptedAt
	}
	request := HostExchange{Host: host, HostRevision: revision, Desired: "closed", Boot: "test-boot", Request: ID(), AcceptedAt: accepted, Reason: reason}
	for _, r := range results {
		request.Sessions = append(request.Sessions, HostSession{Machine: r.Machine, Session: r.Session})
	}
	_, err := q.ReconcileHost(request)
	return err
}
func testConfirmClose(t *testing.T, dir string) {
	t.Helper()
	s, err := ReadHostIntent(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Synthetic fixture only: production tests exercise the queue acknowledgement.
	if err = mutateHostIntent(dir, s.HostRevision, func(s *HostIntent) error {
		now := s.AcceptedAt
		s.LastClose = &HostClose{HostRevision: s.HostRevision, Settlement: "confirmed", SettledAt: &now}
		s.Pending = nil
		s.Holds = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func closeExchange(host string, clock Clock) HostExchange {
	return HostExchange{Host: host, HostRevision: HostRevision{Revision: 1, Operation: ID()}, Desired: "closed", Boot: "boot", Request: ID(), AcceptedAt: clock.Now()}
}
func TestHostReconcileExactEvidenceAndCancellation(t *testing.T) {
	for _, mode := range []string{"empty", "partial", "session", "machine", "stale", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			m.HostID = "host"
			m.MaxJobs = 2
			check(t, q, m, nil)
			other := machine("two", "fake")
			other.HostID = "host"
			other.MaxJobs = 2
			check(t, q, other, nil)
			foreign := machine("elsewhere", "fake")
			foreign.HostID = "foreign"
			check(t, q, foreign, nil)
			s := spec("experiment")
			// Small native copies fit two sessions in the shared host budget.
			s.Needs.MemoryGB = 1
			j := submit(t, q, s)
			j = check(t, q, m, nil).Job
			if err := q.Started(result(j, false)); err != nil {
				t.Fatal(err)
			}
			second := submit(t, q, s)
			secondID := second.ID
			second = check(t, q, other, nil).Job
			if second == nil || second.ID != secondID {
				t.Fatal("partial fixture did not assign second job", second, job(t, q, secondID))
			}
			if mode == "cancel" {
				cancelAck(t, q, j.ID)
			}
			request := closeExchange("host", clock)
			if ack, err := q.ReconcileHost(request); err != nil || len(ack.Pending) != 2 {
				t.Fatal(ack, err)
			}
			clock.Advance(20 * time.Minute)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			before := cloneJob(q.s.Jobs[j.ID])
			switch mode {
			case "partial", "cancel":
				request.Sessions = []HostSession{{Machine: m.ID, Session: m.Session}}
			case "session":
				request.Sessions = []HostSession{{Machine: m.ID, Session: "wrong"}}
			case "machine":
				request.Sessions = []HostSession{{Machine: "elsewhere", Session: m.Session}}
			case "stale":
				request.Operation = ID()
			}
			ack, err := q.ReconcileHost(request)
			switch mode {
			case "cancel":
				if err != nil || len(ack.Pending) != 1 || ackRelease(cancelAck(t, q, j.ID)) != "confirmed" {
					t.Fatal(ack, err)
				}
				assertNoCancellationAttempt(t, q, j)
			case "partial":
				if err != nil || len(ack.Pending) != 1 || ack.Pending[0].Result.ID != second.ID || job(t, q, j.ID).State != "queued" {
					t.Fatal(ack, err)
				}
			case "machine", "stale":
				if err == nil {
					t.Fatal("bad identity accepted")
				}
			default:
				if err != nil || len(ack.Pending) != 2 || !reflect.DeepEqual(&before, q.s.Jobs[j.ID]) {
					t.Fatal(ack, err)
				}
			}
			if job(t, q, j.ID).Losses != 0 || job(t, q, second.ID).Losses != 0 {
				t.Fatal("closed host charged loss")
			}
		})
	}
}
func TestHostReconcilePlannedDisplacementAndLossRefund(t *testing.T) {
	for _, offline := range []bool{false, true} {
		t.Run(stringBool(offline), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			m.HostID = "host"
			other := machine("other", "fake")
			check(t, q, m, nil)
			check(t, q, other, nil)
			s := spec("experiment")
			s.Run = dockerJob().Spec.Run
			j := submit(t, q, s)
			j = check(t, q, m, nil).Job
			request := closeExchange("host", clock)
			if offline {
				clock.Advance(4 * time.Minute)
				q.s.Machines[other.ID].LastSeen = clock.Now()
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if got := job(t, q, j.ID); got.Losses != 1 {
					t.Fatal("fixture did not charge loss", got)
				}
			}
			request.Sessions = []HostSession{{Machine: j.Machine, Session: j.Session}}
			ack, err := q.ReconcileHost(request)
			if err != nil || len(ack.Pending) != 0 {
				t.Fatal(ack, err)
			}
			got := job(t, q, j.ID)
			if got.Losses != 0 || got.State != "queued" || !got.RequeuedByOperator {
				t.Fatal(got)
			}
			clock.Advance(time.Minute)
			if got = job(t, q, j.ID); got.Losses != 0 {
				t.Fatal(got)
			}
			if got := check(t, q, m, nil); !got.Leave || got.Job != nil {
				t.Fatal(got)
			}
		})
	}
}
func TestHostReconcileProductionPinnedFailsWithoutLoss(t *testing.T) {
	q, clock := fixture(t)
	m := machine("one", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	s := spec("production")
	s.AllowedMachines = []string{m.ID}
	j := submit(t, q, s)
	j = check(t, q, m, nil).Job
	if err := q.Started(result(j, true)); err != nil {
		t.Fatal(err)
	}
	request := closeExchange("host", clock)
	if _, err := q.ReconcileHost(request); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.Losses != 0 || got.State != "stopping" {
		t.Fatal("closed running production consumed its loss budget", got)
	}
	request.Sessions = []HostSession{{Machine: j.Machine, Session: j.Session}}
	if _, err := q.ReconcileHost(request); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "failed" || got.Losses != 0 {
		t.Fatal(got)
	}
	queued := submit(t, q, s)
	if got := job(t, q, queued.ID); got.State != "failed" {
		t.Fatal("pinned production did not fail fast", got)
	}
}
func TestHostReconcileEnginesPreparationsAndOldCopies(t *testing.T) {
	q, clock := fixture(t)
	m := machine("one", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	j = check(t, q, m, nil).Job
	q.s.Jobs[j.ID].Copies = append(q.s.Jobs[j.ID].Copies, Copy{Machine: m.ID, Session: "old", Attempt: 3, ReleaseReason: "awaiting_release"})
	q.s.ModelSlots = map[string]*ModelSlot{slotKey(m.ID, "model"): {Machine: m.ID, Session: m.Session, Model: "model", Attempt: "engine-attempt", State: "unloading"}}
	prep := HostRelease{Host: "host", Machine: m.ID, Session: m.Session, Job: "preparation", Preparation: true}
	q.s.HostReleases = map[string]HostRelease{releaseKey(prep): prep}
	request := closeExchange("host", clock)
	ack, err := q.ReconcileHost(request)
	if err != nil || len(ack.Pending) != 4 {
		t.Fatal(ack, err)
	}
	request.Sessions = []HostSession{{Machine: j.Machine, Session: j.Session}}
	ack, err = q.ReconcileHost(request)
	if err != nil || len(ack.Pending) != 1 {
		t.Fatal("registered session did not discharge engines/preparations; unregistered session must stay charged", ack, err)
	}
	request.Sessions = []HostSession{{Machine: m.ID, Session: "old"}}
	ack, err = q.ReconcileHost(request)
	if err != nil || len(ack.Pending) != 0 {
		t.Fatal(ack, err)
	}
}
func TestHostReconcileUnconfirmedCloseCannotOpen(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	testHostOpen(t, dir, clock)
	h := HostControl{Dir: dir, Clock: clock, Service: &fakeHostService{}, Observe: testUnknownExclusion}
	if _, err := h.Disable(context.Background(), "close", 0); err == nil {
		t.Fatal("fixture cleanup confirmed")
	}
	s, err := ReadHostIntent(dir)
	if err != nil || s.LocalExclusion == nil || s.LocalExclusion.Confirmed {
		t.Fatal(s, err)
	}
	boot, _ := HostBootID()
	request := newHostExchange(s, boot)
	ack, err := q.ReconcileHost(request)
	if err != nil {
		t.Fatal(err)
	}
	if err = saveHostAck(dir, "queue", request, ack, clock); err != nil {
		t.Fatal(err)
	}
	if _, err = changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: s.HostRevision, Settled: func(HostIntent) error { return nil }}); err == nil {
		t.Fatal("unconfirmed close opened")
	}
}
func TestHostReconcileFreshAckHistoryAndSupersession(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	testHostOpen(t, dir, clock)
	h := HostControl{Observe: testHostExclusion, Dir: dir, Clock: clock, Service: &fakeHostService{}}
	if r, err := h.Disable(context.Background(), "close", 0); err != nil || !r.Confirmed {
		t.Fatal(r, err)
	}
	s, _ := ReadHostIntent(dir)
	boot, _ := HostBootID()
	request := newHostExchange(s, boot)
	ack, err := q.ReconcileHost(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"empty", "nonce", "boot", "revision", "partial"} {
		bad := ack
		switch mode {
		case "empty":
			bad = HostAcknowledgement{}
		case "nonce":
			bad.Request = ID()
		case "boot":
			bad.Boot = "old"
		case "revision":
			bad.Revision++
		case "partial":
			bad.Pending = nil
		}
		if err = saveHostAck(dir, "queue", request, bad, clock); err == nil {
			t.Fatal("bad ack accepted", mode)
		}
	}
	if err = saveHostAck(dir, "queue", request, ack, clock); err != nil {
		t.Fatal(err)
	}
	s, _ = ReadHostIntent(dir)
	if s.HostSettlement() != "confirmed" {
		t.Fatal(s)
	}
	closeHistory := *s.LastClose
	opened, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: s.HostRevision})
	if err != nil || !reflect.DeepEqual(*opened.LastClose, closeHistory) || len(opened.Pending) > 0 || len(opened.Holds) > 0 {
		t.Fatal(opened, err)
	}
	openRequest := newHostExchange(opened, boot)
	if _, err = q.ReconcileHost(openRequest); err != nil {
		t.Fatal(err)
	}
	newer, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.ReconcileHost(newHostExchange(newer, boot)); err != nil {
		t.Fatal(err)
	}
	if _, err = q.ReconcileHost(openRequest); !errors.Is(err, ErrHostRevision) {
		t.Fatal("delayed open accepted", err)
	}
	if err = saveHostAck(dir, "queue", request, ack, clock); !errors.Is(err, ErrHostRevision) {
		t.Fatal(err)
	}
}
func TestHostReconcileWriteFailureAndQueueRestart(t *testing.T) {
	q, clock := fixture(t)
	request := closeExchange("host", clock)
	if _, err := q.db.Exec(`CREATE TRIGGER fail_host BEFORE INSERT ON queue BEGIN SELECT RAISE(FAIL,'fault'); END`); err != nil {
		t.Fatal(err)
	}
	if ack, err := q.ReconcileHost(request); err == nil || ack.Revision != 0 || len(q.s.HostIntents) != 0 {
		t.Fatal(ack, err)
	}
	if _, err := q.db.Exec(`DROP TRIGGER fail_host`); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ReconcileHost(request); err != nil {
		t.Fatal(err)
	}
	var db string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(new(int), new(string), &db); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err := Open(db, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if ack, err := q.ReconcileHost(request); err != nil || ack.Pending == nil || ack.HostRevision != request.HostRevision {
		t.Fatal(ack, err)
	}
	request.Revision--
	if _, err = q.ReconcileHost(request); err == nil {
		t.Fatal("restart lost fence")
	}
}
func TestHostReconcileHTTPAndTimer(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	request := closeExchange("host", clock)
	var ack HostAcknowledgement
	if err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/hosts/host/reconcile", request, &ack); err != nil || validateHostAck(request, ack) != nil {
		t.Fatal(ack, err)
	}
	config := ServiceConfig{Binary: "/binary path/myce", HostStateDir: "/state path", Role: "worker"}
	service := config.HostReconcileServiceText()
	if !strings.Contains(service, `--state-dir "/state path"`) || strings.Contains(service, "MYCE_TOKEN") {
		t.Fatal(service)
	}
	for _, path := range []string{"myce-host-reconcile.service", "myce-host-reconcile.timer"} {
		b, err := os.ReadFile(filepath.Join("../../deploy", path))
		if err != nil || len(b) == 0 {
			t.Fatal(err)
		}
	}
}

// Unit fixtures for worker behavior use a synthetic empty queue for the host
// protocol; tests of convergence itself always use API{Queue: ...}.
func workerTestServer(handler http.Handler) *httptest.Server {
	return httptest.NewServer(workerTestHandler(handler))
}

func workerTestHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/reconcile") && strings.HasPrefix(r.URL.Path, "/api/hosts/") {
			var request HostExchange
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "bad fixture request", 400)
				return
			}
			json.NewEncoder(w).Encode(HostAcknowledgement{Host: request.Host, HostRevision: request.HostRevision, Desired: request.Desired, Boot: request.Boot, Request: request.Request, Pending: []HostObligation{}, Covered: []HostObligation{}})
			return
		}
		handler.ServeHTTP(w, r)
	})
}

func TestHostReconcileUnmatchedLocalCustodyDiagnosticAndBlockingHolds(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	testHostOpen(t, dir, clock)
	if _, err := (HostControl{Observe: testHostExclusion, Dir: dir, Clock: clock, Service: &fakeHostService{}}).Disable(context.Background(), "close", 0); err != nil {
		t.Fatal(err)
	}
	s, _ := ReadHostIntent(dir)
	boot, _ := HostBootID()
	p := HostObligation{Queue: "queue", Result: Result{Machine: "missing", Session: "missing", Execution: Execution{ID: "uncovered", Attempt: 1}}}
	if err := mutateHostIntent(dir, s.HostRevision, func(s *HostIntent) error { s.Pending = []HostObligation{p}; return nil }); err != nil {
		t.Fatal(err)
	}
	request := newHostExchange(s, boot)
	ack, err := q.ReconcileHost(request)
	if err != nil {
		t.Fatal(err)
	}
	if err = saveHostAck(dir, "queue", request, ack, clock); err != nil {
		t.Fatal(err)
	}
	s, _ = ReadHostIntent(dir)
	if len(s.Pending) != 0 || s.HostSettlement() != "confirmed" || !slices.Contains(s.Holds, "diagnostic:unmatched_local_custody") {
		t.Fatal("unmatched local evidence was not diagnostic", s)
	}
	if err = mutateHostIntent(dir, s.HostRevision, func(s *HostIntent) error { s.Pending = nil; s.Holds = []string{"ambiguous_execution"}; return nil }); err != nil {
		t.Fatal(err)
	}
	if err = saveHostAck(dir, "queue", request, ack, clock); err != nil {
		t.Fatal(err)
	}
	if _, err = changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: s.HostRevision}); err == nil {
		t.Fatal("unresolved hold opened")
	}
}

type reconcileStopHook struct{ stop func() }

func (s reconcileStopHook) Stop(context.Context, time.Duration) error { s.stop(); return nil }
func (s reconcileStopHook) Empty(context.Context) (bool, error)       { return true, nil }
func TestHostReconcileReadsFinalCustodyAfterStop(t *testing.T) {
	dir, spool := newTestStateRoot(t), t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	testHostOpen(t, dir, clock)
	if err := AdoptHostWorker(dir, HostWorker{State: spool, Host: "test-host", Name: "one", Queue: "queue"}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(spool, "state.json", workerState{Session: "session"}); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Observe: testHostExclusion, Dir: dir, Clock: clock, Service: reconcileStopHook{stop: func() {
		if err := saveWorkerJSON(spool, "state.json", workerState{Session: "session", Job: &Job{ID: "final-attempt", Attempt: 1}}); err != nil {
			t.Fatal(err)
		}
	}}}
	if report, err := h.Disable(context.Background(), "close", 0); err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	s, err := ReadHostIntent(dir)
	if err != nil || s.LocalExclusion == nil || len(s.LocalExclusion.Released) != 1 || s.LocalExclusion.Released[0].Result.ID != "final-attempt" || s.Observed == nil {
		t.Fatal("final custody omitted", s, err)
	}
}

func TestHostReconcileWorkerHeartbeatIndependentOfOtherQueue(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	testHostOpen(t, dir, clock)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	for _, endpoint := range []string{server.URL, "http://127.0.0.1:1"} {
		if err := AdoptHostWorker(dir, HostWorker{State: t.TempDir(), Name: ID(), Queue: endpoint}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reconcileWorkerHost(context.Background(), dir, clock, Client{URL: server.URL, Token: "test"}); err != nil {
		t.Fatal("unrelated queue suppressed healthy queue heartbeat", err)
	}
	s, err := ReadHostIntent(dir)
	if err != nil || s.QueueAcks[server.URL].HostRevision != s.HostRevision {
		t.Fatal(s, err)
	}
}

func TestHostReconcileFinalCustodyFailureCannotConfirm(t *testing.T) {
	dir, spool := newTestStateRoot(t), t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	testHostOpen(t, dir, clock)
	if err := AdoptHostWorker(dir, HostWorker{State: spool, Host: "test-host", Name: "one", Queue: "queue"}); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Observe: testHostExclusion, Dir: dir, Clock: clock, Service: reconcileStopHook{stop: func() {
		if err := os.WriteFile(filepath.Join(spool, "state.json"), []byte("broken"), 0600); err != nil {
			t.Fatal(err)
		}
	}}}
	if report, err := h.Disable(context.Background(), "close", 0); err == nil || report.Confirmed || len(report.Unconfirmed) == 0 {
		t.Fatal("final custody error was hidden", report, err)
	}
	s, err := ReadHostIntent(dir)
	if err != nil || s.LocalExclusion == nil || s.LocalExclusion.Confirmed || s.HostSettlement() == "confirmed" {
		t.Fatal(s, err)
	}
}

func TestHostReconcileWorkerMustUseLocalHostIdentity(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	w := Worker{HostStateDir: dir, Machine: machine("one", "fake"), EnvironmentRoot: t.TempDir()}
	w.Machine.HostID = "foreign"
	store := &workerStore{dir: t.TempDir()}
	if err := w.registerHost(store); err == nil {
		t.Fatal("worker registered outside the host's queue fence")
	}
	w.Machine.HostID = ""
	if err := w.registerHost(store); err != nil || w.Machine.HostID != "test-host" {
		t.Fatal(w.Machine.HostID, err)
	}
}

func TestHostReconcileOpenPreservesWorkerLeave(t *testing.T) {
	q, clock := fixture(t)
	m := machine("one", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	if err := q.Leave(m.ID, false); err != nil {
		t.Fatal(err)
	}
	request := closeExchange("host", clock)
	request.Desired = "open"
	for range 2 {
		if _, err := q.ReconcileHost(request); err != nil {
			t.Fatal(err)
		}
		if !q.s.Machines[m.ID].Leaving {
			t.Fatal("unchanged open intent revoked worker leave")
		}
	}
}

func TestHostReconcileCancelledSavedResultAfterConfirmedCleanup(t *testing.T) {
	q, clock := fixture(t)
	dir, spool := newTestStateRoot(t), t.TempDir()
	s := testHostOpen(t, dir, clock)
	m := machine("one", "fake")
	m.HostID = s.Host
	check(t, q, m, nil)
	spec := spec("experiment")
	spec.Run = dockerJob().Spec.Run
	j := submit(t, q, spec)
	j = check(t, q, m, nil).Job
	cancelAck(t, q, j.ID)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	if err := AdoptHostWorker(dir, HostWorker{State: spool, Host: s.Host, Name: m.ID, Queue: server.URL}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(spool, "state.json", workerState{Session: m.Session, Job: j}); err != nil {
		t.Fatal(err)
	}
	pendingResult := result(j, false)
	pendingResult.ReleasePending = true
	if err := saveWorkerJSON(spool, "saved.result.json", pendingResult); err != nil {
		t.Fatal(err)
	}
	// The saved result predates the successful retry of local container cleanup.
	h := HostControl{Observe: testHostExclusion, Dir: dir, Clock: clock, Service: &fakeHostService{}}
	if report, err := h.Disable(context.Background(), "close", 0); err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	if err := ReconcileHostLocal(context.Background(), dir, clock, func(endpoint string) (Client, error) {
		return Client{URL: endpoint, Token: "test"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if ack := cancelAck(t, q, j.ID); ackRelease(ack) != "confirmed" {
		t.Fatal("cancel retained custody after exact confirmed cleanup", ack)
	}
	after, err := ReadHostIntent(dir)
	if err != nil || after.HostSettlement() != "confirmed" || len(after.Pending) > 0 {
		t.Fatal(after, err)
	}
	assertNoCancellationAttempt(t, q, j)
}

func waitHostCleanup(t *testing.T, dir string) {
	t.Helper()
	await(t, func() bool {
		unlock, err := hostOperationLock(dir, "reconcile.lock", false)
		if err != nil {
			return false
		}
		unlock()
		return true
	})
}
