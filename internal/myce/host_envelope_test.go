package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type envelopeClock struct{ at time.Time }

func (c *envelopeClock) Now() time.Time { return c.at }

func TestHostEnvelopeCASAndLastClose(t *testing.T) {
	dir := t.TempDir()
	clock := &envelopeClock{RealClock{}.Now()}
	initial, err := InitializeHostIntent(dir, "fixture-host", clock)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, hostStateFile))
	wrong := HostRevision{Revision: initial.Revision + 1}
	if _, err = changeHostIntent(dir, clock, hostIntentChange{Desired: "closed", FreshClose: true, CloseExpected: &wrong}); err != ErrHostRevision {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, hostStateFile))
	if string(before) != string(after) {
		t.Fatal("CAS rejection wrote state")
	}
	if _, err = changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: initial.HostRevision}); err != ErrPreviousCloseUnsettled {
		t.Fatal(err)
	}
	clock.at = clock.at.Add(time.Second)
	closed, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed", FreshClose: true, Grace: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Revision != initial.Revision+1 || closed.Operation == initial.Operation || !closed.StopDeadline.Equal(*initial.StopDeadline) {
		t.Fatal("fresh operation renewed budget", closed)
	}
	// Unit-only settlement fixture. The dev integration below must settle via SQLite.
	now := clock.Now().UTC()
	if err = mutateHostIntent(dir, closed.HostRevision, func(s *HostIntent) error {
		s.LastClose.Settlement = "confirmed"
		s.LastClose.SettledAt = &now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	settled, _ := ReadHostIntent(dir)
	if err = (HostControl{Dir: dir, Clock: clock, Expected: closed.HostRevision}).Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	opened, _ := ReadHostIntent(dir)
	if opened.Revision != closed.Revision+1 || opened.Operation == closed.Operation || !reflect.DeepEqual(settled.LastClose, opened.LastClose) || opened.HostSettlement() != "confirmed" {
		t.Fatal(opened)
	}
	if err = (HostControl{Dir: dir, Clock: clock, Expected: opened.HostRevision}).Enable(context.Background()); err != ErrHostRevision {
		t.Fatal("open history selector accepted", err)
	}
}

func TestHostEnvelopeStatusReadOnlyAndFreshness(t *testing.T) {
	root := newTestStateRoot(t)
	clock := &envelopeClock{RealClock{}.Now().UTC()}
	dir := filepath.Join(root, "host")
	initial, err := InitializeHostIntent(dir, "fixture-host", clock)
	if err != nil {
		t.Fatal(err)
	}
	q, err := Open(filepath.Join(root, "queue.db"), filepath.Join(root, "cp"), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	boot, _ := HostBootID()
	// Only the read accessor sees this state: maintenance would expire this machine.
	q.mu.Lock()
	q.s.HostIntents = map[string]QueueHostIntent{"fixture-host": {HostRevision: initial.HostRevision, Boot: boot, Desired: "closed"}}
	q.s.Machines["fixture-worker"] = &Machine{ID: "fixture-worker", HostID: "fixture-host", Session: "session", LastSeen: clock.at.Add(-time.Hour)}
	q.mu.Unlock()
	before, _ := json.Marshal(q.s)
	for i := 0; i < 2; i++ {
		v, e := q.ReadHostObservation(context.Background(), "fixture-host")
		if e != nil || v.Revision != initial.Revision {
			t.Fatal(v, e)
		}
	}
	after, _ := json.Marshal(q.s)
	if string(before) != string(after) {
		t.Fatal("read accessor maintained queue")
	}
	at := clock.at
	fixture := HostQueueObservation{Host: "fixture-host", HostRevision: initial.HostRevision, Boot: boot, Desired: "closed", At: at, Pending: []HostObligation{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(fixture) }))
	defer server.Close()
	if err = AdoptHostWorker(dir, HostWorker{State: root, Host: "fixture-host", Name: "fixture-worker", Queue: server.URL, Session: "session"}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TOKEN", "fixture-token")
	v := HostInvocation{StateDir: dir, Config: ServiceConfig{Name: "fixture-worker"}}
	filesBefore, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, age := range []time.Duration{10 * time.Second, 10*time.Second + time.Nanosecond} {
		clock.at = at.Add(age)
		e, err := HostSnapshot(context.Background(), v, clock)
		if err != nil {
			t.Fatal(err)
		}
		if (e.QueueRevision != nil) != (age == 10*time.Second) {
			t.Fatalf("freshness boundary %v: %+v", age, e)
		}
		if age > 10*time.Second && (e.Counts.Active != nil || e.Sources["queue"] != nil) {
			t.Fatal("stale means zero", e)
		}
	}
	filesAfter, _ := os.ReadDir(dir)
	if !reflect.DeepEqual(filesBefore, filesAfter) {
		t.Fatal("status initialized files")
	}
	clock.at = at
	base := UnknownHostEnvelope("status", clock)
	base.Boot = &boot
	base.Operation = &initial.Operation
	base.Desired.Revision = &initial.Revision
	report := testHostExclusion(context.Background(), initial)
	report.At = clock.at
	for k := range report.Sources {
		report.Sources[k] = &at
	}
	base.ApplyExclusion(&report, clock.Now())
	if base.LocalExclusion != "PASS" {
		t.Fatal(base)
	}
	for _, which := range []string{"boot", "revision", "age"} {
		e := UnknownHostEnvelope("status", clock)
		e.Boot = &boot
		e.Operation = &initial.Operation
		e.Desired.Revision = &initial.Revision
		r := report
		switch which {
		case "boot":
			r.Boot = "old-boot"
		case "revision":
			r.Revision++
		case "age":
			r.At = at.Add(-11 * time.Second)
		}
		e.ApplyExclusion(&r, clock.Now())
		if e.LocalExclusion != "UNKNOWN" || e.Counts.Containers != nil {
			t.Fatal(which, e)
		}
	}
}

func TestHostEnvelopeDevLifecycle(t *testing.T) {
	root := newTestStateRoot(t)
	dir := filepath.Join(root, "host")
	db := filepath.Join(root, "queue.db")
	cp := filepath.Join(root, "cp")
	d, err := StartDev(DevConfig{HostStateDir: dir, HostID: "fixture-host", Database: db, Checkpoints: cp, Address: "127.0.0.1:0", Token: "fixture-token", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	originalClosed := false
	defer func() {
		if !originalClosed {
			d.Close()
		}
	}()
	t.Setenv("MYCE_TOKEN", "fixture-token")
	marker := filepath.Join(root, "finished")
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"/bin/sh", "-c", `echo started; sleep 2; printf done > "$1"`, "fixture", marker}
	var job Job
	if err = d.Client().Call(ctx, "POST", "/api/jobs", s, &job); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { logs, e := d.Queue.Logs(job.ID, 0); return e == nil && len(logs) > 0 })
	clock := d.Queue.clock
	d.stopQueue()
	if err = d.server.Close(); err != nil {
		t.Fatal(err)
	}
	d.wg.Wait()
	control := HostControl{Dir: dir, Clock: clock, FreshClose: true, Observe: testHostExclusion, Service: &fakeHostService{}, Timeout: 5 * time.Second}
	report, err := control.Disable(ctx, "fixture offline close", 0)
	if err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	d.cancel()
	d.workerWG.Wait()
	if err = d.Queue.Close(); err != nil {
		t.Fatal(err)
	}
	originalClosed = true
	before, err := ReadHostIntent(dir)
	if err != nil || before.HostSettlement() != "pending" {
		t.Fatal(before, err)
	}
	if _, err = os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("closed attempt executed final effect", err)
	}
	if err = (HostControl{Dir: dir, Clock: clock, Expected: before.HostRevision}).Enable(ctx); err != ErrPreviousCloseUnsettled {
		t.Fatal("offline open admitted", err)
	}
	q, err := Open(db, cp, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	listener, err := net.Listen("tcp", strings.TrimPrefix(d.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: (API{Queue: q, Token: "fixture-token"}).Handler()}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Close(); <-done }()
	if err = q.ConfigureLocalHost(dir, d.URL); err != nil {
		t.Fatal(err)
	}
	clients := func(endpoint string) (Client, error) { return Client{URL: endpoint, Token: "fixture-token"}, nil }
	if err = ReconcileHostLocal(ctx, dir, clock, clients); err != nil {
		t.Fatal(err)
	}
	settled, err := ReadHostIntent(dir)
	if err != nil || settled.HostSettlement() != "confirmed" {
		t.Fatal(settled, err)
	}
	if err = (HostControl{Dir: dir, Clock: clock, Expected: settled.HostRevision}).Enable(ctx); err != nil {
		t.Fatal(err)
	}
	opened, _ := ReadHostIntent(dir)
	if !reflect.DeepEqual(opened.LastClose, settled.LastClose) {
		t.Fatal("open rewrote close history")
	}
	// Cancel interrupted/requeued work before reopening its real worker.
	if err = q.Cancel(job.ID); err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	worker := Worker{HostStateDir: dir, StateDir: filepath.Join(root, "workers", "fake-1"), Client: Client{URL: d.URL, Token: "fixture-token"}, Interval: 10 * time.Millisecond, Machine: Machine{ID: "fake-1", HostID: "fixture-host", GPU: "fake", FreeMemoryGB: 8}}
	go func() { workerDone <- worker.Run(workerCtx) }()
	workerStopped := false
	defer func() {
		if !workerStopped {
			stopWorker()
			<-workerDone
		}
	}()
	v := HostInvocation{StateDir: dir, Config: ServiceConfig{Name: "fake-1"}}
	for {
		e, err := HostSnapshot(ctx, v, clock)
		if err == nil && e.Ready != nil && *e.Ready {
			break
		}
		select {
		case <-ctx.Done():
			observation, _ := q.ReadHostObservation(context.Background(), "fixture-host")
			t.Fatalf("ready deadline: envelope=%+v queue=%+v error=%v", e, observation, err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	var second Job
	if err = d.Client().Call(ctx, "POST", "/api/jobs", s, &second); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { logs, e := q.Logs(second.ID, 0); return e == nil && len(logs) > 0 })
	active, err := HostSnapshot(ctx, v, clock)
	if err != nil {
		t.Fatal("active snapshot", err)
	}
	if active.Counts.Active == nil || *active.Counts.Active != 1 {
		data, _ := json.Marshal(active)
		t.Fatalf("expected one active attempt: %s", data)
	}
	if active.Ready == nil || !*active.Ready {
		observation, _ := q.ReadHostObservation(ctx, "fixture-host")
		data, _ := json.Marshal(observation)
		t.Fatalf("current active work withheld ready: queue=%s", data)
	}
	final, err := control.Disable(ctx, "fixture final close", 0)
	if err != nil || !final.Confirmed {
		t.Fatal(final, err)
	}
	stopWorker()
	err = <-workerDone
	workerStopped = true
	if err != nil && !errors.Is(err, errHostDisabled) && !errors.Is(err, ErrHostRevision) {
		t.Fatal(err)
	}
	if err = ReconcileHostLocal(ctx, dir, clock, clients); err != nil {
		t.Fatal(err)
	}
	closed, _ := ReadHostIntent(dir)
	if closed.Desired != "closed" || closed.HostSettlement() != "confirmed" {
		t.Fatal(closed)
	}
	finalSnapshot, err := HostSnapshot(ctx, v, clock)
	if err != nil {
		t.Fatal("final snapshot", err)
	}
	if finalSnapshot.Observed.State == nil || *finalSnapshot.Observed.State != "closed" || finalSnapshot.Settlement != "confirmed" {
		t.Fatal("final envelope is not CLOSED/settled")
	}
	if finalSnapshot.Counts.Active == nil || *finalSnapshot.Counts.Active != 0 || finalSnapshot.Counts.Unsettled == nil || *finalSnapshot.Counts.Unsettled != 0 {
		t.Fatal("final envelope retains active or unsettled custody")
	}
	if _, err = os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("late command effect", err)
	}
	t.Log("real HTTP/SQLite/worker command interrupted offline; pending open rejected; restart settled; new revision ready; final close settled and worker joined")
}

func TestHostEnvelopeWaitDeadline(t *testing.T) {
	q, clock := fixture(t)
	before := clock.Now()
	q.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := q.ReadHostObservation(ctx, "fixture-host")
	q.mu.Unlock()
	if !errors.Is(err, context.Canceled) || !clock.Now().Equal(before) {
		t.Fatal("read renewed deadline or maintained queue", err)
	}
}

func TestHostEnvelopeOpenLockContention(t *testing.T) {
	for _, lock := range []string{"reconcile.lock", "intake.lock"} {
		t.Run(lock, func(t *testing.T) {
			dir := t.TempDir()
			clock := RealClock{}
			s, err := InitializeHostIntent(dir, "fixture-host", clock)
			if err != nil {
				t.Fatal(err)
			}
			testConfirmClose(t, dir)
			before, _ := os.ReadFile(filepath.Join(dir, hostStateFile))
			unlock, err := hostOperationLock(dir, lock, true)
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- (HostControl{Dir: dir, Clock: clock, Expected: s.HostRevision}).Enable(ctx) }()
			select {
			case err = <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("contended open did not honor deadline", err)
				}
			case <-time.After(time.Second):
				t.Fatal("open blocked past its deadline")
			}
			after, _ := os.ReadFile(filepath.Join(dir, hostStateFile))
			if !reflect.DeepEqual(before, after) {
				t.Fatal("expired open changed intent")
			}
			if lock == "intake.lock" {
				release, err := hostOperationLock(dir, "reconcile.lock", false)
				if err != nil {
					t.Fatal("expired open retained reconcile.lock", err)
				}
				release()
				// Service starts after acceptance use this same cancellable lock.
				startCtx, startCancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
				defer startCancel()
				started := false
				err = hostDispatchContext(startCtx, dir, s.HostRevision, func() error { started = true; return nil })
				if !errors.Is(err, context.DeadlineExceeded) || started {
					t.Fatal("service dispatch ignored deadline", err)
				}
			}
		})
	}
}

func TestHostEnvelopeReadyRequiredWorkers(t *testing.T) {
	root := newTestStateRoot(t)
	dir := filepath.Join(root, "host")
	clock := &envelopeClock{RealClock{}.Now().UTC()}
	initial, err := InitializeHostIntent(dir, "fixture-host", clock)
	if err != nil {
		t.Fatal(err)
	}
	testConfirmClose(t, dir)
	opened, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: initial.HostRevision})
	if err != nil {
		t.Fatal(err)
	}
	boot, _ := HostBootID()
	observation := HostQueueObservation{Host: "fixture-host", HostRevision: opened.HostRevision, Boot: boot, Desired: "open", At: clock.Now(), Pending: []HostObligation{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(observation) }))
	defer server.Close()
	// Reconciliation records current custody as Pending even when no release is
	// unresolved. Readiness must use the fresh queue release inventory instead.
	if err = mutateHostIntent(dir, opened.HostRevision, func(s *HostIntent) error {
		s.Pending = []HostObligation{{Queue: server.URL, Result: Result{Machine: "required-a", Session: "session", Execution: Execution{ID: "current-job", Attempt: 1}}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = AdoptHostWorker(dir, HostWorker{State: root, Host: "fixture-host", Name: "last-seen-only", Queue: server.URL, Session: "session"}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TOKEN", "fixture-token")
	v := HostInvocation{StateDir: dir, Config: ServiceConfig{RequiredWorkers: []string{"required-a", "required-b"}}}
	good := HostReadyWorker{Name: "required-a", HostRevision: opened.HostRevision, Boot: boot, At: clock.Now(), Healthy: true}
	other := good
	other.Name = "required-b"
	for _, mode := range []string{"missing", "boot", "revision", "operation", "stale", "unhealthy", "release", "ready"} {
		t.Run(mode, func(t *testing.T) {
			a := good
			observation.Workers = []HostReadyWorker{a, other}
			observation.Pending = []HostObligation{}
			switch mode {
			case "missing":
				observation.Workers = observation.Workers[:1]
			case "boot":
				observation.Workers[0].Boot = "old-boot"
			case "revision":
				observation.Workers[0].Revision--
			case "operation":
				observation.Workers[0].Operation = "old-operation"
			case "stale":
				observation.Workers[0].At = clock.Now().Add(-HostObservationWindow - time.Nanosecond)
			case "unhealthy":
				observation.Workers[0].Healthy = false
			case "release":
				observation.Pending = []HostObligation{{Result: Result{Machine: "required-a", Session: "session", Execution: Execution{ID: "job", Attempt: 1}}}}
			}
			e, err := HostSnapshot(context.Background(), v, clock)
			if mode == "stale" {
				if err != nil || e.Ready != nil {
					t.Fatal("expired heartbeat must be absent", e.Ready, err)
				}
				return
			}
			if err != nil || e.Ready == nil || *e.Ready != (mode == "ready") {
				t.Fatal(mode, e, err)
			}
		})
	}
}
