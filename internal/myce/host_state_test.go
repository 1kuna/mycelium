package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func testHostOpen(t *testing.T, dir string, clock Clock, host ...string) HostIntent {
	t.Helper()
	identity := "test-host"
	if len(host) > 0 {
		identity = host[0]
	}
	s := HostIntent{Schema: 1, Host: identity, HostRevision: HostRevision{Revision: 1, Operation: ID()}, Desired: "open", AcceptedAt: clock.Now()}
	if err := atomicHostJSON(dir, hostStateFile, s, nil); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestHostRevisionSupersession(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	initial := testHostOpen(t, dir, clock)
	closed, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed", Expected: initial.HostRevision, Grace: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Revision != 2 || closed.Operation == initial.Operation {
		t.Fatal(closed)
	}
	clock.Advance(5 * time.Second)
	repeated, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed", Grace: time.Minute})
	if err != nil || !repeated.StopDeadline.Equal(*closed.StopDeadline) || repeated.HostRevision != closed.HostRevision {
		t.Fatal(repeated, err)
	}
	if _, err = changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: initial.HostRevision, Settled: func(HostIntent) error { return nil }}); !errors.Is(err, ErrHostRevision) {
		t.Fatal(err)
	}
	if _, err = changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: closed.HostRevision}); err == nil {
		t.Fatal("open without settlement")
	}
	testConfirmClose(t, dir)
	// A close accepted while the settlement source is read defeats the old open.
	_, err = changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: closed.HostRevision, Settled: func(HostIntent) error {
		opened, e := changeHostIntent(dir, clock, hostIntentChange{Desired: "open", Expected: closed.HostRevision, Settled: func(HostIntent) error { return nil }})
		if e != nil {
			return e
		}
		_, e = changeHostIntent(dir, clock, hostIntentChange{Desired: "closed", Expected: opened.HostRevision, Grace: time.Second})
		return e
	}})
	if !errors.Is(err, ErrHostRevision) {
		t.Fatal(err)
	}
	final, err := ReadHostIntent(dir)
	if err != nil || final.Desired != "closed" || final.Revision != 4 {
		t.Fatal(final, err)
	}
	if err = PublishHostObservation(dir, closed.HostRevision, "boot-1", "closed", clock); !errors.Is(err, ErrHostRevision) {
		t.Fatal(err)
	}
	if err = CheckHostRevision(dir, initial.HostRevision); !errors.Is(err, ErrHostRevision) {
		t.Fatal(err)
	}
}
func TestHostRevisionExhaustion(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	s := testHostOpen(t, dir, clock)
	s.Revision = ^uint64(0)
	if err := atomicHostJSON(dir, hostStateFile, s, nil); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, hostStateFile))
	if _, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed"}); err == nil {
		t.Fatal("overflow accepted")
	}
	after, _ := os.ReadFile(filepath.Join(dir, hostStateFile))
	if string(before) != string(after) {
		t.Fatal("overflow mutated state")
	}
}
func TestHostRevisionPersistenceBoundaries(t *testing.T) {
	for _, boundary := range []string{"write", "file-sync", "rename", "directory-sync"} {
		t.Run(boundary, func(t *testing.T) {
			dir := t.TempDir()
			clock := &fakeClock{now: time.Unix(1000, 0)}
			s := testHostOpen(t, dir, clock)
			_, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed", Grace: time.Second, boundary: func(step string) error {
				if step == boundary {
					return errors.New("power loss")
				}
				return nil
			}})
			if err == nil {
				t.Fatal("fault missing")
			}
			got, err := ReadHostIntent(dir)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "write" || boundary == "file-sync" {
				if got.HostRevision != s.HostRevision || got.Desired != "open" {
					t.Fatal(got)
				}
			} else {
				if got.Revision != 2 || got.Desired != "closed" {
					t.Fatal(got)
				}
			}
			if got.Revision == 2 {
				if _, err = hostLaunchGate(dir, s.HostRevision); err == nil {
					t.Fatal("stale launch after uncertain commit")
				}
			}
		})
	}
}
func TestHostRevisionMissingCorruptUnsupportedAndMixed(t *testing.T) {
	for _, data := range []string{"", `{`, `{}`, `{"schema_version":2}`, `null`} {
		t.Run(data, func(t *testing.T) {
			dir := t.TempDir()
			if data != "" {
				os.WriteFile(filepath.Join(dir, hostStateFile), []byte(data), 0600)
			}
			if _, err := ReadHostIntent(dir); !errors.Is(err, ErrHostState) {
				t.Fatal(err)
			}
			if _, err := hostLaunchGate(dir); err == nil {
				t.Fatal("invalid state admitted launch")
			}
		})
	}
	dir := t.TempDir()
	testHostOpen(t, dir, RealClock{})
	os.WriteFile(filepath.Join(dir, "DISABLED"), []byte(`{}`), 0600)
	if _, err := ReadHostIntent(dir); !errors.Is(err, ErrHostState) {
		t.Fatal(err)
	}
}

func TestHostStateMissingAndLegacyMessages(t *testing.T) {
	for _, legacy := range []string{"", "DISABLED", "worker-old.json"} {
		t.Run("missing-"+legacy, func(t *testing.T) {
			dir := t.TempDir()
			if legacy != "" {
				if err := os.WriteFile(filepath.Join(dir, legacy), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := ReadHostIntent(dir)
			if !errors.Is(err, ErrHostState) {
				t.Fatal(err)
			}
			if legacy == "" {
				if !strings.Contains(err.Error(), "this host has no Mycelium host state yet; provision it with `myce host open` or `myce dev`") || strings.Contains(err.Error(), "convert") {
					t.Fatal("misleading provisioning error", err)
				}
			} else if !strings.Contains(err.Error(), "myce host convert offline") {
				t.Fatal("missing legacy conversion pointer", err)
			}
		})
	}
}
func TestHostRevisionReboot(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	testHostOpen(t, dir, clock)
	s, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed", Grace: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err = PublishHostObservation(dir, s.HostRevision, "boot-1", "closed", clock); err != nil {
		t.Fatal(err)
	}
	reboot, err := ReadHostIntent(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reboot.Observation("boot-1") == nil || reboot.Observation("boot-2") != nil || reboot.HostRevision != s.HostRevision || !reboot.StopDeadline.Equal(*s.StopDeadline) {
		t.Fatal(reboot)
	}
	clock.Advance(time.Hour)
	if hostDisableGrace(dir, clock) != 0 {
		t.Fatal("reboot renewed deadline")
	}
}
func TestHostRevisionIntakeDuringNetworkAndNativeGate(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	s := testHostOpen(t, dir, clock)
	launch, err := hostLaunchGate(dir, s.HostRevision)
	if err != nil {
		t.Fatal(err)
	}
	defer launch()
	// Represents an in-flight daemon operation holding launch.lock. Closing only
	// needs intake.lock and cannot wait for that operation's network response.
	closed, err := changeHostIntent(dir, clock, hostIntentChange{Desired: "closed"})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err = hostDispatch(dir, s.HostRevision, func() error { called = true; return nil }); !errors.Is(err, ErrHostRevision) || called {
		t.Fatal(err, called)
	}
	if err = PublishHostObservation(dir, s.HostRevision, "boot", "open", clock); !errors.Is(err, ErrHostRevision) {
		t.Fatal(err)
	}
	if closed.Desired != "closed" {
		t.Fatal(closed)
	}
}
func ensureTestHost(t *testing.T, dir string, clock Clock, host ...string) {
	t.Helper()
	if dir == "" {
		return
	}
	if _, err := os.Stat(filepath.Join(dir, hostStateFile)); err == nil {
		return
	}
	if clock == nil {
		clock = RealClock{}
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	testHostOpen(t, dir, clock, host...)
}
func readyTestHost(t *testing.T, dir string, host ...string) string {
	t.Helper()
	ensureTestHost(t, dir, RealClock{}, host...)
	return dir
}
func closeTestHost(dir string) error {
	_, err := changeHostIntent(dir, RealClock{}, hostIntentChange{Desired: "closed"})
	return err
}

func TestHostRevisionDevClosesBothDevicesAndRejectsDelayedLaunch(t *testing.T) {
	root := t.TempDir()
	hostDir := filepath.Join(root, "host")
	d, err := StartDev(DevConfig{HostStateDir: hostDir, HostID: "dev-host", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "test", Workers: 2, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Keep cleanup valid even when an assertion fails after reopen: stop real
		// commands under closed admission before waiting for the dev workers.
		if _, e := changeHostIntent(hostDir, RealClock{}, hostIntentChange{Desired: "closed"}); e != nil {
			t.Error(e)
		}
		if e := d.Close(); e != nil {
			t.Error(e)
		}
	}()
	await(t, func() bool { snap, e := d.Queue.Snapshot(); return e == nil && len(snap.Machines) == 2 })
	s, err := ReadHostIntent(hostDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, machine := range []string{"fake-1", "fake-2"} {
		spec := spec("experiment")
		spec.AllowedMachines = []string{machine}
		spec.Run.Command = []string{"/bin/sh", "-c", "sleep 60"}
		spec.Resumable = false
		if _, err = d.Queue.Submit(spec); err != nil {
			t.Fatal(err)
		}
	}
	var pids []int
	await(t, func() bool {
		pids = nil
		for _, name := range []string{"fake-1", "fake-2"} {
			b, e := os.ReadFile(filepath.Join(root, "workers", name, "state.json"))
			if e != nil {
				return false
			}
			var st workerState
			if json.Unmarshal(b, &st) != nil || st.PID == 0 {
				return false
			}
			pids = append(pids, st.PID)
		}
		return len(pids) == 2
	})
	// Delay queue convergence until the old worker runs have stopped. This
	// forces the ordering that synthetic local settlement used to bypass.
	d.Queue.mu.Lock()
	queueLocked := true
	defer func() {
		if queueLocked {
			d.Queue.mu.Unlock()
		}
	}()
	// Uses the real command runner and workers. Only inventory is fake.
	closed, err := changeHostIntent(hostDir, RealClock{}, hostIntentChange{Desired: "closed", Grace: 0})
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range pids {
		if err = waitWorkerGroup(pid, RealClock{}); err != nil {
			t.Fatal(err)
		}
	}
	w := Worker{HostStateDir: hostDir, hostRevision: s.HostRevision, Clock: RealClock{}}
	if _, err = w.start(nil, Job{}); !errors.Is(err, ErrHostRevision) {
		t.Fatal("delayed launch admitted", err)
	}
	// Process exit alone is insufficient: wait for both worker runs to release
	// their real spool locks before collecting exclusion evidence.
	for _, name := range []string{"fake-1", "fake-2"} {
		await(t, func() bool {
			unlock, e := hostOperationLock(filepath.Join(root, "workers", name), "worker.lock", false)
			if errors.Is(e, syscall.EWOULDBLOCK) || errors.Is(e, syscall.EAGAIN) {
				return false
			}
			if e != nil {
				t.Fatal(e)
			}
			unlock()
			return true
		})
	}
	queueLocked = false
	d.Queue.mu.Unlock()
	if _, err = changeHostIntent(hostDir, RealClock{}, hostIntentChange{Desired: "open", Expected: closed.HostRevision}); err == nil || err.Error() != "previous_close_unsettled" {
		t.Fatal("open before exclusion and queue settlement", err)
	}
	// Only kernel/service inventory is fake. Cleanup records the registered
	// sessions; the real authenticated exchange and SQLite commit settle them.
	report, err := (HostControl{Dir: hostDir, Clock: RealClock{}, Service: &fakeHostService{}, Observe: testHostExclusion}).Disable(context.Background(), "test close", 0)
	if err != nil || !report.Confirmed || report.HostRevision != closed.HostRevision {
		t.Fatal(report, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err = ReconcileHostLocal(ctx, hostDir, RealClock{}, func(string) (Client, error) { return d.Client(), nil }); err != nil {
		t.Fatal(err)
	}
	settled, err := ReadHostIntent(hostDir)
	if err != nil || settled.HostSettlement() != "confirmed" || settled.LocalExclusion == nil || len(settled.LocalExclusion.Sessions) != 2 || len(settled.Pending) != 0 {
		t.Fatal("close did not settle both registered sessions", settled, err)
	}
	ack, ok := settled.QueueAcks[d.URL]
	if !ok || ack.HostRevision != closed.HostRevision || ack.Desired != "closed" || len(ack.Pending) != 0 || len(ack.Covered) != 2 {
		t.Fatal("missing real queue close coverage", ack)
	}
	// Even after a subsequent, settled open the old action cannot launch.
	if _, err = changeHostIntent(hostDir, RealClock{}, hostIntentChange{Desired: "open", Expected: closed.HostRevision}); err != nil {
		t.Fatal(err)
	}
	if _, err = w.start(nil, Job{}); !errors.Is(err, ErrHostRevision) {
		t.Fatal("delayed launch after reopen", err)
	}
	if err = d.Queue.Tick(); err != nil {
		t.Fatal("queue rejected settled reopen", err)
	}
	opened, err := ReadHostIntent(hostDir)
	if err != nil || !hostOpenConverged(opened) {
		t.Fatal("reopen did not converge", opened, err)
	}
}
func readTestHostRevision(t *testing.T, dir string) HostRevision {
	t.Helper()
	s, e := ReadHostIntent(dir)
	if e != nil {
		t.Fatal(e)
	}
	return s.HostRevision
}
func TestHostRevisionLegacyHTTPControlRejected(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	for _, action := range []string{"enable", "disable"} {
		e := client.Call(context.Background(), "POST", "/api/hosts/test-host/"+action, map[string]any{"machine": "card-a", "generation": 999, "cleared": true}, nil)
		var httpErr *HTTPError
		if !errors.As(e, &httpErr) || httpErr.Status != 404 {
			t.Fatal(e)
		}
	}
	if len(q.s.HostIntents) > 0 {
		t.Fatal("legacy control changed queue")
	}
}
