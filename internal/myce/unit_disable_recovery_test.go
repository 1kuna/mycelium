package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnitEnableWithoutSavedSession(t *testing.T) {
	for _, mode := range []string{"fresh-unknown", "fresh-known", "legacy-idle"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("idle", "fake")
			m.HostID = "host"
			if mode != "fresh-unknown" {
				check(t, q, m, nil)
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			shared := t.TempDir()
			unit := "myce-worker@idle.service"
			dir, err := WorkerUnitDirectory(shared, unit)
			if err != nil {
				t.Fatal(err)
			}
			state := t.TempDir()
			if mode == "legacy-idle" {
				if err := saveWorkerJSON(state, "state.json", workerState{Session: m.Session}); err != nil {
					t.Fatal(err)
				}
			}
			if err := AdoptHostWorker(dir, HostWorker{Unit: "myce-worker@idle.service", State: state, Host: "host", Name: m.ID, Queue: server.URL}); err != nil {
				t.Fatal(err)
			}
			h := HostControl{Dir: dir, Service: &fakeHostService{}}
			if mode == "legacy-idle" {
				r, err := h.Disable(context.Background(), "idle", 0)
				if err != nil || !r.Confirmed {
					t.Fatalf("disable: %+v %v", r, err)
				}
			} else {
				c := ServiceConfig{HostStateDir: shared, Service: unit, State: state, HostID: m.HostID, Name: m.ID, Queue: server.URL}
				command := func(context.Context, string, ...string) ([]byte, error) {
					return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
				}
				if err := installDisabled(context.Background(), c, command, true); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "legacy-idle" {
				// Old binaries recorded idle workers without HostWorker.Session.
				var receipt hostReceipt
				data, err := os.ReadFile(filepath.Join(dir, "disable-receipt.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &receipt); err != nil {
					t.Fatal(err)
				}
				receipt.Workers[0].Session = ""
				if err := saveWorkerJSON(dir, "disable-receipt.json", receipt); err != nil {
					t.Fatal(err)
				}
			}
			// Enable offline first, then deliver the receipt through the worker's retry path.
			if err := h.Enable(context.Background()); err != nil {
				t.Fatal(err)
			}
			w := Worker{HostStateDir: dir, Machine: m, Client: Client{URL: server.URL, Token: "test"}}
			if err := w.reconcileEnable(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(dir, "enable-pending.json")); !os.IsNotExist(err) {
				t.Fatalf("enable not archived: %v", err)
			}
			if known := q.s.Machines[m.ID]; known != nil && (known.Disabled != "" || known.Gone) {
				t.Fatalf("idle machine held: %+v", known)
			}
		})
	}
}

func TestUnitControlCounterRecoveryAndHostMove(t *testing.T) {
	q, _ := fixture(t)
	m := machine("idle", "fake")
	m.HostID = "old-host"
	check(t, q, m, nil)
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, nil, 10); err != nil {
		t.Fatal(err)
	}
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, nil, 1); err == nil || !strings.Contains(err.Error(), "at least 10") {
		t.Fatalf("counter recovery guidance: %v", err)
	}
	// Restoring the durable counter to the floor makes the next disable usable.
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, nil, 11); err != nil {
		t.Fatal(err)
	}
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", true, false, nil, 11); err == nil || !strings.Contains(err.Error(), "already records") || strings.Contains(err.Error(), "restore") {
		t.Fatalf("opposite operation guidance: %v", err)
	}
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", true, false, nil, 12); err != nil {
		t.Fatal(err)
	}
	m.HostID = "new-host"
	m.Session = "new-session"
	check(t, q, m, nil)
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", true, false, nil, 2); err != nil {
		t.Fatal(err)
	}
	if q.s.Machines[m.ID].Disabled != "" {
		t.Fatal("moved machine stayed disabled")
	}
	if err := q.ControlMachine("old-host", m.ID, "session-idle", "", false, true, nil, 99); err == nil {
		t.Fatal("former host accepted")
	}
}

func TestLocalDisableReceiptsTakeNoControlLock(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	f.disable(t, 0)
	// An exclusive operator lock excludes any shared reader lock. Reconciliation
	// must still read the atomic evidence, without acquiring or creating a lock.
	unlock, err := hostOperationLock(f.controls[0].Dir, "control.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if err := f.q.ConfigureLocalDisableReceipts("host", f.dir); err != nil {
		t.Fatal(err)
	}
	assertNoUnitDockerHold(t, f.q)
}

func TestLocalDisableReceiptsPreviousCycleIgnored(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	if err := f.q.Close(); err != nil {
		t.Fatal(err)
	}
	f.disable(t, 0)
	old, err := os.ReadFile(filepath.Join(f.controls[0].Dir, "disable-receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.controls[0].Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Even an old receipt and flag left visible cannot defeat enable's advanced counter.
	if err := os.WriteFile(filepath.Join(f.controls[0].Dir, "disable-receipt.json"), old, 0600); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(f.controls[0].Dir, "DISABLED", map[string]any{"generation": 1}); err != nil {
		t.Fatal(err)
	}
	f.restart(t, true)
	f.q.SetRestartGrace(0)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Minute)
	snap, err := f.q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(f.q.s.HostReleases) != 1 || !strings.Contains(strings.Join(snap.HostHolds, " "), "operator action required") {
		t.Fatal("previous cycle released custody")
	}
}

func TestLocalDisableReceiptsPreserveRunningSibling(t *testing.T) {
	f := newUnitDisableFixture(t, true)
	// Report the sibling as started, so its unrelated start deadline cannot fire.
	if err := f.q.Started(result(f.jobs[1], true)); err != nil {
		t.Fatal(err)
	}
	f.disable(t, 0)
	sibling := cloneMachine(f.q.s.Machines["gpu1"])
	before := cloneJob(f.q.s.Jobs[f.jobs[1].ID])
	var release HostRelease
	for _, r := range f.q.s.HostReleases {
		if r.Machine == "gpu1" {
			release = r
		}
	}
	if err := f.q.ConfigureLocalDisableReceipts("host", f.dir); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&sibling, f.q.s.Machines["gpu1"]) || !reflect.DeepEqual(&before, f.q.s.Jobs[f.jobs[1].ID]) {
		t.Fatal("reader changed running sibling")
	}
	if before.State != "running" || release.Machine != "gpu1" || len(f.q.s.HostReleases) != 1 || !reflect.DeepEqual(f.q.s.HostReleases[releaseKey(release)], release) {
		t.Fatal("reader changed sibling custody")
	}
}

func TestLocalDisableReceiptsCorruptJSONLoggedAndLimited(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	f.disable(t, 0)
	if err := os.WriteFile(filepath.Join(f.controls[0].Dir, "disable-receipt.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	if err := f.q.ConfigureLocalDisableReceipts("host", f.dir); err != nil {
		t.Fatal(err)
	}
	reason := "disable-receipt.json-invalid-json"
	if strings.Count(logs.String(), reason) != 1 || len(f.q.s.HostReleases) != 1 {
		t.Fatalf("corrupt receipt: %s", logs.String())
	}
	f.clock.Advance(time.Second)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	if strings.Count(logs.String(), reason) != 1 {
		t.Fatal("skip logging not limited")
	}
	f.clock.Advance(time.Minute)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	if strings.Count(logs.String(), reason) != 2 || len(f.q.s.HostReleases) != 1 {
		t.Fatal("skip logging did not use injected clock or custody changed")
	}
}

func TestLocalDisableReceiptsUnavailableDirectoryIsNonfatal(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	unreadable := filepath.Join(root, "unreadable")
	if err := os.Mkdir(unreadable, 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(unreadable, 0700)
	for _, path := range []string{filepath.Join(root, "missing"), file, unreadable} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			f := newUnitDisableFixture(t, false)
			var logs bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(previous)
			if err := f.q.ConfigureLocalDisableReceipts("host", path); err != nil {
				t.Fatalf("optional reader blocked startup: %v", err)
			}
			reason := "host-state-directory-unreadable-or-missing"
			if !strings.Contains(logs.String(), "continuing without local receipts") || strings.Count(logs.String(), reason) != 1 {
				t.Fatalf("missing startup error or skip log: %s", logs.String())
			}
			f.clock.Advance(time.Second)
			if err := f.q.Tick(); err != nil {
				t.Fatal(err)
			}
			if strings.Count(logs.String(), reason) != 1 {
				t.Fatal("skip logging not limited")
			}
			f.clock.Advance(time.Minute)
			if _, err := f.q.Snapshot(); err != nil {
				t.Fatal(err)
			}
			if strings.Count(logs.String(), reason) != 2 || len(f.q.s.HostReleases) != 1 {
				t.Fatal("queue unavailable, skip logging stopped, or custody changed")
			}
		})
	}
}

func TestUnitDisableClearsOnlyCoveredMachineModels(t *testing.T) {
	for _, path := range []string{"notification", "receipt"} {
		t.Run(path, func(t *testing.T) {
			f := newUnitDisableFixture(t, false)
			if err := f.q.ConfigureHosts(map[string]HostPolicy{"host": {ProductionHold: true}}); err != nil {
				t.Fatal(err)
			}
			if err := f.q.update(func() error {
				f.q.s.ModelSlots = map[string]*ModelSlot{}
				f.q.s.BatchReleases = map[string]batchRelease{}
				for _, m := range f.machines {
					f.q.s.ModelSlots[slotKey(m.ID, "model")] = &ModelSlot{Model: "model", Machine: m.ID, Session: m.Session, State: "ready", LastUsed: f.clock.Now()}
					f.q.s.BatchReleases[m.ID] = batchRelease{Session: m.Session, MemoryGB: 1, Until: f.clock.Now().Add(time.Hour)}
					f.q.s.Machines[m.ID].Models = []string{"model"}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			siblingSlot := *f.q.s.ModelSlots[slotKey("gpu1", "model")]
			siblingRelease := f.q.s.BatchReleases["gpu1"]
			if path == "notification" {
				server := httptest.NewServer(API{Queue: f.q, Token: "test"}.Handler())
				defer server.Close()
				h := f.controls[0]
				h.Notify = func(ctx context.Context, w HostWorker, enable, cleared bool, reason string) error {
					w.Queue = server.URL
					return NotifyUnit(Client{Token: "test"})(ctx, w, enable, cleared, reason)
				}
				r, err := h.Disable(context.Background(), "one unit", 0)
				if err != nil || !r.Confirmed || len(r.QueueErrors) != 0 {
					t.Fatalf("disable: %+v %v", r, err)
				}
			} else {
				if err := f.q.Close(); err != nil {
					t.Fatal(err)
				}
				f.disable(t, 0)
				f.restart(t, true)
			}
			m := f.q.s.Machines["gpu0"]
			if f.q.s.ModelSlots[slotKey("gpu0", "model")] != nil || len(m.Models) != 0 || !m.Gone {
				t.Fatal("cleared unit retained models")
			}
			if _, ok := f.q.s.BatchReleases["gpu0"]; ok {
				t.Fatal("cleared unit retained batch credit")
			}
			if strings.Contains(f.q.hostBlockers("host"), "worker gpu0") {
				t.Fatal("cleared worker still blocks host")
			}
			if got := f.q.s.ModelSlots[slotKey("gpu1", "model")]; got == nil || got.Machine != siblingSlot.Machine || got.Session != siblingSlot.Session || got.Model != siblingSlot.Model {
				t.Fatalf("sibling slot changed: %+v", got)
			}
			if !reflect.DeepEqual(f.q.s.BatchReleases["gpu1"], siblingRelease) || len(f.q.s.Machines["gpu1"].Models) != 1 {
				t.Fatal("sibling model/credit changed")
			}
		})
	}
}

func TestUnitDisableRetainsModelsWithoutMatchingCompleteEvidence(t *testing.T) {
	for _, mode := range []string{"outstanding", "other-session"} {
		t.Run(mode, func(t *testing.T) {
			f := newUnitDisableFixture(t, false)
			m := f.machines[0]
			if mode == "other-session" {
				m = f.machines[1]
			}
			f.q.s.ModelSlots = map[string]*ModelSlot{slotKey(m.ID, "model"): {Model: "model", Machine: m.ID, Session: m.Session, State: "ready"}}
			session := m.Session
			if mode == "other-session" {
				session = "previous-session"
			}
			err := f.q.ControlMachine("host", m.ID, session, "", false, true, nil, 1)
			if mode == "other-session" {
				if err == nil {
					t.Fatal("stale session accepted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if f.q.s.ModelSlots[slotKey(m.ID, "model")] == nil {
				t.Fatal("uncovered model cleared")
			}
		})
	}
}

func TestUnitNotifyStaleRegistryCannotShadowLiveSession(t *testing.T) {
	dir, err := WorkerUnitDirectory(t.TempDir(), "myce-worker@idle.service")
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range []string{"stale", "live"} {
		state := t.TempDir()
		if err := saveWorkerJSON(state, "state.json", workerState{Session: session}); err != nil {
			t.Fatal(err)
		}
		if err := AdoptHostWorker(dir, HostWorker{State: state, Name: "idle", Host: "host", Unit: "myce-worker@idle.service", Queue: "http://queue"}); err != nil {
			t.Fatal(err)
		}
	}
	workers, err := HostWorkers(dir)
	if err != nil || len(workers) != 2 {
		t.Fatalf("registry: %+v %v", workers, err)
	}
	if workers[0].Session != "stale" {
		workers[0], workers[1] = workers[1], workers[0]
	}
	for _, action := range []string{"disable", "cleared-disable", "enable", "rejoin-enable"} {
		t.Run(action, func(t *testing.T) {
			seen := make(chan string, 2)
			h := HostControl{Dir: dir, Notify: func(_ context.Context, w HostWorker, _, _ bool, _ string) error { seen <- w.Session; return nil }}
			enable := strings.Contains(action, "enable")
			if errs := h.notify(context.Background(), workers, enable, action == "cleared-disable", ""); len(errs) != 0 {
				t.Fatal(errs)
			}
			close(seen)
			sessions := map[string]bool{}
			for session := range seen {
				sessions[session] = true
			}
			if !sessions["live"] || !sessions["stale"] {
				t.Fatalf("session shadowed: %+v", sessions)
			}
		})
	}
}

func TestUnitControlStaleIdleSessionCannotShadowCleanup(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	m := f.machines[0]
	// A stale idle registry entry can notify first at this same unit generation.
	if err := f.q.ControlMachine(m.HostID, m.ID, "old-session", "", false, true, nil, 1); err == nil {
		t.Fatal("stale session accepted")
	}
	r := result(f.jobs[0], true)
	if err := f.q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, []Result{r}, 1); err != nil {
		t.Fatal(err)
	}
	assertNoUnitDockerHold(t, f.q)
	if !f.q.s.Machines[m.ID].Gone {
		t.Fatal("live cleanup shadowed by stale idle session")
	}
}

// A failed cleanup replay must preserve its evidence until a later retry settles it.
func TestUnitEnableRetainsPendingAfterFailedClearedReplay(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	f.disable(t, 0)
	if err := f.controls[0].Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: f.q, Token: "test"}.Handler())
	defer server.Close()
	var failed, enableRejected atomic.Bool
	h := f.controls[0]
	h.Notify = func(ctx context.Context, w HostWorker, enable, cleared bool, reason string) error {
		if !enable && failed.CompareAndSwap(false, true) {
			return errors.New("transient cleanup replay failure")
		}
		w.Queue = server.URL
		err := NotifyUnit(Client{Token: "test"})(ctx, w, enable, cleared, reason)
		var httpErr *HTTPError
		if enable && errors.As(err, &httpErr) && httpErr.Status == 400 {
			enableRejected.Store(true)
		}
		return err
	}
	if err := h.ReconcileWorkerEnable(context.Background(), "http://127.0.0.1:1", "host"); err == nil || !enableRejected.Load() {
		t.Fatalf("enable accepted before cleanup acknowledgement: %v", err)
	}
	pending := filepath.Join(h.Dir, "enable-pending.json")
	data, err := os.ReadFile(pending)
	if err != nil {
		t.Fatalf("cleanup evidence lost: %v", err)
	}
	var receipt hostReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if len(receipt.Workers) != 1 || len(receipt.Workers[0].Executions) != 1 || len(f.q.s.HostReleases) != 1 || f.q.s.MachineControls["gpu0"].Generation != 0 {
		t.Fatal("failed replay changed custody or lost pending execution")
	}
	if err := h.ReconcileWorkerEnable(context.Background(), "http://127.0.0.1:1", "host"); err != nil {
		t.Fatalf("later replay failed: %v", err)
	}
	assertNoUnitDockerHold(t, f.q)
	if _, err := os.Stat(pending); !os.IsNotExist(err) {
		t.Fatalf("acknowledged enable still pending: %v", err)
	}
}

func TestLocalDisableReceiptsAlternatingSessionsDoNotReprocess(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	h := f.controls[0]
	workers, err := HostWorkers(h.Dir)
	if err != nil || len(workers) != 1 {
		t.Fatalf("registry: %v", err)
	}
	stale := t.TempDir()
	if err := saveWorkerJSON(stale, "state.json", workerState{Session: "old-session"}); err != nil {
		t.Fatal(err)
	}
	w := workers[0]
	if err := AdoptHostWorker(h.Dir, HostWorker{State: stale, Host: w.Host, Name: w.Name, Unit: w.Unit, Queue: w.Queue}); err != nil {
		t.Fatal(err)
	}
	if err := f.q.Close(); err != nil {
		t.Fatal(err)
	}
	f.disable(t, 0)
	f.restart(t, true)
	assertNoUnitDockerHold(t, f.q)
	for i := 0; i < 4; i++ {
		if err := f.q.update(func() error {
			f.q.s.Jobs[f.jobs[0].ID].ProductionHost = "host"
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		f.clock.Advance(time.Second)
		if err := f.q.Tick(); err != nil {
			t.Fatal(err)
		}
		if f.q.s.Jobs[f.jobs[0].ID].ProductionHost != "host" || f.q.s.MachineControls["gpu0"].Session != f.machines[0].Session {
			t.Fatalf("tick %d reprocessed settled receipt", i)
		}
	}
}
