package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type unitDisableFixture struct {
	q             *Queue
	clock         *fakeClock
	dir, db, root string
	machines      [2]Machine
	jobs          [2]*Job
	controls      [2]HostControl
}

func newUnitDisableFixture(t *testing.T, siblingRunning bool) *unitDisableFixture {
	t.Helper()
	q, clock := fixture(t)
	_, spool, dockerClock, dockerRoot := dockerFixture(t)
	f := &unitDisableFixture{q: q, clock: clock, dir: t.TempDir(), root: q.root}
	if err := q.db.QueryRow("PRAGMA database_list").Scan(new(int), new(string), &f.db); err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"gpu0", "gpu1"} {
		m := machine(name, "fake")
		m.HostID = "host"
		m.Device = name
		f.machines[i] = m
		check(t, q, m, nil)
	}
	for i, m := range f.machines {
		var running *Job
		if i == 0 || siblingRunning {
			spec := dockerJob().Spec
			spec.Priority = "production"
			spec.AllowedMachines = []string{m.ID}
			j := submit(t, q, spec)
			running = check(t, q, m, nil).Job
			if running == nil {
				t.Fatal("not assigned", i, j.ID)
			}
			f.jobs[i] = running
		}
		unit := "myce-worker@" + m.ID + ".service"
		dir, _ := WorkerUnitDirectory(f.dir, unit)
		state := t.TempDir()
		if i == 0 {
			state = spool.dir
		}
		if err := AdoptHostWorker(dir, HostWorker{State: state, Host: m.HostID, Name: m.ID, Unit: unit, Queue: "http://127.0.0.1:1"}); err != nil {
			t.Fatal(err)
		}
		if err := saveWorkerJSON(state, "state.json", workerState{Session: m.Session, Job: running}); err != nil {
			t.Fatal(err)
		}
		f.controls[i] = HostControl{Dir: dir, Clock: dockerClock, Docker: true, Service: &fakeHostService{}, Notify: NotifyUnit(Client{Token: "test"})}
	}
	spool.close()
	for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned", "mode": "term"} {
		if err := os.WriteFile(filepath.Join(dockerRoot, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveWorkerJSON(spool.dir, "owned.docker.json", dockerExecution{Identity: "owned", Endpoint: "unix:///var/run/docker.sock", Container: "id1", Operation: "start"}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *unitDisableFixture) disable(t *testing.T, i int) {
	t.Helper()
	report, err := f.controls[i].Disable(context.Background(), "planned outage", 0)
	if err != nil || !report.Confirmed || len(report.QueueErrors) == 0 {
		t.Fatalf("offline disable: %+v %v", report, err)
	}
}
func (f *unitDisableFixture) restart(t *testing.T, enabled bool) {
	t.Helper()
	if err := f.q.Close(); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(9 * time.Minute)
	q, err := Open(f.db, f.root, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	f.q = q
	t.Cleanup(func() { q.Close() })
	if enabled {
		if err := q.ConfigureLocalDisableReceipts("host", f.dir); err != nil {
			t.Fatal(err)
		}
	}
}
func assertNoUnitDockerHold(t *testing.T, q *Queue) {
	t.Helper()
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(q.s.HostReleases) != 0 || strings.Contains(strings.Join(snap.HostHolds, " "), "operator action required") {
		t.Fatalf("cleanup retained: %+v %+v", q.s.HostReleases, snap.HostHolds)
	}
}
func TestLocalDisableReceiptsPlannedQueueOutage(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	if err := f.q.Close(); err != nil {
		t.Fatal(err)
	} // No queue process is serving the notifications.
	f.disable(t, 0)
	f.disable(t, 1)
	originals := map[string][]byte{}
	for _, h := range f.controls {
		for _, name := range []string{"DISABLED", "disable-receipt.json", "control-generation.json"} {
			path := filepath.Join(h.Dir, name)
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			originals[path] = b
		}
	}
	f.restart(t, true)
	assertNoUnitDockerHold(t, f.q)
	f.clock.Advance(10 * time.Minute)
	assertNoUnitDockerHold(t, f.q)
	got := job(t, f.q, f.jobs[0].ID)
	if got.State != "failed" || !strings.Contains(got.Reason, "production start deadline exceeded") || got.Losses != 0 || !got.RequeuedByOperator {
		t.Fatalf("operator retry: %+v", got)
	}
	for _, m := range f.q.s.Machines {
		if m.Disabled == "" || !m.Gone {
			t.Fatal("machine not disabled/gone", m.ID)
		}
	}
	for path, want := range originals {
		got, err := os.ReadFile(path)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("receipt modified", err)
		}
	}
}

func TestUnitDisableNotificationPreservesSibling(t *testing.T) {
	f := newUnitDisableFixture(t, true)
	server := httptest.NewServer(API{Queue: f.q, Token: "test"}.Handler())
	defer server.Close()
	h := f.controls[0]
	h.Notify = func(ctx context.Context, w HostWorker, enable, cleared bool, reason string) error {
		w.Queue = server.URL
		return NotifyUnit(Client{Token: "test"})(ctx, w, enable, cleared, reason)
	}
	siblingKey := releaseKey(HostRelease{Job: f.jobs[1].ID, Machine: "gpu1", Session: f.machines[1].Session, Attempt: f.jobs[1].Attempt})
	siblingRelease := f.q.s.HostReleases[siblingKey]
	sibling := cloneMachine(f.q.s.Machines["gpu1"])
	report, err := h.Disable(context.Background(), "one card", 0)
	if err != nil || !report.Confirmed || len(report.QueueErrors) > 0 {
		t.Fatalf("disable: %+v %v", report, err)
	}
	if len(f.q.s.HostReleases) != 1 || !reflect.DeepEqual(f.q.s.HostReleases[siblingKey], siblingRelease) {
		t.Fatal("sibling release changed", f.q.s.HostReleases)
	}
	if !reflect.DeepEqual(f.q.s.Machines["gpu1"], &sibling) {
		t.Fatal("sibling machine changed")
	}
	if got := job(t, f.q, f.jobs[1].ID); got.State != "running" || got.Attempt != f.jobs[1].Attempt {
		t.Fatalf("sibling attempt changed: %+v", got)
	}
	if len(f.q.s.DisabledHosts) != 0 || len(f.q.s.HostControls) != 0 {
		t.Fatal("unit changed whole-host controls")
	}
	if got := job(t, f.q, f.jobs[0].ID); got.State != "failed" || !strings.Contains(got.Reason, "no eligible host is enabled") || got.Losses != 0 {
		t.Fatalf("unit retry: %+v", got)
	}
	// Repeating a cleared generation cannot touch the sibling or requeue twice.
	data, err := os.ReadFile(filepath.Join(h.Dir, "disable-receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt hostReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if err := h.Notify(context.Background(), receipt.Workers[0], false, true, "one card"); err != nil {
		t.Fatal(err)
	}
	if len(f.q.s.HostReleases) != 1 || !reflect.DeepEqual(f.q.s.HostReleases[siblingKey], siblingRelease) {
		t.Fatal("replayed disable changed sibling")
	}
}

func TestLocalDisableReceiptsCustody(t *testing.T) {
	for _, mode := range []string{"off", "missing", "generation", "worker-generation", "counter", "session", "attempt", "host", "machine", "unit", "unconfirmed", "stale-queue-generation"} {
		t.Run(mode, func(t *testing.T) {
			f := newUnitDisableFixture(t, false)
			if mode == "unconfirmed" {
				f.controls[0].ConfirmContainers = func(context.Context) error { return errors.New("containers still live or unconfirmed") }
			}
			if mode != "missing" {
				if mode == "unconfirmed" {
					if _, err := f.controls[0].Disable(context.Background(), "unconfirmed", 0); err == nil {
						t.Fatal("cleanup accepted")
					}
				} else {
					f.disable(t, 0)
				}
			}
			path := filepath.Join(f.controls[0].Dir, "disable-receipt.json")
			if mode != "off" && mode != "missing" && mode != "unconfirmed" {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var receipt hostReceipt
				if err := json.Unmarshal(data, &receipt); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "generation":
					receipt.Generation++
				case "worker-generation":
					receipt.Workers[0].Generation++
				case "counter":
					if err := saveWorkerJSON(f.controls[0].Dir, "control-generation.json", receipt.Generation+1); err != nil {
						t.Fatal(err)
					}
				case "unit":
					receipt.Workers[0].Unit = "myce-worker@gpu1.service"
				case "session":
					receipt.Workers[0].Session = "wrong"
					receipt.Workers[0].Executions[0].Session = "wrong"
				case "attempt":
					receipt.Workers[0].Executions[0].Attempt++
				case "host":
					receipt.Workers[0].Host = "other"
				case "machine":
					receipt.Workers[0].Name = "gpu1"
				case "stale-queue-generation":
					f.q.s.MachineControls = map[string]machineControl{"gpu0": {Host: "host", Session: f.machines[0].Session, HostControlGeneration: HostControlGeneration{Generation: receipt.Generation + 1, Enabled: true}}}
					if err := f.q.update(func() error { return nil }); err != nil {
						t.Fatal(err)
					}
				}
				if err := saveWorkerJSON(f.controls[0].Dir, "disable-receipt.json", receipt); err != nil {
					t.Fatal(err)
				}
			}
			f.restart(t, mode != "off")
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
				t.Fatalf("custody weakened: %+v %+v", f.q.s.HostReleases, snap.HostHolds)
			}
		})
	}
}

func TestLocalDisableReceiptsPeriodicClock(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	if err := f.q.ConfigureLocalDisableReceipts("host", f.dir); err != nil {
		t.Fatal(err)
	}
	f.disable(t, 0)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(f.q.s.HostReleases) != 1 {
		t.Fatal("scan ignored injected clock interval")
	}
	f.clock.Advance(time.Second)
	assertNoUnitDockerHold(t, f.q)
}

func TestUnitEnableAfterOutageClearsEscalatedDockerHold(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	f.disable(t, 0)
	f.restart(t, false) // Remote/default-off queues still reconcile on enable/rejoin.
	f.q.SetRestartGrace(0)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Minute)
	snap, err := f.q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(snap.HostHolds, " "), "operator action required") {
		t.Fatal("no escalated hold to reconcile")
	}
	// Enable offline; the worker must deliver its unit receipt on rejoin.
	if err := f.controls[0].Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: f.q, Token: "test"}.Handler())
	defer server.Close()
	data, err := os.ReadFile(filepath.Join(f.controls[0].Dir, "enable-pending.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt hostReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Workers[0].Queue = server.URL
	if err := saveWorkerJSON(f.controls[0].Dir, "enable-pending.json", receipt); err != nil {
		t.Fatal(err)
	}
	w := Worker{HostStateDir: f.controls[0].Dir, Machine: f.machines[0], Client: Client{URL: server.URL, Token: "test"}}
	if err := w.reconcileEnable(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertNoUnitDockerHold(t, f.q)
	if f.q.s.Machines["gpu0"].Disabled != "" {
		t.Fatal("enable did not clear machine fence")
	}
}

func TestUnitControlRejectsStaleAndUncoveredEvidence(t *testing.T) {
	for _, mode := range []string{"generation", "session", "attempt", "host", "uncovered"} {
		t.Run(mode, func(t *testing.T) {
			f := newUnitDisableFixture(t, true)
			m := f.machines[0]
			r := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: f.jobs[0].ID, Attempt: f.jobs[0].Attempt}}
			if err := f.q.ControlMachine(m.HostID, m.ID, m.Session, "one card", false, false, []Result{r}, 2); err != nil {
				t.Fatal(err)
			}
			before := cloneJob(f.q.s.Jobs[r.ID])
			releases := len(f.q.s.HostReleases)
			gen, session, host := uint64(2), m.Session, m.HostID
			executions := []Result{r}
			switch mode {
			case "generation":
				gen = 1
			case "session":
				session = "foreign"
				executions[0].Session = session
			case "attempt":
				executions[0].Attempt++
			case "host":
				host = "foreign"
			case "uncovered":
				executions = nil
			}
			err := f.q.ControlMachine(host, m.ID, session, "one card", false, true, executions, gen)
			if mode == "uncovered" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("stale notification accepted")
			}
			if len(f.q.s.HostReleases) != releases || !reflect.DeepEqual(f.q.s.Jobs[r.ID], &before) {
				t.Fatal("uncovered custody changed")
			}
		})
	}
}

func TestLocalHostReceiptKeepsUnitGenerationIndependent(t *testing.T) {
	f := newUnitDisableFixture(t, true)
	workers, err := HostWorkers(f.controls[0].Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := AdoptHostWorker(f.dir, workers[0]); err != nil {
		t.Fatal(err)
	}
	// A higher unit generation must not suppress a valid host receipt, and the
	// host receipt must never overwrite that unit's independent generation.
	if err := f.q.ControlMachine("host", "gpu0", f.machines[0].Session, "unit", false, false, nil, 10); err != nil {
		t.Fatal(err)
	}
	h := f.controls[0]
	h.Dir = f.dir
	h.Notify = func(context.Context, HostWorker, bool, bool, string) error { return errors.New("offline") }
	report, err := h.Disable(context.Background(), "host", 0)
	if err != nil || !report.Confirmed {
		t.Fatalf("host disable: %+v %v", report, err)
	}
	if err := f.q.ConfigureLocalDisableReceipts("host", f.dir); err != nil {
		t.Fatal(err)
	}
	if f.q.s.MachineControls["gpu0"].Generation != 10 {
		t.Fatal("host receipt overwrote unit generation")
	}
	if len(f.q.s.HostReleases) != 1 {
		t.Fatal("host receipt released uncovered sibling")
	}
	if got := job(t, f.q, f.jobs[1].ID); got.State != "running" {
		t.Fatal("host receipt stopped uncovered sibling")
	}
	if err := f.q.EnableHost("host"); err != nil {
		t.Fatal(err)
	}
	if f.q.s.Machines["gpu0"].Disabled != "unit" {
		t.Fatal("host enable bypassed unit fence")
	}
}

func TestUnitEnableRetryAfterHeldSessionChange(t *testing.T) {
	for _, i := range []int{0, 1} {
		t.Run([]string{"saved-attempt", "idle-unit"}[i], func(t *testing.T) {
			f := newUnitDisableFixture(t, false)
			f.disable(t, i)
			if err := f.controls[i].Enable(context.Background()); err != nil {
				t.Fatal(err)
			}
			m := f.machines[i]
			previous := m.Session
			m.Session = "new-session"
			m.EnablePending = true
			if _, err := f.q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: previous}); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(API{Queue: f.q, Token: "test"}.Handler())
			defer server.Close()
			data, err := os.ReadFile(filepath.Join(f.controls[i].Dir, "enable-pending.json"))
			if err != nil {
				t.Fatal(err)
			}
			var receipt hostReceipt
			if err := json.Unmarshal(data, &receipt); err != nil {
				t.Fatal(err)
			}
			receipt.Workers[0].Queue = server.URL
			if err := saveWorkerJSON(f.controls[i].Dir, "enable-pending.json", receipt); err != nil {
				t.Fatal(err)
			}
			w := Worker{HostStateDir: f.controls[i].Dir, Machine: m, Client: Client{URL: server.URL, Token: "test"}}
			if err := w.reconcileEnable(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.q.s.Machines[m.ID].Disabled != "" || f.q.s.Machines[m.ID].Gone {
				t.Fatal("held rejoin stayed disabled/gone")
			}
			for _, release := range f.q.s.HostReleases {
				if release.Machine == m.ID {
					t.Fatal("saved release remained held")
				}
			}
		})
	}
}
