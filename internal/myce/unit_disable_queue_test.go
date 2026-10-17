package myce

import (
	"context"

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
