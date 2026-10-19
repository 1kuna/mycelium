package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestInstalledWorkerConditionAbsolutePath(t *testing.T) {
	for _, custom := range []bool{false, true} {
		home := t.TempDir()
		t.Setenv("MYCE_HOST_STATE_DIR", "")
		c, err := PlanInstall("worker", home, "", home, "http://queue", "worker", Detection{OS: "linux", Platform: "fake"})
		if err != nil {
			t.Fatal(err)
		}
		if custom {
			c.HostStateDir = filepath.Join(home, "host space%")
		}
		text := c.ServiceText()
		want := "ExecCondition=" + unitQuote(c.Binary) + " _host-start-check --host-state-dir " + unitQuote(c.HostStateDir)
		section := ""
		found := false
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, "[") {
				section = line
			}
			if strings.HasPrefix(line, "ExecCondition=") {
				found = true
				if section != "[Service]" || line != want || !filepath.IsAbs(c.HostStateDir) {
					t.Fatalf("condition section=%s line=%s", section, line)
				}
			}
		}
		if !found {
			t.Fatal("missing startup condition")
		}
	}
}

func TestHostRevisionInitializeRefusesLegacyAndConversion(t *testing.T) {
	for _, name := range []string{"DISABLED", "worker-old.json", ".host-conversion/IN_PROGRESS"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := InitializeHostIntent(dir, "test-host", RealClock{}); err == nil {
				t.Fatal("initialized over legacy/conversion state")
			}
		})
	}
}

func TestHostRevisionDisableWaitsForNonSystemdLaunch(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	clock := &fakeClock{now: time.Unix(1000, 0)}
	unlock, err := hostOperationLock(dir, "launch.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	pauses := 0
	spool := t.TempDir()
	if err = saveWorkerJSON(spool, "state.json", workerState{Session: "session"}); err != nil {
		t.Fatal(err)
	}
	if err = AdoptHostWorker(dir, HostWorker{State: spool, Host: "test-host", Name: "worker"}); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Observe: testHostExclusion, Dir: dir, Clock: clock, Processes: HostProcesses{
		Owned: func(workerState) (bool, error) { return false, nil }, Signal: func(workerState, syscall.Signal) error { return nil },
		Pause: func(d time.Duration) {
			pauses++
			clock.Advance(d)
			if pauses == 3 {
				unlock()
				unlock = nil
			}
		},
	}}
	_, err = h.Disable(context.Background(), "test", 0)
	if err != nil || pauses != 3 {
		t.Fatalf("launch wait pauses=%d error=%v", pauses, err)
	}
}

func TestHostRevisionCloseRequiresExplicitCAS(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	initial, err := ReadHostIntent(dir)
	if err != nil {
		t.Fatal(err)
	}
	stale := HostRevision{Revision: initial.Revision + 10, Operation: "unrelated-open-selector"}
	// Expected is an open selector and must never accidentally turn close into CAS.
	h := HostControl{Observe: testHostExclusion, Dir: dir, Expected: stale}
	_, _ = h.Disable(context.Background(), "unconditional", 0)
	current, err := ReadHostIntent(dir)
	if err != nil || current.Desired != "closed" || current.Revision != initial.Revision+1 {
		t.Fatal(current, err)
	}
	h.CloseExpected = &stale
	if _, err = h.Disable(context.Background(), "explicit CAS", 0); !errors.Is(err, ErrHostRevision) {
		t.Fatal("stale explicit close accepted", err)
	}
	h.CloseExpected = &current.HostRevision
	if _, err = changeHostIntent(dir, RealClock{}, hostIntentChange{Desired: "closed", CloseExpected: h.CloseExpected}); err != nil {
		t.Fatal(err)
	}
}

type closeOnMemoryCreate struct {
	memoryFS
	close func()
}

func (f closeOnMemoryCreate) Mkdir(path string) error { f.close(); return f.memoryFS.Mkdir(path) }

func TestHostRevisionLlamaCloseBeforeDispatch(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	c := fakeCeilings(t)
	closed := false
	c.fs = closeOnMemoryCreate{memoryFS: c.fs, close: func() {
		if closed {
			return
		}
		closed = true
		if err := closeTestHost(dir); err != nil {
			t.Fatal(err)
		}
	}}
	m := serverTestModel(t, "ready")
	m.HostMemoryGB = 1
	e := &LlamaEngine{Dir: t.TempDir(), HostStateDir: dir, Clock: RealClock{}, memoryCeilings: c}
	err := e.Load(context.Background(), m)
	if !closed || !errors.Is(err, ErrHostRevision) {
		t.Fatal("stale engine dispatched", closed, err)
	}
	if e.cmd != nil && e.cmd.Process != nil {
		t.Fatal("engine process created after close")
	}
}

func TestHostRevisionDockerCloseBeforeDispatch(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	revision := readTestHostRevision(t, dir)
	marker := filepath.Join(t.TempDir(), "called")
	binary := filepath.Join(t.TempDir(), "docker")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\ntouch '"+marker+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	closed := false
	d := &dockerAttempt{dir: t.TempDir(), timeout: time.Second, hostDir: dir, hostRevision: revision, clock: RealClock{}, binary: binary, halt: make(chan struct{}), state: dockerExecution{Identity: "attempt", Endpoint: "unix:///fake"}, persist: func(dockerExecution) error {
		if !closed {
			closed = true
			return closeTestHost(dir)
		}
		return nil
	}}
	if _, err := d.operation("create", "create"); !errors.Is(err, ErrHostRevision) {
		t.Fatal("stale daemon operation dispatched", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("daemon CLI ran", err)
	}
}

func TestHostRevisionBenchmarkCommandCustody(t *testing.T) {
	for _, service := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "service"}[service], func(t *testing.T) {
			dir := readyTestHost(t, t.TempDir())
			state := t.TempDir()
			scope := hostCommandScope{HostDir: dir, StateDir: state, Revision: readTestHostRevision(t, dir), Clock: RealClock{}}
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ready := filepath.Join(state, "ready")
			go func() {
				done <- runExtensionCommand(ctx, []string{"/bin/sh", "-c", `printf ready > "$1"; sleep 60`, "benchmark", ready}, state, map[string]string{}, io.Discard, scope)
			}()
			var records []workerState
			await(t, func() bool {
				if _, err := os.Stat(ready); err != nil {
					return false
				}
				workers, err := HostWorkers(dir)
				if err != nil {
					return false
				}
				if len(workers) != 0 {
					t.Fatal("active command registered as worker", workers)
				}
				records, err = hostProcessRecords(workers, hostCommandDirectory(dir))
				return err == nil && len(records) == 1 && records[0].PID > 0
			})
			control := HostControl{Observe: testHostExclusion, Dir: dir}
			if service {
				control.Service = &fakeHostService{}
			}
			report, err := control.Disable(context.Background(), "close benchmark", 0)
			if report.Stopped != 1 {
				t.Fatal("Disable did not find benchmark custody", report, err)
			}
			if err != nil && (runtime.GOOS != "darwin" || !strings.Contains(err.Error(), "macOS can hide process environment markers")) {
				t.Fatal(report, err)
			}
			select {
			case err = <-done:
				if err == nil {
					t.Fatal("benchmark survived close")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("benchmark group not stopped")
			}
			// A delayed benchmark command cannot bind itself to a subsequent open.
			testConfirmClose(t, dir)
			closed, err := ReadHostIntent(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = changeHostIntent(dir, RealClock{}, hostIntentChange{Desired: "open", Expected: closed.HostRevision, Settled: func(HostIntent) error { return nil }}); err != nil {
				t.Fatal(err)
			}
			if err = runExtensionCommand(context.Background(), []string{"/bin/true"}, state, nil, io.Discard, scope); !errors.Is(err, ErrHostRevision) {
				t.Fatal("stale benchmark command admitted", err)
			}
		})
	}
}
