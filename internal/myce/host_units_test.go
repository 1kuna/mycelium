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

func TestWorkerUnitAllowlist(t *testing.T) {
	for _, unit := range []string{"myce-worker.service", "myce-worker@gpu1.service", "myce-worker@accept123.service"} {
		if !ValidWorkerUnit(unit) {
			t.Errorf("rejected %s", unit)
		}
	}
	for _, unit := range []string{"myce-spark-worker.service", "verso.service", "myce-worker@.service", "myce-worker@../bad.service", "myce-worker@gpu 1.service", "myce-worker@a\\x2fb.service"} {
		if ValidWorkerUnit(unit) {
			t.Errorf("accepted %s", unit)
		}
	}
}

func TestWorkerUnitsRegistryScope(t *testing.T) {
	dir := t.TempDir()
	for _, unit := range []string{"myce-worker@gpu0.service", "myce-worker@gpu1.service", "verso.service"} {
		if err := AdoptHostWorker(dir, HostWorker{State: t.TempDir(), Unit: unit}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := WorkerUnits(dir, "")
	if err != nil || !reflect.DeepEqual(got, []string{"myce-worker@gpu0.service", "myce-worker@gpu1.service"}) {
		t.Fatal(got, err)
	}
	got, err = WorkerUnits(dir, "myce-worker@gpu1.service")
	if err != nil || !reflect.DeepEqual(got, []string{"myce-worker@gpu1.service"}) {
		t.Fatal(got, err)
	}
	for _, unit := range []string{"verso.service", "myce-worker@unknown.service"} {
		if _, err = WorkerUnits(dir, unit); err == nil {
			t.Fatal("unknown/unsafe accepted", unit)
		}
	}
}

type rejectingUnit struct{ fakeHostService }

func (*rejectingUnit) Stop(context.Context, time.Duration) error { return os.ErrPermission }

func TestDisableMultipleWorkerUnits(t *testing.T) {
	for _, failed := range []bool{false, true} {
		dir := t.TempDir()
		second := HostService(&fakeHostService{})
		if failed {
			second = &rejectingUnit{}
		}
		h := HostControl{Observe: testHostExclusion, Dir: dir, Services: map[string]HostService{"myce-worker@gpu0.service": &fakeHostService{}, "myce-worker@gpu1.service": second}}
		// A failure in one unit must not manufacture a receipt for all units.
		h.Timeout = 100000000
		ensureTestHost(t, h.Dir, h.Clock)
		r, err := h.Disable(context.Background(), "test", 0)
		if (err != nil) != failed || r.Confirmed == failed || len(r.Units) != 2 {
			t.Fatal(r, err)
		}
		if failed {
			if _, e := os.Stat(filepath.Join(dir, "disable-receipt.json")); !os.IsNotExist(e) {
				t.Fatal("unconfirmed receipt")
			}
		}
	}
}

func TestInstanceInstallerPlan(t *testing.T) {
	c, err := PlanInstall("worker", t.TempDir(), "", "/tmp/local", "http://127.0.0.1:1", "gpu1", Detection{OS: "linux", Platform: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.WorkerInstance("gpu1"); err != nil {
		t.Fatal(err)
	}
	if filepath.Base(c.Service) != "myce-worker@gpu1.service" || filepath.Base(filepath.Dir(c.Config)) != "worker-gpu1" {
		t.Fatal(c)
	}
	if err = c.WorkerInstance("../bad"); err == nil {
		t.Fatal("unsafe instance")
	}
}

func TestInstanceSystemdCgroup(t *testing.T) {
	unit := "myce-worker@gpu1.service"
	stopped := false
	s := &SystemdHostService{Unit: unit, Command: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) > 1 && args[1] == "stop" {
			stopped = true
		}
		state := "active"
		group := "/user.slice/" + unit
		if stopped {
			state = "inactive"
			group = ""
		}
		return []byte("LoadState=loaded\nKillMode=control-group\nSendSIGKILL=yes\nActiveState=" + state + "\nControlGroup=" + group + "\n"), nil
	}, ReadFile: func(string) ([]byte, error) { return nil, os.ErrNotExist }}
	if err := s.Stop(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if empty, err := s.Empty(context.Background()); err != nil || !empty {
		t.Fatal(empty, err)
	}
}

func TestInstanceRegistryUsesSharedHostScope(t *testing.T) {
	dir := t.TempDir()
	unit := "myce-worker@gpu1.service"
	scoped := dir
	w := Worker{Unit: unit, HostStateDir: scoped, Machine: Machine{ID: "gpu1", HostID: "host"}, EnvironmentRoot: t.TempDir()}
	s := &workerStore{dir: t.TempDir()}
	ensureTestHost(t, w.HostStateDir, w.Clock, "host")
	if err := w.registerHost(s); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{dir} {
		got, err := WorkerUnits(d, unit)
		if err != nil || len(got) != 1 {
			t.Fatal(got, err)
		}
	}
}

func TestInstanceDockerUsesOnlySavedAttemptLabels(t *testing.T) {
	w, s, clock, root := dockerFixture(t)
	scoped := w.HostStateDir
	os.MkdirAll(scoped, 0700)
	for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned", "mode": "term"} {
		os.WriteFile(filepath.Join(root, name), []byte(value), 0600)
	}
	// Released custody still supplies ownership if its container was restarted later.
	saveWorkerJSON(s.dir, "owned.docker.json", dockerExecution{Identity: "owned", Endpoint: "unix:///var/run/docker.sock", Container: "id1", Released: true})
	if err := AdoptHostWorker(scoped, HostWorker{State: s.dir, Unit: "myce-worker@gpu1.service"}); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Observe: testHostExclusion, Dir: scoped, Service: &fakeHostService{}, Clock: clock, Docker: true}
	ensureTestHost(t, h.Dir, h.Clock)
	if r, err := h.Disable(context.Background(), "gpu1", 0); err != nil || !r.Confirmed {
		t.Fatal(r, err)
	}
	calls, err := os.ReadFile(filepath.Join(root, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(calls), "\n") {
		if strings.HasPrefix(line, "ps ") && !strings.Contains(line, "label=myce.") {
			t.Fatal("broad Docker scan", line)
		}
	}
}

func TestInstallInstanceTargetsItsOwnUnit(t *testing.T) {
	home := t.TempDir()
	c, err := PlanInstall("worker", home, "", home, "http://127.0.0.1:1", "gpu1", Detection{OS: "linux", Platform: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.WorkerInstance("gpu1"); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(home, "source-myce")
	os.WriteFile(executable, []byte("fixture"), 0700)
	var calls []string
	command := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		calls = append(calls, strings.Join(args, " "))
		return nil, nil
	}
	c.AllowDeviceless = true
	if err = Install(context.Background(), c, executable, "private-fixture", command); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(calls, "\n"), "enable --now myce-worker@gpu1.service") {
		t.Fatal(calls)
	}
	text, err := os.ReadFile(c.Service)
	if err != nil {
		t.Fatal(err)
	}
	for _, setting := range []string{"KillMode=control-group", "SendSIGKILL=yes", "_host-start-check"} {
		if !strings.Contains(string(text), setting) {
			t.Fatal(setting, string(text))
		}
	}
	c.AllowDeviceless = true
	if err = Install(context.Background(), c, executable, "private-fixture", command); err != nil {
		t.Fatal("upgrade", err)
	}
	if err = Uninstall(context.Background(), c, false, command); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(calls, "\n"), "disable --now myce-worker@gpu1.service") {
		t.Fatal(calls)
	}
}

func TestSystemdGraceWithoutMutableTimeoutProperty(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	killed := false
	unit := "myce-worker@gpu1.service"
	s := &SystemdHostService{Unit: unit, Clock: clock, Command: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[1] == "set-property" {
			return nil, os.ErrPermission
		}
		if args[1] == "kill" {
			killed = true
		}
		state := "deactivating"
		group := "/user.slice/" + unit
		if killed {
			state = "inactive"
			group = ""
		}
		return []byte("LoadState=loaded\nKillMode=control-group\nSendSIGKILL=yes\nActiveState=" + state + "\nControlGroup=" + group + "\n"), nil
	}, ReadFile: func(string) ([]byte, error) { return nil, os.ErrNotExist }}
	if err := s.Stop(context.Background(), 3*time.Second); err != nil {
		t.Fatal("normal disable must work without mutable TimeoutStopUSec", err)
	}
	if empty, err := s.Empty(context.Background()); err != nil || empty || killed {
		t.Fatal("killed before grace", empty, err)
	}
	clock.now = clock.now.Add(3 * time.Second)
	if _, err := s.Empty(context.Background()); err != nil || !killed {
		t.Fatal("grace did not kill cgroup", err)
	}
	if empty, err := s.Empty(context.Background()); err != nil || !empty {
		t.Fatal("kill not confirmed", empty, err)
	}
}

func TestSystemdRechecksTransitionalMissingCgroup(t *testing.T) {
	shows := 0
	stopped := false
	s := &SystemdHostService{Unit: "myce-worker@gpu1.service", Command: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[1] == "stop" {
			stopped = true
		}
		state := "active"
		if args[1] == "show" {
			shows++
		}
		if shows > 1 {
			state = "failed"
		}
		return []byte("LoadState=loaded\nKillMode=control-group\nSendSIGKILL=yes\nActiveState=" + state + "\nControlGroup=\n"), nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Stop(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if !stopped || shows < 2 {
		t.Fatal("transitional snapshot not rechecked", shows, stopped)
	}
	if empty, err := s.Empty(ctx); err != nil || !empty {
		t.Fatal(empty, err)
	}
}

func TestSystemdPersistentMissingCgroupFailsClosed(t *testing.T) {
	stopped := false
	s := &SystemdHostService{Unit: "myce-worker@gpu1.service", Command: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[1] == "stop" || args[1] == "kill" {
			stopped = true
		}
		return []byte("LoadState=loaded\nKillMode=control-group\nSendSIGKILL=yes\nActiveState=active\nControlGroup=\n"), nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Stop(ctx, 0); err == nil || stopped {
		t.Fatal("inconsistent unit authorized signaling", err, stopped)
	}
}

func TestInstallerRejectsUnrelatedWorkerUnit(t *testing.T) {
	for _, action := range []string{"install", "uninstall"} {
		t.Run(action, func(t *testing.T) {
			home := t.TempDir()
			c, _ := PlanInstall("worker", home, "", home, "http://127.0.0.1:1", "test", Detection{OS: "linux", Platform: "fake"})
			c.Service = filepath.Join(home, "verso.service")
			executable := filepath.Join(home, "myce-source")
			os.WriteFile(executable, []byte("fixture"), 0700)
			called := false
			command := func(context.Context, string, ...string) ([]byte, error) { called = true; return nil, nil }
			var err error
			if action == "install" {
				err = Install(context.Background(), c, executable, "fixture-token", command)
			} else {
				err = Uninstall(context.Background(), c, false, command)
			}
			if err == nil || called {
				t.Fatal("installer operated on unrelated unit", err, called)
			}
		})
	}
}
