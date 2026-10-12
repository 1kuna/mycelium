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
		h := HostControl{Dir: dir, Services: map[string]HostService{"myce-worker@gpu0.service": &fakeHostService{}, "myce-worker@gpu1.service": second}}
		// A failure in one unit must not manufacture a receipt for all units.
		h.Timeout = 100000000
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

func TestInstanceFenceAlsoChecksHost(t *testing.T) {
	dir := t.TempDir()
	one, _ := WorkerUnitDirectory(dir, "myce-worker@gpu1.service")
	two, _ := WorkerUnitDirectory(dir, "myce-worker@gpu0.service")
	os.MkdirAll(one, 0700)
	os.MkdirAll(two, 0700)
	saveWorkerJSON(one, "DISABLED", map[string]any{"generation": 1, "reason": "gpu1"})
	if reason, err := HostDisabled(two); err != nil || reason != "" {
		t.Fatal(reason, err)
	}
	if reason, err := HostDisabled(one); err != nil || reason != "gpu1" {
		t.Fatal(reason, err)
	}
	saveWorkerJSON(dir, "DISABLED", map[string]any{"generation": 2, "reason": "host"})
	if reason, err := HostDisabled(two); err != nil || reason != "host" {
		t.Fatal(reason, err)
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

func TestInstanceRegistryRegistersBothScopes(t *testing.T) {
	dir := t.TempDir()
	unit := "myce-worker@gpu1.service"
	scoped, _ := WorkerUnitDirectory(dir, unit)
	w := Worker{Unit: unit, HostStateDir: scoped, Machine: Machine{ID: "gpu1", HostID: "host"}, EnvironmentRoot: t.TempDir()}
	s := &workerStore{dir: t.TempDir()}
	if err := w.registerHost(s); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{dir, scoped} {
		got, err := WorkerUnits(d, unit)
		if err != nil || len(got) != 1 {
			t.Fatal(got, err)
		}
	}
}

func TestInstanceDockerUsesOnlySavedAttemptLabels(t *testing.T) {
	w, s, clock, root := dockerFixture(t)
	scoped, _ := WorkerUnitDirectory(w.HostStateDir, "myce-worker@gpu1.service")
	os.MkdirAll(scoped, 0700)
	for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned", "mode": "term"} {
		os.WriteFile(filepath.Join(root, name), []byte(value), 0600)
	}
	// Released custody still supplies ownership if its container was restarted later.
	saveWorkerJSON(s.dir, "owned.docker.json", dockerExecution{Identity: "owned", Endpoint: "unix:///var/run/docker.sock", Container: "id1", Released: true})
	if err := AdoptHostWorker(scoped, HostWorker{State: s.dir, Unit: "myce-worker@gpu1.service"}); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Dir: scoped, Service: &fakeHostService{}, Clock: clock, Docker: true}
	if r, err := h.Disable(context.Background(), "gpu1", 0); err != nil || !r.Confirmed {
		t.Fatal(r, err)
	}
	calls, err := os.ReadFile(filepath.Join(root, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(calls), "\n") {
		if strings.HasPrefix(line, "ps ") && !strings.Contains(line, "label=myce.attempt=owned") {
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
	for _, setting := range []string{"KillMode=control-group", "SendSIGKILL=yes", "myce-worker@gpu1.service/DISABLED"} {
		if !strings.Contains(string(text), setting) {
			t.Fatal(setting, string(text))
		}
	}
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

func TestInstalledWorkerConditionAbsolutePath(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}, Service: "/tmp/myce-worker@gpu1.service", HostStateDir: "/tmp/myce host"}
	text := c.ServiceText()
	for _, path := range []string{c.HostStateDir + "/DISABLED", c.HostStateDir + "/units/myce-worker@gpu1.service/DISABLED"} {
		if !strings.Contains(text, "ConditionPathExists=!"+path+"\n") {
			t.Fatal("systemd condition must begin with the absolute path, without quotes", text)
		}
	}
}
