package myce

import (
	"context"
	"os"
	"path/filepath"

	"strings"
	"testing"
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
	for _, setting := range []string{"KillMode=control-group", "SendSIGKILL=yes"} {
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
