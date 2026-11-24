package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type migrationManager struct {
	c                            ServiceConfig
	slice, score, group, dropins string
	reloads                      int
	refuse                       bool
	properties                   string
}

func migrationFixture(t *testing.T) *migrationManager {
	t.Helper()
	dir := t.TempDir()
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}, Service: filepath.Join(dir, "myce-worker.service"), State: filepath.Join(dir, "state"), Slice: "fixture.slice"}
	if err := os.WriteFile(c.Service, []byte("[Service]\nSlice=app.slice\nOOMScoreAdjust=200\nEnvironment=OPERATOR=kept\nEnvironmentFile=/operator/runtime.env\nExecStart=/operator/myce service\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return &migrationManager{c: c, slice: "app.slice", score: "200", group: "/user.slice/app.slice/myce-worker.service"}
}

func (m *migrationManager) command(ctx context.Context, name string, args ...string) ([]byte, error) {
	if name != "systemctl" || len(args) < 2 || args[0] != "--user" {
		return nil, errors.New("unexpected manager")
	}
	switch args[1] {
	case "show":
		return []byte("FragmentPath=" + m.c.Service + "\nDropInPaths=" + m.dropins + "\nSlice=" + m.slice + "\nOOMScoreAdjust=" + m.score + "\nActiveState=active\nControlGroup=" + m.group + "\n" + m.properties), nil
	case "daemon-reload":
		m.reloads++
		if m.refuse {
			return nil, os.ErrPermission
		}
		m.slice, m.score = m.c.workerSlice(), "0"
		m.dropins = strings.TrimSpace(m.dropins + " " + filepath.Join(m.c.Service+".d", workerUnitMigrationDropIn))
		return nil, nil
	default:
		return nil, errors.New("migration tried to change running work")
	}
}

func TestInstallWorkerUnitLegacyCatalogPlan(t *testing.T) {
	t.Run("darwin-without-systemd-properties", func(t *testing.T) {
		c := ServiceConfig{Role: "worker", Detection: Detection{OS: "darwin"}, Service: filepath.Join(t.TempDir(), "worker.plist")}
		const plist = "synthetic existing launchd definition"
		if err := os.WriteFile(c.Service, []byte(plist), 0600); err != nil {
			t.Fatal(err)
		}
		plan, err := InstallUnitPlan(t.Context(), c, func(context.Context, string, ...string) ([]byte, error) {
			t.Fatal("Darwin plan queried Linux service-manager properties")
			return nil, nil
		})
		if err != nil || plan != "" {
			t.Fatalf("Darwin plan required Linux properties: %q %v", plan, err)
		}
		if got, err := os.ReadFile(c.Service); err != nil || string(got) != plist {
			t.Fatal("Darwin plan changed the launchd definition", err)
		}
	})
	c, source := preserveWorkerFixture(t)
	if err := Install(t.Context(), c, source, "synthetic", fakeInstallManager(c)); err != nil {
		t.Fatal(err)
	}
	m := migrationFixture(t)
	m.c = c
	m.properties = "KillMode=process\nDelegateControllers=cpu memory\nRestartUSec=3000000us\n"
	legacy := strings.ReplaceAll(strings.ReplaceAll(c.ServiceText(), "Slice="+c.workerSlice(), "Slice=app.slice"), "OOMScoreAdjust=0", "OOMScoreAdjust=200")
	legacy = strings.ReplaceAll(legacy, "KillMode=mixed", "KillMode=control-group")
	if err := os.WriteFile(c.Service, []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	beforeConfig, _ := os.ReadFile(c.Config)
	c.ModelCatalog = filepath.Join(c.NAS, "replacement-catalog.yaml")
	for _, staged := range []bool{false, true} {
		if staged {
			if _, err := StageWorkerUnitMigration(t.Context(), c, m.command); err != nil {
				t.Fatal(err)
			}
		}
		plan, err := InstallUnitPlan(t.Context(), c, m.command)
		if !errors.Is(err, ErrWorkerUnitMigrationRequired) || !strings.Contains(err.Error(), "myce migrate worker-unit") || !strings.Contains(err.Error(), "normal myce fleet upgrade") {
			t.Fatal(plan, err)
		}
		if !staged && (!strings.Contains(plan, `Slice: "app.slice" -> "myce.slice"`) || !strings.Contains(plan, `OOMScoreAdjust: "200" -> "0"`)) {
			t.Fatal(plan)
		}
		if !strings.Contains(plan, `KillMode: "process" -> "mixed"`) || !strings.Contains(plan, "Active ControlGroup") {
			t.Fatal(plan)
		}
		if strings.Contains(plan, "[Service] Delegate:") || strings.Contains(plan, "[Service] RestartSec:") {
			t.Fatal("equivalent effective settings reported as changes", plan)
		}
		if err := Install(t.Context(), c, source, "", m.command); !errors.Is(err, ErrWorkerUnitMigrationRequired) {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(c.Config); string(got) != string(beforeConfig) {
			t.Fatal("refusal changed config")
		}
		if got, _ := os.ReadFile(c.Service); string(got) != legacy {
			t.Fatal("refusal changed unit")
		}
		if m.group != "/user.slice/app.slice/myce-worker.service" {
			t.Fatal("moved active worker")
		}
	}
	m.group = "/user.slice/myce.slice/myce-worker.service"
	if _, err := InstallUnitPlan(t.Context(), c, m.command); err != nil {
		t.Fatal("activated migration remains refused", err)
	}
}
