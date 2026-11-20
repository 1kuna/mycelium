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
		return []byte("FragmentPath=" + m.c.Service + "\nDropInPaths=" + m.dropins + "\nSlice=" + m.slice + "\nOOMScoreAdjust=" + m.score + "\nActiveState=active\nControlGroup=" + m.group + "\n"), nil
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

func TestWorkerUnitMigrationStagesAndPreservesOverrides(t *testing.T) {
	m := migrationFixture(t)
	base, _ := os.ReadFile(m.c.Service)
	if err := os.Mkdir(m.c.Service+".d", 0700); err != nil {
		t.Fatal(err)
	}
	override := filepath.Join(m.c.Service+".d", "90-operator.conf")
	text := "[Service]\nEnvironment=EXTRA=kept\nEnvironmentFile=/operator/extra.env\nCPUWeight=42\n"
	if err := os.WriteFile(override, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	m.dropins = override
	for range 2 {
		status, err := StageWorkerUnitMigration(t.Context(), m.c, m.command)
		if err != nil || status.State != "unavailable" || status.Reason != "worker_unit_migration_staged" {
			t.Fatalf("%+v %v", status, err)
		}
		if m.group != "/user.slice/app.slice/myce-worker.service" {
			t.Fatal("moved live worker")
		}
		if got, _ := os.ReadFile(m.c.Service); string(got) != string(base) {
			t.Fatal("rewrote base unit")
		}
		if got, _ := os.ReadFile(override); string(got) != text {
			t.Fatal("rewrote operator overrides/environment")
		}
	}
	data, err := os.ReadFile(filepath.Join(m.c.Service+".d", workerUnitMigrationDropIn))
	if err != nil || !strings.Contains(string(data), m.c.workerContainmentSettings()) || !strings.Contains(m.c.ServiceText(), m.c.workerContainmentSettings()) {
		t.Fatal("fresh and migrated settings differ", err)
	}
	w := Worker{ServiceConfig: &m.c}
	if got := w.unmigratedContainmentStatus(); got.Reason != "worker_unit_migration_staged" {
		t.Fatal(got)
	}
	// A restart into the configured slice is still only placement; runtime
	// MemoryMax readback (TestMemoryCeilingJointAllowance) proves available.
	m.group = "/user.slice/fixture.slice/myce-worker.service"
	status, err := StageWorkerUnitMigration(t.Context(), m.c, m.command)
	if err != nil || status.Reason != "worker_unit_migration_placement_ready" {
		t.Fatal(status, err)
	}
}

func TestWorkerUnitMigrationRefusalKeepsServing(t *testing.T) {
	for _, kind := range []string{"slice", "score", "reserved-name", "reload", "base-slice", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			m := migrationFixture(t)
			if err := os.Mkdir(m.c.Service+".d", 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(m.c.Service+".d", "90-operator.conf")
			text := "[Service]\nSlice=other.slice\n"
			switch kind {
			case "score":
				text = "[Service]\nOOMScoreAdjust=200\n"
			case "reserved-name":
				path = filepath.Join(m.c.Service+".d", workerUnitMigrationDropIn)
			case "reload":
				m.refuse = true
				text = "[Service]\nEnvironment=KEEP=yes\n"
			case "base-slice":
				path = m.c.Service
			case "symlink":
				path = filepath.Join(m.c.Service+".d", workerUnitMigrationDropIn)
				if err := os.Symlink(m.c.Service, path); err != nil {
					t.Fatal(err)
				}
			}
			if kind != "symlink" {
				if err := os.WriteFile(path, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
			}
			m.dropins = path
			before, _ := os.ReadFile(path)
			status, err := StageWorkerUnitMigration(t.Context(), m.c, m.command)
			if err == nil || status == nil || status.State != "unavailable" || status.Reason == "" || status.Next == "" {
				t.Fatal(status, err)
			}
			if got, _ := os.ReadFile(path); string(got) != string(before) {
				t.Fatal("changed conflicting override")
			}
			if m.group != "/user.slice/app.slice/myce-worker.service" || kind != "reload" && m.reloads != 0 {
				t.Fatal("refusal affected service")
			}
			w := Worker{ServiceConfig: &m.c}
			if got := w.unmigratedContainmentStatus(); got.Reason != status.Reason {
				t.Fatal("worker lost typed refusal", got)
			}
			if kind == "reload" {
				m.refuse = false
				if got, err := StageWorkerUnitMigration(t.Context(), m.c, m.command); err != nil || got.Reason != "worker_unit_migration_staged" {
					t.Fatal("partial staging cannot resume", got, err)
				}
			}
		})
	}
}
