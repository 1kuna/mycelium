package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func m4Sources(t *testing.T) ([]ServiceConfig, ServiceConfig) {
	t.Helper()
	home := t.TempDir()
	base, err := PlanInstall("worker", home, "", filepath.Join(home, "nas"), "http://queue.invalid:7331", "host", Detection{OS: "linux", Arch: "amd64", Platform: "xpu", MemoryMode: "fixed", MemoryGB: 24, ReserveGB: 2})
	if err != nil {
		t.Fatal(err)
	}
	base.HostID = "host"
	base.HostMemoryBudgetGB = 64
	base.EnvironmentFile = filepath.Join(home, "secret-reference.env")
	sources := []ServiceConfig{}
	for i, device := range []string{"0", "1"} {
		c := base
		if err := c.WorkerInstance("gpu" + device); err != nil {
			t.Fatal(err)
		}
		c.Name = "host-gpu" + device
		c.Device = device
		c.Detection.MemoryGB += float64(i)
		if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(c)
		if err := os.WriteFile(c.Config, data, 0600); err != nil {
			t.Fatal(err)
		}
		sources = append(sources, c)
	}
	return sources, base
}

func TestM4WorkerSourceUnitOwnership(t *testing.T) {
	src, dest := m4Sources(t)
	src[0].Service = filepath.Join(filepath.Dir(src[0].Service), "foreign.service")
	data, _ := json.Marshal(src[0])
	if err := os.WriteFile(src[0].Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := PlanMachineInstall(dest.Config, []string{src[0].Config}, nil); err == nil {
		t.Fatal("non-Mycelium source unit accepted")
	}
}

func m4Queue(t *testing.T) (*Queue, *fakeClock, []MachineRename) {
	t.Helper()
	q, clock := fixture(t)
	for _, name := range []string{"host-gpu0", "host-gpu1", "spark"} {
		host := "host"
		if name == "spark" {
			host = "spark"
		}
		m := machine(name, "fake", host)
		check(t, q, m, nil)
	}
	for _, host := range []string{"host", "spark"} {
		if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
			t.Fatal(err)
		}
	}
	return q, clock, []MachineRename{{"host-gpu0", "host", "0"}, {"host-gpu1", "host", "1"}, {"spark", "spark", "0"}}
}

func TestM4QueueRefusesQueuedNamesAndConflictingFences(t *testing.T) {
	for _, state := range []string{"queued", "paused", "waiting"} {
		t.Run(state, func(t *testing.T) {
			q, clock, mapping := m4Queue(t)
			s := spec("experiment")
			s.AllowedMachines = []string{"host-gpu1"}
			j := submit(t, q, s)
			if state == "paused" {
				pauseForTest(t, q, j.ID, nil)
			} else if state == "waiting" {
				if err := q.update(func() error { q.touchJob(j.ID).State = state; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			q = reopenCancellationQueue(t, q, clock)
			for _, dry := range []bool{true, false} {
				_, err := q.ConvertMachines(mapping, ModelConfig{}, dry)
				var typed *QueuedRetiredMachinesError
				if !errors.As(err, &typed) || !reflect.DeepEqual(typed.Jobs, []string{j.ID}) || !reflect.DeepEqual(typed.Machines, []string{"host-gpu1"}) {
					t.Fatal(err)
				}
			}
			if got := job(t, q, j.ID); got.State != state || !reflect.DeepEqual(got.Spec, j.Spec) || q.s.Machines["host-gpu1"].Replacement != nil {
				t.Fatal("refusal changed job intent or retired its target", got)
			}
		})
	}
	t.Run("project-partial", func(t *testing.T) {
		q, _, mapping := m4Queue(t)
		if err := q.ConfigureProjects(map[string]ProjectPolicy{"p": {Weight: 1, AllowedMachines: []string{"host-gpu0"}}}); err != nil {
			t.Fatal(err)
		}
		_, err := q.ConvertMachines(mapping, ModelConfig{}, false)
		if err == nil || !strings.Contains(err.Error(), "machine_migration_conflict") {
			t.Fatal(err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		q, _, mapping := m4Queue(t)
		_, err := q.ConvertMachines(mapping[:1], ModelConfig{}, false)
		if err == nil || !strings.Contains(err.Error(), "machine_migration_incomplete") {
			t.Fatal(err)
		}
	})
	t.Run("stale", func(t *testing.T) {
		q, clock, mapping := m4Queue(t)
		var allowance time.Duration
		for _, m := range q.s.Machines {
			allowance = max(allowance, q.workerArrivalAllowance(m))
		}
		clock.Advance(allowance)
		_, err := q.ConvertMachines(mapping, ModelConfig{}, false)
		if err == nil {
			t.Fatal("stale source accepted")
		}
	})
}

func m4InstallFixture(t *testing.T) ([]ServiceConfig, ServiceConfig, string, *installMatrixManager, func(context.Context, string, ...string) ([]byte, error)) {
	return m4InstallFixtureFor(t, false)
}
func m4InstallFixtureFor(t *testing.T, spark bool) ([]ServiceConfig, ServiceConfig, string, *installMatrixManager, func(context.Context, string, ...string) ([]byte, error)) {
	t.Helper()
	src, dest := m4Sources(t)
	if spark {
		src = src[:1]
		src[0].Name = "spark"
		src[0].HostID = "spark"
		src[0].Device = "GPU-spark-uuid"
		src[0].Service = dest.Service
		src[0].Detection.Platform = "cuda"
		src[0].Detection.MemoryMode = "unified"
		src[0].Detection.MemoryGB = 0
		src[0].Detection.ReserveGB = 16
	}
	var stopped atomic.Bool
	for i := range src {
		src[i].AllowDeviceless = true
		// The manager advertises this synthetic NAS mount; preflight must find
		// its mount directory already present rather than create local fallback.
		if err := os.MkdirAll(src[i].NAS, 0700); err != nil {
			t.Fatal(err)
		}
		if err := PrepareStateRoots(src[i].State, src[i].ModelCache, src[i].EnvironmentCache); err != nil {
			t.Fatal(err)
		}
		for path, data := range map[string][]byte{src[i].Binary: []byte("old-worker"), src[i].GPUEnvironmentFile(): []byte("ZE_ENABLE_PCI_ID_DEVICE_ORDER=\"1\"\n")} {
			if err := os.WriteFile(path, data, 0700); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.MkdirAll(filepath.Dir(src[i].Service), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(src[i].Service, []byte(src[i].ServiceText()), 0600); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/inspection" {
			machines := []*Machine{}
			for _, c := range src {
				m := &Machine{ID: c.Name, HostID: c.HostID, Gone: true, Replacement: &MachineReplacement{Machine: c.HostID, Device: c.Device}}
				if spark {
					m.Replacement = nil
					m.Gone = stopped.Load()
				}
				machines = append(machines, m)
			}
			if spark {
				machines = append(machines, &Machine{ID: "host-gpu0", HostID: "host", Replacement: &MachineReplacement{Machine: "host", Device: "0"}}, &Machine{ID: "host-gpu1", HostID: "host", Replacement: &MachineReplacement{Machine: "host", Device: "1"}})
			}
			json.NewEncoder(w).Encode(Snapshot{Machines: machines})
			return
		}
		if r.URL.Path != "/api/hosts/"+src[0].HostID+"/maintenance" {
			t.Error("bad path", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(HostMaintenanceStatus{Host: src[0].HostID, Policy: HostMaintenance{Mode: "disabled"}, Healthy: true, Quiescent: true})
	}))
	t.Cleanup(server.Close)
	t.Setenv("MYCE_TOKEN", "synthetic-local-test")
	paths := []string{}
	for i := range src {
		src[i].Queue = server.URL
		c := src[i]
		data, _ := json.Marshal(c)
		if err := os.WriteFile(c.Config, data, 0600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, c.Config)
	}
	dest, err := PlanMachineInstall(dest.Config, paths, nil)
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "new-myce")
	if err := os.WriteFile(binary, []byte("new-worker"), 0700); err != nil {
		t.Fatal(err)
	}
	manager := &installMatrixManager{t: t, c: dest, states: map[string]installUnitState{}, cached: map[string][]byte{}}
	links := map[string]string{}
	for _, c := range src {
		unit := filepath.Base(c.Service)
		manager.states[unit] = installUnitState{enabled: "enabled", active: true}
		manager.cached[unit] = manager.read(c.Service)
		link := filepath.Join(filepath.Dir(c.Service), "default.target.wants", unit)
		os.MkdirAll(filepath.Dir(link), 0700)
		if err := os.Symlink("../"+unit, link); err != nil {
			t.Fatal(err)
		}
		links[unit] = link
	}
	run := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "systemctl" && args[1] == "disable" && args[2] == "--now" {
			unit := args[len(args)-1]
			stopped.Store(true)
			s := manager.states[unit]
			s.active = false
			manager.states[unit] = s
		}
		out, err := manager.run(ctx, name, args...)
		if name == "systemctl" && err == nil {
			action, unit := args[1], args[len(args)-1]
			if action == "daemon-reload" {
				for _, c := range src {
					n := filepath.Base(c.Service)
					manager.cached[n] = manager.read(c.Service)
					s := manager.states[n]
					if manager.cached[n] == nil {
						s.enabled = "not-found"
					} else if s.enabled == "not-found" {
						s.enabled = "disabled"
					}
					manager.states[n] = s
				}
			}
			if path := links[unit]; path != "" {
				if action == "disable" {
					if e := os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
						t.Fatal(e)
					}
				}
				if action == "enable" {
					if _, e := os.Lstat(path); errors.Is(e, os.ErrNotExist) {
						if e = os.Symlink("../"+unit, path); e != nil {
							t.Fatal(e)
						}
					}
				}
			}
		}
		return out, err
	}
	return src, dest, binary, manager, run
}
func TestM4WorkerConversionDryRunIdempotenceEnableSetRollback(t *testing.T) {
	src, c, binary, m, run := m4InstallFixture(t)
	if err := ConvertWorkerInstall(context.Background(), c, binary, true, run); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(machineJournal(c)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("dry-run wrote journal", err)
	}
	if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err != nil {
		t.Fatal(err)
	}
	if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err != nil {
		t.Fatal("idempotent", err)
	}
	paths := []string{src[0].Config, src[1].Config}
	if _, err := PlanMachineInstall(c.Config, paths, nil); err != nil {
		t.Fatal("idempotent planning", err)
	}
	for _, old := range src {
		unit := filepath.Base(old.Service)
		if s := m.states[unit]; s.active || s.enabled == "enabled" {
			t.Fatal("legacy active/enabled", s)
		}
		for _, path := range []string{old.Config, old.Service, filepath.Join(filepath.Dir(old.Service), "default.target.wants", unit)} {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("persistent enable set survived", path, err)
			}
		}
		for _, path := range []string{old.State, old.ModelCache, old.EnvironmentCache} {
			if _, err := os.Stat(path); err != nil {
				t.Fatal("lost root", err)
			}
		}
	}
	got, err := ReadServiceConfig(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.MachineDevices) != 2 || got.ModelCache != src[0].ModelCache || got.HostMemoryBudgetGB != 64 {
		t.Fatal("saved conversion", got)
	}
	if err := RollbackMachineInstall(context.Background(), c.Config, false, run); err != nil {
		t.Fatal(err)
	}
	for _, old := range src {
		if _, err := ReadServiceConfig(old.Config); err != nil {
			t.Fatal(err)
		}
		s := m.states[filepath.Base(old.Service)]
		if !s.active || s.enabled != "enabled" {
			t.Fatal("source activity not restored", s)
		}
	}
	if m.states[filepath.Base(c.Service)].active {
		t.Fatal("both generations active")
	}
}

func m4OfflineQueue(t *testing.T) (ServiceConfig, *fakeClock, []MachineRename) {
	t.Helper()
	q, clock, mapping := m4Queue(t)
	var seq int
	var dbPath, dbFile string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &dbFile, &dbPath); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	c, err := PlanInstall("queue", home, "", home, "http://queue.invalid", "queue", Detection{OS: "linux", Arch: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	c.DB = dbPath
	c.Checkpoints = q.root
	c.ProjectConfig = filepath.Join(home, "projects.yaml")
	c.HostConfig = filepath.Join(home, "hosts.yaml")
	if err = q.ConfigureProjects(map[string]ProjectPolicy{"project": {Weight: 1, AllowedMachines: []string{"host-gpu0", "host-gpu1", "spark"}}}); err != nil {
		t.Fatal(err)
	}
	if err = q.ConfigureHosts(map[string]HostPolicy{"host": {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{c.ProjectConfig: []byte("project: {weight: 1, allowed_machines: [host-gpu0, host-gpu1, spark]}\n"), c.HostConfig: []byte("hosts:\n  host: {production_hold: true}\n")} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(c)
	if err := os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	return c, clock, mapping
}
func TestM4QueueInstallRestartAndRollbackKeepsNewIntent(t *testing.T) {
	c, clock, mapping := m4OfflineQueue(t)
	runner := func(context.Context, string, ...string) ([]byte, error) { return nil, nil }
	// Preview leaves both state and YAML byte-for-byte unchanged.
	original, err := os.ReadFile(c.ProjectConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := ConvertQueueInstall(context.Background(), c, mapping, true, false, clock, runner); err != nil {
		t.Fatal(err)
	}
	preview, _ := os.ReadFile(c.ProjectConfig)
	if !reflect.DeepEqual(original, preview) {
		t.Fatal("preview published config")
	}
	for i := 0; i < 2; i++ {
		if err := ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, runner); err != nil {
			t.Fatal("conversion/retry", err)
		}
	}
	reopened, err := Open(c.DB, c.Checkpoints, clock)
	if err != nil {
		t.Fatal(err)
	}
	policies, err := ReadProjectPolicies(c.ProjectConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.ConfigureProjects(policies); err != nil {
		t.Fatal("queue restart policy mismatch", err)
	}
	hosts, err := ReadHostPolicies(c.HostConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.ConfigureHosts(hosts); err != nil {
		t.Fatal(err)
	}
	if reopened.s.HostMaintenance["host"].Mode != "disabled" || reopened.s.Machines["host-gpu0"].Replacement == nil {
		t.Fatal("restart lost maintenance/retirement")
	}
	j := submit(t, reopened, spec("experiment"))
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ConvertQueueInstall(context.Background(), c, nil, false, true, clock, runner); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(c.DB, c.Checkpoints, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.s.Jobs[j.ID] == nil || restored.s.Machines["host-gpu1"].Replacement != nil {
		t.Fatal("rollback lost current intent/history")
	}
	final, _ := os.ReadFile(c.ProjectConfig)
	if !reflect.DeepEqual(original, final) {
		t.Fatal("rollback did not restore exact YAML")
	}
}

func TestM4LegacyBesideInventoryRefused(t *testing.T) {
	q, _ := fixture(t)
	old := machine("host-gpu0", "fake", "host")
	check(t, q, old, nil)
	modern := machine("host", "fake", "host")
	modern.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake0", Selector: "0", BudgetGB: 8, FreeMemoryGB: 8}}
	_, err := q.Checkin(Checkin{Machine: modern})
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Code != "machine_worker_conflict" {
		t.Fatal("mixed worker generations accepted", err)
	}

	// Retire the idle legacy fixture before testing the reverse registration order.
	if err := q.update(func() error { delete(q.s.Machines, old.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	{
		modern := machine("host", "fake", "host")
		modern.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake0", Selector: "0", BudgetGB: 8, FreeMemoryGB: 8}}
		check(t, q, modern, nil)
		_, err := q.Checkin(Checkin{Machine: machine("host-gpu1", "fake", "host")})
		var typed *HTTPError
		if !errors.As(err, &typed) || typed.Code != "machine_worker_conflict" {
			t.Fatal("legacy joined beside inventory", err)
		}

	}
}

func TestM4RollbackPreservesPinEdits(t *testing.T) {
	q, _, mapping := m4Queue(t)
	q.s.Pins = []ModelPin{{Project: "removed", Model: "m", Machine: "host-gpu0"}, {Project: "kept", Model: "m", Machine: "host-gpu1"}}
	if _, err := q.ConvertMachines(mapping, ModelConfig{}, false); err != nil {
		t.Fatal(err)
	}
	q.s.Pins = []ModelPin{q.s.Pins[1], {Project: "new", Model: "m", Machine: "host", Device: "0"}}
	q.s.Pins[0].PreemptionCount = 7
	if err := q.RollbackMachines(false); err != nil {
		t.Fatal(err)
	}
	if len(q.s.Pins) != 2 || q.s.Pins[0].Project != "kept" || q.s.Pins[0].Machine != "host-gpu1" || q.s.Pins[0].PreemptionCount != 7 || q.s.Pins[1].Machine != "host-gpu0" {
		t.Fatal("rollback discarded accepted pin intent", q.s.Pins)
	}
}
func TestM4RollbackRefusesUnrepresentableNewPin(t *testing.T) {
	q, _, mapping := m4Queue(t)
	if _, err := q.ConvertMachines(mapping, ModelConfig{}, false); err != nil {
		t.Fatal(err)
	}
	q.s.Pins = []ModelPin{{Project: "new", Model: "m", Machine: "host"}}
	if err := q.RollbackMachines(false); err == nil {
		t.Fatal("host-wide pin silently broadened or discarded")
	}
	if q.s.MachineConversion.RolledBack || len(q.s.Pins) != 1 {
		t.Fatal("refusal mutated state")
	}

	if err := q.update(func() error { q.s.Pins = nil; return nil }); err != nil {
		t.Fatal(err)
	}
	{
		s := spec("experiment")
		s.AllowedMachines = []string{"host"}
		s.Resumable = true
		j := submit(t, q, s)
		for _, state := range []string{"queued", "cancelled"} {
			if err := q.update(func() error { q.s.Jobs[j.ID].State = state; return nil }); err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(q.s)
			for _, dry := range []bool{true, false} {
				err := q.RollbackMachines(dry)
				var typed *QueuedRetiredMachinesError
				if !errors.As(err, &typed) || !reflect.DeepEqual(typed.Jobs, []string{j.ID}) {
					t.Errorf("rollback lost machine-era intent: %v", err)
				}
			}
			after, _ := json.Marshal(q.s)
			if !reflect.DeepEqual(before, after) {
				t.Error("refused rollback mutated state")
			}
		}
	}
}

func TestM4QueuePolicyFileDriftRefused(t *testing.T) {
	for _, phase := range []string{"before", "rollback"} {
		t.Run(phase, func(t *testing.T) {
			c, clock, mapping := m4OfflineQueue(t)
			run := func(context.Context, string, ...string) ([]byte, error) { return nil, nil }
			if phase != "before" {
				if err := ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, run); err != nil {
					t.Fatal(err)
				}
			}
			changed := []byte("project: {weight: 1, allowed_machines: [other-host]}\n")
			if err := os.WriteFile(c.ProjectConfig, changed, 0600); err != nil {
				t.Fatal(err)
			}
			err := ConvertQueueInstall(context.Background(), c, mapping, false, phase == "rollback", clock, run)
			if err == nil || !strings.Contains(err.Error(), "machine_migration_conflict") {
				t.Fatal("unknown policy overwritten", err)
			}
			after, _ := os.ReadFile(c.ProjectConfig)
			if !reflect.DeepEqual(changed, after) {
				t.Fatal("changed fence overwritten")
			}
		})
	}
}

func TestM4QueuePublicationInterruption(t *testing.T) {
	for _, op := range []string{"rename", "directory-fsync"} {
		t.Run(op, func(t *testing.T) {
			c, clock, mapping := m4OfflineQueue(t)
			run := func(context.Context, string, ...string) ([]byte, error) { return nil, nil }
			files := installFiles{before: func(operation, path string) error {
				if operation == op && path == c.ProjectConfig {
					return errors.New("injected publication interruption")
				}
				return nil
			}}
			if err := convertQueueInstall(context.Background(), c, mapping, false, false, clock, run, files); err == nil {
				t.Fatal("injection missed")
			}
			if q, err := Open(c.DB, c.Checkpoints, clock); err == nil {
				q.Close()
				t.Fatal("queue opened partial policy publication")
			}
			if err := ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, run); err != nil {
				t.Fatal("retry", err)
			}
			q, err := Open(c.DB, c.Checkpoints, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			if q.s.HostMaintenance["host"].Mode != "disabled" || q.s.Machines["host-gpu0"].Replacement == nil {
				t.Fatal("recovery lost maintenance/retirement")
			}
		})
	}
}

func TestM4QueuedPinReferencesRetiredMachine(t *testing.T) {
	q, _, mapping := m4Queue(t)
	q.s.Pins = []ModelPin{{Project: "test", Model: "model", Machine: "host-gpu1"}}
	s := spec("experiment")
	s.Project = "test"
	s.Needs.Model = "model"
	// Accepted before the maintenance window; it must be cancelled/resubmitted.
	j := submit(t, q, s)
	_, err := q.ConvertMachines(mapping, ModelConfig{}, false)
	var typed *QueuedRetiredMachinesError
	if !errors.As(err, &typed) || len(typed.Jobs) != 1 || typed.Jobs[0] != j.ID {
		t.Fatal("resolved pin reference ignored", err)
	}
}

func TestM4RetiredResultsKeepExactHistory(t *testing.T) {
	q, _, mapping := m4Queue(t)
	if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.AllowedMachines = []string{"host-gpu0"}
	j := submit(t, q, s)
	reply := check(t, q, machine("host-gpu0", "fake", "host"), nil)
	if reply.Job == nil || reply.Job.ID != j.ID {
		t.Fatal("legacy attempt missing")
	}
	result := Result{Execution: Execution{ID: j.ID, Attempt: reply.Job.Attempt}, Machine: "host-gpu0", Session: "session-host-gpu0", Success: true}
	if err := q.Complete(result); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ConvertMachines(mapping, ModelConfig{}, false); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(result); err != nil && !errors.Is(err, ErrStale) {
		t.Fatal("late result", err)
	}
	result.Success = false
	result.ReleaseOnly = true
	if err := q.Complete(result); err != nil {
		t.Fatal("late exact release", err)
	}
	got := job(t, q, j.ID)
	if got.State != "succeeded" || got.Machine != "host-gpu0" || got.Session != result.Session || !got.ReleaseConfirmed || q.s.Machines["host-gpu0"].Replacement == nil {
		t.Fatal("retirement rewrote custody/history", got)
	}
}

func TestM4SparkLegacyCatalogReconnect(t *testing.T) {
	q, _, mapping := m4Queue(t)
	models := ModelConfig{Models: []Model{{Name: "m", AllowedMachines: []string{"host-gpu1", "spark"}, Device: "1"}}}
	// Both cards are allowed so the shared catalog rewrites without widening.
	models.Models[0].AllowedMachines = []string{"host-gpu0", "host-gpu1", "spark"}
	models.Models[0].Device = ""
	p, err := q.ConvertMachines(mapping, models, false)
	if err != nil {
		t.Fatal(err)
	}
	q.models = p.ConvertedModels
	legacy := machine("spark", "fake", "spark")
	legacy.ModelURL = "http://legacy.invalid"
	legacy.ModelConfigHash = modelConfigHash(models)
	if _, err := q.Checkin(Checkin{Machine: legacy}); err != nil {
		t.Fatal("maintained exact legacy Spark reconnect refused", err)
	}
	if q.s.Machines["spark"].ModelURL != "" {
		t.Fatal("old catalog offered a model endpoint")
	}
	unknown := legacy
	unknown.Session = "unrecorded-session"
	if _, err := q.Checkin(Checkin{Machine: unknown}); err == nil {
		t.Fatal("unknown legacy session accepted")
	}
	modern := legacy
	modern.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "spark-gpu", Selector: "GPU-spark-uuid", BudgetGB: 8, FreeMemoryGB: 8}}
	if _, err := q.Checkin(Checkin{Machine: modern}); err == nil {
		t.Fatal("new machine accepted old catalog")
	}
	if _, err := q.ChangeHostMaintenance("spark", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Checkin(Checkin{Machine: legacy}); err == nil {
		t.Fatal("legacy catalog grace survived maintenance")
	}
}
