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
func TestM4WorkerPlanPreservesSplitSettings(t *testing.T) {
	src, dest := m4Sources(t)
	got, err := PlanMachineInstall(dest.Config, []string{src[1].Config, src[0].Config}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "host" || got.HostID != "host" || got.Device != "0,1" || len(got.MachineDevices) != 2 {
		t.Fatalf("machine identity: %+v", got)
	}
	for _, field := range []string{"Queue", "NAS", "Checkpoints", "ModelRoot", "EnvironmentRoot", "ModelCache", "EnvironmentCache", "ModelCacheBytes", "EnvironmentCacheBytes", "HostMemoryBudgetGB", "ReserveHostMemoryGB", "CPUReserveCores", "EnvironmentFile"} {
		if !reflect.DeepEqual(reflect.ValueOf(got).FieldByName(field).Interface(), reflect.ValueOf(src[0]).FieldByName(field).Interface()) {
			t.Errorf("lost %s", field)
		}
	}
	for i, d := range got.MachineDevices {
		if d.Selector != src[i].Device || d.Detection != src[i].Detection {
			t.Fatalf("lost fixed capacity/reserve: %+v", d)
		}
	}
}
func TestM4WorkerPlanConflictAndMissingSource(t *testing.T) {
	src, dest := m4Sources(t)
	src[1].Queue = "http://different.invalid"
	data, _ := json.Marshal(src[1])
	os.WriteFile(src[1].Config, data, 0600)
	_, err := PlanMachineInstall(dest.Config, []string{src[0].Config, src[1].Config}, nil)
	if err == nil || !strings.Contains(err.Error(), "machine_migration_conflict") || !strings.Contains(err.Error(), "queue") {
		t.Fatal(err)
	}
	_, err = PlanMachineInstall(dest.Config, []string{src[0].Config, src[1].Config}, map[string]bool{"queue": true})
	if err != nil {
		t.Fatal(err)
	}

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
func TestM4QueueRewriteRetirementAndRollback(t *testing.T) {
	q, _, mapping := m4Queue(t)
	if err := q.update(func() error {
		q.s.Policies = map[string]ProjectPolicy{"project": {Weight: 1, AllowedMachines: []string{"host-gpu0", "host-gpu1", "spark"}}}
		q.s.Pins = []ModelPin{{Project: "project", Model: "model", Machine: "host-gpu1"}}
		q.s.Hosts = map[string]*HostState{"host-gpu0": {Policy: HostPolicy{ProductionHold: true}}, "host-gpu1": {Policy: HostPolicy{ProductionHold: true}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	models := ModelConfig{Models: []Model{{Name: "model", AllowedMachines: []string{"host-gpu1"}}}}
	before, _ := json.Marshal(q.s)
	if _, err := q.ConvertMachines(mapping, models, true); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(q.s)
	if string(before) != string(after) {
		t.Fatal("dry run mutated state")
	}
	plan, err := q.ConvertMachines(mapping, models, false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.s.Policies["project"].AllowedMachines, []string{"host", "spark"}) || q.s.Pins[0].Machine != "host" || q.s.Pins[0].Device != "1" || !q.s.Hosts["host"].Policy.ProductionHold {
		t.Fatal("policy rewrite", q.s.Policies, q.s.Pins, q.s.Hosts)
	}
	if plan.ConvertedModels.Models[0].Device != "1" || !reflect.DeepEqual(plan.ConvertedModels.Models[0].AllowedMachines, []string{"host"}) {
		t.Fatal("server fence", plan.ConvertedModels)
	}
	for _, r := range mapping[:2] {
		if m := q.s.Machines[r.From]; m.Replacement == nil || m.Replacement.Device != r.Device || !m.Gone || m.Session != "session-"+r.From {
			t.Fatal("history lost", m)
		}
	}
	s := spec("experiment")
	s.AllowedMachines = []string{"host-gpu0"}
	var typed *HTTPError
	if _, err = q.Submit(s); !errors.As(err, &typed) || typed.Code != "machine_renamed" {
		t.Fatal("rename guidance", err)
	}
	if _, err = q.ConvertMachines(mapping, models, false); err != nil {
		t.Fatal("idempotence", err)
	}
	// New accepted intent must survive rollback; no DB snapshot restoration.
	j := submit(t, q, spec("experiment"))
	if err = q.RollbackMachines(false); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[j.ID] == nil || q.s.Machines["host-gpu0"].Replacement != nil || q.s.Pins[0].Machine != "host-gpu1" {
		t.Fatal("rollback lost intent or policy")
	}
	check(t, q, machine("host-gpu0", "fake", "host"), nil)
}
func TestM4QueueRefusesQueuedNamesAndConflictingFences(t *testing.T) {
	t.Run("queued", func(t *testing.T) {
		q, _, mapping := m4Queue(t)
		s := spec("experiment")
		s.AllowedMachines = []string{"host-gpu1"}
		j := submit(t, q, s)
		_, err := q.ConvertMachines(mapping, ModelConfig{}, false)
		var typed *QueuedRetiredMachinesError
		if !errors.As(err, &typed) || !reflect.DeepEqual(typed.Jobs, []string{j.ID}) || !reflect.DeepEqual(typed.Machines, []string{"host-gpu1"}) {
			t.Fatal(err)
		}
	})
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
		clock.Advance(6 * time.Second)
		_, err := q.ConvertMachines(mapping, ModelConfig{}, false)
		if err == nil {
			t.Fatal("stale source accepted")
		}
	})
}
func TestM4DeployGate(t *testing.T) {
	src, dest := m4Sources(t)
	if err := os.MkdirAll(filepath.Dir(src[0].Service), 0700); err != nil {
		t.Fatal(err)
	}
	for _, c := range src {
		if err := os.WriteFile(c.Service, []byte("legacy"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := CheckMachineInstallGate(dest); err == nil || !strings.Contains(err.Error(), "machine_worker_conflict") {
		t.Fatal(err)
	}
	got, err := PlanMachineInstall(dest.Config, []string{src[0].Config}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = CheckMachineInstallGate(got); err == nil || !strings.Contains(err.Error(), "machine_migration_incomplete") {
		t.Fatal(err)
	}
	if err = CheckMachineInstallGate(src[0]); err == nil {
		t.Fatal("split worker upgrade accepted")
	}
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
		if r.URL.Path == "/api/status" {
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
func TestM4WorkerConversionMutationFailures(t *testing.T) {
	for _, point := range []string{"retire", "write", "fsync", "rename", "directory-fsync", "activation"} {
		t.Run(point, func(t *testing.T) {
			src, c, binary, m, run := m4InstallFixture(t)
			fired := false
			files := installFiles{before: func(op, path string) error {
				if !fired && op == point {
					fired = true
					return errors.New("injected migration mutation")
				}
				return nil
			}}
			runner := func(ctx context.Context, name string, args ...string) ([]byte, error) {
				out, err := run(ctx, name, args...)
				if !fired && point == "activation" && name == "systemctl" && args[1] == "enable" && args[len(args)-1] == filepath.Base(c.Service) {
					fired = true
					return out, errors.New("injected activation")
				}
				return out, err
			}
			if err := convertWorkerInstall(context.Background(), c, binary, false, runner, files); err == nil || !fired {
				t.Fatal("fault did not refuse", err)
			}
			for _, old := range src {
				if _, err := ReadServiceConfig(old.Config); err != nil {
					t.Fatal("source not restored", err)
				}
				if s := m.states[filepath.Base(old.Service)]; !s.active || s.enabled != "enabled" {
					t.Fatal("source registration/activity not restored", s)
				}
			}
			if m.states[filepath.Base(c.Service)].active {
				t.Fatal("new worker survived failed conversion")
			}
		})
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
}

func TestM4PerDeviceFixedCapacityAtRuntime(t *testing.T) {
	w := Worker{Machine: Machine{GPU: "cuda", FreeMemoryGB: 24, ReservedMemoryGB: 2}, DeviceSettings: []DeviceInstall{
		{Selector: "GPU-one", Detection: Detection{Platform: "cuda", MemoryMode: "fixed", MemoryGB: 24, ReserveGB: 2}},
		{Selector: "GPU-two", Detection: Detection{Platform: "cuda", MemoryMode: "fixed", MemoryGB: 31, ReserveGB: 3}},
	}}
	probe := MachineProbe{OS: "linux", Arch: "amd64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "nvidia-smi" && args[0] == "--query-gpu=uuid,driver_version,pci.bus_id" {
			return []byte("GPU-one, driver\nGPU-two, driver\n"), nil
		}
		return nil, errors.New("fixed capacity must not require telemetry")
	}}
	devices, err := w.localInventoryWithProbe(context.Background(), probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 2 {
		t.Fatal(devices)
	}
	for i, want := range []float64{22, 28} {
		d := devices[i]
		if d.MemoryMode != "fixed" || d.BudgetGB != want || d.FreeMemoryGB != want || d.ReserveMemoryGB != float64(i+2) {
			t.Fatalf("device %d lost explicit fixed capacity/reserve: %+v", i, d)
		}
	}
}

func TestM4SparkPlanKeepsSingleCUDAUUID(t *testing.T) {
	sources, dest := m4Sources(t)
	source := sources[0]
	source.HostID = "spark"
	source.Name = "spark"
	source.Device = "GPU-spark-uuid"
	source.Detection.Platform = "cuda"
	source.Detection.MemoryMode = "unified"
	source.Detection.MemoryGB = 0
	source.Detection.ReserveGB = 16
	source.Service = dest.Service
	data, _ := json.Marshal(source)
	if err := os.WriteFile(source.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := PlanMachineInstall(dest.Config, []string{source.Config}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "spark" || got.Device != "GPU-spark-uuid" || len(got.MachineDevices) != 1 || got.MachineDevices[0].Selector != source.Device || got.MachineDevices[0].Detection != source.Detection {
		t.Fatal("Spark UUID/capacity lost", got)
	}
	if got.ModelCache != source.ModelCache || got.EnvironmentFile != source.EnvironmentFile || got.HostMemoryBudgetGB != source.HostMemoryBudgetGB {
		t.Fatal("Spark config lost")
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
}
func TestM4InventoryBesideLegacyRefused(t *testing.T) {
	q, _ := fixture(t)
	modern := machine("host", "fake", "host")
	modern.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake0", Selector: "0", BudgetGB: 8, FreeMemoryGB: 8}}
	check(t, q, modern, nil)
	_, err := q.Checkin(Checkin{Machine: machine("host-gpu1", "fake", "host")})
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Code != "machine_worker_conflict" {
		t.Fatal("legacy joined beside inventory", err)
	}
}

func TestM4QueuePolicyFileDriftRefused(t *testing.T) {
	for _, phase := range []string{"before", "retry", "rollback"} {
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
	for _, op := range []string{"write", "fsync", "rename", "directory-fsync"} {
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
func TestM4PerDeviceMeasuredReserveAtRuntime(t *testing.T) {
	w := Worker{Machine: Machine{GPU: "cuda"}, DeviceSettings: []DeviceInstall{{Selector: "GPU-one", Detection: Detection{Platform: "cuda", MemoryMode: "dedicated", MemoryGB: 24, ReserveGB: 5}}}}
	probe := MachineProbe{OS: "linux", Arch: "amd64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "nvidia-smi" {
			switch args[0] {
			case "--query-gpu=uuid,driver_version,pci.bus_id":
				return []byte("GPU-one, driver\n"), nil
			case "--query-gpu=name,memory.total,pci.bus_id":
				return []byte("CUDA, 32768\n"), nil
			case "--query-gpu=memory.free":
				return []byte("32768\n"), nil
			}
		}
		return nil, errors.New("unexpected probe")
	}}
	d, err := w.localInventoryWithProbe(context.Background(), probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 1 || d[0].ReserveMemoryGB != 5 || d[0].BudgetGB != 19 || d[0].MemoryMode != "dedicated" {
		t.Fatal("lost measured capacity mode/reserve", d)
	}
}

func TestM4SparkInstallRollback(t *testing.T) {
	src, c, binary, m, run := m4InstallFixtureFor(t, true)
	c.ModelCatalog = filepath.Join(t.TempDir(), "models.yaml")
	src[0].ModelCatalog = c.ModelCatalog
	sourceData, _ := json.Marshal(src[0])
	if err := os.WriteFile(src[0].Config, sourceData, 0600); err != nil {
		t.Fatal(err)
	}
	originalModels := []byte("models:\n- {name: model, engine: fake, path: model.gguf, memory_gb: 1, platforms: [cuda], allowed_machines: [host-gpu1]}\n")
	if err := os.WriteFile(c.ModelCatalog, originalModels, 0600); err != nil {
		t.Fatal(err)
	}
	original, _ := os.ReadFile(src[0].Config)
	if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err != nil {
		t.Fatal(err)
	}
	installed, err := ReadServiceConfig(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := ReadModels(c.ModelCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(catalog.Models[0].AllowedMachines, []string{"host"}) || catalog.Models[0].Device != "1" {
		t.Fatal("worker-local catalog fence not converted", catalog)
	}
	if installed.Name != "spark" || installed.Device != "GPU-spark-uuid" || installed.MachineDevices[0].Detection.ReserveGB != 16 || installed.EnvironmentFile != src[0].EnvironmentFile {
		t.Fatal("Spark settings lost", installed)
	}
	if err := RollbackMachineInstall(context.Background(), c.Config, false, run); err != nil {
		t.Fatal(err)
	}
	restoredModels, _ := os.ReadFile(c.ModelCatalog)
	if !reflect.DeepEqual(restoredModels, originalModels) {
		t.Fatal("worker-local catalog not restored")
	}
	after, _ := os.ReadFile(src[0].Config)
	if !reflect.DeepEqual(original, after) || !m.states[filepath.Base(src[0].Service)].active {
		t.Fatal("Spark installation not restored")
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
