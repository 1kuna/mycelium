package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestM4R2CrashRollback(t *testing.T) {
	for _, phase := range []string{"prepared", "retired", "published", "complete"} {
		t.Run(phase, func(t *testing.T) {
			src, c, binary, m, run := m4InstallFixture(t)
			crash := func() { panic("simulated process death") }
			files := installFiles{before: func(op, path string) error {
				if phase == "prepared" && op == "retire" || phase == "retired" && op == "write" && path == c.GPUEnvironmentFile() || phase == "published" && op == "directory-fsync" && path == c.Service {
					crash()
				}
				return nil
			}}
			func() {
				defer func() {
					if r := recover(); r != nil && r != "simulated process death" {
						panic(r)
					}
				}()
				if err := convertWorkerInstall(context.Background(), c, binary, false, run, files); err != nil {
					t.Fatal(err)
				}
			}()
			paths := []string{src[0].Config, src[1].Config}
			if _, err := PlanMachineInstall(c.Config, paths, nil); err != nil {
				t.Errorf("journal plan at %s: %v", phase, err)
			}
			if err := RollbackMachineInstall(context.Background(), c.Config, false, run); err != nil {
				t.Fatalf("rollback at %s: %v", phase, err)
			}
			for _, s := range src {
				if _, err := ReadServiceConfig(s.Config); err != nil {
					t.Fatal(err)
				}
				if st := m.states[filepath.Base(s.Service)]; !st.active || st.enabled != "enabled" {
					t.Fatal("source not restored", st)
				}
			}
		})
	}
}

func TestM4R2CatalogRollbackOwnership(t *testing.T) {
	for _, converted := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged-shared", true: "changed-shared"}[converted], func(t *testing.T) {
			_, c, binary, m, run := m4InstallFixture(t)
			c.ModelCatalog = filepath.Join(t.TempDir(), "models.yaml")
			names := "host"
			if converted {
				names = "host-gpu1"
			}
			original := []byte("models:\n- {name: m, engine: fake, path: m.gguf, memory_gb: 1, platforms: [fake], allowed_machines: [" + names + "]}\n")
			if err := os.WriteFile(c.ModelCatalog, original, 0640); err != nil {
				t.Fatal(err)
			}
			if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err != nil {
				t.Fatal(err)
			}
			if st, _ := os.Stat(c.ModelCatalog); st.Mode().Perm() != 0640 {
				t.Errorf("catalog mode changed to %o", st.Mode().Perm())
			}
			changed := append(append([]byte{}, original...), []byte("# another operator's update\n")...)
			if err := os.WriteFile(c.ModelCatalog, changed, 0640); err != nil {
				t.Fatal(err)
			}
			err := RollbackMachineInstall(context.Background(), c.Config, false, run)
			if converted {
				var typed *HTTPError
				if !errors.As(err, &typed) || typed.Code != "machine_migration_conflict" || !strings.Contains(typed.Message, "models.yaml") {
					t.Errorf("expected typed catalog diff, got %v", err)
				}
				if m.states[filepath.Base(c.Service)].enabled != "enabled" {
					t.Error("conflict mutated manager")
				}
			} else if err != nil {
				t.Error(err)
			}
			after, _ := os.ReadFile(c.ModelCatalog)
			if !reflect.DeepEqual(after, changed) {
				t.Error("shared catalog update overwritten")
			}
		})
	}
}

func TestM4R2RollbackQueuedMachineName(t *testing.T) {
	for _, state := range []string{"queued", "failed", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			q, _, mapping := m4Queue(t)
			if _, err := q.ConvertMachines(mapping, ModelConfig{}, false); err != nil {
				t.Fatal(err)
			}
			s := spec("experiment")
			s.AllowedMachines = []string{"host"}
			s.Resumable = true
			j := submit(t, q, s)
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
		})
	}
}

func TestM4R2ProductionSpoolCustody(t *testing.T) {
	// Literal formats from the 08816cf writers, not full heartbeat workerState.
	cases := map[string]string{
		"attempt-command.json": `{"schema_version":3,"session":"","fresh":false,"pid":1,"identity":"command","offset":0}`,
		"command.result.json":  `{"schema_version":3,"id":"job","attempt":1,"machine":"host-gpu0","session":"s","success":true}`,
		"command.docker.json":  `{"schema_version":3,"identity":"command","endpoint":"unix:///var/run/docker.sock","pending":true,"exit_code":0}`,
		"engine-model.json":    `{"schema_version":3,"session":"","fresh":false,"pid":1,"identity":"engine","offset":0}`,
		"operator-stop.json":   `{"schema_version":3,"session":"s","fresh":false,"offset":0,"job":{"id":"job","attempt":1}}`,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if err := settledMigrationSpool(dir); err == nil {
				t.Fatal("unresolved production custody accepted")
			}
		})
	}
}

func TestM4R2LegacyRegistryRetirement(t *testing.T) {
	src, c, binary, _, run := m4InstallFixture(t)
	dir := filepath.Join(c.WorkerHome, ".local", "share", "myce")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	paths := []string{}
	originals := [][]byte{}
	for i, s := range src {
		data, _ := json.Marshal(map[string]any{"schema_version": 3, "config": s.Config, "unit": filepath.Base(s.Service), "state_dir": s.State, "host_id": s.HostID, "name": s.Name, "queue": s.Queue})
		path := filepath.Join(dir, "worker-"+string(rune('0'+i))+".json")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
		originals = append(originals, data)
	}
	if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Error("production host open can resurrect retired worker", err)
		}
	}
	if err := RollbackMachineInstall(context.Background(), c.Config, false, run); err != nil {
		t.Fatal(err)
	}
	for i, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil || !reflect.DeepEqual(data, originals[i]) {
			t.Fatal("registry not restored", err)
		}
	}
}

func TestM4R2QueuePublicationModes(t *testing.T) {
	c, clock, mapping := m4OfflineQueue(t)
	if err := os.Chmod(c.ProjectConfig, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(c.HostConfig, 0640); err != nil {
		t.Fatal(err)
	}
	run := func(context.Context, string, ...string) ([]byte, error) { return nil, nil }
	if err := ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, run); err != nil {
		t.Fatal(err)
	}
	for p, mode := range map[string]os.FileMode{c.ProjectConfig: 0644, c.HostConfig: 0640} {
		st, err := os.Stat(p)
		if err != nil || st.Mode().Perm() != mode {
			t.Errorf("policy mode: %v %v", st, err)
		}
	}
}

func TestM4R2RollbackDryRunChecks(t *testing.T) {
	_, c, binary, m, run := m4InstallFixture(t)
	if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err != nil {
		t.Fatal(err)
	}
	c.Queue = "http://127.0.0.1:1"
	data, _ := os.ReadFile(machineJournal(c))
	var j workerMigration
	if err := json.Unmarshal(data, &j); err != nil {
		t.Fatal(err)
	}
	j.Config.Queue = c.Queue
	if err := saveMigration(machineJournal(c), j); err != nil {
		t.Fatal(err)
	}
	before := m.states[filepath.Base(c.Service)]
	if err := RollbackMachineInstall(context.Background(), c.Config, true, run); err == nil {
		t.Error("dry-run skipped maintenance check")
	}
	if !reflect.DeepEqual(before, m.states[filepath.Base(c.Service)]) {
		t.Fatal("dry-run mutated manager")
	}
}

func TestM4R2ForwardRollbackForwardSpool(t *testing.T) {
	_, c, binary, _, run := m4InstallFixture(t)
	if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err != nil {
		t.Fatal(err)
	}
	if err := PrepareStateRoots(c.State); err != nil {
		t.Fatal(err)
	}
	// Keep the machine's settled session and spool across a topology rollback.
	path := filepath.Join(c.State, "state.json")
	data := []byte(`{"schema_version":3,"session":"machine-session","fresh":false,"offset":0}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := RollbackMachineInstall(context.Background(), c.Config, false, run); err != nil {
		t.Fatal(err)
	}
	pending := filepath.Join(c.State, "pending.result.json")
	if err := os.WriteFile(pending, []byte(`{"schema_version":3,"id":"pending","attempt":1,"machine":"host","session":"machine-session"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err == nil {
		t.Fatal("second conversion accepted an unresolved result in the reused machine spool")
	}
	if err := os.Remove(pending); err != nil {
		t.Fatal(err)
	} // owned synthetic custody fixture
	if err := ConvertWorkerInstall(context.Background(), c, binary, false, run); err != nil {
		t.Fatal("second conversion", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(after, data) {
		t.Fatal("machine spool not preserved", err)
	}
}

func TestM4R2CatalogEnvAbsentFromSQLite(t *testing.T) {
	q, _, mapping := m4Queue(t)
	models := ModelConfig{Models: []Model{{Name: "m", AllowedMachines: []string{"host-gpu1"}, Env: map[string]string{"TEST": "synthetic-private-env-marker"}, SecretEnv: map[string]string{"KEY": "synthetic-private-secret-ref"}}}}
	if _, err := q.ConvertMachines(mapping, models, false); err != nil {
		t.Fatal(err)
	}
	var data []byte
	if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&data); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "synthetic-private") || strings.Contains(string(data), `"env"`) || strings.Contains(string(data), `"secret_env"`) {
		t.Fatal("catalog environment persisted in queue state")
	}
}

func TestM4R2ProductionDrainSurvivesQueueUpgrade(t *testing.T) {
	q, clock, mapping := m4Queue(t)
	dbPath := q.DBPathForM4Test(t)
	// Shape is QueueHostIntent as written by 57ed4e6, after exact discharge.
	var raw map[string]any
	data, _ := json.Marshal(q.s)
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	delete(raw, "host_maintenance")
	raw["host_intents"] = map[string]any{"host": map[string]any{"revision": 3, "operation_id": "old-close", "desired": "closed", "boot_id": "synthetic-boot", "closed_leaving": map[string]string{"host-gpu0": "session-host-gpu0", "host-gpu1": "session-host-gpu1"}}}
	machines := raw["machines"].(map[string]any)
	for _, name := range []string{"host-gpu0", "host-gpu1"} {
		machines[name].(map[string]any)["gone"] = true
	}
	data, _ = json.Marshal(raw)
	if _, err := q.db.Exec("UPDATE queue SET data=? WHERE id=1", data); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(dbPath, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.s.HostMaintenance["host"].Mode != "disabled" {
		t.Fatal("production close lost during queue upgrade")
	}
	// A settled production receipt replaces freshness of the stopped sources.
	if _, err := restored.ConvertMachines(mapping[:2], ModelConfig{}, false); err != nil {
		t.Fatal("settled production sources cannot convert", err)
	}
}

func (q *Queue) DBPathForM4Test(t *testing.T) string {
	t.Helper()
	var seq int
	var name, path string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}
