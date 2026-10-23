package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestMachineInstallEffectiveHostAllowance(t *testing.T) {
	sources, dest := m4Sources(t)
	hostDir := filepath.Join(t.TempDir(), "legacy-host")
	if err := os.MkdirAll(hostDir, 0700); err != nil {
		t.Fatal(err)
	}
	state := []byte(`{"version":1,"desired":"open","memory_allowance_bytes":25769803776}`)
	statePath := filepath.Join(hostDir, "host-state.json")
	if err := os.WriteFile(statePath, state, 0600); err != nil {
		t.Fatal(err)
	}
	paths := []string{}
	for _, s := range sources {
		s.HostMemoryBudgetGB = 3
		raw, _ := json.Marshal(s)
		var legacy map[string]any
		json.Unmarshal(raw, &legacy)
		legacy["host_state_dir"] = hostDir
		raw, _ = json.Marshal(legacy)
		if err := os.WriteFile(s.Config, raw, 0600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, s.Config)
	}
	got, err := PlanMachineInstall(dest.Config, paths, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.HostMemoryBudgetGB != 24 {
		t.Fatalf("effective allowance = %g GiB; want persisted 24, not stale flag 3 or a sum", got.HostMemoryBudgetGB)
	}
	q, _ := fixture(t)
	m := machine("converted", "fake", got.HostID)
	m.HostMemoryBudgetGB = got.HostMemoryBudgetGB
	if err = q.registerMaintenanceHost(&m); err != nil {
		t.Fatal(err)
	}
	m.HostMemoryBudgetGB = 3
	if err = q.registerMaintenanceHost(&m); err != nil {
		t.Fatal(err)
	}
	if q.s.HostMaintenance[got.HostID].MemoryBudget.GiB != 24 {
		t.Fatal("queue did not seed the effective allowance once")
	}
	if data, err := os.ReadFile(statePath); err != nil || string(data) != string(state) {
		t.Fatal("conversion changed legacy state", err)
	}
	// A legacy local-control config with only an unqualified flag must refuse.
	if err = os.WriteFile(statePath, []byte(`{"version":1,"desired":"open"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = PlanMachineInstall(dest.Config, paths, nil); err == nil {
		t.Fatal("missing effective allowance accepted")
	}
	// An explicit operator budget remains the supported override.
	if _, err = PlanMachineInstall(dest.Config, paths, map[string]bool{"host-memory-budget-gb": true}); err != nil {
		t.Fatal(err)
	}
}
