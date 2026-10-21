package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mycelium/internal/myce"
)

func TestM4VerifiedEmptyCLIExplicitEvidence(t *testing.T) {
	root := t.TempDir()
	c, err := myce.PlanInstall("queue", root, root, root, "http://queue.invalid", "queue", myce.Detection{OS: "linux", Arch: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	q, err := myce.Open(c.DB, c.Checkpoints, myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(c)
	if err := os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	mapping := filepath.Join(root, "map.json")
	if err := os.WriteFile(mapping, []byte(`[{"from":"legacy","to":"host","device":"0"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	err = setupCommandWithProbe([]string{"install", "queue", "--config", c.Config, "--machine-map", mapping, "--drain-evidence=verified-empty", "--dry-run"}, myce.MachineProbe{OS: "linux", Arch: "amd64"}, root)
	var typed *myce.HTTPError
	if !errors.As(err, &typed) || typed.Code != "machine_drain_evidence_missing" {
		t.Fatal("CLI did not enforce explicit source evidence", err)
	}
}
