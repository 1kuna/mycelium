package myce

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMachineInstallRefusesRetiredSplitUnits(t *testing.T) {
	root := t.TempDir()
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}, Service: filepath.Join(root, "myce-worker.service")}
	if err := CheckMachineInstallGate(c); err != nil {
		t.Fatal(err)
	}
	for _, unit := range []string{"myce-worker@gpu0.service", filepath.Join("default.target.wants", "myce-worker@gpu0.service")} {
		path := filepath.Join(root, unit)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		var typed *HTTPError
		if err := CheckMachineInstallGate(c); !errors.As(err, &typed) || typed.Code != "machine_worker_conflict" {
			t.Fatal("retired service accepted", err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	c.Service = filepath.Join(root, "myce-worker@gpu0.service")
	if err := CheckMachineInstallGate(c); err == nil {
		t.Fatal("retired instance accepted")
	}
}
