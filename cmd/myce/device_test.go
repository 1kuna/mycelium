package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDeviceClearQuarantine(t *testing.T) {
	root := t.TempDir()
	for name, data := range map[string]string{
		"devices.json":           `[{"id":"0","platform":"xpu","identity":"fixture-gpu","selector":"0"}]`,
		"device-quarantine.json": `{"boot_id":"boot-a","devices":{"xpu:fixture-gpu":{"reason":"device_fault_xe","event":"fault-a"}}}`,
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := deviceCommand([]string{"clear-quarantine", "--state-dir", root, "--device", "0"}, &out); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Device    string `json:"device"`
		Requested bool   `json:"clear_requested"`
	}
	if err := json.Unmarshal(out.Bytes(), &reply); err != nil || !reply.Requested || reply.Device != "0" {
		t.Fatal("clear request receipt", out.String(), err)
	}
	// A clear request must not directly modify the worker's durable quarantine.
	data, err := os.ReadFile(filepath.Join(root, "device-quarantine.json"))
	if err != nil || !bytes.Contains(data, []byte("fault-a")) {
		t.Fatal("command bypassed worker maintenance", err)
	}
	if err := deviceCommand([]string{"clear-quarantine", "--state-dir", root, "--device", "1"}, &out); err == nil {
		t.Fatal("accepted unknown device")
	}
}
