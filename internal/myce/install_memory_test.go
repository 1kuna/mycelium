package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallQueueMemoryDefaultUpgrade(t *testing.T) {
	home := t.TempDir()
	c, err := PlanInstall("queue", home, "", home, "http://queue.invalid", "queue", Detection{OS: "linux", Arch: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	if c.MemoryMax != "" || !strings.Contains(c.ServiceText(), "MemoryMax=\n") {
		t.Fatal("queue has a default hard memory cap")
	}
	if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	save := func() ServiceConfig {
		t.Helper()
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(c.Config, data, 0600); err != nil {
			t.Fatal(err)
		}
		got, err := ReadServiceConfig(c.Config)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	c.MemoryMax = "512M"
	if got := save(); got.MemoryMax != "" {
		t.Fatal("upgrade retained exact obsolete default")
	}
	c.MemoryMax = "2G"
	if got := save(); got.MemoryMax != "2G" {
		t.Fatal("custom memory cap changed")
	}
	c.Role, c.MemoryMax = "worker", "512M"
	if got := save(); got.MemoryMax != "512M" {
		t.Fatal("worker memory setting changed")
	}
	data, err := os.ReadFile("../../deploy/myce-queue.service")
	if err != nil || !strings.Contains(string(data), "MemoryMax=\n") {
		t.Fatal("manual queue template caps memory", err)
	}
}
