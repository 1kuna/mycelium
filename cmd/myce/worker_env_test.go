package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestWorkerEnvPruneCLI(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	server := httptest.NewServer(myce.API{Queue: q, Token: "prune-test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "prune-test")
	root := t.TempDir()
	if err := myce.PrepareStateRoots(root); err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("a", 64)
	path := filepath.Join(root, key)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(path, "ready.json")
	if err := os.WriteFile(ready, []byte(`{"state":"ready","key":"`+key+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	at := (myce.RealClock{}).Now().Add(-48 * time.Hour)
	if err := os.Chtimes(ready, at, at); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "config.json")
	b, _ := json.Marshal(myce.ServiceConfig{Role: "worker", EnvironmentCache: root})
	if err := os.WriteFile(config, b, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"env", "prune", "--config", config, "--queue", server.URL}
	var out bytes.Buffer
	if err := workerEnvCommand(context.Background(), args, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), key) || !strings.Contains(out.String(), "SIZE_BYTES REFERENCED") || !strings.Contains(out.String(), "apply=false reclaimed_bytes=0") {
		t.Fatal(out.String())
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("default invocation removed environment", err)
	}
	out.Reset()
	if err := workerEnvCommand(context.Background(), append(args, "--apply"), &out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("apply did not remove environment", out.String(), err)
	}
	if !strings.Contains(out.String(), "apply=true") {
		t.Fatal(out.String())
	}
}
