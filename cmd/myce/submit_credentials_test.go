package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestSubmitInstalledCredentials(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	const token = "submit-fixture-private-token"
	server := httptest.NewTLSServer(myce.API{Queue: q, Token: token}.Handler())
	defer server.Close()
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, name := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"} {
		t.Setenv(name, "")
	}
	joining := filepath.Join(home, "joining.json")
	hash := sha256.Sum256(server.Certificate().Raw)
	invitation := myce.Invitation{Version: 1, Token: token, Address: server.URL, Fingerprint: hex.EncodeToString(hash[:])}
	if err := myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local/share/mycelium/worker/config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(myce.ServiceConfig{Role: "worker", JoiningFile: joining})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	spec := filepath.Join(home, "job.yaml")
	if err := os.WriteFile(spec, []byte("name: submit-credentials\nproject: test\npriority: experiment\nrun:\n  command: [echo, hello]\nneeds:\n  cpu_cores: 1\n  memory_gb: 1\n  platform: cpu\n"), 0600); err != nil {
		t.Fatal(err)
	}
	out, stderr, err := captureUsageOutput(t, []string{"submit", spec})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out+stderr, token) {
		t.Fatal("submit printed credential")
	}
	snapshot, err := q.Snapshot()
	if err != nil || len(snapshot.Jobs) != 1 || snapshot.Jobs[0].ID != strings.TrimSpace(out) {
		t.Fatal("submission missing", err)
	}
	invitation.Fingerprint = strings.Repeat("0", 64)
	if err := myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
	}
	out, stderr, err = captureUsageOutput(t, []string{"submit", spec, "--config", config})
	if err == nil {
		t.Fatal("submit ignored installed TLS pin")
	}
	if strings.Contains(out+stderr+err.Error(), token) {
		t.Fatal("submit failure printed credential")
	}
}
