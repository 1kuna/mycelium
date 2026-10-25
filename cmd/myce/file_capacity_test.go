package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestProfileFileCapacityRefusal(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "input"), []byte("input"), 0600); err != nil {
		t.Fatal(err)
	}
	profile := `name: capacity
project: test
class: experiment
source: {directory: source}
run: {command: ["true"]}
check: ["true"]
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: ` + runtime.GOOS + `, arch: ` + runtime.GOARCH + `, platform: cpu}]
`
	path := filepath.Join(root, "profile.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	q, err := myce.Open(filepath.Join(state, "queue.db"), filepath.Join(state, "checkpoints"), &bindClock{now: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.ConfigureFileStore(filepath.Join(state, "files"), 1, true); err != nil {
		t.Fatal(err)
	}
	uploads := 0
	handler := myce.API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/files/uploads/") {
			uploads++
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, devTestBinary(t), "run", "--profile", path, "--queue", server.URL, "--key", "capacity-refusal")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "MYCE_TOKEN=test")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if exitCodeForTest(err) != 75 || !strings.Contains(stderr.String(), "file_store_capacity: queue-file-store: need=") {
		t.Fatalf("exit=%v stderr=%s", err, &stderr)
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.RefusalCode != "file_store_capacity" || receipt.JobID != "" || uploads != 0 {
		t.Fatalf("preflight failed: receipt=%+v uploads=%d", receipt, uploads)
	}
	saved := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", "capacity-refusal"}), "receipt.json")
	if err := readProfileJSON(saved, &receipt); err != nil || receipt.RefusalCode != "file_store_capacity" || receipt.RefusalReason == "" {
		t.Fatal("saved actionable refusal missing", err)
	}
}

func TestFileCapacityStatusOutput(t *testing.T) {
	output := captureJoiningOutput(t, func() error {
		printStatus(myce.Snapshot{FileStore: &myce.FileStoreStatus{Store: "queue-file-store", UsedBytes: 13, FreeBytes: 40, ReclaimableBytes: 9}}, false)
		return nil
	})
	if !strings.Contains(output, "queue-file-store: used=13 bytes free=40 bytes reclaimable=9 bytes") {
		t.Fatal(output)
	}
}
