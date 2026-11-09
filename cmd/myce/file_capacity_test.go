package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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
		if r.Header.Get("Authorization") == "Bearer refusal-fixture-secret" {
			myce.API{Queue: q, Token: "refusal-fixture-secret"}.Handler().ServeHTTP(w, r)
			return
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
	if err := q.ConfigureFileStore(filepath.Join(state, "files"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	{
		if err := os.WriteFile("source/task.py", []byte("pass\n"), 0600); err != nil {
			t.Fatal(err)
		}
		profile := `name: sample
project: test
class: experiment
source: {directory: source}
environment: {lock_file: absent/requirements.txt, python: '3.12', platforms: [cpu]}
run: {command: [python, task.py]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [python, -c, "pass"]
`
		if err := os.WriteFile("sample.yaml", []byte(profile), 0600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		err = runProfile(context.Background(), "sample.yaml", nil, nil, "", "", "", "", server.URL, "test", &out, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "job_environment_invalid") || !strings.Contains(err.Error(), "queue read") {
			t.Fatal(err, out.String())
		}
		var receipt profileReceipt
		if e := json.Unmarshal(out.Bytes(), &receipt); e != nil {
			t.Fatal(e, out.String())
		}
		if receipt.PathDiagnostic == nil || receipt.RefusalReason != receipt.PathDiagnostic.String() || receipt.PathDiagnostic.Path != "absent/requirements.txt" {
			t.Fatal(out.String())
		}
	}
	{
		profileDir := filepath.Join(root, ".mycelium", "profiles")
		if err := os.MkdirAll(profileDir, 0700); err != nil {
			t.Fatal(err)
		}
		profile := `name: fixture
project: test
class: experiment
source: {directory: source}
run: {command: [true]}
needs: {cpu_cores: 2, memory_gb: 8}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`
		path := filepath.Join(profileDir, "fixture.yaml")
		if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
			t.Fatal(err)
		}
		m := myce.Machine{Capabilities: []string{"native_isolation_landlock"}, ID: "worker", Session: "fixture", GPU: "cpu", CPUIsolation: "select-none",
			HostMemoryMeasured: true, HostAvailableMemoryGB: 32, HostMemoryBudgetGB: 8, MemoryCeilingSlackGB: .125,
			CapabilityProfile: &myce.CapabilityProfile{OS: "linux", Arch: "amd64", LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 32 << 30}}
		if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		const token = "refusal-fixture-secret"
		binary := devTestBinary(t)
		ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "run", "--profile", path, "--queue", server.URL, "--key", "refused")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "MYCE_TOKEN="+token)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		want := "host memory need 8.125 GiB (8 + 0.125 slack) exceeds allowance 8 GiB on worker"
		if exitCodeForTest(err) != 78 || !strings.Contains(stderr.String(), "no_qualified_target: "+want) {
			t.Fatalf("exit=%v stderr=%s", err, &stderr)
		}
		var receipt profileReceipt
		if err := json.Unmarshal(out, &receipt); err != nil {
			t.Fatal(err)
		}
		var saved profileReceipt
		receiptPath := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", "refused"}), "receipt.json")
		if err := readProfileJSON(receiptPath, &saved); err != nil {
			t.Fatal(err)
		}
		if receipt.RefusalCode != "no_qualified_target" || !strings.Contains(receipt.RefusalReason, want) || saved.RefusalReason != receipt.RefusalReason || saved.RefusalCode != receipt.RefusalCode {
			t.Fatalf("printed/saved refusal mismatch: %+v / %+v", receipt, saved)
		}
		if strings.Contains(stderr.String(), token) || bytes.Contains(out, []byte(token)) || strings.Contains(receipt.RefusalReason, root) {
			t.Fatal("refusal leaked credential or private path")
		}
	}
}
