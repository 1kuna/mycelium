package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mycelium/internal/myce"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProfilePathDiagnosticSubmitter(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	err := runProfile(context.Background(), "missing.yaml", nil, nil, "", "", "", "", "", "", io.Discard, io.Discard)
	if err == nil {
		t.Fatal("missing profile accepted")
	}
	for _, want := range []string{"profile", "submitter read", "missing.yaml", "submitter-working-directory", "missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatal(err)
		}
	}
	if strings.Contains(err.Error(), root) {
		t.Fatal("expanded submitter root exposed", err)
	}
	// Profile inputs resolve from the project, without publishing that root.
	profile := `name: sample
project: test
class: experiment
source: {directory: source}
run: {command: [python, task.py]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [test, -s, result.txt]
`
	if err := os.WriteFile("sample.yaml", []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	err = runProfile(context.Background(), "sample.yaml", nil, map[string]string{"data": "absent/input.txt"}, "", "", "", "", "", "", io.Discard, io.Discard)
	if err == nil {
		t.Fatal("missing input accepted")
	}
	for _, want := range []string{"inputs.data", "submitter read", "absent/input.txt", "project-root", "missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatal(err)
		}
	}
	if strings.Contains(err.Error(), root) {
		t.Fatal("expanded input root exposed", err)
	}
}

func TestProfileQueuePathDiagnosticReceipt(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	if err := os.Mkdir("source", 0700); err != nil {
		t.Fatal(err)
	}
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
	state := t.TempDir()
	q, err := myce.Open(filepath.Join(state, "queue.db"), filepath.Join(state, "checkpoints"), myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(myce.API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	var out bytes.Buffer
	err = runProfile(context.Background(), "sample.yaml", nil, nil, "", "", "", "", server.URL, "fixture", &out, io.Discard)
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
