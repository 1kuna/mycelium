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
environment: {lock_file: ABSENT, python: '3.12', platforms: [cpu]}
run: {command: [python, task.py]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [python, -c, "pass"]
`
	// Absolute locks stay queue-side reads; relative ones are uploaded.
	absent := filepath.Join(root, "absent", "requirements.txt")
	profile = strings.Replace(profile, "ABSENT", absent, 1)
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
	if receipt.PathDiagnostic == nil || receipt.RefusalReason != receipt.PathDiagnostic.String() || receipt.PathDiagnostic.Path != absent || receipt.PathDiagnostic.Side != "queue" {
		t.Fatal(out.String())
	}
}

// A relative pixi lock is read from the project root and uploaded with the
// submission, so the queue never needs a path it can read.
func TestProfileUploadsRelativePixiLock(t *testing.T) {
	project := t.TempDir()
	for name, data := range map[string]string{
		"source/task.py":                 "pass\n",
		"env/pixi.lock":                  "version: 6\nenvironments: {}\n",
		"env/pixi.toml":                  "[workspace]\nname='example'\n",
		".mycelium/profiles/sample.yaml": "name: sample\nproject: test\nclass: experiment\nsource: {directory: source}\nenvironment: {backend: pixi, lock_file: env/pixi.lock, platforms: [cpu]}\nrun: {command: [python, task.py]}\nneeds: {cpu_cores: 1, memory_gb: 1}\ntargets: [{os: linux, arch: amd64, platform: cpu}]\ncheck: [python, -c, pass]\n",
	} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(project, name)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(project, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// The in-process queue runs elsewhere: env/pixi.lock does not resolve there.
	t.Chdir(t.TempDir())
	state := t.TempDir()
	q, err := myce.Open(filepath.Join(state, "queue.db"), filepath.Join(state, "checkpoints"), myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(myce.API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	profile := filepath.Join(project, ".mycelium", "profiles", "sample.yaml")
	// The fixture queue has no workers. It resolves and freezes the uploaded
	// environment, then refuses only at target qualification (a queue-side
	// lock read would refuse first with job_environment_invalid). Accepted
	// submissions start a receipt observer, which this test must not spawn.
	var out bytes.Buffer
	if err := runProfile(context.Background(), profile, nil, nil, "", "", "", "", server.URL, "fixture", &out, io.Discard); err == nil || !strings.Contains(err.Error(), "no_qualified_target") {
		t.Fatal(err, out.String())
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatal(err, out.String())
	}
	if receipt.RefusalCode != "no_qualified_target" || string(receipt.Spec.Environment.Files["pixi.toml"]) != "[workspace]\nname='example'\n" || len(receipt.Spec.Environment.Files) != 2 {
		t.Fatal(out.String())
	}
	// Lock bytes are intent: reusing an explicit key after an edit conflicts
	// locally, before any request.
	if err := runProfile(context.Background(), profile, nil, nil, "fixed", "", "", "", server.URL, "fixture", io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "no_qualified_target") {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, "env", "pixi.lock"), []byte("version: 6\nenvironments: {edited: {}}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runProfile(context.Background(), profile, nil, nil, "fixed", "", "", "", server.URL, "fixture", io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "submission_key_conflict") {
		t.Fatalf("edited lock reused key: %v", err)
	}
	// Submitter-side failures name the project-root path before any upload.
	for name, mutate := range map[string]func() error{
		"companion_absent": func() error { return os.Remove(filepath.Join(project, "env", "pixi.toml")) },
		"too_large": func() error {
			return os.WriteFile(filepath.Join(project, "env", "pixi.toml"), bytes.Repeat([]byte("x"), (1<<20)+1), 0600)
		},
	} {
		if err := mutate(); err != nil {
			t.Fatal(err)
		}
		err := runProfile(context.Background(), profile, nil, nil, "", "", "", "", server.URL, "fixture", io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "profile_input_invalid") || !strings.Contains(err.Error(), "submitter") || !strings.Contains(err.Error(), name) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
