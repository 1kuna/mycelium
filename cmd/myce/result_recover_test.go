package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestResultRecoverSourceContainment(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "out.txt"), []byte("outside source root"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "nested")); err != nil {
		t.Fatal(err)
	}
	var fileRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/job/recovery-plan" {
			_ = json.NewEncoder(w).Encode(myce.RecoveryPlan{Execution: myce.Execution{ID: "job", Attempt: 1}, Machine: "worker", Session: "session", Outputs: []string{"nested/out.txt"}})
			return
		}
		fileRequests.Add(1)
		http.Error(w, "unexpected file transfer", http.StatusBadRequest)
	}))
	defer server.Close()
	err := resultCommand(context.Background(), []string{"recover", "job", "--queue", server.URL, "--token", "fixture", "--files-from", root, "--key", "containment", "--by", "owner", "--reason", "retained bytes"}, io.Discard)
	if err == nil || fileRequests.Load() != 0 {
		t.Fatalf("source escaped --files-from: err=%v file requests=%d", err, fileRequests.Load())
	}
}

func TestResultRecoverCLI(t *testing.T) {
	root := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), FileStore: filepath.Join(root, "files"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "recovery-cli-authority", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	if err = os.Mkdir(filepath.Join(root, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(root, "nested", "out.txt")
	if err = os.WriteFile(input, []byte("retained output"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := myce.IdentifyFile(input)
	if err != nil {
		t.Fatal(err)
	}
	c := d.Client()
	if err = c.UploadFile(ctx, input, ref); err != nil {
		t.Fatal(err)
	}
	j, err := c.SubmitAndWait(ctx, myce.Spec{Name: "recovery", Project: "demo", SubmissionKey: "recovery-cli", Files: myce.FileManifest{"input": ref}, Outputs: map[string]string{"nested/out.txt": "results"}, Run: myce.RunSpec{Command: []string{"sh", "-c", "mkdir nested; cat input > nested/out.txt; exit 1"}}, Needs: myce.ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: myce.Platform{Required: []string{"cpu"}}}})
	if err != nil || j.State != "failed" {
		t.Fatalf("failed native attempt: %+v %v", j, err)
	}
	args := []string{"recover", j.ID, "--queue", d.URL, "--token", c.Token, "--files-from", root, "--key", "recovery", "--by", "owner", "--reason", "recover retained output", "--source", "nas:results/retained"}
	var record myce.RecoveredResult
	out := captureJoiningOutput(t, func() error { return resultCommand(ctx, args, io.Discard) })
	if err = json.Unmarshal([]byte(out), &record); err != nil || record.Kind != "recovered" {
		t.Fatalf("recovery reply: %s %v", out, err)
	}
	again := captureJoiningOutput(t, func() error { return resultCommand(ctx, args, io.Discard) })
	if again != out {
		t.Fatal("CLI replay changed recovery evidence")
	}
	dest := filepath.Join(root, "download")
	var message strings.Builder
	if err = resultCommand(ctx, []string{j.ID, "--queue", d.URL, "--token", c.Token, "--recovered", record.ResultID, "--out", dest}, &message); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message.String(), "recovered result") {
		t.Fatal("download did not identify recovery")
	}
	if data, err := os.ReadFile(filepath.Join(dest, "nested", "out.txt")); err != nil || string(data) != "retained output" {
		t.Fatalf("download: %q %v", data, err)
	}
	status := captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", d.URL, "--token", c.Token, j.ID}) })
	if !strings.Contains(status, "recovered result "+record.ResultID) || !strings.Contains(status, "original state=failed") {
		t.Fatal("status omitted recovered evidence", status)
	}
	if err = resultCommand(ctx, []string{j.ID, "--queue", d.URL, "--token", c.Token, "--out", filepath.Join(root, "normal")}, io.Discard); err == nil {
		t.Fatal("CLI silently promoted recovery")
	}
	for _, kind := range []string{"symlink", "fifo"} {
		path := filepath.Join(root, kind)
		if kind == "symlink" {
			err = os.Symlink(input, path)
		} else {
			err = syscall.Mkfifo(path, 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = resultCommand(ctx, []string{"recover", j.ID, "--queue", d.URL, "--token", c.Token, "--key", "invalid-source", "--by", "owner", "--reason", "source validation", "--file", "nested/out.txt=" + path}, io.Discard); err == nil || !strings.Contains(err.Error(), "regular files") {
			t.Fatalf("special source %s: %v", kind, err)
		}
	}
}
