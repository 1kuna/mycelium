package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestModelFetchNestedBundleCLI(t *testing.T) {
	root := t.TempDir()
	old := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: waitTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("weights"))}, nil
	})}
	t.Cleanup(func() { http.DefaultClient = old })
	if err := run([]string{"model", "fetch", "bundle/nested/model.bin", "--model-root", root,
		"--url", "https://huggingface.co/repo/resolve/revision/file", "--size-bytes", "7", "--sha256", fmt.Sprintf("%x", sha256.Sum256([]byte("weights")))}); err != nil {
		t.Fatal(err)
	}
	var files []string
	err := filepath.WalkDir(filepath.Join(root, "bundle"), func(p string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			files = append(files, p)
		}
		return err
	})
	if err != nil || len(files) != 1 || files[0] != filepath.Join(root, "bundle/nested/model.bin") {
		t.Fatalf("bundle contains extra files: %v, %v", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil || string(data) != "weights" {
		t.Fatalf("master = %q, %v", data, err)
	}
}

func TestModelPinCLI(t *testing.T) {
	dir := t.TempDir()
	q, e := myce.Open(filepath.Join(dir, "q.db"), dir, myce.RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	if e = q.ConfigureModels(myce.ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []myce.Model{{Name: "m", Path: "fake", Engine: "fake", MemoryGB: 1, Platforms: []string{"fake"}}}}); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test")
	if e := run([]string{"model", "pin", "m", "--project", "atlas", "--platform", "fake", "--queue", server.URL, "--weekdays", "1,2,3,4,5", "--start", "08:00", "--end", "20:00"}); e != nil {
		t.Fatal(e)
	}
	snap, e := q.Snapshot()
	if e != nil || len(snap.Pins) != 1 || snap.Pins[0].Platform != "fake" {
		t.Fatal(snap.Pins, e)
	}
	if e := run([]string{"model", "unpin", "m", "--project", "atlas", "--queue", server.URL}); e != nil {
		t.Fatal(e)
	}
	snap, e = q.Snapshot()
	if e != nil || len(snap.Pins) != 0 {
		t.Fatal("pin not removed")
	}
	if e := run([]string{"model", "fetch", "../outside", "--model-root", dir}); e == nil {
		t.Fatal("fetch traversal accepted")
	}
}

func TestModelSyncCLI(t *testing.T) {
	dir := t.TempDir()
	q, e := myce.Open(filepath.Join(dir, "q.db"), dir, myce.RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	cfg := myce.ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []myce.Model{{Name: "m", Path: "m", Engine: "fake", SizeBytes: 4, SHA256: strings.Repeat("a", 64), MemoryGB: 1, Platforms: []string{"fake"}}}}
	if e = q.ConfigureModels(cfg); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test")
	for _, cancel := range []bool{false, true, false} {
		args := []string{"model", "sync", "m", "--to", "all", "--queue", server.URL}
		if cancel {
			args = append(args, "--cancel")
		}
		if e := run(args); e != nil {
			t.Fatal(e)
		}
		snap, e := q.Snapshot()
		if e != nil || len(snap.SyncRequests) != 1 || snap.SyncRequests[0].Cancel != cancel {
			t.Fatal(snap.SyncRequests, e)
		}
	}
	if e := run([]string{"model", "sync", "m", "--queue", server.URL}); e == nil {
		t.Fatal("missing target accepted")
	}
}

func TestModelApproveLocalCLI(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "")
	folder := t.TempDir()
	approvals := filepath.Join(t.TempDir(), "approvals")
	if err := os.WriteFile(filepath.Join(folder, "weights.bin"), []byte("fake weights"), 0600); err != nil {
		t.Fatal(err)
	}
	output := captureJoiningOutput(t, func() error {
		return run([]string{"model", "approve-local", "approval-placeholder", "--path", folder, "--approved-root", folder, "--model-approval-root", approvals})
	})
	var model myce.Model
	if err := json.Unmarshal([]byte(output), &model); err != nil {
		t.Fatal(err)
	}
	if model.LocalSource != "approval-placeholder" || len(model.Files) != 1 || model.SizeBytes != 12 || len(model.SHA256) != 64 || strings.Contains(output, folder) || strings.Contains(output, approvals) {
		t.Fatal("approval output is not a portable manifest")
	}
}
