package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"mycelium/internal/myce"
)

func TestFailureArtifactsResultCommand(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "tail")
	if err := os.WriteFile(path, []byte("failure tail\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := myce.IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	j := myce.Job{ID: "job", State: "failed", Attempt: 2, Machine: "worker", Session: "execution", AttemptHistory: []myce.AttemptOutcome{{Attempt: 2, Machine: "worker", Session: "execution", State: "failed", Diagnostics: &myce.AttemptDiagnostics{Files: myce.FileManifest{"log-tail.txt": ref}}}}}
	var corrupt, expired atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/job" {
			copy := j
			copy.FilesExpired = expired.Load()
			json.NewEncoder(w).Encode(copy)
			return
		}
		w.Header().Set("ETag", `"`+ref.SHA256+`"`)
		if corrupt.Load() {
			io.WriteString(w, "corrupt tail\n")
		} else {
			io.WriteString(w, "failure tail\n")
		}
	}))
	defer server.Close()
	args := []string{"job", "--queue", server.URL, "--token", "fixture", "--out", filepath.Join(root, "download")}
	var resultErr *ResultError
	if err = resultCommand(ctx, args, io.Discard); !errors.As(err, &resultErr) || resultErr.Code != "result_not_accepted" {
		t.Fatalf("default acceptance changed: %v", err)
	}
	args = append(args, "--attempt", "2")
	if err = resultCommand(ctx, args, io.Discard); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "download", "log-tail.txt"))
	if err != nil || string(data) != "failure tail\n" {
		t.Fatalf("download: %q %v", data, err)
	}
	corrupt.Store(true)
	if err = resultCommand(ctx, args, io.Discard); err == nil {
		t.Fatal("corrupt diagnostics downloaded")
	}
	data, _ = os.ReadFile(filepath.Join(root, "download", "log-tail.txt"))
	if string(data) != "failure tail\n" {
		t.Fatal("failed transfer overwrote existing file")
	}
	expired.Store(true)
	if err = resultCommand(ctx, args, io.Discard); !errors.As(err, &resultErr) || resultErr.Code != "result_expired" {
		t.Fatalf("expiry: %v", err)
	}
}
