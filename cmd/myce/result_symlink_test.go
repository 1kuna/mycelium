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
	"testing"

	"mycelium/internal/myce"
)

func TestResultRefusesSymlinkInsideDestination(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "nested")); err != nil {
		t.Fatal(err)
	}
	transfers := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/job" {
			json.NewEncoder(w).Encode(myce.Job{ID: "job", State: "succeeded", OutputRefs: myce.FileManifest{"nested/out.txt": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}})
			return
		}
		transfers++
		http.Error(w, "unexpected transfer", 500)
	}))
	defer server.Close()
	err := resultCommand(context.Background(), []string{"job", "--queue", server.URL, "--token", "fixture", "--out", root}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "directory") || transfers != 0 {
		t.Fatalf("link accepted: %v, transfers %d", err, transfers)
	}
	if files, err := os.ReadDir(outside); err != nil || len(files) != 0 {
		t.Fatal("outside destination changed", err)
	}
}
