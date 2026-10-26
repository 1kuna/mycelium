package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProfileSharedInputsRetainReferencesWithoutNASUploads(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(root, "runtime.tar")
	if err := os.WriteFile(local, []byte("runtime"), 0600); err != nil {
		t.Fatal(err)
	}
	profile := `name: shared
project: test
class: experiment
source: {directory: source}
run: {command: [true]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
shared_inputs:
  runtime: {local: runtime.tar, archive: tar, unpacked_bytes: 100}
  original: {nas: media/original.mov, sha256: "` + strings.Repeat("0", 64) + `", size_bytes: 2000000000}
`
	path := filepath.Join(dir, "shared.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	var out bytes.Buffer
	err := runProfile(context.Background(), path, nil, "shared", "", "", "", server.URL, "test", &out, io.Discard)
	if exitCode(err) != 69 {
		t.Fatal(err)
	}
	var receipt profileReceipt
	if err = json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Spec.SharedInputs["runtime"].Local != "" || receipt.Spec.SharedInputs["runtime"].SHA256 == "" {
		t.Fatal("local shared input not lowered")
	}
	if len(receipt.Spec.Files) != 1 || len(receipt.Uploads) != 2 || len(receipt.Spec.StoredInputFiles()) != 2 {
		t.Fatal("NAS input uploaded or local input copied per-job", receipt)
	}
	if strings.Contains(receipt.Spec.Run.Command[2], "runtime.tar") || strings.Contains(receipt.Spec.Run.Command[2], "original.mov") {
		t.Fatal("shared input copied by wrapper")
	}
	if err = receipt.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	// A retained explicit key needs neither source nor local import on replay.
	if err = os.Remove(local); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err = runProfile(context.Background(), path, nil, "shared", "", "", "", server.URL, "test", &out, io.Discard); exitCode(err) != 69 {
		t.Fatal(err)
	}
}
