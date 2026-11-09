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

	"mycelium/internal/myce"
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
	server := httptest.NewServer(http.HandlerFunc(refuseProfilePreparation))
	defer server.Close()
	var out bytes.Buffer
	err := runProfile(context.Background(), path, nil, nil, "shared", "", "", "", server.URL, "test", &out, io.Discard)
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
	if err = runProfile(context.Background(), path, nil, nil, "shared", "", "", "", server.URL, "test", &out, io.Discard); exitCode(err) != 69 {
		t.Fatal(err)
	}
}

func TestProfileLocalSharedInputIdentity(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{".mycelium/profiles", "source"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "runtime"), []byte("runtime"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := myce.IdentifyFile(filepath.Join(root, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".mycelium/profiles/test.yaml")
	server := httptest.NewServer(http.HandlerFunc(refuseProfilePreparation))
	defer server.Close()
	for _, tc := range []struct{ name, hash, size, want string }{

		{"hash mismatch", strings.Repeat("0", 64), "", "shared input runtime: sha256 differs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := `name: shared
project: test
class: experiment
source: {directory: source}
run: {command: [true]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
shared_inputs: {runtime: {local: runtime, sha256: "` + tc.hash + `"` + tc.size + `}}
`
			if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
				t.Fatal(err)
			}
			req, findings, err := resolveInput(path, "", "", true)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "derive size" && (len(findings.findings) != 0 || req.Spec.SharedInputs["runtime"].SizeBytes != ref.SizeBytes) {
				t.Fatalf("resolve: %+v %+v", req.Spec.SharedInputs, findings)
			}
			if tc.want != "" && (len(findings.findings) != 1 || !strings.Contains(findings.findings[0].Problem, tc.want)) {
				t.Fatalf("resolve: %+v", findings)
			}
			var out bytes.Buffer
			err = runProfile(context.Background(), path, nil, nil, "", "", "", "", server.URL, "test", &out, io.Discard)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v", err)
				}
				return
			}
			if exitCode(err) != 69 {
				t.Fatalf("prepare: %v", err)
			}
			var receipt profileReceipt
			if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
				t.Fatal(err)
			}
			if got := receipt.Spec.SharedInputs["runtime"]; got.SHA256 != ref.SHA256 || got.SizeBytes != ref.SizeBytes {
				t.Fatalf("identity: %+v", got)
			}
		})
	}
}
