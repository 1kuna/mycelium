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

func TestProfileSharedInputsComposeWithPerItemIdentity(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{".mycelium/profiles", "source"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("runtime", "shared bytes")
	write("common", "private bytes")
	write("item", "first item")
	profile := `name: shared-items
project: test
class: experiment
source: {directory: source}
run: {command: [true]}
inputs: {shared/runtime: common}
shared_inputs: {runtime: {local: runtime}}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`
	path := filepath.Join(root, ".mycelium", "profiles", "shared.yaml")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	invoke := func(arg string) profileReceipt {
		t.Helper()
		var out bytes.Buffer
		err := runProfile(context.Background(), path, []string{arg}, map[string]string{"item": "item"}, "", "", "", "", server.URL, "test", &out, io.Discard)
		if exitCode(err) != 69 {
			t.Fatalf("prepare: %v %s", err, out.String())
		}
		var r profileReceipt
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	write(".mycelium/profiles/shared.yaml", profile)
	first := invoke("first")
	private := first.Spec.Files[".myce-inputs/"+profileHash("shared/runtime")]
	if private.SHA256 == "" || private.SHA256 == first.Spec.SharedInputs["runtime"].SHA256 {
		t.Fatal("shared and private input identities collided")
	}
	if again := invoke("first"); again.Key != first.Key {
		t.Fatal("identical inputs did not adopt saved intent")
	}
	write("item", "second item")
	item := invoke("second")
	if item.Key == first.Key || item.Spec.ProfileMemory != first.Spec.ProfileMemory {
		t.Fatal("item change must rerun while retaining common memory identity")
	}
	write("runtime", "changed shared bytes")
	shared := invoke("second")
	if shared.Key == item.Key || shared.Spec.ProfileMemory == item.Spec.ProfileMemory {
		t.Fatal("shared data change must change both execution and common memory identity")
	}
	write(".mycelium/profiles/shared.yaml", profile+"memory_class: per_item\n")
	perItem := invoke("second")
	perArgs := invoke("different")
	write("item", "third item")
	perFile := invoke("second")
	if perItem.Spec.ProfileMemory == perArgs.Spec.ProfileMemory || perItem.Spec.ProfileMemory == perFile.Spec.ProfileMemory {
		t.Fatal("per_item must still distinguish arguments and item content with shared inputs")
	}
}
