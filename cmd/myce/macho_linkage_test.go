package main

import (
	"bytes"
	"context"
	"debug/macho"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMachOLinkageResolveAndPortability(t *testing.T) {
	root := t.TempDir()
	missing := filepath.ToSlash(filepath.Join(root, "absent.dylib"))
	n := (24 + len(missing) + 1 + 7) &^ 7
	var binaryFile bytes.Buffer
	for _, n := range []uint32{macho.Magic64, uint32(macho.CpuArm64), 0, uint32(macho.TypeExec), 1, uint32(n), 0, 0} {
		_ = binary.Write(&binaryFile, binary.LittleEndian, n)
	}
	command := make([]byte, n)
	binary.LittleEndian.PutUint32(command, uint32(macho.LoadCmdDylib))
	binary.LittleEndian.PutUint32(command[4:], uint32(n))
	binary.LittleEndian.PutUint32(command[8:], 24)
	copy(command[24:], missing)
	binaryFile.Write(command)
	if err := os.WriteFile(filepath.Join(root, "tool"), binaryFile.Bytes(), 0700); err != nil {
		t.Fatal(err)
	}
	adapter := filepath.Join(root, "adapter.yaml")
	if err := os.WriteFile(adapter, []byte(cpuAdapterYAML(root)), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/resolve":
			w.Write([]byte(`{"targets":[{"eligible":true}]}`))
		case "/api/portability":
			w.Write([]byte(`{}`))
		default:
			t.Error("unexpected mutation", r.Method, r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	t.Setenv("MYCE_QUEUE", server.URL)
	t.Setenv("MYCE_TOKEN", "fixture")
	for _, kind := range []string{"resolve", "portability"} {
		json := kind == "resolve"
		var output bytes.Buffer
		var err error
		if kind == "resolve" {
			args := []string{"check", "--resolve"}
			if json {
				args = append(args, "--json")
			}
			args = append(args, adapter)
			err = specCommand(args, &output, &output)
		} else {
			args := []string{"--adapter", adapter}
			if json {
				args = append(args, "--json")
			}
			err = portabilityCommand(context.Background(), args, &output)
		}
		if err != nil || !strings.Contains(output.String(), missing) || !strings.Contains(output.String(), "tool") {
			t.Fatalf("%s json=%t: %v %s", kind, json, err, output.String())
		}
	}
}

// Legacy profiles must lower to worker-prepared source too; extracting in a
// launched shell would hide every packaged executable from prelaunch checks.
func TestMachOLinkageLegacyProfilePreparesSourceAndOverlays(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "tool"), []byte("source helper"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "replacement"), []byte("explicit replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	profile := `name: fixture
project: test
class: experiment
source: {directory: source}
inputs: {tool: replacement}
run: {command: [sh, tool]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: darwin, arch: arm64, platform: cpu}]
check: [true]
`
	file := filepath.Join(dir, "fixture.yaml")
	if err := os.WriteFile(file, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(refuseProfilePreparation))
	defer server.Close()
	var out bytes.Buffer
	err := runProfile(context.Background(), file, nil, nil, "legacy-prelaunch", "", "", "", server.URL, "fixture", &out, io.Discard)
	if exitCode(err) != 69 {
		t.Fatal(err, out.String())
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.Spec.PrepareSource || strings.Contains(strings.Join(receipt.Spec.Run.Command, " "), "tar -xf") {
		t.Fatal("source hidden until payload launch", receipt.Spec.Run)
	}
	if _, ok := receipt.Spec.Files["tool"]; !ok {
		t.Fatal("explicit input lost its replacement destination", receipt.Spec.Files)
	}
	if _, ok := receipt.Uploads["files/tool"]; !ok {
		t.Fatal("input upload not bound to destination", receipt.Uploads)
	}
}
