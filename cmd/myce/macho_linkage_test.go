package main

import (
	"bytes"
	"context"
	"debug/macho"
	"encoding/binary"
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
	for _, json := range []bool{false, true} {
		for _, kind := range []string{"resolve", "portability"} {
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
}

// Legacy profiles must lower to worker-prepared source too; extracting in a
// launched shell would hide every packaged executable from prelaunch checks.
