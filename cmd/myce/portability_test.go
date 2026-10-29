package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestPortabilityCLIAdapterReadOnly(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "adapter.yaml")
	data := strings.Replace(cpuAdapterYAML(root), "    source:", "    requirement_reasons: {backend:cpu: 'authored reason /private/project'}\n    source:", 1)
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/api/portability" {
			t.Errorf("unexpected action %s %s", r.Method, r.URL.Path)
		}
		var request myce.PortabilityRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Spec == nil || request.Spec.Needs.CPUCores != 1 || request.Recipe != nil || request.RequirementReasons["backend:cpu"] == "" {
			t.Errorf("metadata: %+v", request)
		}
		json.NewEncoder(w).Encode(myce.PortabilityReport{Inventory: "unknown", Targets: []myce.PortabilityTarget{}, Requirements: []myce.PortabilityRequirement{}, Notes: []string{"read-only"}})
	}))
	defer server.Close()
	for _, format := range []string{"text", "json"} {
		args := []string{"--adapter", file, "--entry", "work", "--queue", server.URL, "--token", "fixture-token"}
		if format == "json" {
			args = append(args, "--json")
		}
		var out bytes.Buffer
		if err := portabilityCommand(context.Background(), args, &out); err != nil {
			t.Fatal(err)
		}
		if format == "text" && !strings.Contains(out.String(), "Inventory: unknown") {
			t.Fatal(out.String())
		}
		if format == "json" && !json.Valid(out.Bytes()) {
			t.Fatal(out.String())
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
	if _, err := os.Stat(filepath.Join(root, ".mycelium")); !os.IsNotExist(err) {
		t.Fatal("advice created run state")
	}
}

func TestPortabilityCLIInventoryAndExtension(t *testing.T) {
	for _, extension := range []string{"", "example@1"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req myce.PortabilityRequest
			json.NewDecoder(r.Body).Decode(&req)
			if req.Extension != extension || req.Spec != nil || req.Recipe != nil {
				t.Errorf("request: %+v", req)
			}
			json.NewEncoder(w).Encode(myce.PortabilityReport{Inventory: "unknown"})
		}))
		args := []string{"--queue", server.URL, "--token", "fixture"}
		if extension != "" {
			args = append(args, "--extension", extension)
		}
		var out bytes.Buffer
		err := portabilityCommand(context.Background(), args, &out)
		server.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPortabilityCLINativeMetadataDoesNotExecute(t *testing.T) {
	root := t.TempDir()
	for name, value := range map[string]string{"uv.lock": "version = 1\n", "pyproject.toml": "[project]\nname = 'fixture'\n", "build.sh": "touch must-not-run\n"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	data := portabilityNativeAdapterYAML(root)
	file := filepath.Join(root, "adapter.yaml")
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/portability" {
			t.Errorf("unexpected mutation %s", r.URL.Path)
		}
		var req myce.PortabilityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if req.Recipe == nil || len(req.Recipe.Variants) != 1 || len(req.Recipe.Files["uv.lock"]) == 0 || len(req.Recipe.Files["pyproject.toml"]) == 0 || len(req.Recipe.Files["build.sh"]) == 0 || req.Recipe.Digest != "" {
			t.Errorf("missing native metadata: %+v", req)
		}
		json.NewEncoder(w).Encode(myce.PortabilityReport{Inventory: "unknown"})
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := portabilityCommand(context.Background(), []string{"--adapter", file, "--queue", server.URL, "--token", "fixture"}, &out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "must-not-run")); !os.IsNotExist(err) {
		t.Fatal("advice executed payload")
	}
}

func portabilityNativeAdapterYAML(root string) string {
	return `schema: 1
project: native-portability
default: work
entries:
  work:
    source: {directory: "` + root + `"}
    variants:
      - target: {os: linux, arch: arm64, platform: cuda}
        needs: {memory_gb: 1}
        execution:
          main: {command: [sh, '{extension}/build.sh'], environment: main}
        environments:
          main: {backend: uv, python: '3.12', lock_file: uv.lock, build: [sh, '{extension}/build.sh']}
        checks: {qualification: [sh, '{extension}/build.sh']}
        baselines:
          extension:work: {input: input, output: native, measure: exact, native: fixture, variation: exact}
`
}

func TestPortabilityCLIRealAPILargeLock(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "uv.lock"), bytes.Repeat([]byte("#"), 800<<10), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "pyproject.toml"), []byte("[project]\nname='fixture'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "build.sh"), []byte("exit 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "adapter.yaml")
	if err := os.WriteFile(file, []byte(portabilityNativeAdapterYAML(root)), 0600); err != nil {
		t.Fatal(err)
	}
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(myce.API{Queue: q, Token: "local-test-token"}.Handler())
	defer server.Close()
	var out bytes.Buffer
	if err := portabilityCommand(context.Background(), []string{"--adapter", file, "--queue", server.URL, "--token", "local-test-token", "--json"}, &out); err != nil {
		t.Fatal(err)
	}
	var report myce.PortabilityReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Targets) != 1 || len(report.Targets[0].Environments) != 1 || report.Targets[0].Environments[0].Inputs != "present" {
		t.Fatalf("large lock lost from advice: %+v", report)
	}
}
