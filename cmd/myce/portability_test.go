package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := myce.API{Queue: q, Token: "fixture"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/portability" {
			if r.URL.Path != "/api/resolve" && r.URL.Path != "/api/dev/clock/advance" {
				t.Errorf("unexpected mutation %s", r.URL.Path)
			}
			handler.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var req myce.PortabilityRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Error(err)
		}
		if req.Extension != "" {
			extension := "example@1"
			if req.Extension != extension || req.Spec != nil || req.Recipe != nil {
				t.Errorf("request: %+v", req)
			}
			json.NewEncoder(w).Encode(myce.PortabilityReport{Inventory: "unknown"})
			return
		}
		if req.Spec != nil {
			request := req
			if request.Spec == nil || request.Spec.Needs.CPUCores != 1 || request.Recipe != nil || request.RequirementReasons["backend:cpu"] == "" {
				t.Errorf("metadata: %+v", request)
			}
			json.NewEncoder(w).Encode(myce.PortabilityReport{Inventory: "unknown", Targets: []myce.PortabilityTarget{}, Requirements: []myce.PortabilityRequirement{}, Notes: []string{"read-only"}})
			return
		}
		if !req.SourceUnverified && (req.Recipe == nil || len(req.Recipe.Variants) != 1 || len(req.Recipe.Files["uv.lock"]) == 0 || len(req.Recipe.Files["pyproject.toml"]) == 0 || len(req.Recipe.Files["build.sh"]) == 0 || req.Recipe.Digest != "") {
			t.Errorf("missing native metadata: %+v", req)
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := portabilityCommand(context.Background(), []string{"--adapter", file, "--queue", server.URL, "--token", "fixture"}, &out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "must-not-run")); !os.IsNotExist(err) {
		t.Fatal("advice executed payload")
	}
	out.Reset()
	if err := portabilityCommand(context.Background(), []string{"--adapter", file, "--queue", server.URL, "--token", "fixture", "--json"}, &out); err != nil {
		t.Fatal(err)
	}
	var report myce.PortabilityReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Targets) != 1 || len(report.Targets[0].Environments) != 1 || report.Targets[0].Environments[0].Inputs != "present" {
		t.Fatalf("large lock lost from advice: %+v", report)
	}
	out.Reset()
	err = specCommand([]string{"check", "--resolve", "--json", "--queue", server.URL, "--token", "fixture", file}, &out, io.Discard)
	// No worker is registered: the authored native recipe remains unknown,
	// while the valid large lock must survive the same base64 transport.
	if exitCode(err) != 1 {
		t.Fatalf("unqualified native resolve: %v", err)
	}
	var resolved myce.ResolveReport
	if err := json.Unmarshal(out.Bytes(), &resolved); err != nil {
		t.Fatal(err)
	}
	if resolved.Portability == nil || len(resolved.Portability.Targets) != 1 || len(resolved.Portability.Targets[0].Environments) != 1 || resolved.Portability.Targets[0].Environments[0].Inputs != "present" {
		t.Fatalf("large lock lost from resolve: %+v", resolved)
	}

	t.Chdir(root)
	if err := os.WriteFile(filepath.Join(root, "uv.lock"), []byte("working-tree lock"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, selected := range []string{"{git: HEAD, paths: [build.sh]}"} {
		// A nearby lock must not stand in for a lock excluded from Git source.paths
		// or for the contents of a selected snapshot that resolve has not inspected.
		text := strings.Replace(portabilityNativeAdapterYAML(root), `{directory: "`+root+`"}`, selected, 1)
		file := filepath.Join(root, "adapter.yaml")
		if err := os.WriteFile(file, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		req, _, err := resolveInput(file, "", "", true)
		if err != nil {
			t.Fatal(err)
		}
		if !req.SourceUnverified || len(req.Recipe.Files) != 0 {
			t.Fatal("selected source borrowed working-tree bytes")
		}
		report, err := q.Resolve(req)
		if err != nil {
			t.Fatal(err)
		}
		if report.Portability == nil || len(report.Portability.Targets) != 1 {
			t.Fatal(report)
		}
		env := report.Portability.Targets[0].Environments[0]
		if env.Inputs != "unknown: selected source not inspected" || len(env.MissingInputs) != 0 {
			t.Fatalf("uninspected source claimed present or missing: %+v", env)
		}
		var output bytes.Buffer
		if err := portabilityCommand(context.Background(), []string{"--adapter", file, "--queue", server.URL, "--token", "fixture"}, &output); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "unknown: selected source not inspected") {
			t.Fatal(output.String())
		}
	}

	{
		file := filepath.Join(root, "adapter.yaml")
		data := strings.Replace(cpuAdapterYAML(root), "    source:", "    requirement_reasons: {backend:cpu: 'authored reason /private/project'}\n    source:", 1)
		data = strings.Replace(data, "    source:", "    allowed_machines: [private-host]\n    source:", 1)
		if err := os.WriteFile(file, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		for _, format := range []string{"text"} {
			args := []string{"--adapter", file, "--entry", "work", "--queue", server.URL, "--token", "fixture"}
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
			if !strings.Contains(out.String(), "omit allowed_machines") || strings.Contains(out.String(), "private-host") {
				t.Fatal("missing/private placement advice", out.String())
			}
		}

		if _, err := os.Stat(filepath.Join(root, ".mycelium")); !os.IsNotExist(err) {
			t.Fatal("advice created run state")
		}

	}
	{
		var out bytes.Buffer
		err := portabilityCommand(context.Background(), []string{"--queue", server.URL, "--token", "fixture", "--extension", "example@1"}, &out)
		if err != nil {
			t.Fatal(err)
		}
	}

	{
		authority := []string{"--queue", server.URL, "--token", "fixture"}
		inputs := filepath.Join(root, "inputs")
		if err := os.MkdirAll(inputs, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(inputs, "data.txt"), []byte("data"), 0600); err != nil {
			t.Fatal(err)
		}
		job := filepath.Join(root, "job.yaml")
		body := validJobYAML + "inputs: {data.txt: data.txt, absent.txt: absent.txt}\nenvironment: {lock_file: not-on-queue/uv.lock, python: '3.12'}\n"
		if err := os.WriteFile(job, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		err = specCommand(append(append([]string{"check", "--resolve", "--files-from", inputs}, authority...), job), &out, io.Discard)
		text := out.String()
		if exitCode(err) != 1 || !strings.Contains(text, "inputs.absent.txt [submitter]") || !strings.Contains(text, `"absent.txt" (base files-from): missing`) {
			t.Fatalf("submitter side not named: %v\n%s", err, text)
		}
		if !strings.Contains(text, "environment.lock_file [queue]") || !strings.Contains(text, "read on the queue host") || !strings.Contains(text, "environment: NAME") {
			t.Fatalf("queue side or library fix not named: %s", text)
		}

	}

	{
		var out bytes.Buffer
		if err := devClockCommand([]string{"advance", "1s", "--queue", server.URL, "--token", "fixture"}, &out); err == nil {
			t.Fatal("dev CLI reached production clock")
		}
		t.Setenv("MYCE_TOKEN", "fixture")
		if err := run([]string{"queue", "clock", "advance", "1s"}); err == nil || err.Error() != "queue accepts flags only" {
			t.Fatalf("production CLI: %v", err)
		}
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
