package main

import (
	"bytes"
	"context"
	"io"
	"mycelium/internal/myce"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSpecResolveRejectsEscapingInputMappings(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "input")
	if err := os.WriteFile(input, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{input, "../input"} {
		s := myce.Spec{}
		s.Inputs = map[string]string{"data": source}
		var found submitterFindings
		lowerInputs(&s, root, "files-from", &found)
		if len(found.findings) != 1 || !strings.Contains(found.findings[0].Problem, "must be relative") || len(s.Inputs) != 0 {
			t.Fatalf("submit/resolve mismatch: %+v", found)
		}
	}
}

func TestSpecResolveNativeAdapterMetadata(t *testing.T) {
	root := t.TempDir()
	// Reuse the authored variant fixture; a local native entry has no frozen
	// qualification, so resolve must report unknown rather than reject the reader
	// or claim it is launch-ready.
	source := `schema: 1
project: example
default: work
entries:
  work:
    version: v1
    purpose: Offline native resolve fixture
    source: {directory: .}
    variants:
      - target: {os: linux, arch: amd64, platform: cpu}
        needs: {cpu_cores: 1, memory_gb: 1}
        execution:
          main:
            command: [python3, run.py]
            environment: tools
        checks:
          qualification: [sh, check.sh]
        baselines:
          extension:work: {input: input, output: output, measure: exact, native: fixture, variation: exact}
        environments:
          tools: {python: '3.12', lock_file: requirements.txt}
`
	path := filepath.Join(root, "adapter.yaml")
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	request, _, err := resolveInput(path, "", "", true)
	if err != nil {
		t.Fatal(err)
	}
	if request.Recipe == nil || len(request.Recipe.Variants) != 1 {
		t.Fatalf("native recipe lost: %+v", request)
	}
	var out bytes.Buffer
	// --inspect-nas requires --resolve and must never create authority implicitly.
	if err := specCommand([]string{"check", "--inspect-nas", path}, &out, io.Discard); exitCode(err) != 2 {
		t.Fatalf("inspect without resolve: %v", err)
	}
}

func TestSpecResolveNativeSelectedSourceStaysUnknown(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	for name, body := range map[string]string{"uv.lock": "working-tree lock", "pyproject.toml": "[project]\nname='fixture'\n", "build.sh": "exit 0\n"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(myce.API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	for _, selected := range []string{"{git: HEAD, paths: [build.sh]}", "{snapshot: selected-source.tar}"} {
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
}
