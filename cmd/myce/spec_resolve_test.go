package main

import (
	"bytes"
	"io"
	"mycelium/internal/myce"
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
