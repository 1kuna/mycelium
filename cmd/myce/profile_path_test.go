package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

func TestProfilePathDiagnosticSubmitter(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	err := runProfile(context.Background(), "missing.yaml", nil, nil, "", "", "", "", "", "", io.Discard, io.Discard)
	if err == nil {
		t.Fatal("missing profile accepted")
	}
	for _, want := range []string{"profile", "submitter read", "missing.yaml", "submitter-working-directory", "missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatal(err)
		}
	}
	if strings.Contains(err.Error(), root) {
		t.Fatal("expanded submitter root exposed", err)
	}
	// Profile inputs resolve from the project, without publishing that root.
	profile := `name: sample
project: test
class: experiment
source: {directory: source}
run: {command: [python, task.py]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [test, -s, result.txt]
`
	if err := os.WriteFile("sample.yaml", []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	err = runProfile(context.Background(), "sample.yaml", nil, map[string]string{"data": "absent/input.txt"}, "", "", "", "", "", "", io.Discard, io.Discard)
	if err == nil {
		t.Fatal("missing input accepted")
	}
	for _, want := range []string{"inputs.data", "submitter read", "absent/input.txt", "project-root", "missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatal(err)
		}
	}
	if strings.Contains(err.Error(), root) {
		t.Fatal("expanded input root exposed", err)
	}
}
