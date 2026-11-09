package main

import (
	"bytes"
	"context"
	"mycelium/internal/myce"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
}
