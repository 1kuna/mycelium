package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// #1341: the smallest entry is a project and a command; everything else has a
// documented default, and only production must name targets.
func TestSmallestAdapterEntryDefaults(t *testing.T) {
	a, err := decodeAdapter([]byte("project: tiny\nentries:\n  test:\n    run: {command: [python3, -m, pytest]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	want := myce.ResourceNeeds{CPUCores: 1, Memory: "auto", Platform: myce.Platform{Required: []string{"cpu"}}}
	if a.Schema != 1 || a.Default != "test" || p.Class != "experiment" || p.Source.Git != "HEAD" || len(p.Targets) != 0 || len(p.Check) != 0 || p.Environment.Key() != "" || !reflect.DeepEqual(p.Needs, want) {
		t.Fatalf("defaults: schema=%d default=%q class=%q source=%+v targets=%v needs=%+v", a.Schema, a.Default, p.Class, p.Source, p.Targets, p.Needs)
	}
	several, err := decodeAdapter([]byte("project: tiny\nentries:\n  a: {run: {command: [true]}}\n  b: {run: {command: [true]}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := adapterProfile(several, ""); err == nil || !strings.Contains(err.Error(), "--entry") {
		t.Fatal("several entries without a default picked one silently", err)
	}
	if _, err := decodeAdapter([]byte("project: tiny\nentries:\n  test:\n    class: production\n    run: {command: [true]}\n")); err == nil || !strings.Contains(err.Error(), "explicit targets") {
		t.Fatal("production entry without targets accepted", err)
	}
	if _, err := decodeAdapter([]byte("project: tiny\nentries:\n  test:\n    class: production\n    run: {command: [true]}\n    targets: [{os: linux, arch: amd64, platform: cpu}]\n")); err != nil {
		t.Fatal("production entry with targets refused", err)
	}
}

func TestInitWritesOneEntryFromTheLock(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		args  []string
		want  myce.Environment
		argv  []string
	}{
		{"uv", map[string]string{"uv.lock": "version = 1\n", "pyproject.toml": "[project]\nname = 'tiny'\nrequires-python = '>=3.12'\n"}, nil, myce.Environment{LockFile: "uv.lock", Python: "3.12"}, []string{"python", "-m", "pytest"}},
		{"uv-python-version", map[string]string{"uv.lock": "version = 1\n", "pyproject.toml": "[project]\nname = 'tiny'\n", ".python-version": "3.13.1\n"}, []string{"--", "python", "run.py"}, myce.Environment{LockFile: "uv.lock", Python: "3.13"}, []string{"python", "run.py"}},
		{"pixi", map[string]string{"pixi.lock": "version: 6\n", "pixi.toml": "[workspace]\nname = 'tiny'\n"}, nil, myce.Environment{Backend: "pixi", LockFile: "pixi.lock"}, []string{"python", "-m", "pytest"}},
		{"installed-tools", nil, []string{"--", "sh", "test.sh"}, myce.Environment{}, []string{"sh", "test.sh"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "tiny")
			if err := os.MkdirAll(root, 0755); err != nil {
				t.Fatal(err)
			}
			for name, content := range tc.files {
				if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Chdir(root)
			var out bytes.Buffer
			if err := initCommand(tc.args, &out, &out); err != nil {
				t.Fatal(err, out.String())
			}
			data, err := os.ReadFile(filepath.Join(root, ".mycelium", "adapter.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			for _, absent := range []string{"targets", "needs", "check", "marker", "schema", "default"} {
				if strings.Contains(string(data), absent) {
					t.Fatalf("starter contains %s:\n%s", absent, data)
				}
			}
			a, err := decodeAdapter(data)
			if err != nil {
				t.Fatal(err)
			}
			e := a.Entries["test"]
			if a.Project != "tiny" || !reflect.DeepEqual(e.Run.Command, tc.argv) || !reflect.DeepEqual(e.Environment, tc.want) {
				t.Fatalf("starter:\n%s", data)
			}
			if !strings.Contains(out.String(), "myce run --entry test") {
				t.Fatal(out.String())
			}
			if err := initCommand(tc.args, &out, &out); err == nil {
				t.Fatal("existing adapter overwritten")
			}
		})
	}
	root := t.TempDir()
	t.Chdir(root)
	if err := initCommand(nil, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "myce init -- COMMAND") {
		t.Fatal("no lock and no command accepted", err)
	}
	for _, name := range []string{"pixi.lock", "pixi.toml", "uv.lock", "pyproject.toml"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := initCommand(nil, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "--lock") {
		t.Fatal("ambiguous locks picked silently", err)
	}
}

// A command-only entry reaches the real queue API without targets and carries
// the queue's coverage stamp; a production override still needs targets.
func TestCommandOnlyEntrySubmitsWithoutTargets(t *testing.T) {
	root := t.TempDir()
	conversionWrite(t, filepath.Join(root, "source", "run.sh"), "echo ok\n")
	q, clock, db, _ := waitQueue(t)
	if err := q.ConfigureFileStore(db+".files", 1<<20, true); err != nil {
		t.Fatal(err)
	}
	if _, err := currentCLICheckin(q, clock, myce.Checkin{Machine: myce.Machine{ID: "cpu-worker", Session: "session", GPU: "cpu", CPUIsolation: "select-none", CapabilityProfile: &myce.CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH, LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 16 << 30}, Capabilities: []string{"native_isolation_landlock", "native_isolation_seatbelt"}, HostMemoryMeasured: true, HostAvailableMemoryGB: 12}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "entry-fixture"}.Handler())
	defer server.Close()
	a, err := decodeAdapter([]byte("project: tiny\nentries:\n  test:\n    source: {directory: source}\n    run: {command: [sh, run.sh]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	p, data, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	var out, diagnostics bytes.Buffer
	// Missing notification configuration returns 74 after a durable submit.
	if err := runPreparedProfile(t.Context(), root, p, data, nil, nil, "smallest", filepath.Join(root, "absent-notifier"), "", "", server.URL, "entry-fixture", &out, &diagnostics); exitCode(err) != 74 {
		t.Fatalf("submit: %v %s %s", err, &out, &diagnostics)
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out.Bytes(), &receipt); err != nil || receipt.JobID == "" || len(receipt.Spec.Targets) != 0 {
		t.Fatalf("receipt: %v %s", err, &out)
	}
	j, err := (&myce.Client{URL: server.URL, Token: "entry-fixture"}).GetJob(t.Context(), receipt.JobID)
	if err != nil || j.EntryCoverage == nil || j.EntryCoverage.Key != "tiny/test" {
		t.Fatalf("queue job lacks the coverage stamp: %+v %v", j, err)
	}
	production := "production"
	out.Reset()
	if err := runPreparedProfile(t.Context(), root, p, data, nil, nil, "smallest-production", "", "", "", server.URL, "entry-fixture", &out, &diagnostics, profileOverrides{Class: &production}); exitCode(err) != 64 || !strings.Contains(err.Error(), "explicit targets") {
		t.Fatal("production override without targets submitted", err)
	}
}
