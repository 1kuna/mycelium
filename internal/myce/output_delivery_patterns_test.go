package myce

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// One real queue, worker and file store: folder and glob outputs, the store's
// limit refusal, and keep_on_failure for a failed attempt (#1341).
func TestOutputPatternsAndKeepOnFailureDev(t *testing.T) {
	root := t.TempDir()
	d, err := startTestDev(t, DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "q.db"), FileStore: filepath.Join(root, "files"), FileStoreLimit: 256 << 10, Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c := d.Client()
	input := filepath.Join(root, "input")
	if err = os.WriteFile(input, []byte(pytestFailureXML), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.UploadJobInput(ctx, input, ref); err != nil {
		t.Fatal(err)
	}
	run := func(name, command string, edit func(*Spec)) *Job {
		t.Helper()
		s := Spec{Name: name, Project: "test", Files: FileManifest{"input": ref}, Run: RunSpec{Command: []string{"sh", "-c", command}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}}
		edit(&s)
		j, err := c.Submit(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		if j, err = c.Wait(ctx, j.ID); err != nil || j == nil {
			t.Fatalf("%s: %+v %v", name, j, err)
		}
		return j
	}

	t.Run("folder and glob", func(t *testing.T) {
		j := run("patterns", `mkdir -p out/sub && echo a > out/a.txt && echo b > out/sub/b.txt && echo 1 > r1.json && echo 2 > r2.json && echo no > skip.log`, func(s *Spec) {
			s.Outputs = map[string]string{"out/": "results", "*.json": "results"}
		})
		if j.State != "succeeded" {
			t.Fatalf("pattern job: %s %s %+v", j.State, j.Reason, j.ExecutionCause)
		}
		want := []string{"out/a.txt", "out/sub/b.txt", "r1.json", "r2.json"}
		if got := slices.Sorted(maps.Keys(j.OutputRefs)); !slices.Equal(got, want) {
			t.Fatalf("manifest %v, want %v (input, skip.log and worker files excluded)", got, want)
		}
		for name, content := range map[string]string{"out/sub/b.txt": "b\n", "r2.json": "2\n"} {
			dest := filepath.Join(root, "download", name)
			if err := c.DownloadResult(ctx, j.ID, name, dest); err != nil {
				t.Fatal(err)
			}
			if got, _ := os.ReadFile(dest); string(got) != content {
				t.Fatalf("%s content %q", name, got)
			}
		}
	})

	t.Run("limit refusal", func(t *testing.T) {
		j := run("too-big", `mkdir big && for i in 1 2 3; do head -c 200000 /dev/urandom > big/$i.bin; done`, func(s *Spec) {
			s.Outputs = map[string]string{"big/": "results"}
		})
		f := j.ExecutionCause
		if j.State != "failed" || f == nil || f.Finalization == nil || f.Finalization.Step != "output_preflight" || f.Finalization.Error != "limit_exceeded" || len(j.OutputRefs) != 0 {
			t.Fatalf("want one limit_exceeded refusal: %s %s %+v", j.State, j.Reason, f)
		}
		if !strings.Contains(j.Reason+f.String(), "exceed file-store") {
			t.Fatalf("refusal is not clear: %q %q", j.Reason, f.String())
		}
	})

	t.Run("keep on failure", func(t *testing.T) {
		j := run("kept", `mkdir -p logs && echo first > logs/one.log && echo second > logs/two.log && cp input report.xml && exit 3`, func(s *Spec) {
			s.Outputs = map[string]string{"never.txt": "results"}
			s.KeepOnFailure = []KeepFile{{Path: "logs/"}, {Path: "report.xml", Format: "pytest-junit"}, {Path: "absent/*.log"}}
		})
		if j.State != "failed" || len(j.OutputRefs) != 0 {
			t.Fatalf("failed attempt accepted outputs: %s %v", j.State, j.OutputRefs)
		}
		files, err := DiagnosticFiles(j, j.Attempt)
		if err != nil {
			t.Fatalf("kept files: %v history=%+v", err, j.AttemptHistory)
		}
		for _, name := range []string{"kept/logs/one.log", "kept/logs/two.log", "kept/report.xml", "summary.json", "log-tail.txt"} {
			if _, ok := files[name]; !ok {
				t.Fatalf("missing %s in %v", name, slices.Sorted(maps.Keys(files)))
			}
		}
		dest := filepath.Join(root, "diag")
		for _, name := range []string{"kept/logs/two.log", "summary.json"} {
			if err := c.DownloadDiagnostic(ctx, j.ID, j.Attempt, name, filepath.Join(dest, name)); err != nil {
				t.Fatal(err)
			}
		}
		if got, _ := os.ReadFile(filepath.Join(dest, "kept/logs/two.log")); string(got) != "second\n" {
			t.Fatalf("kept content %q", got)
		}
		var summary FailureSummary
		data, _ := os.ReadFile(filepath.Join(dest, "summary.json"))
		if err := json.Unmarshal(data, &summary); err != nil {
			t.Fatal(err)
		}
		if len(summary.FailedTests) != 1 || summary.FailedTests[0].ID != "tests/test_math.py::TestMath::test_sum[two]" || len(summary.ReportErrors) != 1 || !strings.Contains(summary.ReportErrors[0], "keep_on_failure 2") {
			t.Fatalf("format parse or missing-entry report: %s", data)
		}
	})
}

// Results stay attempt-specific: a superseded attempt cannot publish an
// expanded folder manifest, and the current one cannot add undeclared files.
func TestOutputPatternsRejectStaleAttempt(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	ctx := context.Background()
	refs := FileManifest{}
	for _, name := range []string{"input", "out/a.txt", "other.txt"} {
		src := filepath.Join(t.TempDir(), "f")
		if err := os.WriteFile(src, []byte("bytes of "+name), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.UploadJobInput(ctx, src, ref); err != nil {
			t.Fatal(err)
		}
		refs[name] = ref
	}
	s := spec("experiment")
	s.Files = FileManifest{"input": refs["input"]}
	s.Outputs = map[string]string{"out/": "results"}
	submit(t, q, s)
	m := machine("worker", "fake")
	first := check(t, q, m, nil).Job
	if err := q.update(func() error { q.requeue(q.s.Jobs[first.ID], "retry", false); return nil }); err != nil {
		t.Fatal(err)
	}
	second := check(t, q, machine("retry-worker", "fake"), nil).Job
	if second == nil || second.Attempt == first.Attempt {
		t.Fatal("missing retry")
	}
	old := result(first, true)
	old.OutputRefs = FileManifest{"out/a.txt": refs["out/a.txt"]}
	if err := q.Complete(old); !errors.Is(err, ErrStale) {
		t.Fatalf("stale folder result: %v", err)
	}
	current := result(second, true)
	current.OutputRefs = FileManifest{"out/a.txt": refs["out/a.txt"], "other.txt": refs["other.txt"]}
	if err := q.Complete(current); err == nil || errors.Is(err, ErrStale) {
		t.Fatalf("undeclared file accepted: %v", err)
	}
	current.OutputRefs = FileManifest{}
	if err := q.Complete(current); err == nil {
		t.Fatal("empty folder output accepted")
	}
	current.OutputRefs = FileManifest{"out/a.txt": refs["out/a.txt"]}
	if err := q.Complete(current); err != nil {
		t.Fatal(err)
	}
	if j := job(t, q, second.ID); j.State != "succeeded" || !maps.Equal(j.OutputRefs, current.OutputRefs) {
		t.Fatalf("current folder result: %s %v", j.State, j.OutputRefs)
	}
}

func TestOutputDeclarationsAndKeepFileForms(t *testing.T) {
	for decl, ok := range map[string]bool{"out/": true, "a/b/": true, "*.json": true, "runs/*/log.txt": true, "file.txt": true, "/abs/": false, "../up/": false, "out//": false, "[bad": false, "a/../b/*": false} {
		if validOutputDeclaration(decl) != ok {
			t.Errorf("validOutputDeclaration(%q) != %v", decl, ok)
		}
	}
	outputs := map[string]string{"out/": "r", "runs/*": "r", "a.txt": "r"}
	if err := validateOutputManifest(outputs, FileManifest{"out/x": {}, "runs/1/y": {}, "a.txt": {}}); err != nil {
		t.Fatal(err)
	}
	if err := validateOutputManifest(outputs, FileManifest{"out/x": {}, "a.txt": {}}); err == nil {
		t.Fatal("glob with no files accepted")
	}
	if err := validateOutputManifest(outputs, FileManifest{"out/x": {}, "runs/1/y": {}, "a.txt": {}, "outside": {}}); err == nil {
		t.Fatal("undeclared file accepted")
	}
	var fromJSON, fromYAML struct {
		Keep []KeepFile `json:"keep_on_failure" yaml:"keep_on_failure"`
	}
	if err := json.Unmarshal([]byte(`{"keep_on_failure":["logs/",{"path":"junit.xml","format":"vitest-junit"}]}`), &fromJSON); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte("keep_on_failure:\n  - logs/\n  - {path: junit.xml, format: vitest-junit}\n"), &fromYAML); err != nil {
		t.Fatal(err)
	}
	want := []KeepFile{{Path: "logs/"}, {Path: "junit.xml", Format: "vitest-junit"}}
	if !slices.Equal(fromJSON.Keep, want) || !slices.Equal(fromYAML.Keep, want) {
		t.Fatalf("keep forms: %+v %+v", fromJSON.Keep, fromYAML.Keep)
	}
	if data, _ := json.Marshal(want); string(data) != `[{"path":"logs/"},{"path":"junit.xml","format":"vitest-junit"}]` {
		t.Fatalf("keep encoding %s", data)
	}
	s := spec("experiment")
	s.Files = FileManifest{"input": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	s.Outputs = map[string]string{"out/": "results"}
	if err := s.Validate(); err != nil {
		t.Fatalf("folder output refused: %v", err)
	}
	s.After = &PieceDependency{Job: "parent", Outputs: map[string]string{"in": "out"}}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "folder and glob outputs") {
		t.Fatalf("after-stage folder output: %v", err)
	}
	s.After, s.KeepOnFailure = nil, []KeepFile{{Path: "logs/", Format: "junit"}}
	if err := s.Validate(); err == nil {
		t.Fatal("unknown keep format accepted")
	}
	if err := validateDiagnosticManifest([]KeepFile{{Path: "logs/"}}, &AttemptDiagnostics{Files: FileManifest{"summary.json": {}, "log-tail.txt": {}, "kept/secret.txt": {}}}); err == nil {
		t.Fatal("undeclared kept file accepted")
	}
}
