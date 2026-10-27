package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const pytestFailureXML = `<testsuites><testsuite><testcase classname="tests.test_math.TestMath" name="test_sum[two]" file="tests/test_math.py"><failure>assert 1 == 2</failure></testcase><testcase classname="tests.test_math" name="passing" file="tests/test_math.py"/></testsuite></testsuites>`
const vitestFailureXML = `<testsuites><testsuite><testcase classname="src/math.test.ts" name="sum &gt; handles negatives"><failure>expected 2</failure></testcase></testsuite></testsuites>`

func TestFailureArtifactsWorker(t *testing.T) {
	cases := []struct{ name, format, report, id string }{
		{"pytest", "pytest-junit", pytestFailureXML, "tests/test_math.py::TestMath::test_sum[two]"},
		{"vitest", "vitest-junit", vitestFailureXML, "sum > handles negatives"},
		{"startup", "pytest-junit", "", ""},
		{"exec-startup", "pytest-junit", "", ""},
		{"malformed", "pytest-junit", "<testsuites><broken>", ""},
	}
	if os.Getenv("MYCE_PYTEST_PYTHON") != "" {
		cases = append(cases, struct{ name, format, report, id string }{"real-pytest", "pytest-junit", "def test_failure():\n    assert 1 == 2\n", "test_failure.py::test_failure"})
	}
	if os.Getenv("MYCE_VITEST_CLI") != "" && os.Getenv("MYCE_TEST_NODE") != "" {
		module := filepath.ToSlash(filepath.Join(filepath.Dir(os.Getenv("MYCE_VITEST_CLI")), "dist", "index.js"))
		cases = append(cases, struct{ name, format, report, id string }{"real-vitest", "vitest-junit", "import {test, expect} from 'file://" + module + "';\ntest('deliberate frontend failure', () => expect(1).toBe(2));\n", "deliberate frontend failure"})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "q.db"), FileStore: filepath.Join(root, "files"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			c := d.Client()
			input := filepath.Join(root, "report")
			if err = os.WriteFile(input, []byte(tc.report), 0600); err != nil {
				t.Fatal(err)
			}
			ref, err := IdentifyFile(input)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.UploadJobInput(ctx, input, ref); err != nil {
				t.Fatal(err)
			}
			command := `cp input report.xml; echo failing-command-tail; exit 7`
			if tc.name == "startup" {
				command = `echo 'npm: command not found'; exit 127`
			}
			inputName := "input"
			commandArgs := []string{"sh", "-c", command}
			if tc.name == "real-pytest" {
				inputName = "test_failure.py"
				commandArgs = []string{os.Getenv("MYCE_PYTEST_PYTHON"), "-m", "pytest", "-q", "--junitxml=report.xml", "-o", "junit_family=xunit1", "test_failure.py"}
			}
			if tc.name == "real-vitest" {
				inputName = "math.test.mjs"
				commandArgs = []string{os.Getenv("MYCE_TEST_NODE"), os.Getenv("MYCE_VITEST_CLI"), "run", "math.test.mjs", "--reporter=junit", "--outputFile=report.xml", "--maxWorkers=1", "--no-file-parallelism"}
			}
			if tc.name == "exec-startup" {
				commandArgs = []string{"/no-such-mycelium-test-command"}
			}
			j, err := c.Submit(ctx, Spec{Name: tc.name, Project: "test", Files: FileManifest{inputName: ref}, Outputs: map[string]string{"success.txt": "results"}, Run: RunSpec{Command: commandArgs}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}, Diagnostics: &DiagnosticSpec{Reports: []DiagnosticReport{{Path: "report.xml", Format: tc.format}}}})
			if err != nil {
				t.Fatal(err)
			}
			j, err = c.Wait(ctx, j.ID)
			if err != nil || j == nil || j.State != "failed" {
				t.Fatalf("failure lost: %+v %v", j, err)
			}
			if len(j.OutputRefs) != 0 {
				t.Fatal("failure accepted success outputs")
			}
			files, err := DiagnosticFiles(j, j.Attempt)
			if err != nil {
				t.Fatalf("diagnostics: %+v; history=%+v cause=%+v reason=%s", err, j.AttemptHistory, j.ExecutionCause, j.Reason)
			}
			for name := range files {
				if err = c.DownloadDiagnostic(ctx, j.ID, j.Attempt, name, filepath.Join(root, "download", name)); err != nil {
					t.Fatal(err)
				}
			}
			data, err := os.ReadFile(filepath.Join(root, "download", "summary.json"))
			if err != nil {
				t.Fatal(err)
			}
			var summary FailureSummary
			if err = json.Unmarshal(data, &summary); err != nil {
				t.Fatal(err)
			}
			ended := j.AttemptHistory[len(j.AttemptHistory)-1]
			if summary.ID != j.ID || summary.Attempt != ended.Attempt || summary.Machine != ended.Machine || summary.Session != ended.Session || summary.Success || summary.ExecutionCause == nil || tc.name != "exec-startup" && summary.ExecutionCause.ExitCode == nil {
				t.Fatalf("identity/cause missing: %s", data)
			}
			if tc.id != "" {
				if len(summary.FailedTests) != 1 || summary.FailedTests[0].ID != tc.id {
					t.Fatalf("identifier missing: %s", data)
				}
			} else if len(summary.FailedTests) != 0 || len(summary.ReportErrors) == 0 {
				t.Fatalf("invented test or lost report error: %s", data)
			}
			tail, _ := os.ReadFile(filepath.Join(root, "download", "log-tail.txt"))
			if tc.name == "real-pytest" && !strings.Contains(string(tail), "FAILED test_failure.py::test_failure") {
				t.Fatalf("real pytest failure missing: %s", tail)
			}
			if tc.name == "exec-startup" && len(tail) != 0 {
				t.Fatalf("invented startup tail: %s", tail)
			}
			if tc.name != "real-pytest" && tc.name != "real-vitest" && tc.name != "exec-startup" && !strings.Contains(string(tail), map[bool]string{true: "npm: command not found", false: "failing-command-tail"}[tc.name == "startup"]) {
				t.Fatalf("tail missing: %q", tail)
			}
			if err = c.DownloadResult(ctx, j.ID, "success.txt", filepath.Join(root, "success")); err == nil {
				t.Fatal("failed job became accepted result")
			}
		})
	}
}

func TestFailureArtifactsFenceRetentionAndVerification(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureFileRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "file")
	data := []byte("diagnostic bytes")
	if err := os.WriteFile(src, data, 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.UploadJobInput(ctx, src, ref); err != nil {
		t.Fatal(err)
	}
	m := machine("worker", "fake")
	check(t, q, m, nil)
	s := spec("experiment")
	s.Files = FileManifest{"input": ref}
	s.Diagnostics = &DiagnosticSpec{}
	submit(t, q, s)
	first := check(t, q, m, nil).Job
	old := result(first, false)
	old.Diagnostics = &AttemptDiagnostics{Files: FileManifest{"summary.json": ref, "log-tail.txt": ref}}
	if err = q.update(func() error { q.requeue(q.s.Jobs[first.ID], "retry", false); return nil }); err != nil {
		t.Fatal(err)
	}
	second := check(t, q, m, nil).Job
	if second == nil || second.Attempt == first.Attempt {
		t.Fatal("missing retry")
	}
	if err = q.Complete(old); !errors.Is(err, ErrStale) {
		t.Fatalf("stale diagnostics: %v", err)
	}
	current := result(second, false)
	current.Error = "exit status 7"
	current.Diagnostics = old.Diagnostics
	// Corruption cannot publish diagnostic references.
	if err = os.WriteFile(q.files.blob(ref), []byte("corrupt bytes!!!"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = q.Complete(current); err == nil {
		t.Fatal("corrupt diagnostic accepted")
	}
	if err = os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = q.Complete(current); err != nil {
		t.Fatal(err)
	}
	j, err := c.GetJob(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(j.OutputRefs) != 0 {
		t.Fatal("failure outputs accepted")
	}
	if _, err = DiagnosticFiles(j, first.Attempt); err == nil {
		t.Fatal("stale diagnostics published")
	}
	dest := filepath.Join(t.TempDir(), "tail")
	if err = c.DownloadDiagnostic(ctx, j.ID, j.Attempt, "log-tail.txt", dest); err != nil {
		t.Fatal(err)
	}
	// Downloads use length/hash verification and atomic replacement.
	if err = os.WriteFile(q.files.blob(ref), []byte("corrupt bytes!!!"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.DownloadDiagnostic(ctx, j.ID, j.Attempt, "log-tail.txt", dest); err == nil {
		t.Fatal("corrupt download accepted")
	}
	got, _ := os.ReadFile(dest)
	if string(got) != string(data) {
		t.Fatal("corrupt download replaced destination")
	}
	if err = os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(q.files.blob(ref), clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + 2*time.Hour)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err = c.DownloadDiagnostic(ctx, j.ID, j.Attempt, "log-tail.txt", dest); !isResultExpired(err) {
		t.Fatalf("diagnostic did not expire: %v", err)
	}
	if _, err = os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatalf("expired diagnostics retained: %v", err)
	}
}

func TestFailureArtifactsAdaptersAndBounds(t *testing.T) {
	tests, err := junitFailures([]byte(`<testsuite><testcase name="test_x" classname="a"><properties><property name="nodeid" value="tests/a.py::test_x"/></properties><error/></testcase></testsuite>`), "pytest-junit")
	if err != nil || len(tests) != 1 || tests[0].ID != "tests/a.py::test_x" {
		t.Fatalf("xunit2 property: %+v %v", tests, err)
	}
	if _, err = junitFailures([]byte(`<testsuite><testcase name="test_x" classname="ambiguous"><failure/></testcase></testsuite>`), "pytest-junit"); err == nil {
		t.Fatal("guessed pytest ID")
	}
	path := filepath.Join(t.TempDir(), "large")
	if err = os.WriteFile(path, []byte(strings.Repeat("x", diagnosticTailBytes)+"tail"), 0600); err != nil {
		t.Fatal(err)
	}
	tail, err := readDiagnostic(path, diagnosticTailBytes, true)
	if err != nil || len(tail) != diagnosticTailBytes || !strings.HasSuffix(string(tail), "tail") {
		t.Fatalf("bounded tail: %d %v", len(tail), err)
	}
	if _, err = readDiagnostic(path, 10, false); err == nil {
		t.Fatal("oversize report read")
	}
}
