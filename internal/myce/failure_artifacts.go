package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Limits bound post-failure transfer and parsing, independently of job output size.
// 64 KiB preserves startup errors; 1 MiB per report allows targeted test suites.
const diagnosticTailBytes = 64 << 10
const diagnosticReportBytes = 1 << 20
const diagnosticReportLimit = 8

type DiagnosticSpec struct {
	Reports []DiagnosticReport `json:"reports,omitempty" yaml:"reports,omitempty"`
}
type DiagnosticReport struct {
	Path   string `json:"path" yaml:"path"`
	Format string `json:"format" yaml:"format"`
}
type AttemptDiagnostics struct {
	Files FileManifest `json:"files"`
}
type FailedTest struct {
	ID        string `json:"id"`
	Classname string `json:"classname,omitempty"`
	File      string `json:"file,omitempty"`
}
type FailureSummary struct {
	Execution
	Machine        string        `json:"machine"`
	Session        string        `json:"session"`
	Success        bool          `json:"success"`
	Error          string        `json:"error,omitempty"`
	ExecutionCause *RuntimeCause `json:"execution_cause,omitempty"`
	CleanupCause   *RuntimeCause `json:"cleanup_cause,omitempty"`
	FailedTests    []FailedTest  `json:"failed_tests"`
	ReportErrors   []string      `json:"report_errors,omitempty"`
}

func validateDiagnosticSpec(s Spec) error {
	if s.Diagnostics == nil {
		return nil
	}
	if len(s.Files) == 0 || s.Run.Docker || s.Execution != nil || s.extensionJob() {
		return errors.New("diagnostics require a plain command with stored input files")
	}
	if len(s.Diagnostics.Reports) > diagnosticReportLimit {
		return errors.New("diagnostics allow at most eight reports")
	}
	for _, r := range s.Diagnostics.Reports {
		if !safeRelative(r.Path) || (r.Format != "pytest-junit" && r.Format != "vitest-junit") {
			return errors.New("diagnostic reports require a relative path and pytest-junit or vitest-junit format")
		}
	}
	return nil
}

// Adapter code runs on the worker. The queue only verifies immutable references.
// pytest xunit1 records file and classname; xunit2 needs an explicit nodeid property.
func junitFailures(data []byte, format string) ([]FailedTest, error) {
	type property struct {
		Name  string `xml:"name,attr"`
		Value string `xml:"value,attr"`
	}
	type testCase struct {
		Name       string     `xml:"name,attr"`
		Classname  string     `xml:"classname,attr"`
		File       string     `xml:"file,attr"`
		NodeID     string     `xml:"nodeid,attr"`
		Properties []property `xml:"properties>property"`
		Failures   []struct{} `xml:"failure"`
		Errors     []struct{} `xml:"error"`
	}
	d := xml.NewDecoder(bytes.NewReader(data))
	var failed []FailedTest
	root, depth, closed := "", 0, false
	for {
		token, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if depth == 0 {
				if closed || (token.Name.Local != "testsuites" && token.Name.Local != "testsuite") {
					return nil, errors.New("expected JUnit testsuite(s)")
				}
				root = token.Name.Local
			}
			if token.Name.Local != "testcase" {
				depth++
				continue
			}
			var tc testCase
			if err := d.DecodeElement(&tc, &token); err != nil {
				return nil, err
			}
			if len(tc.Failures)+len(tc.Errors) == 0 {
				continue
			}
			id := tc.Name
			if format == "pytest-junit" {
				id = tc.NodeID
				for _, p := range tc.Properties {
					if p.Name == "nodeid" {
						id = p.Value
					}
				}
				if id == "" && tc.File != "" {
					module := strings.ReplaceAll(strings.TrimSuffix(tc.File, ".py"), "/", ".")
					if tc.Classname == module {
						id = tc.File + "::" + tc.Name
					} else if strings.HasPrefix(tc.Classname, module+".") {
						id = tc.File + "::" + strings.ReplaceAll(strings.TrimPrefix(tc.Classname, module+"."), ".", "::") + "::" + tc.Name
					}
				}
			}
			if id == "" {
				return nil, errors.New("failed testcase has no exact identifier; pytest requires xunit1 file/classname or nodeid property")
			}
			failed = append(failed, FailedTest{ID: id, Classname: tc.Classname, File: tc.File})
		case xml.EndElement:
			depth--
			if depth == 0 {
				closed = true
			}
		}
	}
	if root == "" || !closed || depth != 0 {
		return nil, errors.New("incomplete JUnit report")
	}
	return failed, nil
}

func readDiagnostic(path string, limit int64, tail bool) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	if tail && st.Size() > limit {
		if _, err = f.Seek(-limit, io.SeekEnd); err != nil {
			return nil, err
		}
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(data)) > limit {
		err = errors.New("diagnostic exceeds byte limit")
	}
	return data, err
}

func (w Worker) finishFailureDiagnostics(j Job, result Result, parent, logPath string) (Result, error) {
	if j.StorageAllocation != nil && j.StorageAllocation.WorkerCustody {
		return result, nil
	}
	if j.Spec.Diagnostics == nil || result.Success || result.ReleaseOnly {
		return result, nil
	}
	if result.Diagnostics != nil {
		return result, nil
	}
	stage, err := os.MkdirTemp(parent, ".failure-diagnostics-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(stage)
	summary := FailureSummary{Execution: result.Execution, Machine: result.Machine, Session: result.Session, Error: result.Error,
		ExecutionCause: result.ExecutionCause, CleanupCause: result.CleanupCause, FailedTests: []FailedTest{}}
	files := map[string][]byte{}
	tail := []byte{}
	if j.Spec.Logs != "none" && logPath != "" {
		tail, err = readDiagnostic(logPath, diagnosticTailBytes, true)
		if err != nil {
			summary.ReportErrors = append(summary.ReportErrors, "log tail unavailable")
			tail = nil
		}
	}
	files["log-tail.txt"] = tail
	for i, report := range j.Spec.Diagnostics.Reports {
		path, e := confinedPath(j.Spec.Run.Workdir, report.Path)
		var data []byte
		if e == nil {
			data, e = readDiagnostic(path, diagnosticReportBytes, false)
		}
		if e != nil {
			summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("report %d unavailable or oversized", i))
			continue
		}
		files[fmt.Sprintf("reports/%d.xml", i)] = data
		tests, e := junitFailures(data, report.Format)
		if e != nil {
			summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("report %d invalid or lacks exact identifiers", i))
			continue
		}
		summary.FailedTests = append(summary.FailedTests, tests...)
	}
	files["summary.json"], err = json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return result, err
	}
	if len(files["summary.json"]) > diagnosticReportBytes {
		summary.FailedTests = []FailedTest{}
		summary.ReportErrors = append(summary.ReportErrors, "structured identifiers exceed summary byte limit; see raw reports")
		files["summary.json"], err = json.MarshalIndent(summary, "", "  ")
		if err != nil {
			return result, err
		}
	}
	refs := FileManifest{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for name, data := range files {
		path := filepath.Join(stage, filepath.FromSlash(name))
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return result, err
		}
		if err = os.WriteFile(path, data, 0600); err != nil {
			return result, err
		}
		ref, e := IdentifyFile(path)
		if e != nil {
			return result, e
		}
		// Job-input upload eligibility also reclaims diagnostics orphaned before completion.
		if err = w.Client.UploadJobInput(ctx, path, ref); err != nil {
			return result, err
		}
		refs[name] = ref
	}
	result.Diagnostics = &AttemptDiagnostics{Files: refs}
	return result, nil
}

func validateDiagnosticManifest(d *AttemptDiagnostics) error {
	if d == nil {
		return nil
	}
	if err := d.Files.validate(); err != nil {
		return err
	}
	if len(d.Files) < 2 || len(d.Files) > diagnosticReportLimit+2 {
		return errors.New("invalid diagnostic manifest")
	}
	for name, ref := range d.Files {
		limit := int64(diagnosticReportBytes)
		switch name {
		case "log-tail.txt":
			limit = diagnosticTailBytes
		case "summary.json": // Bounded by report input, plus structured identities.
			limit = diagnosticReportBytes
		default:
			valid := false
			for i := 0; i < diagnosticReportLimit; i++ {
				valid = valid || name == fmt.Sprintf("reports/%d.xml", i)
			}
			if !valid {
				return errors.New("invalid diagnostic filename")
			}
		}
		if ref.SizeBytes > limit {
			return errors.New("diagnostic exceeds byte limit")
		}
	}
	if _, ok := d.Files["summary.json"]; !ok {
		return errors.New("diagnostic summary required")
	}
	if _, ok := d.Files["log-tail.txt"]; !ok {
		return errors.New("diagnostic log tail required")
	}
	return nil
}

// DiagnosticFiles selects queue-recorded failed execution evidence. A caller
// cannot turn arbitrary worker references into accepted results through this API.
func DiagnosticFiles(j *Job, attempt int) (FileManifest, error) {
	if j.FilesExpired {
		return nil, resultExpired()
	}
	for _, a := range j.AttemptHistory {
		if a.Attempt == attempt && a.State == "failed" && a.Diagnostics != nil {
			return a.Diagnostics.Files, nil
		}
	}
	return nil, &HTTPError{Status: 404, Code: "diagnostics_unknown", Message: "failed attempt diagnostics not found"}
}

func (c Client) DownloadDiagnostic(ctx context.Context, id string, attempt int, name, dest string) error {
	j, err := c.GetJob(ctx, id)
	if err != nil {
		return err
	}
	files, err := DiagnosticFiles(j, attempt)
	if err != nil {
		return err
	}
	ref, ok := files[name]
	if !ok {
		return &HTTPError{Status: 404, Code: "artifact_unknown", Message: "diagnostic artifact not found"}
	}
	return c.downloadResultFile(ctx, ref, dest)
}
