package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type DiagnosticSpec struct {
	Reports []DiagnosticReport `json:"reports,omitempty" yaml:"reports,omitempty"`
}
type DiagnosticReport struct {
	Path   string `json:"path" yaml:"path"`
	Format string `json:"format" yaml:"format"`
}

// KeepFile is one keep_on_failure entry: a file, folder ("dir/") or glob, given
// as a plain string or as {path, format}; replies use the object form. An
// optional pytest-junit or vitest-junit format also parses failed test
// identifiers into summary.json.
type KeepFile struct {
	Path   string `json:"path" yaml:"path"`
	Format string `json:"format,omitempty" yaml:"format,omitempty"`
}

func (k *KeepFile) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		*k = KeepFile{}
		return json.Unmarshal(data, &k.Path)
	}
	type plain KeepFile
	return json.Unmarshal(data, (*plain)(k))
}
func (k *KeepFile) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		*k = KeepFile{}
		return node.Decode(&k.Path)
	}
	type plain KeepFile
	return node.Decode((*plain)(k))
}

func (s Spec) capturesFailure() bool { return s.Diagnostics != nil || len(s.KeepOnFailure) > 0 }

func junitFormat(format string) bool { return format == "pytest-junit" || format == "vitest-junit" }

type AttemptDiagnostics struct {
	Files FileManifest `json:"files"`
	// Queue-owned safe metadata, available even without optional artifact capture.
	Summary *FailureSummary `json:"summary,omitempty"`
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
	if !s.capturesFailure() {
		return nil
	}
	if len(s.Files) == 0 || s.Execution != nil || s.extensionJob() {
		return errors.New("diagnostics and keep_on_failure require a plain command with stored input files")
	}
	if s.Diagnostics != nil {
		for _, r := range s.Diagnostics.Reports {
			if !safeRelative(r.Path) || !junitFormat(r.Format) {
				return errors.New("diagnostic reports require a relative path and pytest-junit or vitest-junit format")
			}
		}
	}
	seen := map[string]bool{}
	for _, k := range s.KeepOnFailure {
		if !validOutputDeclaration(k.Path) || k.Format != "" && !junitFormat(k.Format) || seen[k.Path] {
			return errors.New("keep_on_failure requires unique relative files, folders (dir/) or globs, with optional pytest-junit or vitest-junit format")
		}
		seen[k.Path] = true
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

func (w Worker) finishFailureDiagnostics(j Job, result Result, parent, logPath string) (finished Result, err error) {
	defer func() {
		if err != nil {
			finished = finalizationResult(finished, "diagnostic_publication", "", err, false)
			err = finalizationErrorFor("diagnostic_publication", "", err)
		}
	}()
	if !j.Spec.capturesFailure() || result.Success || result.ReleaseOnly {
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
	if result.ExecutionCause != nil && result.ExecutionCause.Finalization != nil {
		summary.ExecutionCause = safeJobRuntimeCause(j.Spec, result.ExecutionCause)
		summary.CleanupCause = safeJobRuntimeCause(j.Spec, result.CleanupCause)
		summary.Error = summary.ExecutionCause.String()
	}
	remaining, releaseBudget := reserveDiagnosticBytes(parent, ^uint64(0))
	defer releaseBudget()
	if remaining == 0 {
		return result, nil
	}
	files := map[string][]byte{}
	tail := []byte{}
	if j.Spec.Logs != "none" && logPath != "" {
		tail, err = readDiagnostic(logPath, remaining, true)
		if err != nil {
			summary.ReportErrors = append(summary.ReportErrors, "log tail unavailable")
			tail = nil
		}
	}
	files["log-tail.txt"] = tail
	remaining -= int64(len(tail))
	var reports []DiagnosticReport
	if j.Spec.Diagnostics != nil {
		reports = j.Spec.Diagnostics.Reports
	}
	for i, report := range reports {
		path, e := confinedPath(j.Spec.Run.Workdir, report.Path)
		var data []byte
		if e == nil {
			data, e = readDiagnostic(path, remaining, false)
		}
		if e != nil {
			summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("report %d unavailable or oversized", i))
			continue
		}
		files[fmt.Sprintf("reports/%d.xml", i)] = data
		remaining -= int64(len(data))
		tests, e := junitFailures(data, report.Format)
		if e != nil {
			summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("report %d invalid or lacks exact identifiers", i))
			continue
		}
		summary.FailedTests = append(summary.FailedTests, tests...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = withFileStoreAttempt(ctx, j)
	kept, keptRefs, err := w.collectKeptFiles(ctx, j, &summary, &remaining)
	if err != nil {
		return result, err
	}
	files["summary.json"], err = json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return result, err
	}
	if int64(len(files["summary.json"])) > remaining {
		summary.FailedTests = []FailedTest{}
		summary.ReportErrors = append(summary.ReportErrors, "structured identifiers exceed summary byte limit; see raw reports")
		files["summary.json"], err = json.MarshalIndent(summary, "", "  ")
		if err != nil {
			return result, err
		}
	}
	refs := FileManifest{}
	for _, name := range slices.Sorted(maps.Keys(kept)) {
		// Kept files stream from the workdir; their identity was taken above.
		if err = w.Client.UploadJobInput(ctx, kept[name], keptRefs[name]); err != nil {
			return result, err
		}
		refs["kept/"+name] = keptRefs[name]
	}
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

// collectKeptFiles resolves keep_on_failure entries one at a time, so one
// missing entry never drops the others, and preflights the whole kept set:
// a set the file store cannot admit is dropped with one summary refusal.
// Formatted entries are parsed within the diagnostic memory budget.
func (w Worker) collectKeptFiles(ctx context.Context, j Job, summary *FailureSummary, remaining *int64) (map[string]string, FileManifest, error) {
	kept, refs := map[string]string{}, FileManifest{}
	for i, k := range j.Spec.KeepOnFailure {
		sources, err := collectOutputFiles(j.Spec.Run.Workdir, map[string]string{k.Path: ""}, jobInputFiles(&j))
		if err != nil {
			summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("keep_on_failure %d matched no regular files or is unreadable", i))
			continue
		}
		for _, name := range slices.Sorted(maps.Keys(sources)) {
			if _, ok := refs[name]; ok {
				continue
			}
			ref, err := IdentifyFile(sources[name])
			if err != nil {
				summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("keep_on_failure %d has an unreadable file", i))
				continue
			}
			kept[name], refs[name] = sources[name], ref
			if k.Format == "" {
				continue
			}
			data, err := readDiagnostic(sources[name], *remaining, false)
			if err != nil {
				summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("keep_on_failure %d report oversized for parsing", i))
				continue
			}
			*remaining -= int64(len(data))
			tests, err := junitFailures(data, k.Format)
			if err != nil {
				summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("keep_on_failure %d report invalid or lacks exact identifiers", i))
				continue
			}
			summary.FailedTests = append(summary.FailedTests, tests...)
		}
	}
	if len(refs) == 0 {
		return kept, refs, nil
	}
	if err := w.Client.PreflightFileStore(ctx, refs); fileStoreLimitRefusal(err) {
		log.Printf("job %s kept files refused by the file store (%d files): %v", ShortID(j.ID), len(refs), err)
		summary.ReportErrors = append(summary.ReportErrors, fmt.Sprintf("keep_on_failure files exceed file-store limits (%d files); none kept", len(refs)))
		return map[string]string{}, FileManifest{}, nil
	} else if err != nil {
		return nil, nil, err
	}
	return kept, refs, nil
}

func validateDiagnosticManifest(keep []KeepFile, d *AttemptDiagnostics) error {
	if d == nil {
		return nil
	}
	if err := d.Files.validate(); err != nil {
		return err
	}
	if len(d.Files) < 2 {
		return errors.New("invalid diagnostic manifest")
	}
	for name := range d.Files {
		switch name {
		case "log-tail.txt", "summary.json":
		default:
			if kept, ok := strings.CutPrefix(name, "kept/"); ok {
				covered := false
				for _, k := range keep {
					covered = covered || outputCovers(k.Path, kept)
				}
				if !covered {
					return errors.New("kept file is not declared by keep_on_failure")
				}
				continue
			}
			index, ok := strings.CutPrefix(name, "reports/")
			index, suffix := strings.CutSuffix(index, ".xml")
			n, err := strconv.Atoi(index)
			if !ok || !suffix || err != nil || n < 0 || strconv.Itoa(n) != index {
				return errors.New("invalid diagnostic filename")
			}
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
			if data := inlineDiagnosticSummary(a.Diagnostics); data != nil {
				hash := sha256.Sum256(data)
				return FileManifest{"summary.json": {SHA256: hex.EncodeToString(hash[:]), SizeBytes: int64(len(data))}}, nil
			}
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
	for _, a := range j.AttemptHistory {
		if a.Attempt == attempt && name == "summary.json" {
			if data := inlineDiagnosticSummary(a.Diagnostics); data != nil {
				if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
					return err
				}
				f, err := os.CreateTemp(filepath.Dir(dest), ".diagnostic-")
				if err != nil {
					return err
				}
				defer os.Remove(f.Name())
				_, writeErr := f.Write(data)
				if err = errors.Join(writeErr, f.Close()); err != nil {
					return err
				}
				if err = verifyFile(ctx, f.Name(), ref); err != nil {
					return err
				}
				return os.Rename(f.Name(), dest)
			}
		}
	}
	return c.downloadResultFile(ctx, ref, dest)
}

func inlineDiagnosticSummary(d *AttemptDiagnostics) []byte {
	if d == nil || len(d.Files) != 0 || d.Summary == nil {
		return nil
	}
	data, _ := json.MarshalIndent(d.Summary, "", "  ")
	return data
}

func retainFinalizationSummary(j *Job, outcome *AttemptOutcome) {
	if j.State != "failed" || outcome.ExecutionCause == nil || outcome.ExecutionCause.Finalization == nil || outcome.Diagnostics != nil && len(outcome.Diagnostics.Files) != 0 {
		return
	}
	outcome.Diagnostics = &AttemptDiagnostics{Summary: &FailureSummary{
		Execution: Execution{ID: j.ID, Attempt: outcome.Attempt}, Machine: outcome.Machine, Session: outcome.Session,
		Error: outcome.ExecutionCause.String(), ExecutionCause: outcome.ExecutionCause, CleanupCause: outcome.CleanupCause,
		FailedTests: []FailedTest{},
	}}
}
