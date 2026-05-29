package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSmokeGateRequiresPassingTests(t *testing.T) {
	path := writeJSON(t, `
{"Action":"run","Test":"TestLocal"}
{"Action":"pass","Test":"TestLocal"}
{"Action":"run","Test":"TestFleet"}
{"Action":"skip","Test":"TestFleet"}
{"Action":"run","Test":"TestBroken"}
{"Action":"fail","Test":"TestBroken"}
`)
	if err := run(path, gateConfig{MinPass: 1, Requires: []string{"TestLocal"}}); err != nil {
		t.Fatalf("run pass: %v", err)
	}
	if err := execute([]string{"-json", path, "-min-pass", "1", "-require", "TestLocal"}); err != nil {
		t.Fatalf("execute pass: %v", err)
	}
	if err := run(path, gateConfig{Requires: []string{"TestFleet"}}); err == nil || !strings.Contains(err.Error(), "skipped") {
		t.Fatalf("skip err = %v", err)
	}
	if err := run(path, gateConfig{Requires: []string{"TestBroken"}}); err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("fail err = %v", err)
	}
	if err := run(path, gateConfig{Requires: []string{"Missing"}}); err == nil || !strings.Contains(err.Error(), "did not run") {
		t.Fatalf("missing err = %v", err)
	}
	if err := run(path, gateConfig{MinPass: 2}); err == nil || !strings.Contains(err.Error(), "only 1") {
		t.Fatalf("min err = %v", err)
	}
}

func TestSmokeGateRejectsBadInput(t *testing.T) {
	if err := run("", gateConfig{}); err == nil {
		t.Fatal("expected missing json path")
	}
	if err := run(writeJSON(t, `{`), gateConfig{}); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("bad json err = %v", err)
	}
	if err := run(writeJSON(t, `{"Action":"pass"}`), gateConfig{}); err == nil || !strings.Contains(err.Error(), "no test events") {
		t.Fatalf("empty err = %v", err)
	}
	if _, _, err := readStatuses(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("expected missing file error")
	}
	if err := execute([]string{"-require", ""}); err == nil {
		t.Fatal("expected bad flag error")
	}
	var flags requireFlags
	if err := flags.Set(""); err == nil {
		t.Fatal("expected empty require error")
	}
	if err := flags.Set("TestLocal"); err != nil || flags.String() != "TestLocal" {
		t.Fatalf("flags = %q err=%v", flags.String(), err)
	}
	if counts := countStatuses(map[string]testStatus{
		"pass": {Passed: true},
		"skip": {Skipped: true},
		"fail": {Failed: true, Passed: true},
	}); counts.Passed != 1 || counts.Skipped != 1 || counts.Failed != 1 {
		t.Fatalf("counts = %+v", counts)
	}
}

func writeJSON(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "smoke.json")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0644); err != nil {
		t.Fatalf("write json: %v", err)
	}
	return path
}
