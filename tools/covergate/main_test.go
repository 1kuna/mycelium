package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCovergatePassesAndFailsThresholds(t *testing.T) {
	profile := writeProfile(t, `mode: atomic
mycelium/internal/scheduler/service.go:1.1,2.1 2 1
mycelium/internal/lease/allocator.go:1.1,2.1 1 1
mycelium/internal/gateway/router.go:1.1,2.1 1 0
`)
	if err := run(profile, gateConfig{
		TotalMin:     0.75,
		Requires:     []string{"internal/scheduler=1.0", "mycelium/internal/lease=100"},
		FileRequires: []string{"internal/scheduler/service.go=1.0"},
	}); err != nil {
		t.Fatalf("run pass: %v", err)
	}
	if err := run(profile, gateConfig{TotalMin: 0.76}); err == nil || !strings.Contains(err.Error(), "total coverage") {
		t.Fatalf("total err = %v", err)
	}
	if err := run(profile, gateConfig{Requires: []string{"internal/gateway=1.0"}}); err == nil || !strings.Contains(err.Error(), "internal/gateway") {
		t.Fatalf("package err = %v", err)
	}
	if err := run(profile, gateConfig{Requires: []string{"missing=1.0"}}); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing err = %v", err)
	}
	if err := run(profile, gateConfig{FileRequires: []string{"internal/gateway/router.go=1.0"}}); err == nil || !strings.Contains(err.Error(), "internal/gateway/router.go") {
		t.Fatalf("file err = %v", err)
	}
	if err := run(profile, gateConfig{FileRequires: []string{"missing.go=1.0"}}); err == nil || !strings.Contains(err.Error(), "missing.go") {
		t.Fatalf("missing file err = %v", err)
	}
	if err := run(profile, gateConfig{PackageMin: 0.25, PackagePrefixes: []string{"internal/"}, PackageExcludes: []string{"internal/gateway"}}); err != nil {
		t.Fatalf("package min pass: %v", err)
	}
	if err := run(profile, gateConfig{PackageMin: 0.75, PackagePrefixes: []string{"internal/"}, PackageExcludes: []string{"internal/gateway"}}); err != nil {
		t.Fatalf("package min exclude pass: %v", err)
	}
	if err := run(profile, gateConfig{PackageMin: 1.0, PackagePrefixes: []string{"internal/"}}); err == nil || !strings.Contains(err.Error(), "package minimum") {
		t.Fatalf("package min err = %v", err)
	}
	if _, _, err := parseRequirement("bad"); err == nil {
		t.Fatal("expected bad requirement")
	}
}

func TestCovergateRejectsBadProfiles(t *testing.T) {
	if err := run("", gateConfig{}); err == nil {
		t.Fatal("expected missing profile")
	}
	if _, _, _, err := readProfile(writeProfile(t, "mode: atomic\nbad\n")); err == nil {
		t.Fatal("expected bad line")
	}
	if _, _, _, err := readProfile(writeProfile(t, "mode: atomic\nx.go:1 a 1\n")); err == nil {
		t.Fatal("expected bad statement count")
	}
	if _, _, _, err := readProfile(writeProfile(t, "mode: atomic\nx.go:1 1 b\n")); err == nil {
		t.Fatal("expected bad hit count")
	}
	if err := run(writeProfile(t, "mode: atomic\n"), gateConfig{}); err == nil {
		t.Fatal("expected empty profile")
	}
	if prefixes := normalizePrefixes([]string{"mycelium/internal/", "pkg/"}); prefixes[0] != "internal/" || prefixes[1] != "pkg/" {
		t.Fatalf("prefixes = %+v", prefixes)
	}
	if packageSelected("cmd/mycelium", []string{"internal/"}, nil) {
		t.Fatal("cmd package selected")
	}
	if packagePath("mycelium/internal/foo/bar.go:1.1,2.1") != "internal/foo" {
		t.Fatal("package path mismatch")
	}
	if filePath("mycelium/internal/foo/bar.go:1.1,2.1") != "internal/foo/bar.go" {
		t.Fatal("file path mismatch")
	}
	var flags requireFlags
	if flags.String() != "" {
		t.Fatalf("empty flags = %q", flags.String())
	}
	if err := flags.Set("internal/scheduler=1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if flags.String() != "internal/scheduler=1" {
		t.Fatalf("flags = %q", flags.String())
	}
	if (counters{}).percent() != 0 {
		t.Fatal("empty counters percent should be zero")
	}
}

func writeProfile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cover.out")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write profile: %v", err)
	}
	return path
}
