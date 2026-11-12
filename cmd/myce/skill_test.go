package main

import (
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestSkillInstallMatchesEmbeddedAndUpgradeRefreshes(t *testing.T) {
	files, digest, err := embeddedSkill()
	if err != nil || files["SKILL.md"] == nil || files["references/submit.md"] == nil {
		t.Fatalf("embedded skill incomplete: %v %v", slices.Sorted(maps.Keys(files)), err)
	}
	dir := filepath.Join(t.TempDir(), "skills", "mycelium")
	old := skillInstaller{Version: "v3.1.0", Out: io.Discard}
	if err := old.install([]string{dir}, false, false); err != nil {
		t.Fatal(err)
	}
	assertSkillCopy := func(version string) {
		t.Helper()
		got, err := readSkillDir(dir)
		if err != nil || !maps.EqualFunc(got, files, func(a, b []byte) bool { return string(a) == string(b) }) {
			t.Fatalf("installed copy differs from embedded skill: %v", err)
		}
		if r, err := inspectSkill(dir, version, digest); err != nil || r.State != skillCurrent {
			t.Fatalf("state = %+v, %v; want current %s", r, err, version)
		}
	}
	assertSkillCopy("v3.1.0")

	// A development build never rewrites a release's copy.
	skillInstaller{Version: "development", Out: io.Discard}.refresh([]string{dir})
	assertSkillCopy("v3.1.0")

	// Simulate a release whose guide changed: the next command refreshes it.
	if err := os.WriteFile(filepath.Join(dir, "references", "submit.md"), []byte("old guidance"), 0o644); err != nil {
		t.Fatal(err)
	}
	marker := `{"version":"v3.1.0","sha256":"` + skillDigest(mustReadSkill(t, dir)) + `"}`
	if err := os.WriteFile(filepath.Join(dir, skillMarker), []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	skillInstaller{Version: "v3.2.0", Out: &out}.refresh([]string{dir})
	assertSkillCopy("v3.2.0")
	if !strings.Contains(out.String(), "from v3.1.0 to v3.2.0") {
		t.Fatalf("refresh output = %q", out.String())
	}
	entries, _ := os.ReadDir(filepath.Dir(dir))
	if len(entries) != 1 {
		t.Fatalf("staging left behind in skills folder: %v", entries)
	}

	// A copy the user edited is left for them.
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("my notes"), 0o644); err != nil {
		t.Fatal(err)
	}
	skillInstaller{Version: "v3.3.0", Out: io.Discard}.refresh([]string{dir})
	if r, _ := inspectSkill(dir, "v3.3.0", digest); r.State != skillModified {
		t.Fatalf("edited copy state = %s, want modified", r.State)
	}
}

func TestSkillInstallReplacesLinksAndForeignCopiesOnlyWhenAsked(t *testing.T) {
	root := t.TempDir()
	checkout := filepath.Join(root, "checkout", "skills", "mycelium")
	if err := os.MkdirAll(checkout, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "SKILL.md"), []byte("stale branch"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "claude", "skills", "mycelium")
	copied := filepath.Join(root, "codex", "skills", "mycelium")
	for _, d := range []string{filepath.Dir(link), copied} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(checkout, link); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copied, "SKILL.md"), []byte("manual cp"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := skillInstaller{Version: "v3.1.0", BackupRoot: filepath.Join(root, "backups"), Out: io.Discard}
	if err := s.install([]string{link, copied}, false, false); err != nil {
		t.Fatal(err)
	}
	if info, _ := os.Lstat(link); info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("symlink replaced without --replace")
	}
	if err := s.install([]string{link, copied}, true, false); err != nil {
		t.Fatal(err)
	}
	_, digest, _ := embeddedSkill()
	for _, d := range []string{link, copied} {
		if r, err := inspectSkill(d, "v3.1.0", digest); err != nil || r.State != skillCurrent {
			t.Fatalf("%s state = %+v, %v", d, r, err)
		}
	}
	if data, _ := os.ReadFile(filepath.Join(checkout, "SKILL.md")); string(data) != "stale branch" {
		t.Fatal("symlink target was modified")
	}
	backups, _ := filepath.Glob(filepath.Join(root, "backups", "*", "mycelium", "SKILL.md"))
	if len(backups) != 1 {
		t.Fatalf("foreign copy not preserved in backups: %v", backups)
	}
}

func TestDefaultSkillDirsUseExistingAgentHomes(t *testing.T) {
	home := t.TempDir()
	codex := filepath.Join(t.TempDir(), "codex")
	for _, d := range []string{filepath.Join(home, ".claude"), codex} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	got := defaultSkillDirs(home, codex)
	want := []string{filepath.Join(home, ".claude", "skills", "mycelium"), filepath.Join(codex, "skills", "mycelium")}
	if !slices.Equal(got, want) {
		t.Fatalf("dirs = %v, want %v", got, want)
	}
}

func mustReadSkill(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files, err := readSkillDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return files
}
