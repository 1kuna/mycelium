package main

import (
	"encoding/json"
	"io"
	"maps"
	"mycelium/internal/myce"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestSkillInstallMatchesEmbeddedAndUpgradeRefreshes(t *testing.T) {
	files, digest, err := embeddedSkill()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "skills", "mycelium")
	const oldVersion = "v3.1.0-rc.20261124.s094r6.9"
	const newVersion = "v3.1.0-rc.20261124.s094r6.10"
	old := skillInstaller{Version: oldVersion, Out: io.Discard}
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
	assertSkillCopy(oldVersion)

	// A development build never rewrites a release's copy.
	skillInstaller{Version: "development", Out: io.Discard}.refresh([]string{dir})
	assertSkillCopy(oldVersion)

	// Simulate a release whose guide changed: the next command refreshes it.
	if err := os.WriteFile(filepath.Join(dir, "references", "submit.md"), []byte("old guidance"), 0o644); err != nil {
		t.Fatal(err)
	}
	marker := `{"version":"` + oldVersion + `","sha256":"` + skillDigest(mustReadSkill(t, dir)) + `"}`
	if err := os.WriteFile(filepath.Join(dir, skillMarker), []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	skillInstaller{Version: newVersion, Out: &out}.refresh([]string{dir})
	assertSkillCopy(newVersion)
	if !strings.Contains(out.String(), "from "+oldVersion+" to "+newVersion) {
		t.Fatalf("refresh output = %q", out.String())
	}
	old.refresh([]string{dir})
	assertSkillCopy(newVersion)
	equal := skillInstaller{Version: newVersion + "+other-build", Out: io.Discard}
	equal.refresh([]string{dir})
	assertSkillCopy(newVersion)
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

func TestSkillRefreshRepairsMissingAndCorruptCopies(t *testing.T) {
	s := skillInstaller{Version: "v3.2.0", Out: io.Discard}
	root := t.TempDir()
	missing, corrupt, manual := filepath.Join(root, "missing"), filepath.Join(root, "corrupt"), filepath.Join(root, "manual")
	if err := s.install([]string{corrupt}, false, false); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corrupt, skillMarker), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(manual, 0o755); err != nil {
		t.Fatal(err)
	}
	s.refresh([]string{missing, corrupt, manual})
	_, digest, _ := embeddedSkill()
	for _, dir := range []string{missing, corrupt} {
		if r, err := inspectSkill(dir, s.Version, digest); err != nil || r.State != skillCurrent {
			t.Fatalf("%s state = %+v, %v; want current", dir, r, err)
		}
	}
	if r, err := inspectSkill(manual, s.Version, digest); err != nil || r.State != skillUnmanaged {
		t.Fatalf("manual state = %+v, %v; want unmanaged", r, err)
	}
}

func TestSkillConcurrentRefreshKeepsNewest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "skills", "mycelium")
	old := skillInstaller{Version: "v3.1.0", Out: io.Discard}
	if err := old.install([]string{dir}, false, false); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var writers sync.WaitGroup
	for _, version := range []string{"v3.4.0", "v3.3.0", "v3.2.0", "v3.4.0", "v3.3.0", "v3.2.0"} {
		writers.Go(func() {
			<-start
			skillInstaller{Version: version, Out: io.Discard}.refresh([]string{dir})
		})
	}
	close(start)
	writers.Wait()
	_, digest, _ := embeddedSkill()
	if r, err := inspectSkill(dir, "v3.4.0", digest); err != nil || r.State != skillCurrent {
		t.Fatalf("final state = %+v, %v; want current v3.4.0", r, err)
	}
}

func TestSkillInstallRequiresReplaceForDowngrade(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "skills", "mycelium")
	release := skillInstaller{Version: "v3.2.0", Out: io.Discard}
	if err := release.install([]string{dir}, false, false); err != nil {
		t.Fatal(err)
	}
	_, digest, _ := embeddedSkill()
	for _, version := range []string{"v3.1.0", "v3.2.0+other-build", "development"} {
		s := skillInstaller{Version: version, Out: io.Discard}
		if err := s.install([]string{dir}, false, false); err != nil {
			t.Fatal(err)
		}
		if r, err := inspectSkill(dir, release.Version, digest); err != nil || r.State != skillCurrent {
			t.Fatalf("after install %s: state = %+v, %v; want current %s", version, r, err, release.Version)
		}
	}
	dev := skillInstaller{Version: "development", Out: io.Discard}
	if err := dev.install([]string{dir}, true, false); err != nil {
		t.Fatal(err)
	}
	if r, err := inspectSkill(dir, dev.Version, digest); err != nil || r.State != skillCurrent {
		t.Fatalf("after install --replace: state = %+v, %v; want current %s", r, err, dev.Version)
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

// Older fleet helpers parse combined output, so a candidate version probe must
// neither refresh the installed guide nor print the refresh notice (#1450).
func TestSkillVersionProbeIsReadOnly(t *testing.T) {
	if alias := os.Getenv("MYCE_TEST_VERSION_PROBE"); alias != "" {
		if err := os.Setenv("HOME", os.Getenv("MYCE_TEST_VERSION_HOME")); err != nil {
			t.Fatal(err)
		}
		myce.BuildVersion = "v3.2.0"
		os.Args = []string{"myce", alias, "--json"}
		main()
		os.Exit(0)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"version", "--version", "-v"} {
		t.Run(alias, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("CODEX_HOME", filepath.Join(home, ".codex"))
			t.Setenv("MYCE_TEST_VERSION_PROBE", alias)
			t.Setenv("MYCE_TEST_VERSION_HOME", home)
			dir := filepath.Join(home, ".agents", "skills", "mycelium")
			if err := (skillInstaller{Version: "v3.1.0", Out: io.Discard}).install([]string{dir}, false, false); err != nil {
				t.Fatal(err)
			}
			before := mustReadSkill(t, dir)
			marker, err := os.ReadFile(filepath.Join(dir, skillMarker))
			if err != nil {
				t.Fatal(err)
			}
			output, err := exec.Command(binary, "-test.run=^TestSkillVersionProbeIsReadOnly$").CombinedOutput()
			var info myce.VersionInfo
			if err != nil || json.Unmarshal(output, &info) != nil || info.Version != "v3.2.0" {
				t.Fatalf("old helper cannot decode candidate version: %v: %s", err, output)
			}
			afterMarker, err := os.ReadFile(filepath.Join(dir, skillMarker))
			if err != nil || string(afterMarker) != string(marker) || !maps.EqualFunc(before, mustReadSkill(t, dir), func(a, b []byte) bool { return string(a) == string(b) }) {
				t.Fatal("version probe changed installed guide", err)
			}
		})
	}
}
