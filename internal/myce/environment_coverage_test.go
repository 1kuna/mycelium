package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func breadthRecipe(platforms ...string) EnvironmentRecipe {
	data := "version: 6\nenvironments:\n  selected:\n    packages:\n"
	for _, platform := range platforms {
		data += "      " + platform + ": []\n"
	}
	data += "  unused:\n    packages:\n      win-64: []\n"
	r := EnvironmentRecipe{Backend: "pixi", Environment: "selected", LockFile: "pixi.lock", Files: map[string][]byte{"pixi.lock": []byte(data), "pixi.toml": []byte("[workspace]\n")}}
	r.Key = identityHash(r)
	return r
}

func TestEnvironmentLockCoverageFrozenSelectedEnvironment(t *testing.T) {
	q, _ := fixture(t)
	dir := t.TempDir()
	r := breadthRecipe("linux-64", "osx-arm64", "private-unknown-platform")
	for name, data := range r.Files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	r, err := snapshotEnvironment(EnvironmentRecipe{Backend: "pixi", Environment: "selected", LockFile: filepath.Join(dir, "pixi.lock")}, "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	// Readback uses frozen bytes, not changed paths or a different lock environment.
	if err := os.WriteFile(r.LockFile, []byte("invalid new contents"), 0600); err != nil {
		t.Fatal(err)
	}
	before := identityHash(q.s)
	p := q.EnvironmentEntries()[0].Platforms[0]
	want := []ExecutionTarget{{"darwin", "arm64", "cpu"}, {"linux", "amd64", "cpu"}}
	if p.Key != r.Key || !slices.Equal(p.LockCoverage.Targets, want) || p.LockCoverage.Environment != "selected" || !p.LockCoverage.UnmappedPlatforms {
		t.Fatalf("selected immutable coverage: %+v", p)
	}
	b, _ := json.Marshal(p)
	if strings.Contains(string(b), "private-") || strings.Contains(string(b), dir) || identityHash(q.s) != before {
		t.Fatalf("readback leaked or mutated state: %s", b)
	}
	r.Environment = "absent"
	if got := environmentLockCoverage(r, nil, "cpu"); !strings.HasPrefix(got.State, "unknown:") || len(got.Targets) != 0 {
		t.Fatal(got)
	}
	r.Environment = "selected"
	r.Files["pixi.lock"] = []byte("environments: [invalid]")
	if got := environmentLockCoverage(r, nil, "cpu"); got.State != "unknown: invalid lock" {
		t.Fatal(got)
	}
	if got := environmentLockCoverage(EnvironmentRecipe{Backend: "uv"}, nil, "cpu"); !strings.HasPrefix(got.State, "unknown:") || len(got.Targets) != 0 {
		t.Fatal(got)
	}
}
