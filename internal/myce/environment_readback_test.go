package myce

import (
	"encoding/json"
	"slices"
)

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestEnvironmentCatalogReadback(t *testing.T) {
	q, _ := fixture(t)
	dir := t.TempDir()
	lock := filepath.Join(dir, "requirements.txt")
	data := []byte("example==1\n# private-secret\n")
	if err := os.WriteFile(lock, data, 0600); err != nil {
		t.Fatal(err)
	}
	recipe, err := snapshotEnvironment(EnvironmentRecipe{LockFile: lock, Python: "3.12"}, "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err = q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": recipe}}}); err != nil {
		t.Fatal(err)
	}
	// Readback uses loaded bytes even if disk inputs are replaced or removed.
	if err = os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "fixture"}
	var entries []EnvironmentEntry
	if err = client.Call(context.Background(), "GET", "/api/environments", nil, &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || len(entries[0].Platforms) != 1 {
		t.Fatal(entries)
	}
	if entries[0].Library != "environment-library" || entries[0].ReadingSide != "queue" || entries[0].ResolutionBase != "environment-library" {
		t.Fatal(entries[0])
	}
	p := entries[0].Platforms[0]
	if p.Backend != "uv" || p.Platform != "cpu" || p.Key != recipe.Key || p.InputDigests["requirements.txt"] != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Fatal(p)
	}
	var entry EnvironmentEntry
	if err = client.Call(context.Background(), "GET", "/api/environments/tools", nil, &entry); err != nil || !reflect.DeepEqual(entry, entries[0]) {
		t.Fatal(entry, err)
	}
	for _, path := range []string{"/api/environments", "/api/environments/tools"} {
		client.Token = "wrong"
		if err = client.Call(context.Background(), "GET", path, nil, nil); err == nil {
			t.Fatal("authentication missing")
		}
	}
	client.Token = "fixture"
	if err = client.Call(context.Background(), "GET", "/api/environments/absent", nil, nil); err == nil || !strings.Contains(err.Error(), "environment_unknown") {
		t.Fatal(err)
	}
	body := string(wireJSON(t, entries))
	for _, private := range []string{dir, "private-secret", "lock_file", "python", "files"} {
		if strings.Contains(body, private) {
			t.Fatal("private recipe leaked", body)
		}
	}
	{
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
		if err := os.WriteFile(r.LockFile, []byte("invalid new contents"), 0600); err != nil {
			t.Fatal(err)
		}
		var entries []EnvironmentEntry
		if err := client.Call(context.Background(), "GET", "/api/environments", nil, &entries); err != nil {
			t.Fatal(err)
		}
		p := entries[0].Platforms[0]
		want := []ExecutionTarget{{"darwin", "arm64", "cpu"}, {"linux", "amd64", "cpu"}}
		if p.Key != r.Key || !slices.Equal(p.LockCoverage.Targets, want) || p.LockCoverage.Environment != "selected" || !p.LockCoverage.UnmappedPlatforms {
			t.Fatalf("selected immutable coverage: %+v", p)
		}
		b, _ := json.Marshal(p)
		if strings.Contains(string(b), "private-") || strings.Contains(string(b), dir) {
			t.Fatalf("readback leaked or mutated state: %s", b)
		}
	}
}
