package myce

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
	before, err := q.Snapshot()
	if err != nil {
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
	after, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("readback mutated queue")
	}
}
