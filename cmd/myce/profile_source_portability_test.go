package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestProfileScopedSourceRefusesInputStagingAlias(t *testing.T) {
	root := t.TempDir()
	// This aliases a verified item's staging path on a macOS worker, even
	// when the source was frozen on a case-sensitive submitter.
	name := filepath.Join(".MYCE-INPUTS", profileHash("item"))
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(name)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), []byte("source instead of item"), 0644); err != nil {
		t.Fatal(err)
	}
	profileGit(t, root, "init", "--quiet")
	profileGit(t, root, "add", ".")
	profileGit(t, root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture")
	source := profileSource{Git: "HEAD", Paths: []string{".MYCE-INPUTS"}}
	if _, err := freezeProfileSource(context.Background(), root, source, filepath.Join(t.TempDir(), "source.tar")); err == nil {
		t.Fatal("scoped source can replace a verified item through a reserved-name alias")
	}
}
