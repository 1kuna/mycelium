package main

import (
	"archive/tar"
	"context"
	"io"
	"mycelium/internal/myce"
	"os"
	"path/filepath"
	"strings"
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
	for name, data := range map[string]string{"script": "$Format:%H$", "outside": "other", ".gitattributes": "script export-subst\n"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	profileGit(t, root, "init", "--quiet")
	profileGit(t, root, "add", ".")
	profileGit(t, root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture")
	source := profileSource{Git: "HEAD", Paths: []string{".MYCE-INPUTS"}}
	if _, err := freezeProfileSource(context.Background(), root, source, filepath.Join(t.TempDir(), "source.tar")); err == nil {
		t.Fatal("scoped source can replace a verified item through a reserved-name alias")
	}

	{
		source := "../input"
		s := myce.Spec{}
		s.Inputs = map[string]string{"data": source}
		var found submitterFindings
		lowerInputs(&s, root, "files-from", &found)
		if len(found.findings) != 1 || !strings.Contains(found.findings[0].Problem, "must be relative") || len(s.Inputs) != 0 {
			t.Fatalf("submit/resolve mismatch: %+v", found)
		}
	}

	{
		ctx := context.Background()
		source := profileSource{Git: "HEAD", Paths: []string{"script"}}
		archive := filepath.Join(t.TempDir(), "source.tar")
		_, err := freezeProfileSource(ctx, root, source, archive)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(archive)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		tr := tar.NewReader(f)
		h, err := tr.Next()
		if err != nil || h.Name != "script" {
			t.Fatal(h, err)
		}
		content, err := io.ReadAll(tr)
		if err != nil || string(content) != "$Format:%H$" {
			t.Fatalf("substituted blob: %q %v", content, err)
		}
		if _, err = tr.Next(); err != io.EOF {
			t.Fatal("unselected file archived", err)
		}

	}
}
