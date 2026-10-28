package myce

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExtensionFilesPublicationPreservesExistingAttempt(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	j := Job{ID: "job", Attempt: 1, Spec: Spec{Run: RunSpec{Workdir: work}, Outputs: map[string]string{"one": "results", "two": "results"}}}
	dest := filepath.Join(root, "results", j.ID, "1")
	if err := os.MkdirAll(dest, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, "existing"), []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	for name := range j.Spec.Outputs {
		if err := os.WriteFile(filepath.Join(work, name), []byte("new"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := publishJobOutputs(j, root); err == nil {
		t.Fatal("publication modified an existing attempt directory")
	}
	files, err := os.ReadDir(dest)
	if err != nil || len(files) != 1 || files[0].Name() != "existing" {
		t.Fatalf("partial publication in retained attempt: %v, %v", files, err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "existing"))
	if err != nil || string(data) != "retained" {
		t.Fatal("existing attempt changed", err)
	}
}

func TestExtensionFilesPublicationRejectsEscapingAncestor(t *testing.T) {
	root, outside, work := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "results")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "output"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	j := Job{ID: "job", Attempt: 1, Spec: Spec{Run: RunSpec{Workdir: work}, Outputs: map[string]string{"output": "results"}}}
	if err := publishJobOutputs(j, root); err == nil {
		t.Fatal("publication followed escaping ancestor")
	}
	files, err := os.ReadDir(outside)
	if err != nil || len(files) != 0 {
		t.Fatalf("publication escaped root: %v, %v", files, err)
	}
}
