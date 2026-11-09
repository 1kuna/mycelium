package myce

import (
	"os"
	"path/filepath"

	"testing"
)

func TestExtensionFilesFailedNASInputsDoNotPublishSnapshot(t *testing.T) {
	root, state, outside := t.TempDir(), t.TempDir(), t.TempDir()
	for _, dir := range []string{root, outside} {
		if err := os.WriteFile(filepath.Join(dir, "input"), []byte("input"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	j := Job{ID: "failed-inputs", Attempt: 1, Spec: Spec{
		Kind: "sample", Inputs: map[string]string{"valid": "input", "invalid": "escape/input"},
	}}
	if err := (Worker{JobFileRoot: root}).prepareExtensionFiles(&j, state); err == nil {
		t.Fatal("input escaping the NAS root was accepted")
	}
	if j.Spec.Run.Workdir != "" {
		t.Fatal("failed inputs exposed a work directory")
	}
	entries, err := os.ReadDir(filepath.Join(state, "job-files", j.ID))
	if err != nil || len(entries) != 0 {
		t.Fatalf("partial snapshot or staging scratch remains: %v, %v", entries, err)
	}
}
