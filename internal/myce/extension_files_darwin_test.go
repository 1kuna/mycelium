package myce

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExtensionFilesDarwinExclusivePublication(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	for _, name := range []string{"output", "nested/second"} {
		p := filepath.Join(work, name)
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("complete output"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	j := Job{ID: "job", Attempt: 1, Spec: Spec{Run: RunSpec{Workdir: work}, Outputs: map[string]string{"output": "results", "nested/second": "results"}}}
	renames := 0
	publish := func() error {
		return publishDirectoryJobOutputs(j, root, func(from, to string) error {
			if renames == 0 {
				if _, err := os.Stat(to); !os.IsNotExist(err) {
					t.Fatal("output visible before publication", err)
				}
				for name := range j.Spec.Outputs {
					b, err := os.ReadFile(filepath.Join(from, name))
					if err != nil || string(b) != "complete output" {
						t.Fatal("incomplete staged output", err)
					}
				}
			}
			renames++
			return os.Rename(from, to)
		})
	}
	if err := publish(); err != nil {
		t.Fatal("grouped publication", err)
	}
	if err := os.WriteFile(filepath.Join(work, "output"), []byte("late attempt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := publish(); err == nil {
		t.Fatal("published output overwritten")
	}
	for name := range j.Spec.Outputs {
		b, err := os.ReadFile(filepath.Join(root, outputRelative(j, name, "results")))
		if err != nil || string(b) != "complete output" {
			t.Fatal("published bytes changed", err)
		}
	}
	if renames != 2 {
		t.Fatal("expected one complete publication per call", renames)
	}
}
