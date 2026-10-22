package myce

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// SMB refuses both link and exclusive file rename, but supports a directory
// rename that cannot replace an existing directory containing published output.
type smbOutputFS struct{ links, renames int }

func (f *smbOutputFS) link(string, string) error             { f.links++; return syscall.ENOTSUP }
func (f *smbOutputFS) renameExclusive(string, string) error  { return syscall.ENOTSUP }
func (f *smbOutputFS) renameDirectory(from, to string) error { f.renames++; return os.Rename(from, to) }

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
	fs := &smbOutputFS{}
	publish := func() error {
		return publishDarwinOutputs(j, root, func(from, to string) error {
			if fs.renames == 0 {
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
			return fs.renameDirectory(from, to)
		})
	}
	if err := publish(); err != nil {
		t.Fatal("SMB publication", err)
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
	if fs.links != 0 || fs.renames != 2 {
		t.Fatal("unexpected publication operations", fs)
	}
}
