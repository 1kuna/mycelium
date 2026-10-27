package myce

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestNativeCheckOutputCannotEscapeAttempt(t *testing.T) {
	protected := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(protected, []byte("synthetic-private-credential"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, attack := range []string{"leaf-symlink", "ancestor-symlink", "fifo"} {
		t.Run(attack, func(t *testing.T) {
			work, destination := t.TempDir(), t.TempDir()
			dir := filepath.Join(work, ".myce-check")
			if attack == "ancestor-symlink" {
				if err := os.Symlink(filepath.Dir(protected), dir); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(protected, filepath.Join(filepath.Dir(protected), "actual")); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				actual := filepath.Join(dir, "actual")
				if attack == "fifo" {
					if err := syscall.Mkfifo(actual, 0600); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Symlink(protected, actual); err != nil {
					t.Fatal(err)
				}
			}
			j := Job{Spec: Spec{Run: RunSpec{Env: map[string]string{"MYCE_CHECK_OUTPUT": filepath.Join(dir, "actual")}}, ExtensionCheck: &ExtensionCheckRequest{Directory: destination}}}
			if err := (Worker{}).publishExtensionFiles(j, work); err == nil {
				t.Fatal("malicious output was published")
			}
			if _, err := os.Stat(filepath.Join(destination, "actual")); !os.IsNotExist(err) {
				t.Fatal("protected output reached shared scratch", err)
			}
		})
	}
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "actual"), []byte("valid"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := readExtensionCheckOutput(work); err != nil || string(got) != "valid" {
		t.Fatal("ordinary bounded output rejected", string(got), err)
	}
}
