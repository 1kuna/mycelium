package myce

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// #123: SMB rejects links; publication must remain atomic and no-overwrite.
func TestExtensionFilesDarwinExclusivePublication(t *testing.T) {
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "staged"), filepath.Join(dir, "output")
	content := []byte("complete attempt output")
	if err := os.WriteFile(source, content, 0600); err != nil {
		t.Fatal(err)
	}
	links, renames := 0, 0
	fs := outputPublishFS{
		link: func(string, string) error { links++; return syscall.ENOTSUP },
		renameExclusive: func(from, to string) error {
			renames++
			if _, err := os.Stat(to); !errors.Is(err, os.ErrNotExist) && renames == 1 {
				t.Fatal("partial output visible")
			}
			return unix.RenamexNp(from, to, unix.RENAME_EXCL)
		},
	}
	if err := publishDarwinOutput(source, dest, fs); err != nil {
		t.Fatal("SMB publication:", err)
	}
	if err := os.WriteFile(source, []byte("late attempt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := publishDarwinOutput(source, dest, fs); !errors.Is(err, os.ErrExist) {
		t.Fatal("existing output not protected:", err)
	}
	b, err := os.ReadFile(dest)
	if err != nil || string(b) != string(content) || links != 0 || renames != 2 {
		t.Fatalf("publication violated contract: %q %v links=%d renames=%d", b, err, links, renames)
	}
}
