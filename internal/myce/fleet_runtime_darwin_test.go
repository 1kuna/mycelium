package myce

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFleetDarwinUnmappedExecutableCannotProveRuntime(t *testing.T) {
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	copy, err := os.Create(filepath.Join(t.TempDir(), "replacement"))
	if err != nil {
		t.Fatal(err)
	}
	defer copy.Close()
	if _, err := io.Copy(copy, source); err != nil {
		t.Fatal(err)
	}
	// The bytes and Mach-O identity are identical; the new inode is not the
	// image this process executed. A late pathname open must fail closed.
	if err := verifyFleetRunningImage(copy); err == nil {
		t.Fatal("unmapped executable certified as running image")
	}
}
