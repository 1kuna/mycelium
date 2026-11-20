package myce

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNativeLaunchFileIntegrity(t *testing.T) {
	work := filepath.Join(t.TempDir(), "work")
	if err := os.Mkdir(work, 0700); err != nil {
		t.Fatal(err)
	}
	path, digest, err := writeNativeLaunchFile(work, []byte("synthetic policy"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 || nativePathContains(work, path) {
		t.Fatal("policy is not private metadata outside the payload workdir")
	}
	data, err := readNativeLaunchFile(path, digest)
	if err != nil || string(data) != "synthetic policy" {
		t.Fatal("launch policy did not round trip")
	}
	if err := os.WriteFile(path, []byte("changed policy"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readNativeLaunchFile(path, digest); err == nil {
		t.Fatal("modified launch authority was accepted")
	}
}
