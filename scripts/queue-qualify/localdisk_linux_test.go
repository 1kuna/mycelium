package main

import "testing"

// The harness removes its own scratch files only on a local disk; a test's
// temporary directory is one.
func TestTempDirIsALocalDisk(t *testing.T) {
	local, err := isLocalDisk(t.TempDir())
	if err != nil || !local {
		t.Fatalf("local %v, err %v", local, err)
	}
}
