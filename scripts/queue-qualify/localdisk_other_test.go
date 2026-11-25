//go:build !darwin && !linux

package main

import "testing"

// On a platform where the harness cannot tell, it refuses to remove scratch
// files rather than guess.
func TestLocalDiskIsRefusedWhereUnknown(t *testing.T) {
	if local, err := isLocalDisk(t.TempDir()); err == nil || local {
		t.Fatalf("local %v, err %v", local, err)
	}
}
