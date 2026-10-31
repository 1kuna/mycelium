package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestStorageIdentityNativeAPFS(t *testing.T) {
	if os.Getenv("MYCE_TEST_STORAGE_IDENTITY_NATIVE") != "1" {
		t.Skip("opt-in: creates two disposable APFS images without root")
	}
	scratch := os.Getenv("MYCE_TEST_STORAGE_IDENTITY_SCRATCH")
	if scratch == "" {
		t.Fatal("set MYCE_TEST_STORAGE_IDENTITY_SCRATCH outside the isolated test HOME/TMPDIR")
	}
	dir, err := os.MkdirTemp(scratch, "apfs-identity-")
	if err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(dir, "mount")
	if err := os.Mkdir(mount, 0700); err != nil {
		t.Fatal(err)
	}
	mounted := false
	run := func(args ...string) error {
		out, err := exec.Command("/usr/bin/hdiutil", args...).CombinedOutput()
		if err != nil {
			t.Logf("hdiutil: %s", out)
		}
		return err
	}
	// Retain scratch if detach fails: never recursively remove a mounted volume.
	t.Cleanup(func() {
		if mounted {
			if err := run("detach", mount); err != nil {
				t.Errorf("detach failed; retain native identity scratch %s: %v", dir, err)
				return
			}
		}
		parentID, parentErr := ObserveStorageBackingIdentity(dir)
		mountID, mountErr := ObserveStorageBackingIdentity(mount)
		if parentErr != nil || mountErr != nil || parentID != mountID {
			t.Errorf("mount remains attached or unverifiable; retain %s", dir)
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	images := []string{filepath.Join(dir, "a.dmg"), filepath.Join(dir, "b.dmg")}
	for _, image := range images {
		if err := run("create", "-size", "64m", "-fs", "APFS", "-volname", "MyceIdentity", image); err != nil {
			t.Fatal("create disposable APFS image", err)
		}
	}
	switchVolume := func(which int) {
		t.Helper()
		if mounted {
			if err := run("detach", mount); err != nil {
				t.Fatal("detach disposable APFS image", err)
			}
			mounted = false
		}
		if err := run("attach", "-nobrowse", "-mountpoint", mount, images[which]); err != nil {
			t.Fatal("attach disposable APFS image", err)
		}
		mounted = true
	}
	switchVolume(0)
	proveStorageIdentityVolumes(t, mount, switchVolume)
}
