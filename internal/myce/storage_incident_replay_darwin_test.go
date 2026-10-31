package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Rootless disposable image proof, separate from the mocked scheduling replay.
// This qualifies actual bounded-volume denial and independent publication/WAL,
// not arbitrary sidecar confinement or a production APFS quota/reserve backend.
func TestStorageIncidentReplayNativeBurstAPFS(t *testing.T) {
	if os.Getenv("MYCE_TEST_STORAGE_MIGRATION_NATIVE") != "1" {
		t.Skip("opt-in: disposable fully allocated APFS image")
	}
	scratch := os.Getenv("MYCE_TEST_STORAGE_IDENTITY_SCRATCH")
	if scratch == "" {
		t.Fatal("explicit scratch root outside test HOME/TMPDIR required")
	}
	dir, err := os.MkdirTemp(scratch, "admission-burst-")
	if err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(dir, "mount")
	if err = os.Mkdir(mount, 0700); err != nil {
		t.Fatal(err)
	}
	attached := false
	run := func(args ...string) error {
		out, e := exec.Command("/usr/bin/hdiutil", args...).CombinedOutput()
		if e != nil {
			t.Logf("hdiutil: %s", out)
		}
		return e
	}
	t.Cleanup(func() {
		if attached {
			if e := run("detach", mount); e != nil {
				t.Errorf("detach failed; retain %s: %v", dir, e)
				return
			}
		}
		parentID, e1 := ObserveStorageBackingIdentity(dir)
		mountID, e2 := ObserveStorageBackingIdentity(mount)
		if e1 != nil || e2 != nil || parentID != mountID {
			t.Errorf("cannot verify detached image; retain %s", dir)
			return
		}
		if e := os.RemoveAll(dir); e != nil {
			t.Error(e)
		}
	})
	image := filepath.Join(dir, "burst.dmg")
	if err = run("create", "-size", "64m", "-fs", "APFS", "-volname", "MyceBurst", image); err != nil {
		t.Fatal(err)
	}
	if err = run("attach", "-nobrowse", "-mountpoint", mount, image); err != nil {
		t.Fatal(err)
	}
	attached = true
	t.Setenv("MYCE_TEST_BOUNDED_VOLUME_ROOT", mount)
	t.Setenv("MYCE_TEST_BOUNDED_VOLUME_BACKING", image)
	f := migrationQualifiedCycle(t)
	migrationAction(t, f.q, "activate")
	proveStorageProtectionNativeBoundedVolume(t, f.q)
}
