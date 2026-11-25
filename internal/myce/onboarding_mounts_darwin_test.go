package myce

import (
	"context"
	"testing"

	"golang.org/x/sys/unix"
)

func TestOnboardingMountsDarwinIncludesLocalShadow(t *testing.T) {
	var nas, local unix.Statfs_t
	copy(nas.Fstypename[:], "smbfs")
	copy(nas.Mntonname[:], "/storage")
	copy(nas.Mntfromname[:], "//storage.invalid/share")
	copy(local.Fstypename[:], "apfs")
	copy(local.Mntonname[:], "/storage/nested")
	ctx := context.Background()
	data, err := cachedDarwinMountInventory(ctx, func(out []unix.Statfs_t, flags int) (int, error) {
		if flags != unix.MNT_NOWAIT {
			t.Fatal("requested a filesystem refresh instead of cached mount metadata")
		}
		copy(out, []unix.Statfs_t{nas, local})
		return 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	command := func(context.Context, string, ...string) ([]byte, error) { return data, nil }
	mounts, err := mountedStorageInventory(ctx, "darwin", command, true)
	if err != nil || !checkpointMountConfirmed(mounts, "/storage/checkpoints", "/storage") || checkpointMountConfirmed(mounts, "/storage/nested/checkpoints", "/storage") {
		t.Fatal("cached mount table failed to distinguish local shadowing", err)
	}
	roots, err := MountedNASRoots(ctx, "darwin", command)
	if err != nil || len(roots) != 1 || roots[0] != "/storage" {
		t.Fatal("local shadow appeared in NAS discovery", roots, err)
	}
}
