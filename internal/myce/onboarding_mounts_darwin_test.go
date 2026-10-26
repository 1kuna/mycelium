package myce

import (
	"context"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCachedDarwinMountInventoryUsesNowait(t *testing.T) {
	calls := 0
	b, err := cachedDarwinMountInventory(context.Background(), func(mounts []unix.Statfs_t, flags int) (int, error) {
		calls++
		if flags != unix.MNT_NOWAIT {
			t.Fatal("inventory can refresh unrelated filesystems")
		}
		if mounts == nil {
			return 1, nil
		}
		copy(mounts[0].Fstypename[:], "smbfs")
		copy(mounts[0].Mntfromname[:], "//server.invalid/share")
		copy(mounts[0].Mntonname[:], "/Volumes/selected share")
		return 1, nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("cached inventory: %d calls, %v", calls, err)
	}
	if !strings.Contains(string(b), "//server.invalid/share on /Volumes/selected share (smbfs)") {
		t.Fatal("cached mount identity/path lost")
	}
}
