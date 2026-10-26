package myce

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

// Getfsstat with MNT_NOWAIT copies the kernel's cached mount table, without
// refreshing any filesystem (the same no-wait inventory used by getmntinfo).
// Never fall back to mount(8), which refreshes unrelated network filesystems.
func cachedMountInventory(ctx context.Context) ([]byte, error) {
	return cachedDarwinMountInventory(ctx, unix.Getfsstat)
}

func cachedDarwinMountInventory(ctx context.Context, getfsstat func([]unix.Statfs_t, int) (int, error)) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	count, err := getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return nil, err
	}
	mounts := make([]unix.Statfs_t, count+1)
	n, err := getfsstat(mounts, unix.MNT_NOWAIT)
	if err != nil {
		return nil, err
	}
	if n >= len(mounts) {
		return nil, errors.New("mount table changed during cached inventory; retry selection")
	}
	var out strings.Builder
	for _, m := range mounts[:n] {
		kind := unix.ByteSliceToString(m.Fstypename[:])
		if !networkMountType(kind) {
			continue
		}
		fmt.Fprintf(&out, "%s on %s (%s)\n", unix.ByteSliceToString(m.Mntfromname[:]), unix.ByteSliceToString(m.Mntonname[:]), kind)
	}
	return []byte(out.String()), ctx.Err()
}
