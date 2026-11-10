package myce

import "syscall"

func modelRootLocalFilesystem(path string) bool {
	var st syscall.Statfs_t
	if syscall.Statfs(path, &st) != nil {
		return false
	}
	// Known local disk filesystems. Unknown/FUSE/network filesystems cannot
	// establish local staging, even when absent from network mount inventory.
	switch uint64(st.Type) {
	case 0xef53, 0x58465342, 0x9123683e, 0x2fc12fc1, 0xf2f52010, 0x794c7630:
		return true // ext, xfs, btrfs, zfs, f2fs, overlay
	}
	return false
}
