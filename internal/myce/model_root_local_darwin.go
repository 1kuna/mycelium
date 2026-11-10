package myce

import "golang.org/x/sys/unix"

func modelRootLocalFilesystem(path string) bool {
	var st unix.Statfs_t
	return unix.Statfs(path, &st) == nil && st.Flags&unix.MNT_LOCAL != 0
}
