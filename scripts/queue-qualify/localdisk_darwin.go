package main

import "golang.org/x/sys/unix"

// isLocalDisk reports whether path is on a local filesystem, not a network
// mount such as SMB or NFS.
func isLocalDisk(path string) (bool, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false, err
	}
	return st.Flags&unix.MNT_LOCAL != 0, nil
}
