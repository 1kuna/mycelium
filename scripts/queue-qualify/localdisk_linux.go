package main

import "golang.org/x/sys/unix"

// networkFilesystems are the statfs types of mounts that can reach a NAS.
// FUSE is included because sshfs and similar mounts use it.
var networkFilesystems = map[int64]bool{
	unix.SMB_SUPER_MAGIC: true, unix.SMB2_SUPER_MAGIC: true, unix.CIFS_SUPER_MAGIC: true,
	unix.NFS_SUPER_MAGIC: true, unix.FUSE_SUPER_MAGIC: true,
}

// isLocalDisk reports whether path is on a local filesystem, not a network
// mount such as SMB or NFS.
func isLocalDisk(path string) (bool, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false, err
	}
	return !networkFilesystems[int64(st.Type)], nil
}
