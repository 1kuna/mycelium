//go:build linux

package myce

import "syscall"

func diskBlockSize(st *syscall.Statfs_t) uint64 {
	// Linux statfs counts Blocks/Bavail in fragment units, not optimal I/O blocks.
	if st.Frsize > 0 {
		return uint64(st.Frsize)
	}
	return uint64(st.Bsize)
}
