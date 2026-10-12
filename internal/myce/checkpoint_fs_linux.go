//go:build linux

package myce

import (
	"golang.org/x/sys/unix"
	"os"
)

func checkpointCIFS(f *os.File) (bool, error) {
	var st unix.Statfs_t
	err := unix.Fstatfs(int(f.Fd()), &st)
	return st.Type == unix.CIFS_SUPER_MAGIC, err
}
