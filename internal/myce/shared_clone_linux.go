package myce

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// cloneSharedFile makes target a reflink of the open source on filesystems that
// support FICLONE (for example Btrfs or XFS). It reports false otherwise.
func cloneSharedFile(in *os.File, target string) bool {
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return false
	}
	err = unix.IoctlFileClone(int(out.Fd()), int(in.Fd()))
	if err == nil {
		err = out.Sync()
	}
	if err = errors.Join(err, out.Close()); err != nil {
		_ = os.Remove(target)
		return false
	}
	return true
}
