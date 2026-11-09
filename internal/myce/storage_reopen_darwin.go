package myce

import (
	"io/fs"

	"golang.org/x/sys/unix"
)

func storageCacheBypass(file fs.File, offset, length int64) error {
	f, ok := file.(interface{ Fd() uintptr })
	if !ok {
		return errStorageCacheBypass
	}
	_, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1)
	return err
}
