package myce

import (
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

func storageCacheBypass(file fs.File, offset, length int64) error {
	f, ok := file.(interface{ Fd() uintptr })
	if !ok {
		return errStorageCacheBypass
	}
	// DONTNEED discards whole clean pages. Round the requested window out so
	// short files and tail pages do not silently leave the page cached.
	page := int64(os.Getpagesize())
	start := offset / page * page
	end := (offset + length + page - 1) / page * page
	return unix.Fadvise(int(f.Fd()), start, end-start, unix.FADV_DONTNEED)
}
