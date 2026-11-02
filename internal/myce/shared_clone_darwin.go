package myce

import (
	"os"

	"golang.org/x/sys/unix"
)

// cloneSharedFile makes target an APFS copy-on-write clone of the open source.
// It reports false when the volume or path pair cannot clone; callers copy.
func cloneSharedFile(in *os.File, target string) bool {
	if unix.Fclonefileat(int(in.Fd()), unix.AT_FDCWD, target, 0) != nil {
		return false
	}
	f, err := os.Open(target)
	if err == nil {
		err = f.Sync()
		f.Close()
	}
	if err != nil {
		_ = os.Remove(target)
		return false
	}
	return true
}
