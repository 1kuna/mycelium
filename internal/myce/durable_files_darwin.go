//go:build darwin

package myce

import (
	"os"

	"golang.org/x/sys/unix"
)

// fsync alone does not flush a macOS device's volatile write cache.
func syncLocalFile(f *os.File) error {
	if err := f.Sync(); err != nil {
		return err
	}
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
