package myce

import (
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// GuardRootStartup runs before command dispatch or worker-file access.
func GuardRootStartup() error {
	if os.Geteuid() != 0 {
		return nil
	}
	if e := CheckRootBinary(RootBinaryProbe{Executing: func() (os.FileInfo, error) {
		f, e := os.Open("/proc/self/exe")
		if e != nil {
			return nil, e
		}
		defer f.Close()
		return f.Stat()
	}, Readlink: os.Readlink, Lstat: os.Lstat}); e != nil {
		return e
	}
	return rootCloseOnExec()
}
func rootCloseOnExec() error {
	if e := unix.CloseRange(3, ^uint(0), unix.CLOSE_RANGE_CLOEXEC); e == nil {
		return nil
	}
	entries, e := os.ReadDir("/proc/self/fd")
	if e != nil {
		return ErrHostTarget
	}
	for _, entry := range entries {
		fd, e := strconv.Atoi(entry.Name())
		if e != nil || fd < 3 {
			continue
		}
		if _, e = unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); e != nil && e != unix.EBADF {
			return ErrHostTarget
		}
	}
	return nil
}
func hostChildExecutable(force bool) (string, error) { return "/proc/self/exe", nil }
