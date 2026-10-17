//go:build linux

package myce

import (
	"golang.org/x/sys/unix"
	"strconv"
)

func hostDeviceNumber(path string) (string, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return "", err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFCHR {
		return "", nil
	}
	return strconv.FormatUint(uint64(unix.Major(uint64(st.Rdev))), 10) + ":" + strconv.FormatUint(uint64(unix.Minor(uint64(st.Rdev))), 10), nil
}
