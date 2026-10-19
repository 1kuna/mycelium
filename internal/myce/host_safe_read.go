package myce

import (
	"encoding/json"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Open nonblocking and without following the leaf, then validate the opened
// inode. A FIFO/device or concurrent leaf replacement cannot turn this into an
// unbounded read. All callers are the worker, never the root controller.
func readHostRegular(path string, uid uint32, limit int64) ([]byte, error) {
	if os.Geteuid() == 0 {
		return nil, ErrHostIdentity
	}
	f, e := openHostRegular(path, uid, limit)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, limit+1))
	if e != nil || int64(len(b)) > limit {
		return nil, ErrHostTarget
	}
	return b, nil
}

// Marker probes share the same opened-inode safety checks without reading the
// persisted payload. The caller owns the returned file.
func openHostRegular(path string, uid uint32, limit int64) (*os.File, error) {
	fd, e := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if e != nil {
		if e == unix.ENOENT {
			return nil, os.ErrNotExist
		}
		return nil, ErrHostTarget
	}
	f := os.NewFile(uintptr(fd), "host-data")
	if e = hostFileAccessACL(fd); e != nil {
		f.Close()
		return nil, ErrHostTarget
	}
	st, e := f.Stat()
	if e != nil {
		f.Close()
		return nil, ErrHostTarget
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uid || !st.Mode().IsRegular() || st.Mode().Perm()&0022 != 0 || st.Size() > limit {
		f.Close()
		return nil, ErrHostTarget
	}
	return f, nil
}
func readHostServiceConfig(path string) (ServiceConfig, error) {
	var c ServiceConfig
	b, e := readHostRegular(path, uint32(os.Geteuid()), 1<<20)
	if e != nil {
		return c, e
	}
	if e = json.Unmarshal(b, &c); e != nil {
		return c, ErrHostState
	}
	return c, nil
}
