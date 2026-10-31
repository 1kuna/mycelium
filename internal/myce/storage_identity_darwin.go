package myce

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func nativeStorageFilesystemIdentity(fd int) (storageFilesystemIdentity, error) {
	// f_fsid can be reused for a different APFS volume mounted later at the
	// same path. ATTR_VOL_UUID identifies the actual volume across reattachment.
	// x/sys supplies attrlist/constants but no fgetattrlist wrapper.
	attrs := unix.Attrlist{Bitmapcount: unix.ATTR_BIT_MAP_COUNT, Volattr: unix.ATTR_VOL_UUID | unix.ATTR_VOL_INFO}
	var result struct {
		Length uint32
		UUID   [16]byte
	}
	_, _, errno := syscall.Syscall6(syscall.SYS_FGETATTRLIST, uintptr(fd), uintptr(unsafe.Pointer(&attrs)), uintptr(unsafe.Pointer(&result)), unsafe.Sizeof(result), 0, 0)
	if errno != 0 {
		return storageFilesystemIdentity{}, errno
	}
	if result.Length != uint32(unsafe.Sizeof(result)) || result.UUID == [16]byte{} {
		return storageFilesystemIdentity{}, syscall.EIO
	}
	return storageFilesystemIdentity{Platform: "darwin", UUID: result.UUID}, nil
}
