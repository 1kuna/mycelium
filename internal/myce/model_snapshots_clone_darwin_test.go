package myce

import (
	"encoding/binary"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func assertSharedFileClone(t *testing.T, source, target string) {
	t.Helper()
	cloneID := func(path string) uint64 {
		t.Helper()
		name, err := unix.BytePtrFromString(path)
		if err != nil {
			t.Fatal(err)
		}
		// Darwin's extended common attributes occupy forkattr. The packed
		// result is a uint32 length followed by ATTR_CMNEXT_CLONEID (uint64).
		attrs := unix.Attrlist{Bitmapcount: 5, Forkattr: 0x100}
		var result [12]byte
		_, _, errno := syscall.Syscall6(unix.SYS_GETATTRLIST,
			uintptr(unsafe.Pointer(name)), uintptr(unsafe.Pointer(&attrs)),
			uintptr(unsafe.Pointer(&result[0])), uintptr(len(result)), unix.FSOPT_ATTR_CMN_EXTENDED, 0)
		if errno != 0 {
			t.Fatal("query clone identity:", errno)
		}
		if binary.LittleEndian.Uint32(result[:4]) != uint32(len(result)) {
			t.Fatal("incomplete clone identity")
		}
		return binary.LittleEndian.Uint64(result[4:])
	}
	if a, b := cloneID(source), cloneID(target); a == 0 || a != b {
		t.Fatalf("snapshot does not share the source's clone identity: source=%d snapshot=%d", a, b)
	}
}
