package myce

import (
	"os"
	"syscall"
	"testing"
	"unsafe"
)

func assertSharedFileClone(t *testing.T, source, target string) {
	t.Helper()
	// FIEMAP reports each file's shared extents independently of activity on
	// its volume. Query one extent at a time so fragmentation has no bound.
	type extent struct {
		Logical, Physical, Length uint64
		Reserved64                [2]uint64
		Flags                     uint32
		Reserved                  [3]uint32
	}
	type fiemap struct {
		Start, Length                  uint64
		Flags, Mapped, Count, Reserved uint32
		Extent                         extent
	}
	for _, path := range []string{source, target} {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		for offset := uint64(0); offset < uint64(info.Size()); {
			m := fiemap{Start: offset, Length: uint64(info.Size()) - offset, Flags: 1, Count: 1}               // FIEMAP_FLAG_SYNC
			_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), 0xc020660b, uintptr(unsafe.Pointer(&m))) // FS_IOC_FIEMAP
			if errno != 0 {
				t.Fatal("query shared extents:", errno)
			}
			e := m.Extent
			if m.Mapped != 1 || e.Logical > offset || e.Length == 0 || e.Logical+e.Length <= offset || e.Flags&0x2000 == 0 { // FIEMAP_EXTENT_SHARED
				t.Fatalf("snapshot/source data is not shared at offset %d: mapped=%d extent=%+v", offset, m.Mapped, e)
			}
			offset = e.Logical + e.Length
		}
	}
}
