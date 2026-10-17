package myce

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestHostLinuxAccessACLRejected(t *testing.T) {
	// An ACL with mask 0 keeps mode &022 == 0; presence must still be rejected.
	acl := make([]byte, 4+5*8)
	binary.LittleEndian.PutUint32(acl, 2)
	for i, entry := range []struct {
		tag, perm uint16
		id        uint32
	}{
		{1, 7, ^uint32(0)}, {2, 6, uint32(os.Geteuid() + 1)},
		{4, 0, ^uint32(0)}, {16, 0, ^uint32(0)}, {32, 0, ^uint32(0)},
	} {
		b := acl[4+i*8:]
		binary.LittleEndian.PutUint16(b, entry.tag)
		binary.LittleEndian.PutUint16(b[2:], entry.perm)
		binary.LittleEndian.PutUint32(b[4:], entry.id)
	}
	for _, directory := range []bool{false, true} {
		dir := t.TempDir()
		path := filepath.Join(dir, "target")
		if directory {
			if e := os.Mkdir(path, 0700); e != nil {
				t.Fatal(e)
			}
		} else if e := os.WriteFile(path, []byte("{}"), 0600); e != nil {
			t.Fatal(e)
		}
		if e := unix.Setxattr(path, "system.posix_acl_access", acl, 0); e != nil {
			t.Fatal(e)
		}
		if e := validateHostTarget(path, uint32(os.Geteuid()), directory); !errors.Is(e, ErrHostTarget) {
			t.Fatal(e)
		}
		if directory {
			child := filepath.Join(path, "child")
			if e := os.WriteFile(child, []byte("{}"), 0600); e != nil {
				t.Fatal(e)
			}
			if e := validateHostTarget(child, uint32(os.Geteuid()), false); !errors.Is(e, ErrHostTarget) {
				t.Fatal(e)
			}
		} else if _, e := readHostRegular(path, uint32(os.Geteuid()), 1024); !errors.Is(e, ErrHostTarget) {
			t.Fatal(e)
		}
	}
	for _, e := range []error{nil, unix.EACCES, unix.EIO} {
		if !errors.Is(hostACLResult(e), ErrHostTarget) {
			t.Fatal(e)
		}
	}
}
