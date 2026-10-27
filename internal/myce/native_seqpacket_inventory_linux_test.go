package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNativeSeqpacketStartupLiveListener(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "myce-seq-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	name := filepath.Join(dir, "listener")
	if err = unix.Bind(fd, &unix.SockaddrUnix{Name: name}); err != nil {
		t.Fatal(err)
	}
	if err = unix.Listen(fd, 1); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("/proc/net/unix")
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := nativeSeqpacketEndpoints(data)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, endpoint := range endpoints {
		if endpoint.Name == name {
			found = true
			uid, known := nativeSeqpacketPathOwner(name)
			if !known || uid != uint32(os.Getuid()) {
				t.Fatalf("listener owner=%d known=%t", uid, known)
			}
			unexpected := nativeUnexpectedSeqpacket([]nativeSeqpacketEndpoint{endpoint}, nativeSeqpacketPathOwner)
			if (len(unexpected) != 0) != (os.Getuid() != 0) {
				t.Fatal("non-root seqpacket warning did not match real listener ownership")
			}
		}
	}
	if !found {
		t.Fatal("live seqpacket listener missing from startup inventory")
	}
	if warning := nativeSeqpacketStartupWarning(nil, errors.New("probe unavailable")); !strings.Contains(warning, "inventory unavailable") {
		t.Fatal("inventory failure was not visible", warning)
	}
}
