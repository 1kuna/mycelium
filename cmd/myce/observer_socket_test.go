package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestObserverSocketRetirementSerializesStartup(t *testing.T) {
	root := t.TempDir()
	path, err := sharedProfileSocket(root)
	if err != nil {
		t.Fatal(err)
	}
	old, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	defer os.Remove(filepath.Dir(path))
	defer os.Remove(path)
	identity, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	startup, err := profileLock(filepath.Join(root, "startup.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer startup.Close()
	// A replacement holds startup while removing/rebinding the endpoint. The old
	// owner's check/unlink must not overlap this whole transaction, including
	// the interval before the replacement has published its new socket.
	if err = retireSharedProfileSocket(root, path, identity); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("old cleanup removed socket during replacement startup", err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	startup.Close()
	// After startup unlocks, the previous identity must still protect the new
	// endpoint. Verify a real datagram wake reaches its replacement receiver.
	if err = retireSharedProfileSocket(root, path, identity); err != nil {
		t.Fatal(err)
	}
	wake, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	if _, err = wake.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err = replacement.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var message [1]byte
	if n, _, err := replacement.ReadFromUnix(message[:]); err != nil || n != 1 {
		t.Fatal("replacement lost registration wake", err)
	}
}
