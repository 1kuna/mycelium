package main

import (
	"context"
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	acquired := make(chan *os.File, 1)
	failed := make(chan error, 1)
	go func() {
		next, err := sharedProfileLock(ctx, filepath.Join(root, "startup.lock"))
		if err != nil {
			failed <- err
		} else {
			acquired <- next
		}
	}()
	select {
	case next := <-acquired:
		next.Close()
		t.Fatal("duplicate lock owner")
	case err := <-failed:
		t.Fatal("registration refused instead of waiting", err)
	case <-time.After(20 * time.Millisecond):
	}
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
	select {
	case next := <-acquired:
		next.Close()
	case err := <-failed:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("registration did not recover lock")
	}

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
