package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
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

func TestObserverSupervisedStartupWaitsForOldSocketCleanup(t *testing.T) {
	t.Setenv("MYCE_SUPERVISED_OBSERVER", "1")
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
	checked, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	cleaned := make(chan error, 1)
	go func() {
		cleaned <- retireSharedProfileSocket(root, path, identity, func() { close(checked); <-resume })
	}()
	<-checked // Old cleanup holds startup.lock after checking the old identity.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	priorLog := log.Writer()
	defer log.SetOutput(priorLog)
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- runSupervisedObserver(ctx, root) }()
	defer func() { release(); cancel(); <-ownerDone }()
	wait(t, ctx, func() bool {
		lock, err := profileLock(filepath.Join(root, "observer.lock"))
		if lock != nil {
			lock.Close()
		}
		return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
	})
	// Give the lifetime owner an opportunity to bind while old cleanup is
	// deliberately paused. With the old bug, it replaces the checked socket.
	if err := waitPause(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-cleaned; err != nil {
		t.Fatal(err)
	}
	var before observerDiagnostic
	wait(t, ctx, func() bool {
		return readProfileJSON(filepath.Join(root, "observer.status"), &before) == nil && before.Phase == "idle"
	})
	wake, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal("old cleanup unlinked supervised replacement", err)
	}
	_, err = wake.Write([]byte{1})
	wake.Close()
	if err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var after observerDiagnostic
		return readProfileJSON(filepath.Join(root, "observer.status"), &after) == nil && after.CapturedAt.After(before.CapturedAt)
	})
}
