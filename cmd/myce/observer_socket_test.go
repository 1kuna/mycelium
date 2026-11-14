package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
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

func TestObserverStartupWaitsForOldSocketCleanup(t *testing.T) {
	binary := devTestBinary(t)
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/wait" {
			fmt.Fprint(w, `{"jobs":[{"id":"job","state":"running","revision":1}]}`)
			return
		}
		fmt.Fprint(w, `{"id":"job","state":"running"}`)
	}))
	defer server.Close()
	receipt, entry, prior := sharedWatchFixture(t, root, server.URL, &waitClock{now: time.Now()})
	prior.Close() // The idle old owner already released lifetime ownership.
	defer func() {
		// Killing only the child lets its supervisor restart and write into a
		// concurrently removed TempDir. Retire this registration and wait for
		// both owners to exit before removing the fixture.
		if err := os.RemoveAll(receipt); err != nil {
			t.Error(err)
		}
		if err := os.Remove(entry); err != nil && !os.IsNotExist(err) {
			t.Error(err)
		}
		if wake, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"}); err == nil {
			_, _ = wake.Write([]byte{1})
			wake.Close()
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		waitCLIObserverFixture(t, cleanup, root)
	}()
	checked, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	cleaned := make(chan error, 1)
	go func() {
		cleaned <- retireSharedProfileSocket(root, path, identity, func() { close(checked); <-resume })
	}()
	<-checked // Pause after the old identity check, while startup.lock is held.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := make(chan error, 1)
	go func() {
		// Same caller contract as registration/handoff: startup.lock stays held
		// through the child's socket replacement and readiness acknowledgement.
		startup, err := sharedProfileLock(ctx, filepath.Join(root, "startup.lock"))
		if err != nil {
			started <- err
			return
		}
		defer startup.Close()
		owner, err := profileLock(filepath.Join(root, "observer.lock"))
		if err != nil {
			started <- err
			return
		}
		defer owner.Close()
		started <- startSharedObserver(ctx, binary, root, owner)
	}()
	if err := waitPause(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-cleaned; err != nil {
		t.Fatal(err)
	}
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	var owner observerDiagnostic
	if err := readProfileJSON(filepath.Join(root, "observer.status"), &owner); err != nil {
		t.Fatal(err)
	}
	wake, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal("old cleanup unlinked ordinary replacement", err)
	}
	defer wake.Close()
	if _, err := wake.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
}
