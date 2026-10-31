package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func TestStorageIdentityObservation(t *testing.T) {
	root := t.TempDir()
	before, err := ObserveStorageBackingIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(root, "child")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(child, "data"), []byte("usage is not identity"), 0600); err != nil {
		t.Fatal(err)
	}
	// A directory/inode or path change on the same filesystem is not a swap.
	if after, err := ObserveStorageBackingIdentity(child); err != nil || after != before {
		t.Fatal("same filesystem changed identity", after, err)
	}
	for _, path := range []string{filepath.Join(root, "missing"), filepath.Join(child, "data")} {
		if id, err := ObserveStorageBackingIdentity(path); err == nil || id != "" {
			t.Fatal("invalid root granted volume identity", id, err)
		}
	}
}

// Both native platform tests use this queue/SQLite proof. The caller has mounted
// its first disposable filesystem; switchVolume detaches it and attaches A or B
// at exactly the same path. Native setup is opt-in, never part of normal gates.
func proveStorageIdentityVolumes(t *testing.T, root string, switchVolume func(int)) {
	t.Helper()
	q, c, dbpath := protectionQueue(t)
	m := protectionMachine("producer", "fake")
	b := protectionBackend(m)
	observe := func() string {
		t.Helper()
		id, err := ObserveStorageBackingIdentity(root)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	original := observe()
	b.snapshot.Backings[1].NativeIdentity = original
	control := protectionControlWork(t, c)
	if err := q.ConfigureStorageProtection(context.Background(), b, control); err != nil {
		t.Fatal(err)
	}
	submit(t, q, protectionSpec("experiment"))
	if check(t, q, m, nil).Job == nil {
		t.Fatal("missing retained allocation")
	}
	retained := q.s.StorageProtection
	refreshSame := func() {
		t.Helper()
		b.snapshot.Backings[1].NativeIdentity = observe()
		if b.snapshot.Backings[1].NativeIdentity != original {
			t.Fatal("usage or same-volume reattach changed identity")
		}
		if err := q.ConfigureStorageProtection(context.Background(), b, control); err != nil {
			t.Fatal("same volume failed verification", err)
		}
		if _, _, err := q.files.capacity(context.Background()); err != nil {
			t.Fatal("same volume blocked publication capacity", err)
		}
	}
	// Refuse to fill anything except an explicitly mounted small test filesystem.
	var fs syscall.Statfs_t
	if err := syscall.Statfs(root, &fs); err != nil || fs.Blocks*diskBlockSize(&fs) > 128<<20 || fs.Blocks == 0 {
		t.Fatal("native identity test requires a disposable filesystem <=128 MiB", err)
	}
	f, err := os.Create(filepath.Join(root, "identity-fill"))
	if err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i * 17)
	}
	var written uint64
	for written <= fs.Blocks*diskBlockSize(&fs) {
		n, e := f.Write(chunk)
		written += uint64(n)
		if e != nil {
			err = e
			break
		}
	}
	closeErr := f.Close()
	if !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, syscall.EDQUOT) {
		t.Fatal("disposable filesystem did not fill", written, err, closeErr)
	}
	refreshSame()
	if err := os.Remove(filepath.Join(root, "identity-fill")); err != nil {
		t.Fatal(err)
	}
	refreshSame()
	// Same-volume detach/reattach and queue restart must preserve identity.
	switchVolume(0)
	queueRoot, files := q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(dbpath, queueRoot, c, WithFileStore(files, 185<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	refreshSame()
	// A distinct filesystem at the identical path is a genuine replacement.
	switchVolume(1)
	b.snapshot.Backings[1].NativeIdentity = observe()
	if b.snapshot.Backings[1].NativeIdentity == original {
		t.Fatal("different filesystems shared an identity")
	}
	if err := q.ConfigureStorageProtection(context.Background(), b, control); err == nil || err.Error() != "retained storage backing changed" {
		t.Fatal("different volume was not fenced", err)
	}
	if q.storageProtectionVerified || !reflect.DeepEqual(retained.Allocations, q.s.StorageProtection.Allocations) {
		t.Fatal("replacement renewed verification or lost retained allocation")
	}
	if _, _, err := q.files.capacity(context.Background()); err == nil {
		t.Fatal("replacement granted unverified capacity")
	}
	switchVolume(0)
	refreshSame()
	t.Logf("native fill (%d bytes), empty, same-volume reattach/restart accepted; different-volume swap fenced; original restored", written)
}
