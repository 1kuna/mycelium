package myce

import (
	"errors"
	"os"
)

// storageFilesystemIdentity deliberately excludes paths, root inodes, mount
// incarnations and usage. Reattaching the same filesystem is not replacement.
type storageFilesystemIdentity struct {
	Platform string
	FSID     [2]int32
	UUID     [16]byte
	Device   uint64 `json:"device,omitempty"` // Linux st_dev; not used on macOS.
}

// ObserveStorageBackingIdentity observes the filesystem containing root without
// root privileges. macOS uses ATTR_VOL_UUID; Linux uses statfs.f_fsid and st_dev.
// Both syscalls use one open descriptor so a path swap cannot mix observations.
// Backends call this outside q.mu and supply its result as NativeIdentity.
// This identifies the volume, not a quota, confinement or availability guarantee.
func ObserveStorageBackingIdentity(root string) (string, error) {
	f, err := os.Open(root)
	if err != nil {
		return "", errors.New("native storage filesystem identity unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.IsDir() {
		return "", errors.New("native storage filesystem root unavailable")
	}
	identity, err := nativeStorageFilesystemIdentity(int(f.Fd()))
	if err != nil || identity.FSID == [2]int32{} && identity.UUID == [16]byte{} {
		return "", errors.New("native storage filesystem identity unavailable")
	}
	return identityHash(identity), nil
}
