package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// linkBlob makes dst name the bytes at src without replacing an existing dst,
// like os.Link. Callers ignore an os.IsExist error, as they did with os.Link.
//
// Some filesystems refuse hard links (macOS smbfs returns EOPNOTSUPP). There
// it copies src to a private temporary name beside src, checks the copy
// against hash, and renames it into place, so a partial copy is never visible
// under a blob name. The temporary name is new for every call, so a trash
// entry is never renamed back into blobs (#2125).
func (s *FileStore) linkBlob(src, dst, hash string) error {
	return s.placeBlob(src, dst, hash, copyBlob)
}

// moveBlob places a verified upload, which publication unlinks next anyway.
// Where links are refused it renames the upload into place rather than
// copying it: a copy reads and writes the whole file, which on a share
// outlasted the final PATCH's response window (#2155). Only a rename across
// devices copies.
func (s *FileStore) moveBlob(src, dst, hash string) error {
	return s.placeBlob(src, dst, hash, func(src, dst, hash string) error {
		if err := os.Rename(src, dst); !errors.Is(err, syscall.EXDEV) {
			return err
		}
		return copyBlob(src, dst, hash)
	})
}

func (s *FileStore) placeBlob(src, dst, hash string, fallback func(src, dst, hash string) error) error {
	link := os.Link
	if s.link != nil {
		link = s.link
	}
	err := link(src, dst)
	if err == nil || !linkUnsupported(err) {
		return err
	}
	// Recheck just before the fallback, as copyBlob does before its rename.
	if _, e := os.Lstat(dst); e == nil {
		return &os.LinkError{Op: "link", Old: src, New: dst, Err: os.ErrExist}
	}
	return fallback(src, dst, hash)
}

func linkUnsupported(err error) bool {
	for _, errno := range []syscall.Errno{syscall.EOPNOTSUPP, syscall.ENOTSUP, syscall.EPERM, syscall.EXDEV} {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
}

func copyBlob(src, dst, hash string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(src), ".copy-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	sum := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, sum), in)
	if err = errors.Join(err, tmp.Sync(), tmp.Close()); err != nil {
		return err
	}
	if got := hex.EncodeToString(sum.Sum(nil)); got != hash {
		return fmt.Errorf("copy of %s has digest %s, want %s", filepath.Base(src), got, hash)
	}
	// Recheck just before the rename: a concurrent publisher may have placed
	// the same verified bytes. Losing this race replaces identical bytes.
	if _, err := os.Lstat(dst); err == nil {
		return &os.LinkError{Op: "link", Old: src, New: dst, Err: os.ErrExist}
	}
	return os.Rename(tmp.Name(), dst)
}
