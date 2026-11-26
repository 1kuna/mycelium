package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Job-file collection never unlinks a live blob path (#1176, #2117). It moves
// blobs/<digest> to a unique trash name, checks the committed state, and then
// purges that name or restores the bytes. A move that stalls and lands late
// can only hide bytes: readers and the next pass restore a referenced blob. A
// late purge can only hit its own never-reused name.
//
// Restores copy the bytes into a new file and never rename an entry back. On
// SMB a pending delete follows the open file, not the path, so a late purge
// could otherwise follow a renamed entry back into blobs.

func (s *FileStore) trashDir() string { return filepath.Join(s.root, "trash") }

func (s *FileStore) trashBlob(hash string) (string, error) {
	entry := filepath.Join(s.trashDir(), hash+"."+ID())
	if s.beforeTrash != nil {
		s.beforeTrash(entry)
	}
	return entry, os.Rename(s.blob(FileRef{SHA256: hash}), entry)
}

// trashEntries maps each entry path to its digest.
func (s *FileStore) trashEntries() (map[string]string, error) {
	entries, err := os.ReadDir(s.trashDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, entry := range entries {
		hash, _, ok := strings.Cut(entry.Name(), ".")
		if ok && (FileRef{SHA256: hash}).validate() == nil {
			out[filepath.Join(s.trashDir(), entry.Name())] = hash
		}
	}
	return out, nil
}

// restoreBlob makes blobs/<digest> exist again if any trash entry holds the bytes.
func (s *FileStore) restoreBlob(hash string) error {
	if _, err := os.Lstat(s.blob(FileRef{SHA256: hash})); err == nil {
		return nil
	}
	entries, err := filepath.Glob(filepath.Join(s.trashDir(), hash+".*"))
	if err != nil {
		return err
	}
	err = os.ErrNotExist
	for _, entry := range entries {
		if err = s.restoreFrom(hash, entry); err == nil {
			return nil
		}
	}
	return err
}

// Readers call this before opening a committed blob. A late move then costs
// a copy instead of a failure.
func (s *FileStore) restoreMissing(hash string) {
	if _, err := os.Lstat(s.blob(FileRef{SHA256: hash})); errors.Is(err, os.ErrNotExist) {
		if err := s.restoreBlob(hash); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("file restore failed hash=%s error=%q", hash, err)
		}
	}
}

// restoreFrom copies one entry into a new blob, verifying its digest. Bytes
// that already exist are left alone.
func (s *FileStore) restoreFrom(hash, entry string) error {
	blob := s.blob(FileRef{SHA256: hash})
	if _, err := os.Lstat(blob); err == nil {
		return nil
	}
	in, err := os.Open(entry)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Join(s.root, "uploads"), ".restore-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	sum := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, sum), in)
	if err = errors.Join(err, tmp.Sync(), tmp.Close()); err != nil {
		return err
	}
	if hex.EncodeToString(sum.Sum(nil)) != hash {
		return errors.New("trash entry does not match its digest")
	}
	if err := os.Link(tmp.Name(), blob); err != nil && !os.IsExist(err) {
		return err
	}
	return s.syncDir(filepath.Dir(blob))
}

func (s *FileStore) verify(ctx context.Context, ref FileRef) error {
	err := verifyFile(ctx, s.blob(ref), ref)
	if errors.Is(err, os.ErrNotExist) && s.restoreBlob(ref.SHA256) == nil {
		err = verifyFile(ctx, s.blob(ref), ref)
	}
	return err
}
