package myce

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/tus/tusd/v2/pkg/filestore"
	tus "github.com/tus/tusd/v2/pkg/handler"
	"os"
	"path/filepath"
	"strings"
)

func (s *FileStore) reserveUpload(hash string, remaining int64) error {
	s.hostMu.Lock()
	defer s.hostMu.Unlock()
	if s.hostPending == nil {
		s.hostPending = map[string]*cacheReservation{}
	}
	if s.hostPending[hash] != nil {
		return nil
	}
	partial := filepath.Join(s.root, "uploads", hash)
	// The final blob can be independently published by another writer. Only
	// this upload's private partial/metadata may discharge its pending bytes.
	r, err := s.budgets.reserveClaim(s.root, remaining, s.clock, "upload", identityHash(partial), partial, partial+".info")
	if err != nil {
		return err
	}
	s.hostPending[hash] = r
	return nil
}
func (s *FileStore) releaseUpload(hash string) {
	s.hostMu.Lock()
	r := s.hostPending[hash]
	delete(s.hostPending, hash)
	s.hostMu.Unlock()
	r.release()
}
func (s *FileStore) closeHostClaims() {
	s.hostMu.Lock()
	pending := s.hostPending
	s.hostPending = nil
	s.hostMu.Unlock()
	for _, r := range pending {
		r.detach()
	}
}

func (s *FileStore) reserveUploadMetadata(info tus.FileInfo) error {
	s.hostMu.Lock()
	held := s.hostPending[info.ID]
	s.hostMu.Unlock()
	if held == nil {
		return errors.New("upload lacks shared filesystem reservation")
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	// Account for filestore's backend paths as well as the actual caller metadata.
	// JSON size supplies a protocol-derived bound, including escaped strings.
	storage, err := json.Marshal(map[string]string{"Type": "filestore", "Path": filepath.Join(s.root, "uploads", info.ID), "InfoPath": filepath.Join(s.root, "uploads", info.ID+".info")})
	if err != nil {
		return err
	}
	disk, err := s.budgets.probe(s.root)
	if err != nil {
		return err
	}
	return held.ensureCharge(s.root, cacheSum(held.bytes, cacheWriteCharge(disk, int64(len(data)+len(storage)))))
}

// Keep following this upload's inode after link publication, without claiming
// another publisher's preexisting blob. The volume census deduplicates aliases.
func (s *FileStore) trackUploadPublication(hash, source, dest string) error {
	s.hostMu.Lock()
	held := s.hostPending[hash]
	s.hostMu.Unlock()
	if held == nil {
		return nil
	}
	a, err := os.Stat(source)
	if err != nil {
		return err
	}
	b, err := os.Stat(dest)
	if err != nil {
		return err
	}
	if os.SameFile(a, b) {
		return held.addPath(dest)
	}
	return nil
}

// Restore from tus metadata rather than the deduplicated blob inventory: an
// independent publisher may already own the same hash while this partial still
// has a distinct resumable write obligation.
func (s *FileStore) restoreUploadClaims() error {
	disk, err := s.budgets.probe(s.root)
	if err != nil {
		return err
	}
	view, err := s.budgets.hostView(disk.Volume)
	if err != nil {
		return err
	}
	hashes := map[string]bool{}
	for _, path := range view.persistent {
		f, e := os.Open(path)
		if e != nil {
			view.close()
			return e
		}
		var claim hostCacheClaim
		e = readCacheRecord(f, &claim)
		f.Close()
		if e != nil {
			view.close()
			return e
		}
		if claim.Root == s.root && len(claim.Paths) > 0 {
			hashes[filepath.Base(claim.Paths[0])] = true
		}
	}
	view.close()
	entries, err := os.ReadDir(filepath.Join(s.root, "uploads"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".info") {
			hashes[strings.TrimSuffix(entry.Name(), ".info")] = true
		}
	}
	for hash := range hashes {
		if (FileRef{SHA256: hash}).validate() != nil {
			continue
		}
		partial := filepath.Join(s.root, "uploads", hash)
		if _, err := os.Lstat(partial + ".info"); os.IsNotExist(err) {
			if _, err := os.Lstat(partial); os.IsNotExist(err) {
				// Complete publication or fenced reclamation may have crashed
				// just before retiring the scalar commitment. No upload can resume.
				if err := s.reserveUpload(hash, 0); err != nil {
					return err
				}
				s.releaseUpload(hash)
			} else if err != nil {
				return err
			}
			continue
		} else if err != nil {
			return err
		}
		upload, err := filestore.New(filepath.Join(s.root, "uploads")).GetUpload(context.Background(), hash)
		if err != nil {
			return err
		}
		info, err := upload.GetInfo(context.Background())
		if err != nil || info.Offset < 0 || info.Size < info.Offset {
			return errors.Join(err, errors.New("invalid accepted upload length"))
		}
		if err := s.reserveUpload(hash, info.Size-info.Offset); err != nil {
			return err
		}
	}
	return nil
}
