package myce

import (
	"encoding/json"
	"errors"
	tus "github.com/tus/tusd/v2/pkg/handler"
	"path/filepath"
)

func (s *FileStore) reserveUpload(hash string, remaining int64, adopt bool) error {
	s.hostMu.Lock()
	defer s.hostMu.Unlock()
	if s.hostPending == nil {
		s.hostPending = map[string]*cacheReservation{}
	}
	if s.hostPending[hash] != nil {
		return nil
	}
	r, err := s.budgets.reserveClaim(s.root, remaining, s.clock, adopt,
		filepath.Join(s.root, "uploads", hash), s.blob(FileRef{SHA256: hash}))
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
		r.release()
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
