package myce

import "path/filepath"

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
