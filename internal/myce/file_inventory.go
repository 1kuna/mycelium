package myce

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// A shared member's store is a network share, where one full inventory walk
// took 80–93 s and every upload request waited for it (#2144). Shared members
// therefore walk once per turn, then re-read only the identities this process
// changed. The periodic walk adopts writes this process cannot see, such as
// a retiring coordinator's draining requests or an operator's edit. It runs
// off the request path. Its cost is one walk of store metadata per interval.
// Stale entries only matter for the configured allowance: free space and the
// physical floor are read from the volume on every snapshot.
const fileInventoryRewalk = 30 * time.Minute

type fileInventory struct {
	read     sync.Mutex // Serializes readers. Never held with q.mu.
	payloads map[string]storedPayload
	walkedAt time.Time
	walking  bool
	marks    sync.Mutex // Memory only.
	dirty    map[string]bool
	touched  map[string]bool // Non-nil while a background walk runs.
}

// changed marks one identity for re-reading after this process changed its
// store paths. Call after the filesystem change, never before it.
func (s *FileStore) changed(name string) {
	hash := strings.TrimSuffix(filepath.Base(name), ".info")
	if changed, ok := s.claimReleaseObservations.Load(hash); ok {
		changed.(*atomic.Bool).Store(true)
	}
	inv := s.inventory
	if inv == nil {
		return
	}
	inv.marks.Lock()
	inv.dirty[hash] = true
	if inv.touched != nil {
		inv.touched[hash] = true
	}
	inv.marks.Unlock()
}

// Caller holds uploadFilesMu.RLock, so creations and bundle writes are fenced
// exactly as for a full walk. Returns a private copy.
func (s *FileStore) currentInventory(ctx context.Context) (map[string]storedPayload, error) {
	inv := s.inventory
	if inv == nil {
		return s.inventoryUnlocked(ctx)
	}
	if err := lockUploadContext(ctx, inv.read.Lock, inv.read.Unlock); err != nil {
		return nil, err
	}
	defer inv.read.Unlock()
	if inv.payloads == nil {
		// Changes before the walk are in it; later ones stay marked.
		inv.marks.Lock()
		clear(inv.dirty)
		inv.marks.Unlock()
		payloads, err := s.inventoryUnlocked(ctx)
		if err != nil {
			return nil, err
		}
		inv.payloads, inv.walkedAt = payloads, s.clock.Now()
	} else if !inv.walking && !s.clock.Now().Before(inv.walkedAt.Add(fileInventoryRewalk)) {
		inv.walking = true
		go s.rewalkInventory()
	}
	inv.marks.Lock()
	dirty := inv.dirty
	inv.dirty = map[string]bool{}
	inv.marks.Unlock()
	for hash := range dirty {
		p, ok, err := s.storedPayloadOf(ctx, hash)
		if err != nil {
			inv.marks.Lock()
			for hash := range dirty {
				inv.dirty[hash] = true
			}
			inv.marks.Unlock()
			return nil, err
		}
		if ok {
			inv.payloads[hash] = p
		} else {
			delete(inv.payloads, hash)
		}
	}
	return maps.Clone(inv.payloads), nil
}

// The walk holds no fence, so it may see a creation or publication midway.
// Every such change marks its identity when done, inside the walk's window,
// and adoption re-reads those identities before the next snapshot.
func (s *FileStore) rewalkInventory() {
	inv := s.inventory
	inv.marks.Lock()
	inv.touched = map[string]bool{}
	inv.marks.Unlock()
	payloads, err := s.inventoryUnlocked(context.Background())
	inv.read.Lock()
	defer inv.read.Unlock()
	inv.marks.Lock()
	for hash := range inv.touched {
		inv.dirty[hash] = true
	}
	inv.touched = nil
	inv.marks.Unlock()
	if err == nil {
		inv.payloads = payloads
	}
	inv.walkedAt, inv.walking = s.clock.Now(), false
}

// primeInventory takes the turn's one full walk before the store serves, so
// no upload request waits for it.
func (s *FileStore) primeInventory(ctx context.Context) {
	if s.inventory == nil {
		return
	}
	if lockUploadContext(ctx, s.uploadFilesMu.RLock, s.uploadFilesMu.RUnlock) != nil {
		return
	}
	defer s.uploadFilesMu.RUnlock()
	_, _ = s.currentInventory(ctx)
}

// storedPayloadOf reads one identity as a full walk would. A published blob
// wins over a second upload link, as in the walk.
func (s *FileStore) storedPayloadOf(ctx context.Context, hash string) (storedPayload, bool, error) {
	for _, dir := range []string{"blobs", "uploads"} {
		st, err := os.Lstat(filepath.Join(s.root, dir, hash))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return storedPayload{}, false, err
		}
		if !st.Mode().IsRegular() {
			return storedPayload{}, false, errors.New("invalid store payload")
		}
		p, err := s.storedPayloadAt(ctx, dir, hash, st)
		return p, err == nil, err
	}
	return storedPayload{}, false, nil
}
