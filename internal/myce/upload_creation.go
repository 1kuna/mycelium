package myce

import (
	"context"
	"maps"
	"net/http"
	"sync"

	tus "github.com/tus/tusd/v2/pkg/handler"
)

type uploadCreationContextKey struct{}

func creationContext(ctx context.Context) context.Context {
	if original, ok := ctx.Value(uploadCreationContextKey{}).(context.Context); ok {
		return original
	}
	return ctx
}

// A creation owns its identity until tus has finished creating the upload.
// Reserved lengths bridge the interval before tus metadata exists on disk.
type uploadCreation struct {
	done   chan struct{}
	length uint64
}

// Queue waits must not keep abandoned HTTP requests (or their reservations)
// alive. The waiter only acquires/releases the lock; it never executes work.
func lockUploadContext(ctx context.Context, lock, unlock func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Done() == nil {
		lock()
		return nil
	}
	acquired := make(chan struct{})
	go func() {
		lock()
		select {
		case acquired <- struct{}{}:
		case <-ctx.Done():
			unlock()
		}
	}()
	select {
	case <-acquired:
		if err := ctx.Err(); err != nil {
			unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *FileStore) beginCreation(ctx context.Context, hash string) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.creations == nil {
			s.creations = map[string]*uploadCreation{}
		}
		existing := s.creations[hash]
		if existing == nil {
			entry := &uploadCreation{done: make(chan struct{})}
			s.creations[hash] = entry
			s.mu.Unlock()
			return func() { s.mu.Lock(); delete(s.creations, hash); close(entry.done); s.mu.Unlock() }, nil
		}
		s.mu.Unlock()
		select {
		case <-existing.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Only the filesystem pair creation is serialized with inventory. In
// particular, tus's purpose callback runs before this lock, outside admission.
type creationDataStore struct {
	tus.DataStore
	mu    *sync.RWMutex
	store *FileStore
}

func (s creationDataStore) NewUpload(ctx context.Context, info tus.FileInfo) (tus.Upload, error) {
	ctx = creationContext(ctx)
	if err := lockUploadContext(ctx, s.mu.Lock, s.mu.Unlock); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	upload, err := s.DataStore.NewUpload(ctx, info)
	if err == nil {
		s.store.noteAllocation(info.ID, uint64(info.Size))
	}
	return upload, uploadFailure(ctx, "upload_create", err)
}

func uploadCancelled(w http.ResponseWriter, ctx context.Context) bool {
	if ctx.Err() == nil {
		return false
	}
	http.Error(w, "upload request cancelled", http.StatusRequestTimeout)
	return true
}

// Disk work precedes queue acquisition. Keep allocations published since
// this snapshot in memory until its final admission check has consumed them.
type completedAllocation struct{ generation, length uint64 }
type fileCapacitySnapshot struct {
	store      *FileStore
	generation uint64
	status     FileStoreStatus
	payloads   map[string]storedPayload
}

func (s *FileStore) snapshotCapacity(ctx context.Context) *fileCapacitySnapshot {
	view := &fileCapacitySnapshot{store: s}
	s.mu.Lock()
	view.generation = s.allocationGeneration
	if s.capacitySnapshots == nil {
		s.capacitySnapshots = map[*fileCapacitySnapshot]bool{}
	}
	s.capacitySnapshots[view] = true
	s.mu.Unlock()
	var err error
	view.status, view.payloads, err = s.diskCapacity(ctx)
	if err != nil {
		view.status.Error = "file store capacity unavailable"
	}
	return view
}
func (s *FileStore) noteAllocation(hash string, length uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocationGeneration++
	if s.completedAllocations == nil {
		s.completedAllocations = map[string]completedAllocation{}
	}
	s.completedAllocations[hash] = completedAllocation{s.allocationGeneration, length}
}
func (v *fileCapacitySnapshot) release() {
	s := v.store
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.capacitySnapshots, v)
	oldest := s.allocationGeneration
	for view := range s.capacitySnapshots {
		oldest = min(oldest, view.generation)
	}
	for hash, allocation := range s.completedAllocations {
		if allocation.generation <= oldest {
			delete(s.completedAllocations, hash)
		}
	}
}

// Caller holds s.mu; queue admission also holds q.mu. This is memory only.
func (v *fileCapacitySnapshot) current() (FileStoreStatus, map[string]storedPayload) {
	s := v.store
	status := v.status
	payloads := maps.Clone(v.payloads)
	if payloads == nil {
		payloads = map[string]storedPayload{}
	}
	include := func(hash string, length uint64) {
		p := payloads[hash]
		p.length = max(p.length, length)
		payloads[hash] = p
	}
	for hash, allocation := range s.completedAllocations {
		if allocation.generation > v.generation {
			include(hash, allocation.length)
		}
	}
	for hash, creation := range s.creations {
		if creation.length > 0 {
			include(hash, creation.length)
		}
	}
	var allocated, remaining uint64
	status.UsedBytes = 0
	for _, p := range payloads {
		status.UsedBytes = addDiskBytes(status.UsedBytes, p.size)
		allocated = addDiskBytes(allocated, p.length)
		remaining = addDiskBytes(remaining, p.length-p.size)
	}
	unavailable := addDiskBytes(status.FloorBytes, remaining)
	status.FreeBytes = min(status.LimitBytes-min(status.LimitBytes, allocated), status.DiskFreeBytes-min(status.DiskFreeBytes, unavailable))
	return status, payloads
}
