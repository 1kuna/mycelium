package myce

import (
	"context"
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
	mu *sync.RWMutex
}

func (s creationDataStore) NewUpload(ctx context.Context, info tus.FileInfo) (tus.Upload, error) {
	ctx = creationContext(ctx)
	if err := lockUploadContext(ctx, s.mu.Lock, s.mu.Unlock); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	return s.DataStore.NewUpload(ctx, info)
}

func uploadCancelled(w http.ResponseWriter, ctx context.Context) bool {
	if ctx.Err() == nil {
		return false
	}
	http.Error(w, "upload request cancelled", http.StatusRequestTimeout)
	return true
}

// Keep future queue output commitments visible to uploads while their queue
// transaction waits. The transaction checks its original (pre-reservation)
// snapshot against current queue commitments; concurrent provisional budgets
// can conservatively reduce space until their transactions settle.
type capacityReservation struct{ bytes uint64 }

func (s *FileStore) reserveCapacity(ctx context.Context, bytes func(map[string]storedPayload) uint64) (FileStoreStatus, map[string]storedPayload, func(), error) {
	if err := lockUploadContext(ctx, s.admissionMu.Lock, s.admissionMu.Unlock); err != nil {
		return FileStoreStatus{}, nil, nil, err
	}
	defer s.admissionMu.Unlock()
	status, payloads, err := s.capacity(ctx)
	if err != nil {
		status.Error = "file store capacity unavailable"
	}
	reservation := &capacityReservation{bytes: bytes(payloads)}
	s.mu.Lock()
	if s.capacityReservations == nil {
		s.capacityReservations = map[*capacityReservation]bool{}
	}
	s.capacityReservations[reservation] = true
	s.mu.Unlock()
	return status, payloads, func() { s.mu.Lock(); delete(s.capacityReservations, reservation); s.mu.Unlock() }, nil
}
