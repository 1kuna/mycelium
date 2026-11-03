package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func creationRequest(ctx context.Context, ref FileRef) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/files/uploads/", nil).WithContext(ctx)
	r.Header.Set("Tus-Resumable", "1.0.0")
	setUploadHeaders(r, ref)
	return r
}
func creationRef(text string) FileRef {
	sum := sha256.Sum256([]byte(text))
	return FileRef{SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(text))}
}
func creationStore(t *testing.T, limit int64) (*Queue, *FileStore) {
	t.Helper()
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), limit, false); err != nil {
		t.Fatal(err)
	}
	return q, q.files
}

func TestUploadCreationBurst(t *testing.T) {
	_, s := creationStore(t, 1<<20)
	record := s.recordUpload
	// Model a one-second queue-lock wait at each purpose lookup. Readers can
	// proceed together once a check-in releases q.mu; a global store lock cannot.
	s.recordUpload = func(hash, purpose string, created bool, contexts ...context.Context) error {
		ctx := requestContext(contexts)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
		return record(hash, purpose, created, contexts...)
	}
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := httptest.NewRecorder()
			s.serveUpload(w, creationRequest(context.Background(), creationRef(fmt.Sprintf("input-%02d", i))))
			if w.Code != http.StatusCreated {
				t.Errorf("creation %d: %d %s", i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("30 creations with one-second queue waits: %s", elapsed)
	if elapsed >= 10*time.Second {
		t.Fatalf("creations serialized: %s", elapsed)
	}
}

func TestUploadCreationCancelledQueueWait(t *testing.T) {
	q, s := creationStore(t, 8)
	entered := make(chan struct{})
	record := s.recordUpload
	var once sync.Once
	s.recordUpload = func(hash, purpose string, created bool, contexts ...context.Context) error {
		once.Do(func() { q.mu.Lock(); close(entered) })
		return record(hash, purpose, created, contexts...)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	ref := creationRef("12345678")
	go func() { defer close(done); s.serveUpload(httptest.NewRecorder(), creationRequest(ctx, ref)) }()
	<-entered
	cancel()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		q.mu.Unlock()
		<-done
		t.Fatal("cancelled creation waited for q.mu")
	}
	q.mu.Unlock()
	w := httptest.NewRecorder()
	s.serveUpload(w, creationRequest(context.Background(), creationRef("abcdefgh")))
	if w.Code != http.StatusCreated {
		t.Fatalf("reservation not released: %d %s", w.Code, w.Body.String())
	}
	if _, err := os.Stat(filepath.Join(s.root, "uploads", ref.SHA256)); !os.IsNotExist(err) {
		t.Fatalf("abandoned creation wrote bytes: %v", err)
	}
}

func TestUploadCreationConcurrentCapacityAndDedupe(t *testing.T) {
	_, s := creationStore(t, 8)
	start := make(chan struct{})
	responses := make(chan int, 30)
	for i := 0; i < 30; i++ {
		go func(i int) {
			<-start
			w := httptest.NewRecorder()
			s.serveUpload(w, creationRequest(context.Background(), creationRef(fmt.Sprintf("%08d", i))))
			responses <- w.Code
		}(i)
	}
	close(start)
	accepted := 0
	for i := 0; i < 30; i++ {
		code := <-responses
		if code == 201 {
			accepted++
		} else if code != 507 {
			t.Errorf("unexpected status %d", code)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d uploads for one slot", accepted)
	}
}

func TestUploadCreationExistingBlobDedupe(t *testing.T) {
	_, s := creationStore(t, 8)
	ref := creationRef("12345678")
	if err := os.WriteFile(s.blob(ref), []byte("12345678"), 0600); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			s.serveUpload(w, creationRequest(context.Background(), ref))
			if w.Code != 201 {
				t.Errorf("dedupe: %d %s", w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	if err := s.verify(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.root, "uploads", ref.SHA256)); !os.IsNotExist(err) {
		t.Fatalf("dedupe created upload: %v", err)
	}
}
