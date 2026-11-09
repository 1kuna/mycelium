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
	"strings"
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

func TestUploadCreationResultReservationHandoff(t *testing.T) {
	for _, cancelCreation := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelCreation), func(t *testing.T) {
			q, s := creationStore(t, 100)
			q.deliveryRoot = q.root
			m := machine("publisher", "fake")
			check(t, q, m, nil)
			spec := asyncDeliverySpec()
			spec.Disk.OutputBytes = 40
			if cancelCreation {
				spec.Disk.OutputBytes = 10
			}
			j := submit(t, q, spec)
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != j.ID {
				t.Fatal("missing assignment")
			}
			output := creationRef(strings.Repeat("x", 40))
			if cancelCreation {
				output = creationRef(strings.Repeat("x", 80))
			}
			var cancel context.CancelFunc
			var creationDone chan struct{}
			if cancelCreation {
				ctx, stop := context.WithCancel(context.Background())
				cancel = stop
				defer cancel()
				entered := make(chan struct{})
				record := s.recordUpload
				s.recordUpload = func(hash, purpose string, created bool, contexts ...context.Context) error {
					if hash == output.SHA256 {
						close(entered)
						<-ctx.Done()
						return ctx.Err()
					}
					return record(hash, purpose, created, contexts...)
				}
				creationDone = make(chan struct{})
				go func() { defer close(creationDone); s.serveUpload(httptest.NewRecorder(), creationRequest(ctx, output)) }()
				<-entered
			}
			entered, release := make(chan struct{}), make(chan struct{})
			q.checkpointProbe = func() bool { close(entered); <-release; return true }
			r := result(assigned, true)
			r.OutputRefs = FileManifest{"out.txt": output}
			reservationDone := make(chan error, 1)
			go func() { reservationDone <- q.reserveDeliveryResult(context.Background(), r) }()
			<-entered
			if cancelCreation {
				cancel()
				<-creationDone
			}
			otherSize := 30
			if cancelCreation {
				otherSize = 90
			}
			w := httptest.NewRecorder()
			s.serveUpload(w, creationRequest(context.Background(), creationRef(strings.Repeat("y", otherSize))))
			if w.Code != 201 {
				t.Errorf("unrelated upload was double-counted: %d %s", w.Code, w.Body.String())
			}
			close(release)
			err := <-reservationDone
			if cancelCreation && err == nil {
				t.Fatal("result admitted against cancelled backing allocation")
			}
			if !cancelCreation && err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Retained regression: #691 customer30upload serialization.
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

// Retained regression: #693 pressure-clear cancellation.
func TestUploadCreationCancelledPressureClear(t *testing.T) {
	q, s := creationStore(t, 9)
	q.deliveryRoot = q.root
	spec := asyncDeliverySpec()
	spec.Class = "production"
	spec.Priority = "production"
	spec.Disk.OutputBytes = 1
	j := submit(t, q, spec)
	assigned := check(t, q, machine("writer", "fake"), nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("missing production assignment")
	}
	// The real result-reservation refusal creates the job pressure condition.
	output := creationRef("0123456789")
	result := result(assigned, true)
	result.OutputRefs = FileManifest{"out.txt": output}
	if err := q.reserveDeliveryResult(context.Background(), result); err == nil {
		t.Fatal("expected capacity refusal")
	}
	if !q.s.Health.Conditions["storage-write/job/"+j.ID].Active {
		t.Fatal("missing real pressure condition")
	}
	entered := make(chan struct{})
	q.checkpointProbe = func() bool { q.mu.Lock(); close(entered); return true }
	ctx, cancel := context.WithCancel(withFileStoreAttempt(context.Background(), *assigned))
	defer cancel()
	done := make(chan struct{})
	ref := creationRef("12345678")
	go func() {
		defer close(done)
		r := creationRequest(ctx, ref)
		fileStoreSubmissionHeaders(ctx, r)
		s.serveUpload(httptest.NewRecorder(), r)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("pressure transaction not reached")
	}
	cancel()
	select {
	case <-done:
		q.mu.Unlock()
	case <-time.After(100 * time.Millisecond):
		s.mu.Lock()
		held := s.creations[ref.SHA256].length
		s.mu.Unlock()
		q.mu.Unlock()
		<-done
		t.Fatalf("cancelled POST remained blocked on pressure-clear updateRequest with %d bytes reserved", held)
	}
	s.mu.Lock()
	_, held := s.creations[ref.SHA256]
	s.mu.Unlock()
	if held {
		t.Fatal("cancelled pressure-clear POST retained its reservation")
	}
}
