package myce

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A failed finish leaves all bytes accepted but no verified blob. tus skips
// its finish callback for PATCH at offset==length; replay must finish custody
// rather than repeatedly acknowledging an unpublished upload.
func TestUploadPublicationRetryCompletedOffset(t *testing.T) {
	q, s := creationStore(t, 32)
	clock := q.clock.(*fakeClock)
	clock.Advance(time.Since(clock.Now()))
	c := retentionClient(t, q)
	s = q.files // retentionClient configures the API's authoritative store.
	_, input := retentionUpload(t, c, "input")
	spec := spec("experiment")
	spec.Files = FileManifest{"input": input}
	spec.Outputs = map[string]string{"output": "results"}
	submitted := submit(t, q, spec)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != submitted.ID {
		t.Fatal("no assignment")
	}
	ctx := withFileStoreAttempt(context.Background(), *j)
	ref := creationRef("finished")
	w := httptest.NewRecorder()
	s.serveUpload(w, creationRequest(ctx, ref))
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	// A conflicting storage entry fails publication after the complete PATCH.
	if err := os.Mkdir(s.blob(ref), 0700); err != nil {
		t.Fatal(err)
	}
	patch := func(offset string, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(body)).WithContext(ctx)
		r.Header.Set("Tus-Resumable", "1.0.0")
		r.Header.Set("Content-Type", "application/offset+octet-stream")
		r.Header.Set("Upload-Offset", offset)
		w := httptest.NewRecorder()
		s.serveUpload(w, r)
		return w
	}
	assertUploadError(t, patch("0", "finished"), 500, "file_upload_unavailable")
	if st, err := os.Stat(filepath.Join(s.root, "uploads", ref.SHA256)); err != nil || st.Size() != ref.SizeBytes {
		t.Fatal("failed finish lost accepted bytes", st, err)
	}
	if err := os.Remove(s.blob(ref)); err != nil {
		t.Fatal(err)
	}
	// Re-enter through the real client, which must resume at the accepted end.
	api := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer api.Close()
	c = Client{URL: api.URL, Token: "fixture", Pause: func(context.Context, time.Duration) error { return context.Canceled }}
	c.Clock = clock
	patches := 0
	c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != "PATCH" {
			return http.DefaultTransport.RoundTrip(r)
		}
		patches++
		if r.Header.Get("Upload-Offset") != "8" {
			t.Error("replayed accepted bytes", r.Header.Get("Upload-Offset"))
		}
		q.mu.Lock()
		type response struct {
			res *http.Response
			err error
		}
		done := make(chan response, 1)
		go func() { res, err := http.DefaultTransport.RoundTrip(r); done <- response{res, err} }()
		await(t, func() bool { return q.mu.waiting.Load() > 0 })
		clock.Advance(time.Minute)
		q.mu.Unlock()
		got := <-done
		return got.res, got.err
	})}
	if err := c.uploadFileReader(ctx, strings.NewReader("finished"), ref, ""); err != nil {
		t.Fatal("completed upload could not resume publication", err)
	}
	if patches != 1 {
		t.Fatal("repeated completed PATCH", patches)
	}
	if err := s.verify(context.Background(), ref); err != nil {
		t.Fatal("acknowledged unpublished bytes", err)
	}
	finished := result(j, true)
	finished.OutputRefs = FileManifest{"output": ref}
	if err := q.Complete(finished); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "succeeded" || got.Attempt != j.Attempt || got.Session != j.Session || got.OutputRefs["output"] != ref {
		t.Fatal("lost exact attempt or output", got.State, got.Attempt)
	}
}

// The last PATCH verifies the whole blob, not just its four-MiB chunk. Model
// real verification reads lasting beyond the protocol idle floor without a
// large fixture. The ordinary HTTP path must keep making progress in one PATCH.
func TestUploadPublicationVerificationProgress(t *testing.T) {
	q, s := creationStore(t, 8)
	s.clock = RealClock{}
	record := s.recordUpload
	delayed := false
	s.recordUpload = func(hash, purpose string, created bool, contexts ...context.Context) error {
		if !created && !delayed {
			delayed = true
			ctx := requestContext(contexts)
			for range 6 {
				time.Sleep(250 * time.Millisecond)
				if _, err := io.Copy(io.Discard, contextReader{ctx, strings.NewReader("verified bytes")}); err != nil {
					return err
				}
			}
		}
		return record(hash, purpose, created, contexts...)
	}
	api := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer api.Close()
	c := Client{URL: api.URL, Token: "fixture", Pause: func(context.Context, time.Duration) error { return context.Canceled }}
	if err := c.uploadFileReader(context.Background(), strings.NewReader("finished"), creationRef("finished"), ""); err != nil {
		t.Fatal("active verification mistaken for dead connection", err)
	}
	if !delayed {
		t.Fatal("verification did not run")
	}
}

func TestUploadPublicationErrorDiagnostics(t *testing.T) {
	_, s := creationStore(t, 8)
	var journal bytes.Buffer
	old := log.Writer()
	log.SetOutput(&journal)
	defer log.SetOutput(old)
	ref := creationRef("finished")
	s.serveUpload(httptest.NewRecorder(), creationRequest(context.Background(), ref))
	s.syncDir = func(string) error {
		return &os.PathError{Op: "sync", Path: "/private/synthetic-secret", Err: syscall.EIO}
	}
	r := httptest.NewRequest("PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader("finished"))
	r.Header.Set("Tus-Resumable", "1.0.0")
	r.Header.Set("Content-Type", "application/offset+octet-stream")
	r.Header.Set("Upload-Offset", "0")
	r.Header.Set("Authorization", "Bearer synthetic-credential")
	w := httptest.NewRecorder()
	s.serveUpload(w, r)
	assertUploadError(t, w, 500, "file_upload_unavailable")
	for _, want := range []string{"method=PATCH", "status=500", "accepted_offset=8", "length=8", "operation=blob_directory_sync", "error_class=EIO"} {
		if !strings.Contains(journal.String(), want) {
			t.Errorf("missing %s: %s", want, journal.String())
		}
	}
	for _, secret := range []string{"synthetic-secret", "synthetic-credential", "/private/"} {
		if strings.Contains(journal.String(), secret) || strings.Contains(w.Body.String(), secret) {
			t.Fatal("private error escaped", secret)
		}
	}
}

func TestUploadPublicationRetainsMeasuredService(t *testing.T) {
	q, _ := creationStore(t, 32)
	clock := q.clock.(*fakeClock)
	api := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer api.Close()
	c := Client{URL: api.URL, Token: "fixture", Clock: clock, publication: &publicationProgress{}}
	patches := 0
	c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "PATCH" {
			patches++
			if patches == 1 {
				clock.Advance(4 * time.Second)
			} else {
				period, err := time.ParseDuration(r.Header.Get("X-Myce-Upload-Progress"))
				if err != nil || period < 4*time.Second {
					t.Error("next file forgot measured queue service", period, err)
				}
			}
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	for _, data := range []string{"first", "second"} {
		if err := c.uploadFileReader(context.Background(), strings.NewReader(data), creationRef(data), ""); err != nil {
			t.Fatal(err)
		}
	}
	if patches != 2 {
		t.Fatal("unexpected upload retries", patches)
	}
}
