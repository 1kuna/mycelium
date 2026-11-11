package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func assertUploadError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var body struct{ Code, Error string }
	if w.Code != status || w.Header().Get("Content-Type") != "application/json" ||
		json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Code != code || body.Error == "" ||
		w.Header().Get("X-Myce-Error-Code") != code {
		t.Fatalf("upload error: status=%d headers=%v body=%s; want %d %s", w.Code, w.Header(), w.Body.String(), status, code)
	}
}

func TestUploadErrorsEnvelope(t *testing.T) {
	for _, name := range []string{"identity", "id", "missing", "version", "method", "content_type", "offset", "offset_mismatch", "size", "unindexed", "metadata", "integrity", "capacity", "claim", "attempt", "cancel", "freshness", "durability", "purpose", "creation_callback", "publication_callback"} {
		t.Run(name, func(t *testing.T) {
			_, s := creationStore(t, 8)
			ref := creationRef("data")
			ctx := context.Background()
			r := creationRequest(ctx, ref)
			status, code := 400, "file_upload_invalid"
			if name == "content_type" || strings.HasPrefix(name, "offset") || name == "size" || name == "metadata" || name == "integrity" || name == "publication_callback" {
				w := httptest.NewRecorder()
				s.serveUpload(w, r)
				if w.Code != 201 {
					t.Fatal(w.Code, w.Body.String())
				}
				r = httptest.NewRequest("PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader("data"))
				r.Header.Set("Tus-Resumable", "1.0.0")
				r.Header.Set("Upload-Offset", "0")
				r.Header.Set("Content-Type", "application/offset+octet-stream")
			}
			fail := func() error { return errors.New("private storage detail") }
			switch name {
			case "identity":
				r.Header.Set("Upload-Length", "-1")
			case "id":
				r.URL.Path += "invalid"
			case "missing":
				r.Method, r.URL.Path = "HEAD", r.URL.Path+ref.SHA256
				status, code = 404, "file_upload_not_found"
			case "version":
				r.Header.Set("Tus-Resumable", "unsupported")
				status, code = 412, "file_upload_version_unsupported"
			case "method":
				r.Method = "DELETE"
				status, code = 405, "file_upload_method_unsupported"
			case "content_type":
				r.Header.Del("Content-Type")
			case "offset":
				r.Header.Set("Upload-Offset", "invalid")
			case "offset_mismatch":
				r.Header.Set("Upload-Offset", "1")
				status, code = 409, "file_upload_offset_mismatch"
			case "size":
				r.Body, r.ContentLength = io.NopCloser(strings.NewReader("extra")), 5
				status, code = 413, "file_upload_size_exceeded"
			case "unindexed":
				if err := os.WriteFile(filepath.Join(s.root, "uploads", ref.SHA256), nil, 0600); err != nil {
					t.Fatal(err)
				}
				status, code = 409, "file_upload_conflict"
			case "metadata":
				if err := os.WriteFile(filepath.Join(s.root, "uploads", ref.SHA256+".info"), []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
				status, code = 500, "file_upload_unavailable"
			case "integrity":
				r.Body = io.NopCloser(strings.NewReader("evil"))
				status, code = 500, "file_upload_unavailable"
			case "capacity":
				r = creationRequest(ctx, creationRef("too large"))
				status, code = 507, "file_store_capacity"
			case "claim":
				s.inputClaim = func(context.Context, FileRef, bool) (context.Context, error) { return ctx, inputClaimError() }
				status, code = 409, "file_input_claim_required"
			case "attempt":
				s.checkAttempt = func(context.Context) error { return ErrStale }
				status, code = 409, "request_stale"
			case "cancel":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				r = r.WithContext(cancelled)
				status, code = 408, "file_upload_interrupted"
			case "freshness", "durability", "purpose":
				if err := os.WriteFile(s.blob(ref), []byte("data"), 0600); err != nil {
					t.Fatal(err)
				}
				if name == "freshness" {
					claim := s.inputClaim
					s.inputClaim = func(ctx context.Context, ref FileRef, acquire bool) (context.Context, error) {
						if acquire {
							if err := os.Remove(s.blob(ref)); err != nil {
								t.Fatal(err)
							}
						}
						return claim(ctx, ref, acquire)
					}
				}
				if name == "durability" {
					s.syncDir = func(string) error { return fail() }
				}
				if name == "purpose" {
					s.recordUpload = func(string, string, bool, ...context.Context) error { return fail() }
				}
				status, code = 503, "file_upload_unavailable"
			case "creation_callback":
				s.recordUpload = func(string, string, bool, ...context.Context) error { return fail() }
				status, code = 500, "file_upload_unavailable"
			case "publication_callback":
				s.publishUpload = func(string, ...context.Context) error { return fail() }
				status, code = 500, "file_upload_unavailable"
			}
			w := httptest.NewRecorder()
			s.serveUpload(w, r)
			assertUploadError(t, w, status, code)
			if strings.Contains(w.Body.String(), "private storage detail") {
				t.Fatal("internal error escaped")
			}
		})
	}
}

func TestUploadDuplicateFinalizeAndLateRetry(t *testing.T) {
	_, s := creationStore(t, 8)
	ref := creationRef("data")
	w := httptest.NewRecorder()
	s.serveUpload(w, creationRequest(context.Background(), ref))
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	start := make(chan struct{})
	results := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			r := httptest.NewRequest("PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader("data"))
			r.Header.Set("Tus-Resumable", "1.0.0")
			r.Header.Set("Upload-Offset", "0")
			r.Header.Set("Content-Type", "application/offset+octet-stream")
			w := httptest.NewRecorder()
			<-start
			s.serveUpload(w, r)
			results <- w
		}()
	}
	close(start)
	completed := 0
	for range 2 {
		w := <-results
		if w.Code == 204 {
			completed++
			if w.Body.Len() != 0 || w.Header().Get("Upload-Offset") != "4" {
				t.Fatal("success changed", w)
			}
		} else {
			assertUploadError(t, w, 404, "file_upload_not_found")
		}
	}
	if completed != 1 {
		t.Fatal("duplicate completion", completed)
	}
	w = httptest.NewRecorder()
	r := httptest.NewRequest("HEAD", "/api/files/uploads/"+ref.SHA256, nil)
	r.Header.Set("Tus-Resumable", "1.0.0")
	s.serveUpload(w, r)
	assertUploadError(t, w, 404, "file_upload_not_found")
	api := httptest.NewServer(http.HandlerFunc(s.serveUpload))
	defer api.Close()
	c := Client{URL: api.URL}
	_, err := c.uploadOffset(context.Background(), r.URL.Path, ref.SizeBytes)
	var missing *HTTPError
	if !errors.As(err, &missing) || missing.Status != 404 || missing.Code != "file_upload_not_found" {
		t.Fatal("HEAD lost typed error on the wire", err)
	}
	if err := s.verify(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
}

func TestFilesMissingUploadReconcilesPublication(t *testing.T) {
	for _, phase := range []string{"HEAD", "PATCH"} {
		for _, outcome := range []string{"published", "pending", "absent", "corrupt"} {
			t.Run(phase+"/"+outcome, func(t *testing.T) {
				q, s := creationStore(t, 8)
				ref := creationRef("data")
				backend := API{Queue: q, Token: "fixture"}.Handler()
				var mu sync.Mutex
				injected := false
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					if r.Method == phase && r.URL.Path == "/api/files/uploads/"+ref.SHA256 && (!injected || outcome == "absent") {
						injected = true
						if outcome == "published" || outcome == "corrupt" {
							finish := httptest.NewRequest("PATCH", r.URL.Path, strings.NewReader("data"))
							finish.Header.Set("Tus-Resumable", "1.0.0")
							finish.Header.Set("Content-Type", "application/offset+octet-stream")
							finish.Header.Set("Upload-Offset", "0")
							finished := httptest.NewRecorder()
							s.serveUpload(finished, finish)
							if finished.Code != 204 {
								t.Error(finished.Code, finished.Body.String())
							}
							if outcome == "corrupt" {
								if err := os.WriteFile(s.blob(ref), []byte("evil"), 0600); err != nil {
									t.Error(err)
								}
							}
						} else {
							respond(w, nil, &HTTPError{Status: 404, Code: "file_upload_not_found", Message: "missing"})
							return
						}
					}
					backend.ServeHTTP(w, r)
				}))
				defer api.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				pauses := 0
				c := Client{URL: api.URL, Token: "fixture", Pause: func(context.Context, time.Duration) error {
					pauses++
					if outcome == "absent" {
						cancel()
						return context.DeadlineExceeded
					}
					return nil
				}}
				var err error
				if outcome == "pending" {
					// Profile/adapter recovery owns retries across single-attempt calls.
					err = c.RetryTransient(ctx, func(ctx context.Context, one Client) error {
						return one.uploadFileReader(ctx, bytes.NewReader([]byte("data")), ref, jobInputUploadPurpose)
					}, nil)
				} else {
					err = c.uploadFileReader(ctx, bytes.NewReader([]byte("data")), ref, jobInputUploadPurpose)
				}
				switch outcome {
				case "absent":
					if !errors.Is(err, context.DeadlineExceeded) || pauses != 1 {
						t.Fatal(err, pauses)
					}
				case "corrupt":
					var failure *HTTPError
					if !errors.As(err, &failure) || failure.Code != "file_blob_unavailable" || pauses != 0 {
						t.Fatal(err, pauses)
					}
				default:
					if err != nil || (pauses == 1) != (outcome == "pending") {
						t.Fatal(err, pauses)
					}
					if err := c.HasFile(ctx, ref); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestUploadCompletedRetryRejectsChangedSource(t *testing.T) {
	for _, changed := range []string{"evil", "dat", "data!"} {
		t.Run(changed, func(t *testing.T) {
			q, s := creationStore(t, 8)
			ref := creationRef("data")
			path := filepath.Join(t.TempDir(), "input")
			if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
				t.Fatal(err)
			}
			backend := API{Queue: q, Token: "fixture"}.Handler()
			sent := make(chan []byte, 1)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/files/uploads/"+ref.SHA256 {
					if r.Method == "HEAD" {
						if err := os.WriteFile(path, []byte(changed), 0600); err != nil {
							t.Error(err)
							w.WriteHeader(500)
							return
						}
					}
					if r.Method == "PATCH" {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
						}
						sent <- body
						// Restore the source after sending changed bytes: rehashing
						// the file now would lose the failed PATCH's identity.
						if changed != "data!" {
							if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
								t.Error(err)
							}
						}
						finish := httptest.NewRequest("PATCH", r.URL.Path, strings.NewReader("data"))
						finish.Header.Set("Tus-Resumable", "1.0.0")
						finish.Header.Set("Content-Type", "application/offset+octet-stream")
						finish.Header.Set("Upload-Offset", "0")
						finished := httptest.NewRecorder()
						s.serveUpload(finished, finish)
						if finished.Code != 204 {
							t.Error(finished.Code, finished.Body.String())
						}
						late := httptest.NewRecorder()
						backend.ServeHTTP(late, r)
						assertUploadError(t, late, 404, "file_upload_not_found")
						for key, values := range late.Header() {
							w.Header()[key] = values
						}
						w.WriteHeader(late.Code)
						_, _ = w.Write(late.Body.Bytes())
						return
					}
				}
				backend.ServeHTTP(w, r)
			}))
			defer api.Close()
			ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
			defer cancel()
			var pauses atomic.Int32
			c := Client{URL: api.URL, Token: "fixture", Pause: func(context.Context, time.Duration) error {
				pauses.Add(1)
				return context.DeadlineExceeded
			}}
			err := c.UploadJobInput(ctx, path, ref)
			if err == nil || !strings.Contains(err.Error(), "source size or SHA-256 mismatch") || pauses.Load() != 0 {
				t.Fatal("changed source reconciled or retried", err, pauses.Load())
			}
			select {
			case body := <-sent:
				if string(body) != changed[:min(len(changed), 4)] {
					t.Fatalf("sent %q", body)
				}
			default:
				t.Fatal("concurrent completion was not exercised")
			}
			if err := c.HasFile(ctx, ref); err != nil {
				t.Fatal("original blob missing", err)
			}
		})
	}
}
