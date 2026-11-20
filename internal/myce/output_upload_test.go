package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Exercise the actual queue/tus finish callback, including its durability
// barrier and old wrapper responses, without sockets or wall-clock sleeps.
func TestOutputUploadCreationAndReconciliation(t *testing.T) {
	for _, mode := range []string{"new", "duplicate", "partial", "finish-refusal", "stale", "empty", "changed"} {
		t.Run(mode, func(t *testing.T) {
			q, store := creationStore(t, 1<<20)
			content := "one chunk of output"
			if mode == "empty" {
				content = ""
			}
			ref := creationRef(content)
			path := filepath.Join(t.TempDir(), "output")
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "duplicate" {
				if err := os.WriteFile(store.blob(ref), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "partial" {
				w := httptest.NewRecorder()
				store.serveUpload(w, creationRequest(context.Background(), ref))
				if w.Code != 201 {
					t.Fatal(w.Code, w.Body.String())
				}
				r := httptest.NewRequest("PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(content[:4]))
				r.Header.Set("Tus-Resumable", "1.0.0")
				r.Header.Set("Upload-Offset", "0")
				r.Header.Set("Content-Type", "application/offset+octet-stream")
				w = httptest.NewRecorder()
				store.serveUpload(w, r)
				if w.Code != 204 {
					t.Fatal(w.Code, w.Body.String())
				}
			}
			if mode == "finish-refusal" {
				store.syncDir = func(string) error { return errors.New("fixture sync refusal") }
			}
			if mode == "stale" {
				store.checkAttempt = func(context.Context) error { return ErrStale }
			}
			if mode == "changed" {
				if err := os.WriteFile(path, []byte("different output"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			requests := 0
			c := Client{URL: "http://queue.invalid", Clock: q.clock, singleAttempt: true, HTTP: &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				requests++
				w := httptest.NewRecorder()
				if strings.HasPrefix(r.URL.Path, "/api/files/blobs/") {
					// serveUpload handles only tus routes. Match the queue's blob
					// identity contract for the legacy reconciliation probe.
					w = httptest.NewRecorder()
					if store.verify(r.Context(), ref) != nil {
						w.WriteHeader(404)
					} else {
						w.Header().Set("Content-Length", strconv.FormatInt(ref.SizeBytes, 10))
						w.Header().Set("ETag", `"`+ref.SHA256+`"`)
						w.WriteHeader(200)
					}
				} else {
					store.serveUpload(w, r)
				}
				return w.Result(), nil
			})}}
			err := c.uploadOutputFile(context.Background(), path, ref)
			if mode == "finish-refusal" || mode == "stale" || mode == "changed" {
				if err == nil {
					t.Fatal("refused or changed output acknowledged")
				}
				return
			}
			if err != nil || store.verify(context.Background(), ref) != nil {
				t.Fatal("output not published", err)
			}
			if (mode == "new" || mode == "empty") && requests != 1 {
				t.Fatal("fresh file request count", requests)
			}
		})
	}
}
