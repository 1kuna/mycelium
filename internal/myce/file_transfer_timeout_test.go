package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// #218: a large result streamed for longer than the API client's whole-request
// timeout must succeed while bytes keep arriving, and fail typed once they stop.
func TestDownloadResultIdleTimeoutNotWholeRequest(t *testing.T) {
	t.Parallel()
	body := bytes.Repeat([]byte("frame"), 2000)
	h := sha256.Sum256(body)
	ref := FileRef{SHA256: hex.EncodeToString(h[:]), SizeBytes: int64(len(body))}
	serve := func(stall bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/jobs/job" {
				json.NewEncoder(w).Encode(Job{ID: "job", State: "succeeded", OutputRefs: FileManifest{"result": ref}})
				return
			}
			w.Header().Set("ETag", `"`+ref.SHA256+`"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			chunk := len(body) / 10
			for i := 0; i < 10; i++ {
				if stall && i == 5 {
					<-r.Context().Done()
					return
				}
				w.Write(body[i*chunk : (i+1)*chunk])
				w.(http.Flusher).Flush()
				time.Sleep(60 * time.Millisecond)
			}
		}))
	}
	// The old fixed client timeout is shorter than the ~600 ms stream.
	client := func(url string) Client {
		return Client{URL: url, Token: "fixture", HTTP: &http.Client{Timeout: 200 * time.Millisecond}, transferResponse: time.Second, transferIdle: 250 * time.Millisecond}
	}

	slow := serve(false)
	defer slow.Close()
	dest := filepath.Join(t.TempDir(), "result")
	if err := client(slow.URL).DownloadResult(context.Background(), "job", "result", dest); err != nil {
		t.Fatal("slow streamed result failed:", err)
	}
	if got, err := os.ReadFile(dest); err != nil || !bytes.Equal(got, body) {
		t.Fatal("wrong result", err)
	}

	stalled := serve(true)
	defer stalled.Close()
	dest = filepath.Join(t.TempDir(), "result")
	start := time.Now()
	err := client(stalled.URL).DownloadResult(context.Background(), "job", "result", dest)
	if !errors.Is(err, errFileTransferStalled) {
		t.Fatalf("stalled body: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatal("stall detected late:", elapsed)
	}
	if entries, _ := os.ReadDir(filepath.Dir(dest)); len(entries) != 0 {
		t.Fatal("stalled download published or leaked a file", entries)
	}
}
