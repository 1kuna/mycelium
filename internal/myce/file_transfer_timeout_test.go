package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
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
		return Client{URL: url, Token: "fixture", HTTP: &http.Client{Timeout: 200 * time.Millisecond}, transferIdle: 250 * time.Millisecond}
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

// Unknown-length bodies use HTTP chunking, as UploadFile's io.LimitReader does.
// Throttle actual socket writes rather than application reads: loopback TCP can
// otherwise acknowledge the whole body while the server is still draining it.
func TestFileRequestProgressingUnknownLengthUpload(t *testing.T) {
	t.Parallel()
	body := bytes.Repeat([]byte("upload"), 32*1024)
	received := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength != -1 {
			t.Errorf("expected unknown length, got %d", r.ContentLength)
		}
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		received <- got
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	transport := &http.Transport{}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return slowUploadConn{conn}, nil
	}
	defer transport.CloseIdleConnections()
	// This opaque fixture returns progress only after its aggregated Write.
	// Darwin measured that operation at 312 ms despite continuing socket writes;
	// allow three such periods while the complete upload still outlasts idle.
	c := Client{URL: server.URL, HTTP: &http.Client{Transport: transport, Timeout: 200 * time.Millisecond}, transferIdle: time.Second}
	start := time.Now()
	res, err := c.fileRequest(context.Background(), "PATCH", "/upload", io.LimitReader(bytes.NewReader(body), int64(len(body))), nil, int64(len(body)))
	if err != nil {
		t.Fatal("progressing upload failed:", err)
	}
	if err := fileResponse(res, http.StatusNoContent); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) <= c.transferIdle {
		t.Fatal("upload did not exceed idle budget")
	}
	if got := <-received; !bytes.Equal(got, body) {
		t.Fatal("upload truncated", len(got))
	}
}

type slowUploadConn struct{ net.Conn }

func (c slowUploadConn) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n, err := c.Conn.Write(p[:min(len(p), 1024)])
		total += n
		p = p[n:]
		if err != nil {
			return total, err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return total, nil
}
