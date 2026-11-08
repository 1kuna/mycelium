package myce

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// An accepting peer pauses inside the first PATCH, after persisting a prefix.
// Small send buffers make the pause visible at the socket rather than merely
// queueing the complete request in loopback TCP. 12 MiB matches the incident's
// scale without depending on any production data or queue storage.
func TestUploadFilePausedPATCH(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		pause   time.Duration
		lostAck bool
	}{
		{"tolerates", 700 * time.Millisecond, false},
		{"resumes", 1500 * time.Millisecond, false},
		{"published-without-ack", 0, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			pause := scenario.pause
			body := bytes.Repeat([]byte("source"), 2<<20)
			path := filepath.Join(t.TempDir(), "source")
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			ref, err := IdentifyFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var accepted []byte
			patches := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				if r.Method == "HEAD" {
					defer mu.Unlock()
					if r.URL.Path == "/api/files/blobs/"+ref.SHA256 {
						if len(accepted) != len(body) {
							w.WriteHeader(404)
							return
						}
						w.Header().Set("ETag", `"`+ref.SHA256+`"`)
						w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					} else {
						w.Header().Set("Upload-Offset", strconv.Itoa(len(accepted)))
						w.Header().Set("Upload-Length", strconv.Itoa(len(body)))
					}
					return
				}
				if r.Method == "POST" {
					mu.Unlock()
					w.WriteHeader(201)
					return
				}
				patches++
				first := patches == 1
				offset, _ := strconv.Atoi(r.Header.Get("Upload-Offset"))
				if offset != len(accepted) {
					current := len(accepted)
					mu.Unlock()
					t.Errorf("replayed offset %d, accepted %d", offset, current)
					w.WriteHeader(409)
					return
				}
				mu.Unlock()
				if first {
					prefix := make([]byte, 64<<10)
					n, _ := io.ReadFull(r.Body, prefix)
					mu.Lock()
					accepted = append(accepted, prefix[:n]...)
					mu.Unlock()
					time.Sleep(pause)
					// For the longer pause the first connection must have stalled. Do not
					// append its unacknowledged buffered bytes after a resume starts.
					if pause > time.Second {
						return
					}
				}
				rest, e := io.ReadAll(r.Body)
				if e != nil {
					return
				}
				mu.Lock()
				accepted = append(accepted, rest...)
				next := len(accepted)
				mu.Unlock()
				if scenario.lostAck && next == len(body) {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
					return
				}
				w.Header().Set("Upload-Offset", strconv.Itoa(next))
				w.WriteHeader(204)
			}))
			defer server.Close()
			transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if tcp, ok := conn.(*net.TCPConn); ok {
					tcp.SetWriteBuffer(4096)
				}
				return conn, err
			}}
			defer transport.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
			defer cancel()
			client := Client{URL: server.URL, HTTP: &http.Client{Transport: transport}, Jitter: func() float64 { return 0 }}
			if err := client.UploadFile(ctx, path, ref); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !bytes.Equal(accepted, body) {
				t.Fatal("resumed content differs", len(accepted))
			}
			expected := 3
			if pause > time.Second {
				expected = 4
			}
			if patches != expected {
				t.Fatalf("PATCH count %d, want %d", patches, expected)
			}
		})
	}
}

func TestUploadTimingProtocolAndPressure(t *testing.T) {
	timing := &uploadTiming{}
	if got := timing.window(); got != time.Second {
		t.Fatal("TCP retransmission floor", got)
	}
	timing.observe(131*time.Millisecond, 0)
	if got := timing.window(); got != time.Second {
		t.Fatal("fast wire sample shrank floor", got)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Header().Set("Retry-After", "13"); w.WriteHeader(503) }))
	defer server.Close()
	client := Client{URL: server.URL, uploadTiming: timing}
	response, err := client.fileRequestOnce(context.Background(), "HEAD", "/upload", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if got := timing.window(); got != 13*time.Second {
		t.Fatal("ignored server pressure", got)
	}
	timing.observe(4*time.Second, 0)
	if got := timing.window(); got != 16*time.Second {
		t.Fatal("ignored service time", got)
	}
}

func TestUploadDeadConnectionDetected(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)}
	source := &adaptiveTransferConn{clock: clock, stalled: true}
	conn := &fileTransferConn{Conn: source, clock: clock, timing: &uploadTiming{}}
	start := clock.Now()
	if _, err := conn.Read(make([]byte, 256)); !errors.Is(err, errFileTransferStalled) {
		t.Fatal("dead peer", err)
	}
	if clock.Now().Sub(start) != time.Second {
		t.Fatal("unbounded dead connection")
	}
}
