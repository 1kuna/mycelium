package myce

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	tus "github.com/tus/tusd/v2/pkg/handler"
)

// keepAliveConn sends every request over one client connection, as a
// pooling proxy or worker transport does.
type keepAliveConn struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	host string
}

func dialKeepAlive(t *testing.T, address string) *keepAliveConn {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &keepAliveConn{t: t, conn: conn, r: bufio.NewReader(conn), host: address}
}

func (c *keepAliveConn) do(method, path string, header http.Header, body []byte) (*http.Response, string) {
	c.t.Helper()
	req, err := http.NewRequest(method, "http://"+c.host+path, bytes.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	if err := c.conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil { // Socket I/O only.
		c.t.Fatal(err)
	}
	if err := req.Write(c.conn); err != nil {
		c.t.Fatal(err)
	}
	for {
		res, err := http.ReadResponse(c.r, req)
		if err != nil {
			c.t.Fatal(err)
		}
		if res.StatusCode >= 200 {
			data, err := io.ReadAll(res.Body)
			if err != nil {
				c.t.Fatal(err)
			}
			res.Body.Close()
			return res, string(data)
		}
	}
}

// slowUploadPublication models a slow share, where publishing a final chunk
// runs well past tusd's body read timeout after the last byte arrived. It
// stops early only if its request ends, as blob_verify does. tusd's write
// deadline is twice its read timeout; the field's 60 s and 10 s grace let
// the 408 out before it, as these do.
func slowUploadPublication(q *Queue) func(*FileStore) {
	const timeout = 200 * time.Millisecond
	q.configureUploads = func(c *tus.Config) {
		c.NetworkTimeout, c.GracefulRequestCompletionTimeout = timeout, timeout/4
	}
	return func(s *FileStore) {
		record := s.recordUpload
		s.recordUpload = func(hash, purpose string, created bool, contexts ...context.Context) error {
			if !created {
				select {
				case <-requestContext(contexts).Done():
				case <-time.After(5 * timeout):
				}
			}
			return record(hash, purpose, created, contexts...)
		}
	}
}

// #2156: after a final-chunk PATCH whose publication outlasted tusd's body
// read timeout, net/http's background read timed out on the idle socket and
// cancelled the connection's context. The PATCH failed with 408 (in the field
// at 70.2 s: the 60 s timeout plus tusd's 10 s grace), and every later
// request on that keep-alive connection started cancelled: a shared
// coordinator answered queue_stopping in under a millisecond, a single queue
// status_unavailable, for as long as the client kept the connection.
func assertSlowPublicationKeepsConnection(t *testing.T, address, token string) {
	t.Helper()
	c := dialKeepAlive(t, address)
	payload := []byte("published over a slow store")
	ref := creationRef(string(payload))
	header := http.Header{"Authorization": {"Bearer " + token}, "Tus-Resumable": {"1.0.0"}}
	create := header.Clone()
	create.Set("Upload-Length", strconv.Itoa(len(payload)))
	create.Set("Upload-Metadata", "sha256 "+base64.StdEncoding.EncodeToString([]byte(ref.SHA256)))
	if res, body := c.do(http.MethodPost, "/api/files/uploads/", create, nil); res.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d %s", res.StatusCode, body)
	}
	patch := header.Clone()
	patch.Set("Upload-Offset", "0")
	patch.Set("Content-Type", "application/offset+octet-stream")
	res, body := c.do(http.MethodPatch, "/api/files/uploads/"+ref.SHA256, patch, payload)
	if res.StatusCode != http.StatusNoContent || res.Close {
		t.Fatalf("final PATCH: %d close=%t %s; its own idle socket cancelled the publication", res.StatusCode, res.Close, body)
	}
	for _, path := range []string{"/api/ping", "/api/status", "/api/files/blobs/" + ref.SHA256} {
		if res, body := c.do(http.MethodGet, path, header, nil); res.StatusCode != http.StatusOK {
			t.Fatalf("GET %s on the same connection: %d %s", path, res.StatusCode, body)
		}
	}
}

func TestUploadSlowPublicationKeepsConnectionUsable(t *testing.T) {
	q, _ := fixture(t)
	slow := slowUploadPublication(q)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	slow(q.files)
	server := httptest.NewUnstartedServer(API{Queue: q, Token: "secret"}.Handler())
	server.Config.ConnContext = QueueConnectionContext
	server.Start()
	defer server.Close()
	assertSlowPublicationKeepsConnection(t, server.Listener.Addr().String(), "secret")
}

func TestSharedQueueMemberSlowPublicationKeepsConnectionUsable(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	server := httptest.NewUnstartedServer(nil)
	server.Config.ConnContext = QueueConnectionContext
	var slow func(*FileStore)
	m, err := OpenSharedQueueMember(t.Context(), SharedQueueMemberConfig{
		Postgres: pg.dsn, Database: filepath.Join(dir, "member", "queue.db"),
		FileStore: filepath.Join(dir, "files"), Checkpoints: filepath.Join(dir, "checkpoints"),
		Token: proofToken, Clock: proofClock(), Endpoint: "http://" + server.Listener.Addr().String(),
		interval: 100 * time.Millisecond, lease: proofMemberLease,
		Options: []QueueOption{func(q *Queue) { slow = slowUploadPublication(q) }},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.Config.Handler = m
	server.Start()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); m.Run(ctx) }()
	defer func() {
		cancel()
		<-done
		server.Close()
		_ = m.Close()
	}()
	proofEventually(t, "coordinator serves files", func() bool {
		turn, err := a.current(t.Context())
		if err != nil || !turn.ready || turn.owner != m.owner {
			return false
		}
		m.mu.Lock()
		l := m.leader
		m.mu.Unlock()
		if l == nil {
			return false
		}
		// Handlers read the hook after taking q.mu for the file store check.
		l.queue.mu.Lock()
		defer l.queue.mu.Unlock()
		if !l.queue.sharedFilesReady() {
			return false
		}
		slow(l.queue.files)
		return true
	})
	assertSlowPublicationKeepsConnection(t, server.Listener.Addr().String(), proofToken)
}

// Any other way a connection's context ends while the socket stays open must
// cost at most the request that finds it: the reply closes the connection.
func TestCancelledRequestClosesItsConnection(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, path := range []string{"/api/status", "/api/files/uploads/" + creationRef("x").SHA256} {
		r := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
		if path != "/api/status" {
			r.Method = http.MethodPatch
		}
		r.Header.Set("Authorization", "Bearer secret")
		r.Header.Set("Tus-Resumable", "1.0.0")
		w := httptest.NewRecorder()
		API{Queue: q, Token: "secret"}.Handler().ServeHTTP(w, r)
		if w.Code < 400 || w.Header().Get("Connection") != "close" {
			t.Fatalf("%s: %d %v", path, w.Code, w.Header())
		}
	}
}
