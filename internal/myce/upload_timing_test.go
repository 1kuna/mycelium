package myce

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// Regression tests for #2165 (upload windows below a slow store's latency) and
// #2164 (retry delay scaled with the whole upload). The tus peer runs over
// in-memory pipes inside a synctest bubble, so socket deadlines, server work
// and client pauses all run on the same fake clock. The peer reuses the real
// server's hint and service recording on a bare queue.

type pipeListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *pipeListener) Addr() net.Addr { return &net.UnixAddr{Name: "pipe", Net: "unix"} }
func (l *pipeListener) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.closed:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// tusd's per-upload lock: a HEAD waits for a running PATCH. A channel, since
// synctest advances time only while waiters block durably.
type timedUpload struct {
	lock     chan struct{}
	size     int64
	accepted int64
}

func (u *timedUpload) acquire() func() {
	u.lock <- struct{}{}
	return func() { <-u.lock }
}

type timedRequest struct {
	method, kind string
	arrived      time.Time
	left         time.Time // Client gone before the reply; zero if answered.
}

// timedTusServer answers the tus exchange with fixed per-route service times.
type timedTusServer struct {
	hints   *Queue
	create  time.Duration
	chunk   func(index int) time.Duration // Negative: never answers.
	drop    bool                          // Unanswered chunks are not written.
	mu      sync.Mutex
	uploads map[string]*timedUpload
	log     []timedRequest
}

func (s *timedTusServer) upload(sha string) *timedUpload {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uploads == nil {
		s.uploads = map[string]*timedUpload{}
	}
	u := s.uploads[sha]
	if u == nil {
		u = &timedUpload{size: -1, lock: make(chan struct{}, 1)}
		s.uploads[sha] = u
	}
	return u
}

func (s *timedTusServer) record(r timedRequest) {
	s.mu.Lock()
	s.log = append(s.log, r)
	s.mu.Unlock()
}

func (s *timedTusServer) requests() []timedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]timedRequest(nil), s.log...)
}

func (s *timedTusServer) abandoned(method string) int {
	n := 0
	for _, r := range s.requests() {
		if r.method == method && !r.left.IsZero() {
			n++
		}
	}
	return n
}

func (s *timedTusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	s.hints.reportUploadQueueService(w, r)
	s.serve(w, r, started)
	s.hints.recordUploadService(r.Method, time.Since(started))
}

func (s *timedTusServer) serve(w http.ResponseWriter, r *http.Request, arrived time.Time) {
	entry := timedRequest{method: r.Method, arrived: arrived}
	defer func() { s.record(entry) }()
	// Hold the reply for service; the server finishes its work either way.
	work := func(service time.Duration) {
		if service < 0 {
			<-r.Context().Done()
		} else {
			time.Sleep(service)
		}
		if r.Context().Err() != nil {
			entry.left = time.Now()
		}
	}
	switch {
	case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/api/files/blobs/"):
		entry.kind = "blob"
		u := s.upload(strings.TrimPrefix(r.URL.Path, "/api/files/blobs/"))
		defer u.acquire()()
		if u.size < 0 || u.accepted != u.size {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"`+strings.TrimPrefix(r.URL.Path, "/api/files/blobs/")+`"`)
		w.Header().Set("Content-Length", strconv.FormatInt(u.size, 10))
	case r.Method == http.MethodPost:
		entry.kind = "create"
		sha := uploadMetadataSHA(r.Header.Get("Upload-Metadata"))
		size, _ := strconv.ParseInt(r.Header.Get("Upload-Length"), 10, 64)
		body, _ := io.ReadAll(r.Body)
		work(s.create)
		if !entry.left.IsZero() {
			return // tus answers an interrupted creation with 408.
		}
		u := s.upload(sha)
		defer u.acquire()()
		if u.size < 0 {
			u.size = size
			u.accepted = int64(len(body))
		}
		w.Header().Set("Upload-Offset", strconv.FormatInt(u.accepted, 10))
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodHead:
		entry.kind = "offset"
		u := s.upload(strings.TrimPrefix(r.URL.Path, "/api/files/uploads/"))
		defer u.acquire()()
		w.Header().Set("Upload-Offset", strconv.FormatInt(u.accepted, 10))
		w.Header().Set("Upload-Length", strconv.FormatInt(u.size, 10))
	case r.Method == http.MethodPatch:
		entry.kind = "chunk"
		u := s.upload(strings.TrimPrefix(r.URL.Path, "/api/files/uploads/"))
		defer u.acquire()()
		offset, _ := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
		if offset != u.accepted {
			w.WriteHeader(http.StatusConflict)
			return
		}
		n, _ := io.Copy(io.Discard, r.Body)
		work(s.chunk(int(offset / fileUploadChunkSize)))
		if !entry.left.IsZero() && s.drop {
			return
		}
		u.accepted += n // Like the soak's store, an abandoned chunk is still written.
		w.Header().Set("Upload-Offset", strconv.FormatInt(u.accepted, 10))
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func uploadMetadataSHA(metadata string) string {
	for _, pair := range strings.Split(metadata, ",") {
		key, value, _ := strings.Cut(pair, " ")
		if key == "sha256" {
			var decoded []byte
			decoded, _ = base64.StdEncoding.DecodeString(value)
			return string(decoded)
		}
	}
	return ""
}

// startTimedTus serves s on in-memory pipes until the test ends.
func startTimedTus(t *testing.T, s *timedTusServer) Client {
	if s.hints == nil {
		s.hints = &Queue{clock: RealClock{}}
	}
	l := &pipeListener{conns: make(chan net.Conn), closed: make(chan struct{})}
	server := &http.Server{Handler: s}
	go server.Serve(l)
	t.Cleanup(func() { server.Close() })
	return Client{URL: "http://pipe.invalid", Token: "fixture", Clock: RealClock{}, Jitter: func() float64 { return 0 },
		HTTP: &http.Client{Transport: &http.Transport{DialContext: l.dial}}}
}

func timedUploadSource(t *testing.T, name string, size int, fill byte) (string, FileRef) {
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, bytes.Repeat([]byte{fill}, size), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, ref
}

// A worker publishes a result through a fresh client per job and retries a
// failed pass. Return how many passes that took.
func publishTimed(ctx context.Context, c Client, path string, ref FileRef) (int, error) {
	c.publication = &publicationProgress{}
	for passes := 1; ; passes++ {
		err := c.uploadOutputFile(ctx, path, ref)
		if err == nil || !retryTransient(err) || ctx.Err() != nil {
			return passes, err
		}
	}
}

// #2165: a store whose create takes 1.4 s and whose chunks take 8 s. Once the
// server has measured that, a new job's upload (fresh client timing, as each
// result has) is never abandoned; the first job may lose one cold request per
// route while nothing is measured yet.
func TestUploadWindowCoversObservedServerLatency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &timedTusServer{create: 1400 * time.Millisecond, chunk: func(int) time.Duration { return 8 * time.Second }}
		c := startTimedTus(t, server)
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		for job, fill := range []byte{'a', 'b'} {
			before := server.requests()
			for _, file := range []struct {
				name string
				size int
			}{{"small", 64 << 10}, {"large", 2*fileUploadChunkSize + 1}} {
				path, ref := timedUploadSource(t, file.name, file.size, fill)
				if _, err := publishTimed(ctx, c, path, ref); err != nil {
					t.Fatal(job, file.name, err)
				}
			}
			abandoned := map[string]int{}
			for _, r := range server.requests()[len(before):] {
				if !r.left.IsZero() {
					abandoned[r.kind]++
				}
			}
			if job == 0 && (abandoned["create"] > 1 || abandoned["chunk"] > 1) || job == 1 && len(abandoned) != 0 {
				t.Fatalf("job %d abandoned requests the server was completing: %v", job, abandoned)
			}
		}
	})
}

// #2165: with nothing measured on either side, a slow route's first request
// may outlast the protocol's initial window. Its retry costs one short pause,
// and the route's next window covers the server.
func TestUploadColdSlowRouteCostsOneShortRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &timedTusServer{create: 1400 * time.Millisecond, chunk: func(int) time.Duration { return 8 * time.Second }}
		c := startTimedTus(t, server)
		var pauses []time.Duration
		c.Pause = func(ctx context.Context, d time.Duration) error {
			pauses = append(pauses, d)
			return waitStatusPause(ctx, d)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		path, ref := timedUploadSource(t, "large", 3*fileUploadChunkSize, 'c')
		if err := c.UploadFile(ctx, path, ref); err != nil {
			t.Fatal(err)
		}
		if n := server.abandoned(http.MethodPatch); n > 1 {
			t.Fatalf("abandoned %d chunks after the first measured the route", n)
		}
		for _, d := range pauses {
			if d > clientRecoveryPeriod {
				t.Fatalf("cold retry waited %s, want at most one %s recovery period: %v", d, clientRecoveryPeriod, pauses)
			}
		}
	})
}

// #2164: one chunk late in a long upload outlasts its window. The retry comes
// within seconds of giving up, not after a pause scaled to the whole upload.
func TestUploadLateAbandonedChunkRetriesWithinSeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const chunks = 5
		server := &timedTusServer{create: time.Minute, chunk: func(index int) time.Duration {
			if index == chunks-1 {
				return -1 // The last chunk stalls until the client gives up.
			}
			return time.Minute
		}}
		c := startTimedTus(t, server)
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		path, ref := timedUploadSource(t, "large", chunks*fileUploadChunkSize, 'd')
		started := time.Now()
		if err := c.UploadFile(ctx, path, ref); err != nil {
			t.Fatal(err)
		}
		log := server.requests()
		sortTimedRequests(log)
		for _, r := range log {
			t.Logf("%s %s arrived=%s left=%v", r.method, r.kind, r.arrived.Sub(started), !r.left.IsZero())
		}
		for i, r := range log {
			if r.left.IsZero() {
				continue
			}
			if r.left.Sub(started) < 5*time.Minute {
				t.Fatalf("stall came %s into the upload, want a long upload", r.left.Sub(started))
			}
			if i+1 == len(log) {
				t.Fatal("no retry after the abandoned chunk")
			}
			if gap := log[i+1].arrived.Sub(r.left); gap > 2*clientRecoveryPeriod {
				t.Fatalf("idle %s after one abandoned chunk %s into the upload", gap, r.left.Sub(started))
			}
			return
		}
		t.Fatal("the stalled chunk was never abandoned")
	})
}

// #2165: a server that stops answering chunks is given up within a bounded
// window per request; abandonments do not compound into longer and longer waits.
func TestUploadSilentServerGivenUpWithinBoundedTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &timedTusServer{create: 2 * time.Second, chunk: func(index int) time.Duration {
			if index == 0 {
				return 2 * time.Second
			}
			return -1
		}, drop: true}
		c := startTimedTus(t, server)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		path, ref := timedUploadSource(t, "large", 2*fileUploadChunkSize, 'e')
		err := c.UploadFile(ctx, path, ref)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("silent server should hold the upload until its owner's deadline", err)
		}
		holds := 0
		for _, r := range server.requests() {
			if r.kind != "chunk" || r.left.IsZero() {
				continue
			}
			holds++
			if held := r.left.Sub(r.arrived); held > rfc6298MaxRTO+time.Second {
				t.Fatalf("abandoned chunk %d after %s, want at most %s", holds, held, rfc6298MaxRTO)
			}
		}
		if holds < 8 {
			t.Fatalf("only %d chunk attempts in 10 minutes against a silent server", holds)
		}
	})
}

func sortTimedRequests(log []timedRequest) {
	for i := 1; i < len(log); i++ {
		for j := i; j > 0 && log[j].arrived.Before(log[j-1].arrived); j-- {
			log[j], log[j-1] = log[j-1], log[j]
		}
	}
}

// The real upload route measures the store's service per route and hints it
// to a client that has not measured that server yet (#2165).
func TestUploadServiceHintReportsStoreLatency(t *testing.T) {
	q, s := creationStore(t, 1<<20)
	clock := q.clock.(*fakeClock)
	original := s.recordUpload
	s.recordUpload = func(hash, purpose string, created bool, contexts ...context.Context) error {
		clock.Advance(1400 * time.Millisecond) // A slow store's claim, write and sync.
		return original(hash, purpose, created, contexts...)
	}
	r := creationRequest(context.Background(), creationRef("slow store"))
	r.Header.Set("Authorization", "Bearer fixture")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "fixture"}.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		t.Fatal(w.Code, w.Body)
	}
	hint := func(method string) time.Duration {
		r := httptest.NewRequest(method, "/api/files/uploads/", nil)
		r.Header.Set(uploadQueueServiceHeader, "1")
		r.Header.Set("X-Myce-Upload-Progress", (clientRecoveryPeriod / 4).String())
		w := httptest.NewRecorder()
		q.reportUploadQueueService(w, r)
		if w.Code != http.StatusProcessing {
			return 0
		}
		d, _ := time.ParseDuration(w.Result().Header.Get(uploadQueueServiceHeader))
		return d
	}
	if got := hint(http.MethodPost); got < 1400*time.Millisecond {
		t.Fatalf("create hint %s, want the measured 1.4s store service", got)
	}
	if got := hint(http.MethodPatch); got != 0 {
		t.Fatalf("chunk hint %s borrowed the create route's service", got)
	}
	clock.Advance(2 * checkinServiceBucket)
	if got := hint(http.MethodPost); got != 0 {
		t.Fatalf("stale store service %s still advertised", got)
	}
}
