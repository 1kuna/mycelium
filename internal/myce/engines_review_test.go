package myce

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"

	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type reviewStreamEngine struct {
	started       chan struct{}
	contextDone   chan struct{}
	mu            sync.Mutex
	body          string
	contentLength int64
	headers       http.Header
}

func (*reviewStreamEngine) Load(context.Context, Model) error { return nil }
func (*reviewStreamEngine) Unload() error                     { return nil }
func (*reviewStreamEngine) Call(context.Context, string, []byte) (*http.Response, error) {
	return nil, errors.New("buffered call used for generic request")
}
func (e *reviewStreamEngine) StreamRequest(ctx context.Context, _ string, _ string, body io.Reader, contentLength int64, headers http.Header) (*http.Response, error) {
	close(e.started)
	data, err := io.ReadAll(body)
	if err != nil {
		if errors.Is(err, context.Canceled) && e.contextDone != nil {
			close(e.contextDone)
		}
		return nil, err
	}
	e.mu.Lock()
	e.body, e.contentLength, e.headers = string(data), contentLength, headers.Clone()
	e.mu.Unlock()
	return &http.Response{StatusCode: http.StatusCreated, Header: http.Header{"X-Upstream": []string{"yes"}}, Body: io.NopCloser(strings.NewReader("done"))}, nil
}

func TestWorkerStreamsGenericBodyHeadersAndLength(t *testing.T) {
	engine := &reviewStreamEngine{started: make(chan struct{})}
	rt := &modelRuntime{
		models:  map[string]*workerModel{"general": {engine: engine, state: "ready"}},
		session: "session",
		token:   "token",
	}
	server := workerTestServer(http.HandlerFunc(rt.serve))
	defer server.Close()

	reader, writer := io.Pipe()
	req, err := http.NewRequest("PUT", server.URL+"/models/general/audio?language=en", reader)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len("first-last"))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("X-Myce-Session", "session")
	req.Header.Set("X-Custom", "preserved")
	result := make(chan error, 1)
	go func() {
		res, err := http.DefaultClient.Do(req)
		if err == nil {
			defer res.Body.Close()
			data, readErr := io.ReadAll(res.Body)
			if readErr != nil {
				err = readErr
			} else if res.StatusCode != http.StatusCreated || res.Header.Get("X-Upstream") != "yes" || string(data) != "done" {
				err = errors.New("upstream response contract changed")
			}
		}
		result <- err
	}()
	if _, err = io.WriteString(writer, "first-"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-engine.started:
		// The engine sees the request before the uploader reaches EOF.
	case <-time.After(testWaitTimeout):
		t.Fatal("worker buffered generic request before calling engine")
	}
	if _, err = io.WriteString(writer, "last"); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.body != "first-last" || engine.contentLength != int64(len("first-last")) || engine.headers.Get("X-Custom") != "preserved" {
		t.Fatalf("body=%q length=%d headers=%v", engine.body, engine.contentLength, engine.headers)
	}
	if engine.headers.Get("Authorization") != "" || engine.headers.Get("X-Myce-Session") != "" {
		t.Fatal("routing credentials reached engine")
	}
}

func TestGenericDiskSpoolReplaysTransportRetry(t *testing.T) {
	q, _, firstMachine := modelFixture(t)
	q.models.Models[0].RetrySafe = true
	firstMachine.ModelConfigHash = modelConfigHash(q.models)
	var firstCalls, secondCalls atomic.Int32
	first := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		panic(http.ErrAbortHandler)
	}))
	defer first.Close()
	received := make(chan struct {
		body   string
		length int64
	}, 1)
	second := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		data, _ := io.ReadAll(r.Body)
		received <- struct {
			body   string
			length int64
		}{string(data), r.ContentLength}
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "accepted")
	}))
	defer second.Close()

	firstMachine.ModelURL = first.URL
	check(t, q, firstMachine, nil)
	secondMachine := machine("two", "fake")
	secondMachine.ModelURL = second.URL
	secondMachine.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, secondMachine, nil)
	for _, m := range []Machine{firstMachine, secondMachine} {
		q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: "ready", Priority: "experiment", LastUsed: q.clock.Now()}
	}

	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	payload := "audio-needing-replay"
	req, err := http.NewRequest("PUT", server.URL+"/models/test/transcribe", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Project", "verso")
	req.Header.Set("X-Myce-Priority", "production")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	var got struct {
		body   string
		length int64
	}
	select {
	case got = <-received:
	case <-time.After(testWaitTimeout):
		t.Fatalf("retry did not reach second worker: status=%d response=%q", res.StatusCode, data)
	}
	if res.StatusCode != http.StatusAccepted || string(data) != "accepted" || firstCalls.Load() != 1 || secondCalls.Load() != 1 || got.body != payload || got.length != int64(len(payload)) {
		t.Fatalf("status=%d response=%q calls=%d/%d body=%q length=%d", res.StatusCode, data, firstCalls.Load(), secondCalls.Load(), got.body, got.length)
	}
}

func TestBundleRecoveryClosesEachSourceFile(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "recovery")
	if err = os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	// macOS commonly limits a process to 256 open files. The old WalkDir defer
	// kept every source open and failed a perfectly ordinary many-file bundle.
	for i := 0; i < 300; i++ {
		if err = os.WriteFile(filepath.Join(source, strconv.Itoa(i)), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cache := &ModelCache{}
	if err = cache.recoverModelDirectory(context.Background(), source, destination); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(destination)
	if err != nil || len(entries) != 300 {
		t.Fatalf("recovered=%d err=%v", len(entries), err)
	}
}
