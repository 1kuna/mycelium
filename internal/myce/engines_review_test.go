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
	"sync/atomic"
	"testing"
	"time"
)

func TestGenericRequestSpoolIsBoundedReplayableAndCancellable(t *testing.T) {
	spool := &uploadSpool{limit: 32}
	f, n, err := spool.request(context.Background(), strings.NewReader("large-audio"), 32, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if n != int64(len("large-audio")) {
		t.Fatalf("size=%d", n)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f)
		if err != nil || string(got) != "large-audio" {
			t.Fatalf("attempt %d: %q %v", attempt, got, err)
		}
	}
	if _, _, err = spool.request(context.Background(), strings.NewReader("12345"), 4, -1); !errors.Is(err, errModelRequestTooLarge) {
		t.Fatalf("oversize error=%v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = spool.request(cancelled, strings.NewReader("data"), 4, -1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
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
