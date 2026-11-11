package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #1016: control calls were cut off by a fixed 10 s client timeout while a
// loaded queue was still answering. The progress watch keeps a call alive while
// the queue releases its lock and fails it once the queue stops progressing.
func TestClientProgressWatch(t *testing.T) {
	type pingReply func(n int, elapsed time.Duration) (string, error)
	run := func(t *testing.T, ping pingReply, finishAfter time.Duration) (error, time.Duration, int) {
		t.Helper()
		clock := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
		start := clock.Now()
		finished := make(chan struct{})
		var once sync.Once
		var pings atomic.Int32
		c := Client{URL: "http://fixture.invalid", Clock: clock, Pause: func(_ context.Context, d time.Duration) error {
			clock.Advance(d)
			return nil
		}, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			reply := func(body string) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
			}
			if r.URL.Path == "/api/ping" {
				elapsed := clock.Now().Sub(start)
				if finishAfter > 0 && elapsed >= finishAfter {
					once.Do(func() { close(finished) })
				}
				body, err := ping(int(pings.Add(1)), elapsed)
				if err != nil {
					return nil, err
				}
				return reply(body)
			}
			select {
			case <-finished:
				return reply(`{"targets":[{"eligible":true}]}`)
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		})}}.WithProgressWatch()
		var report ResolveReport
		err := c.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{}, &report)
		return err, clock.Now().Sub(start), int(pings.Load())
	}
	progress := func(releases uint64, waiting int64, p99 time.Duration) string {
		data, _ := json.Marshal(map[string]any{"version": 1, "queue_progress": QueueProgress{Releases: releases, Waiting: waiting, P99MS: float64(p99) / float64(time.Millisecond)}})
		return string(data)
	}

	t.Run("slow queue that progresses", func(t *testing.T) {
		err, waited, _ := run(t, func(n int, _ time.Duration) (string, error) {
			return progress(uint64(n), 3, 2*time.Second), nil
		}, time.Minute)
		if err != nil || waited < time.Minute {
			t.Fatalf("progressing queue cut off: err=%v waited=%s", err, waited)
		}
	})
	t.Run("idle lock is not a stall", func(t *testing.T) {
		err, _, _ := run(t, func(int, time.Duration) (string, error) {
			return progress(9, 0, 10*time.Millisecond), nil
		}, 30*time.Second)
		if err != nil {
			t.Fatal("unblocked queue reported stalled:", err)
		}
	})
	t.Run("held lock with waiters stalls", func(t *testing.T) {
		err, waited, pings := run(t, func(int, time.Duration) (string, error) {
			return progress(7, 2, 3*time.Second), nil
		}, 0)
		var stall *QueueStalledError
		// Window: four measured 3 s request periods.
		if !errors.As(err, &stall) || stall.Window != 12*time.Second || waited < 12*time.Second || waited > 20*time.Second || pings < 2 {
			t.Fatalf("stall not reported within its window: %v waited=%s pings=%d", err, waited, pings)
		}
		if !strings.Contains(err.Error(), "queue_stalled") || !strings.Contains(err.Error(), "myce status") || stall.Cause != nil {
			t.Fatalf("stall lacks an action: %v", err)
		}
	})
	t.Run("unanswered probes stall with their cause", func(t *testing.T) {
		probe := errors.New("probe connection reset")
		err, waited, _ := run(t, func(int, time.Duration) (string, error) { return "", probe }, 0)
		var stall *QueueStalledError
		if !errors.As(err, &stall) || !errors.Is(err, probe) || waited < 4*clientRecoveryPeriod {
			t.Fatalf("silent queue not bounded: %v waited=%s", err, waited)
		}
	})
	t.Run("progress after a quiet spell resets the window", func(t *testing.T) {
		err, waited, _ := run(t, func(n int, elapsed time.Duration) (string, error) {
			// Quiet for under one window, then a release, repeatedly.
			return progress(uint64(elapsed/(10*time.Second)), 1, 3*time.Second), nil
		}, 90*time.Second)
		if err != nil || waited < 90*time.Second {
			t.Fatalf("intermittent progress cut off: err=%v waited=%s", err, waited)
		}
	})
}

func TestClientProgressWatchWithoutEvidence(t *testing.T) {
	for _, body := range []string{`{"version":1}`, `[{"seq":1}]`, ""} {
		clock := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
		pings := 0
		c := Client{URL: "http://fixture.invalid", Clock: clock, Pause: func(_ context.Context, d time.Duration) error {
			clock.Advance(d)
			return nil
		}, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			pings++
			code := 200
			if body == "" {
				code = 404
			}
			return &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		if err := c.watchProgress(context.Background(), "POST", "/api/resolve"); err != nil || pings != 1 {
			t.Fatalf("server without progress evidence (%q) ended the call: %v pings=%d", body, err, pings)
		}
	}
}

func TestClientReplaySafePostRetriesOverload(t *testing.T) {
	for _, path := range []string{"/api/resolve", "/api/files/preflight", "/api/extensions"} {
		clock := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
		calls := 0
		var sleeps []time.Duration
		c := Client{URL: "http://fixture.invalid", Clock: clock, Jitter: func() float64 { return 0 }, Pause: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			clock.Advance(d)
			return nil
		}, HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": {"3"}}, Body: io.NopCloser(strings.NewReader(`{"code":"request_failed"}`))}, nil
			}
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		})}}
		if err := c.Call(context.Background(), "POST", path, map[string]string{}, nil); err != nil || calls != 2 || len(sleeps) != 1 || sleeps[0] < 3*time.Second {
			t.Fatalf("%s overload not retried after Retry-After: %v calls=%d sleeps=%v", path, err, calls, sleeps)
		}
	}
	// Submission replays depend on the caller's saved key; Call never replays it.
	calls := 0
	c := Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": {"3"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}}
	if err := c.Call(context.Background(), "POST", "/api/jobs", map[string]string{}, nil); err == nil || calls != 1 {
		t.Fatalf("submission replayed by the transport: %v calls=%d", err, calls)
	}
}

func TestPingReportsQueueProgress(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	read := func() QueueProgress {
		t.Helper()
		var reply struct {
			Progress *QueueProgress `json:"queue_progress"`
		}
		if err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "GET", "/api/ping", nil, &reply); err != nil || reply.Progress == nil {
			t.Fatal(reply, err)
		}
		return *reply.Progress
	}
	before := read()
	q.mu.Lock()
	blocked := make(chan struct{})
	go func() { q.mu.RLock(); q.mu.RUnlock(); close(blocked) }()
	await(t, func() bool { return read().Waiting >= 1 })
	held := read()
	q.mu.Unlock()
	<-blocked
	after := read()
	if held.Releases < before.Releases || after.Releases < held.Releases+2 || after.Waiting != 0 {
		t.Fatalf("progress not reported: before=%+v held=%+v after=%+v", before, held, after)
	}
	if fmt.Sprint(after.P99MS) == "NaN" || after.P99MS < 0 {
		t.Fatal("bad p99", after)
	}
}
