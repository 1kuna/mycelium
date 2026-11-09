package myce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Exercise the real learned deadline with a fake socket clock. The first
// connection makes two progress observations, then exceeds its learned window.
type inputProgressConn struct {
	net.Conn
	clock    *fakeClock
	data     *bytes.Reader
	reads    int
	stall    bool
	deadline time.Time
}

func (c *inputProgressConn) SetReadDeadline(end time.Time) error { c.deadline = end; return nil }
func (c *inputProgressConn) Read(p []byte) (int, error) {
	if c.stall && c.reads == 2 {
		if c.deadline.IsZero() {
			return 0, errors.New("missing learned deadline")
		}
		c.clock.now = c.deadline
		return 0, os.ErrDeadlineExceeded
	}
	c.reads++
	c.clock.Advance(time.Second)
	return c.data.Read(p[:min(len(p), 8)])
}

func TestInputTransferLearnedStallResumesVerifiedPartial(t *testing.T) {
	body := "immutable dependency bytes after a slow queue response"
	ref := sharedFixtureFile(t, t.TempDir(), "input", body)
	clock := &fakeClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
	var recovering atomic.Bool
	ctx := context.WithValue(context.Background(), inputTransferRecoveryKey{}, &recovering)
	gets, pauses := 0, 0
	root, dest := t.TempDir(), filepath.Join(t.TempDir(), "input")
	c := Client{URL: "http://fixture.invalid", Clock: clock, Jitter: func() float64 { return 0 }}
	c.Pause = func(_ context.Context, delay time.Duration) error {
		pauses++
		partial, err := os.ReadFile(filepath.Join(root, ref.SHA256+".partial"))
		if err != nil || string(partial) != body[:16] || !recovering.Load() || delay <= 0 {
			t.Fatalf("recovery lost partial/status/pacing: bytes=%q pending=%v delay=%v err=%v", partial, recovering.Load(), delay, err)
		}
		if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("published before verification", err)
		}
		clock.Advance(delay)
		return nil
	}
	c.HTTP = &http.Client{Timeout: time.Millisecond, Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		headers := http.Header{"Etag": {fmt.Sprintf("%q", ref.SHA256)}}
		if r.Method == "HEAD" {
			return &http.Response{StatusCode: 200, Header: headers, ContentLength: ref.SizeBytes, Body: http.NoBody}, nil
		}
		gets++
		status, offset := 200, 0
		if gets == 2 {
			status, offset = 206, 16
			if r.Header.Get("Range") != "bytes=16-" || r.Header.Get("If-Range") != fmt.Sprintf("%q", ref.SHA256) {
				t.Error("lost exact resume identity", r.Header)
			}
			headers.Set("Content-Range", fmt.Sprintf("bytes 16-%d/%d", ref.SizeBytes-1, ref.SizeBytes))
		}
		conn := &inputProgressConn{clock: clock, data: bytes.NewReader([]byte(body[offset:])), stall: gets == 1}
		return &http.Response{StatusCode: status, Header: headers, ContentLength: ref.SizeBytes - int64(offset), Body: io.NopCloser(&fileTransferConn{Conn: conn, clock: clock})}, nil
	})}
	cache := FileCache{Root: root, Limit: 1 << 20, Clock: clock, Client: c}
	if err := cache.copyTo(ctx, ref, dest); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != body || gets != 2 || pauses != 1 || recovering.Load() {
		t.Fatalf("resume failed: gets=%d pauses=%d data=%q pending=%v err=%v", gets, pauses, got, recovering.Load(), err)
	}
}

func TestInputTransferDelayedHeadersHaveOnlyOwnerDeadline(t *testing.T) {
	ref := sharedFixtureFile(t, t.TempDir(), "input", "small dependency")
	clock := &fakeClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
	start := clock.Now()
	cache := FileCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock, Client: Client{URL: "http://fixture.invalid", Clock: clock, HTTP: &http.Client{Timeout: time.Second, Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		clock.Advance(125 * time.Second)
		body := ""
		if r.Method != "HEAD" {
			body = "small dependency"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Etag": {fmt.Sprintf("%q", ref.SHA256)}}, ContentLength: ref.SizeBytes, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}}
	if err := cache.copyTo(context.Background(), ref, filepath.Join(t.TempDir(), "input")); err != nil {
		t.Fatal(err)
	}
	if clock.Now().Sub(start) != 250*time.Second {
		t.Fatal("did not exercise both slow responses")
	}
}

func TestInputTransferRecoveryOwnerWithdrawal(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint("deadline=", deadline), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ref := sharedFixtureFile(t, t.TempDir(), "input", "input")
				ctx, cancel := context.WithCancel(context.Background())
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
				}
				defer cancel()
				var pending atomic.Bool
				ctx = context.WithValue(ctx, inputTransferRecoveryKey{}, &pending)
				calls, pauses := 0, 0
				c := Client{Clock: RealClock{}, Jitter: func() float64 { return 0 }, Pause: func(ctx context.Context, delay time.Duration) error {
					pauses++
					if !pending.Load() {
						t.Error("missing recovery status")
					}
					if !deadline {
						cancel()
					}
					return waitStatusPause(ctx, delay)
				}}
				err := c.recoverInputTransfer(ctx, ref, "cache_download", func(Client) error {
					if ctx.Err() != nil {
						t.Error("started another read after owner withdrawal")
					}
					if end, ok := ctx.Deadline(); ok && !c.clock().Now().Before(end) {
						t.Error("started another read at the owner's deadline")
					}
					calls++
					time.Sleep(100 * time.Millisecond)
					return errFileTransferStalled
				})
				want := error(context.Canceled)
				if deadline {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) || pending.Load() || calls < 1 || pauses < 1 || calls > pauses+1 || !deadline && calls != 1 {
					t.Fatalf("owner withdrawal lost: calls=%d pauses=%d pending=%v err=%v", calls, pauses, pending.Load(), err)
				}
			})
		})
	}
}

func TestInputTransferRealOutageReturnsWithoutRecovery(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	address := server.URL
	server.Close()
	ref := sharedFixtureFile(t, t.TempDir(), "input", "dependency")
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	c := Client{URL: address, Pause: func(context.Context, time.Duration) error {
		t.Error("retried completed dial failure")
		return context.Canceled
	}}
	cache := FileCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}, Client: c}
	dest := filepath.Join(t.TempDir(), "input")
	err := cache.copyTo(ctx, ref, dest)
	if !errors.Is(err, errInputTransferUnreachable) || stagingErrorName(err) != "storage_unreachable" || ctx.Err() != nil {
		t.Fatal("outage was masked", err, ctx.Err())
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published on outage", err)
	}
}

// The actual HTTP transport normalizes dial timeouts before cache recovery
// sees them. A completed timed-out dial is not an ambiguous live body stall.

func TestInputTransferPermanentRefusalsDoNotRecover(t *testing.T) {
	for _, failure := range []string{"auth", "typed_storage", "etag", "checksum"} {
		t.Run(failure, func(t *testing.T) {
			ref := sharedFixtureFile(t, t.TempDir(), "input", "original")
			gets := 0
			c := Client{URL: "http://fixture.invalid", Pause: func(context.Context, time.Duration) error {
				t.Error("retried permanent refusal")
				return context.Canceled
			}}
			c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				headers := http.Header{"Etag": {fmt.Sprintf("%q", ref.SHA256)}}
				status, body := 200, "original"
				if r.Method == "GET" {
					gets++
					switch failure {
					case "auth":
						status, body = 401, "{}"
					case "typed_storage":
						status, body = 503, `{"code":"file_store_unavailable"}`
					case "etag":
						headers.Set("Etag", `"wrong"`)
					case "checksum":
						body = "corrupt!"
					}
				}
				return &http.Response{StatusCode: status, Header: headers, ContentLength: ref.SizeBytes, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			cache := FileCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}, Client: c}
			dest := filepath.Join(t.TempDir(), "input")
			err := cache.copyTo(context.Background(), ref, dest)
			if err == nil || gets != 1 {
				t.Fatal("permanent failure hidden", gets, err)
			}
			if failure == "etag" || failure == "checksum" {
				if stagingErrorName(err) != "identity_mismatch" {
					t.Fatal("lost integrity reason", err)
				}
			}
			if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("published refused bytes", err)
			}
		})
	}
}

type inputTransferFaultSink struct {
	data              bytes.Buffer
	writeErr, syncErr error
	syncs             int
}

func (s *inputTransferFaultSink) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return s.data.Write(p)
}
func (s *inputTransferFaultSink) Sync() error { s.syncs++; return s.syncErr }

type inputTransferReadError struct{ err error }

func (r inputTransferReadError) Read([]byte) (int, error) { return 0, r.err }
