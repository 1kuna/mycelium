package main

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

type bindClock struct{ now time.Time }

func (c *bindClock) Now() time.Time { return c.now }

func TestListenRetry(t *testing.T) {
	for _, tc := range []struct {
		name                string
		cause               error
		failures, wantCalls int
		wantErr             bool
	}{
		{"address appears", syscall.EADDRNOTAVAIL, 3, 4, false},
		{"network appears", syscall.ENETUNREACH, 2, 3, false},
		{"in use", syscall.EADDRINUSE, 1, 1, true},
		{"permission", syscall.EACCES, 1, 1, true},
		{"deadline", syscall.EADDRNOTAVAIL, 1000, 31, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &bindClock{now: time.Unix(100, 0)}
			start := c.now
			calls, logs := 0, 0
			listener := new(net.TCPListener)
			listen := func(network, address string) (net.Listener, error) {
				calls++
				if network != "tcp" || address != "host:7331" {
					t.Fatal(network, address)
				}
				if calls <= tc.failures {
					return nil, &net.OpError{Op: "listen", Err: os.NewSyscallError("bind", tc.cause)}
				}
				return listener, nil
			}
			got, err := listenWithRetry(context.Background(), "host:7331", listen, c, func(_ context.Context, d time.Duration) error { c.now = c.now.Add(d); return nil }, func(string, ...any) { logs++ })
			if (err != nil) != tc.wantErr || calls != tc.wantCalls {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
			if err != nil && !errors.Is(err, tc.cause) {
				t.Fatal(err)
			}
			if !tc.wantErr && got != listener {
				t.Fatal("listener lost")
			}
			if c.now.Sub(start) > time.Minute || logs > 12 {
				t.Fatalf("elapsed=%v logs=%d", c.now.Sub(start), logs)
			}
		})
	}
}

func TestListenRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := listenWithRetry(ctx, "host:7331", func(string, string) (net.Listener, error) { calls++; cancel(); return nil, syscall.EADDRNOTAVAIL }, &bindClock{}, waitForBind, func(string, ...any) {})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
