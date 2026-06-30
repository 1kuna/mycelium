package membership

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
)

func TestLANTunnelOpensReusesAndClosesFakeListener(t *testing.T) {
	first := newFakeListener("127.0.0.1:6001")
	second := newFakeListener("127.0.0.1:6002")
	factory := &fakeListenerFactory{listeners: []*fakeListener{first, second}}
	tunnel := NewLANTunnel()
	tunnel.Listener = factory
	node := domain.Node{ID: "node-a", Address: "198.51.100.2:51846"}
	loopback, err := tunnel.Open(context.Background(), node)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if loopback != "127.0.0.1:6001" {
		t.Fatalf("loopback = %s", loopback)
	}

	loopbackAgain, err := tunnel.Open(context.Background(), node)
	if err != nil {
		t.Fatalf("Open again: %v", err)
	}
	if loopbackAgain != loopback {
		t.Fatalf("loopback was not reused: first=%s second=%s", loopback, loopbackAgain)
	}

	changed := domain.Node{ID: "node-a", Address: "http://198.51.100.3:51846"}
	loopbackChanged, err := tunnel.Open(context.Background(), changed)
	if err != nil {
		t.Fatalf("Open changed: %v", err)
	}
	if loopbackChanged != "127.0.0.1:6002" {
		t.Fatalf("changed loopback = %s", loopbackChanged)
	}
	if !first.isClosed() {
		t.Fatal("old listener was not closed after target changed")
	}
	if err := tunnel.Close(context.Background(), "node-a"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !second.isClosed() {
		t.Fatal("current listener was not closed")
	}
}

func TestLANTunnelRejectsBadInputAndClosesMissing(t *testing.T) {
	tunnel := NewLANTunnel()
	if err := shutdownTunnelEntry(context.Background(), nil); err != nil {
		t.Fatalf("nil shutdown entry: %v", err)
	}
	if _, err := tunnel.Open(context.Background(), domain.Node{}); err == nil || !strings.Contains(err.Error(), "node id") {
		t.Fatalf("missing id err = %v", err)
	}
	if _, err := tunnel.Open(context.Background(), domain.Node{ID: "node-a"}); err == nil || !strings.Contains(err.Error(), "address") {
		t.Fatalf("missing address err = %v", err)
	}
	if err := tunnel.Close(context.Background(), "node-a"); err != nil {
		t.Fatalf("Close missing: %v", err)
	}
	if _, err := tunnel.Open(context.Background(), domain.Node{ID: "node-a", Address: "http:///missing-host"}); err == nil || !strings.Contains(err.Error(), "missing host") {
		t.Fatalf("missing host err = %v", err)
	}
	tunnel.Listener = errListenerFactory{err: fmt.Errorf("listen failed")}
	if _, err := tunnel.Open(context.Background(), domain.Node{ID: "node-a", Address: "127.0.0.1:1"}); err == nil || !strings.Contains(err.Error(), "listen failed") {
		t.Fatalf("listen err = %v", err)
	}
	if target, err := tunnelTarget(" 127.0.0.1:1 "); err != nil || target.String() != "http://127.0.0.1:1" {
		t.Fatalf("trimmed target = %s %v", target, err)
	}
}

func TestLANTunnelConcurrentOpenReusesSingleListener(t *testing.T) {
	listener := newFakeListener("127.0.0.1:6003")
	factory := &fakeListenerFactory{listeners: []*fakeListener{listener}, delay: 10 * time.Millisecond}
	tunnel := NewLANTunnel()
	tunnel.Listener = factory
	node := domain.Node{ID: "node-a", Address: "198.51.100.2:51846"}

	var wg sync.WaitGroup
	results := make(chan string, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := tunnel.Open(context.Background(), node)
			if err != nil {
				errs <- err
				return
			}
			results <- addr
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("Open err = %v", err)
	}
	for addr := range results {
		if addr != "127.0.0.1:6003" {
			t.Fatalf("addr = %s", addr)
		}
	}
	if factory.calls != 1 {
		t.Fatalf("listen calls = %d", factory.calls)
	}
	if err := tunnel.Close(context.Background(), node.ID); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestLANTunnelServeErrorDoesNotPanic(t *testing.T) {
	listener := &errorListener{addr: fakeAddr("127.0.0.1:6004"), err: fmt.Errorf("serve failed"), accepted: make(chan struct{})}
	tunnel := NewLANTunnel()
	tunnel.Listener = &singleListenerFactory{listener: listener}
	if _, err := tunnel.Open(context.Background(), domain.Node{ID: "node-a", Address: "198.51.100.2:51846"}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	select {
	case <-listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("serve did not reach listener accept")
	}
}

type errListenerFactory struct {
	err error
}

func (f errListenerFactory) Listen(context.Context, string, string) (net.Listener, error) {
	return nil, f.err
}

type fakeListenerFactory struct {
	mu        sync.Mutex
	listeners []*fakeListener
	delay     time.Duration
	calls     int
}

func (f *fakeListenerFactory) Listen(_ context.Context, _, _ string) (net.Listener, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if len(f.listeners) == 0 {
		return nil, fmt.Errorf("no fake listeners left")
	}
	listener := f.listeners[0]
	f.listeners = f.listeners[1:]
	return listener, nil
}

type fakeListener struct {
	addr     net.Addr
	accepted chan struct{}
	closed   chan struct{}
	once     sync.Once
}

func newFakeListener(addr string) *fakeListener {
	return &fakeListener{addr: fakeAddr(addr), accepted: make(chan struct{}, 1), closed: make(chan struct{})}
}

func (l *fakeListener) Accept() (net.Conn, error) {
	l.accepted <- struct{}{}
	<-l.closed
	return nil, http.ErrServerClosed
}

func (l *fakeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *fakeListener) Addr() net.Addr {
	return l.addr
}

func (l *fakeListener) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

type fakeAddr string

func (a fakeAddr) Network() string { return "tcp" }
func (a fakeAddr) String() string  { return string(a) }

type singleListenerFactory struct {
	listener net.Listener
}

func (f *singleListenerFactory) Listen(context.Context, string, string) (net.Listener, error) {
	return f.listener, nil
}

type errorListener struct {
	addr     net.Addr
	err      error
	accepted chan struct{}
	once     sync.Once
}

func (l *errorListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.accepted) })
	return nil, l.err
}

func (l *errorListener) Close() error { return nil }

func (l *errorListener) Addr() net.Addr { return l.addr }
