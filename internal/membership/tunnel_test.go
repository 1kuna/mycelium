package membership

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"

	"mycelium/internal/domain"
)

func TestLANTunnelOpensReusesAndClosesFakeListener(t *testing.T) {
	first := newFakeListener("127.0.0.1:6001")
	second := newFakeListener("127.0.0.1:6002")
	factory := &fakeListenerFactory{listeners: []*fakeListener{first, second}}
	tunnel := NewLANTunnel()
	tunnel.Listener = factory
	node := domain.Node{ID: "node-a", Address: "10.0.0.2:51846"}
	loopback, err := tunnel.Open(context.Background(), node)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if loopback != "127.0.0.1:6001" {
		t.Fatalf("loopback = %s", loopback)
	}
	waitAccepted(t, first)

	loopbackAgain, err := tunnel.Open(context.Background(), node)
	if err != nil {
		t.Fatalf("Open again: %v", err)
	}
	if loopbackAgain != loopback {
		t.Fatalf("loopback was not reused: first=%s second=%s", loopback, loopbackAgain)
	}

	changed := domain.Node{ID: "node-a", Address: "http://10.0.0.3:51846"}
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
	waitAccepted(t, second)
	if err := tunnel.Close(context.Background(), "node-a"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !second.isClosed() {
		t.Fatal("current listener was not closed")
	}
}

func TestLANTunnelRejectsBadInputAndClosesMissing(t *testing.T) {
	tunnel := NewLANTunnel()
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

type errListenerFactory struct {
	err error
}

func (f errListenerFactory) Listen(context.Context, string, string) (net.Listener, error) {
	return nil, f.err
}

type fakeListenerFactory struct {
	mu        sync.Mutex
	listeners []*fakeListener
}

func (f *fakeListenerFactory) Listen(_ context.Context, _, _ string) (net.Listener, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func waitAccepted(t *testing.T, listener *fakeListener) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		select {
		case <-listener.accepted:
			return
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("server did not start accepting")
}
