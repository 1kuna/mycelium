package membership

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestLANTunnelForwardsThroughLoopback(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	})}
	go func() { _ = server.Serve(listener) }()
	defer server.Shutdown(context.Background())

	tunnel := NewLANTunnel()
	node := domain.Node{ID: "node-a", Address: addr}
	loopback, err := tunnel.Open(context.Background(), node)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tunnel.Close(context.Background(), node.ID)

	resp, err := http.Get("http://" + loopback)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	loopbackAgain, err := tunnel.Open(context.Background(), node)
	if err != nil {
		t.Fatalf("Open again: %v", err)
	}
	if loopbackAgain != loopback {
		t.Fatalf("loopback was not reused: first=%s second=%s", loopback, loopbackAgain)
	}
}

func TestLANTunnelAcceptsURLTarget(t *testing.T) {
	server := http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	defer server.Shutdown(context.Background())

	tunnel := NewLANTunnel()
	loopback, err := tunnel.Open(context.Background(), domain.Node{ID: "node-a", Address: "http://" + listener.Addr().String()})
	if err != nil {
		t.Fatalf("Open url: %v", err)
	}
	defer tunnel.Close(context.Background(), "node-a")
	resp, err := http.Get("http://" + loopback)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
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
}
