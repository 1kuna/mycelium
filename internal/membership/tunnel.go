package membership

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type LANTunnel struct {
	mu    sync.Mutex
	known map[string]*tunnelEntry
}

type tunnelEntry struct {
	target   string
	listener net.Listener
	server   *http.Server
}

func NewLANTunnel() *LANTunnel {
	return &LANTunnel{known: map[string]*tunnelEntry{}}
}

func (t *LANTunnel) Open(ctx context.Context, node domain.Node) (string, error) {
	if node.ID == "" {
		return "", fmt.Errorf("node id is required")
	}
	if node.Address == "" {
		return "", fmt.Errorf("node address is required")
	}
	target, err := url.Parse("http://" + node.Address)
	if err != nil {
		return "", err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := &http.Server{Handler: proxy}
	entry := &tunnelEntry{target: node.Address, listener: listener, server: server}
	t.mu.Lock()
	old := t.known[node.ID]
	t.known[node.ID] = entry
	t.mu.Unlock()
	if old != nil {
		_ = old.server.Shutdown(ctx)
	}
	go func() {
		err := server.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return listener.Addr().String(), nil
}

func (t *LANTunnel) Close(ctx context.Context, nodeID string) error {
	t.mu.Lock()
	entry := t.known[nodeID]
	delete(t.known, nodeID)
	t.mu.Unlock()
	if entry == nil {
		return nil
	}
	return entry.server.Shutdown(ctx)
}

var _ ports.Tunnel = (*LANTunnel)(nil)
