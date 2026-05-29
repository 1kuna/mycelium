package membership

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type LANTunnel struct {
	AuthToken string

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
	target, err := tunnelTarget(node.Address)
	if err != nil {
		return "", err
	}
	t.mu.Lock()
	if old := t.known[node.ID]; old != nil && old.target == node.Address {
		addr := old.listener.Addr().String()
		t.mu.Unlock()
		return addr, nil
	}
	t.mu.Unlock()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	proxy.Director = func(req *http.Request) {
		director(req)
		if t.AuthToken != "" {
			req.Header.Set("Authorization", "Bearer "+t.AuthToken)
		}
	}
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

func tunnelTarget(address string) (*url.URL, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("node address is required")
	}
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	target, err := url.Parse(address)
	if err != nil {
		return nil, err
	}
	if target.Host == "" {
		return nil, fmt.Errorf("node address %q is missing host", address)
	}
	return target, nil
}

var _ ports.Tunnel = (*LANTunnel)(nil)
