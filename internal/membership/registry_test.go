package membership

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestRegistryJoinDiscoversNodeAndRejectsBadToken(t *testing.T) {
	manager, err := NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	registry := NewRegistry(manager, NewLANTunnel())
	node := readyJoinNode("node-a", "127.0.0.1:1")
	if _, err := registry.Join(context.Background(), JoinRequest{Token: "bad", Node: node}); err == nil {
		t.Fatal("bad token joined")
	}
	if _, err := registry.Join(context.Background(), JoinRequest{Token: "secret", Node: node}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	nodes, err := registry.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != node.ID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if _, err := registry.NodeAgent(node.ID); err != nil {
		t.Fatalf("NodeAgent: %v", err)
	}
	if _, err := registry.NodeAgent("missing"); err == nil {
		t.Fatal("missing node agent succeeded")
	}
	if err := registry.Announce(context.Background(), node); err == nil {
		t.Fatal("tokenless announce succeeded")
	}
}

func TestRegistryHTTPNodes(t *testing.T) {
	manager, err := NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	registry := NewRegistry(manager, NewLANTunnel())
	if _, err := registry.Join(context.Background(), JoinRequest{Token: "secret", Node: readyJoinNode("node-a", "127.0.0.1:1")}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	rec := httptest.NewRecorder()
	registry.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "node-a") {
		t.Fatalf("status/body = %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	registry.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/join", strings.NewReader(`{`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	registry.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/join", strings.NewReader(`{"token":"bad","node":{"id":"node-b","address":"127.0.0.1:1"}}`)))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("bad join status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	registry.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d", rec.Code)
	}
}

func TestRegistryHandlesMissingTokenManagerAndDefaultTunnel(t *testing.T) {
	registry := NewRegistry(nil, nil)
	if registry.tunnel == nil {
		t.Fatal("default tunnel was not installed")
	}
	if _, err := registry.Join(context.Background(), JoinRequest{Token: "secret", Node: readyJoinNode("node-a", "127.0.0.1:1")}); err == nil {
		t.Fatal("join without token manager succeeded")
	}
}

func TestOverlayDiscoveryAndTunnelErrorPaths(t *testing.T) {
	if err := (OverlayDiscovery{}).Announce(context.Background(), readyJoinNode("node-a", "127.0.0.1:1")); err == nil {
		t.Fatal("overlay announce succeeded")
	}
	if _, err := (OverlayDiscovery{}).Discover(context.Background()); err == nil {
		t.Fatal("overlay discover succeeded")
	}
	tunnel := NewLANTunnel()
	if _, err := tunnel.Open(context.Background(), domain.Node{Address: "127.0.0.1:1"}); err == nil {
		t.Fatal("missing node id opened tunnel")
	}
	if _, err := tunnel.Open(context.Background(), domain.Node{ID: "node-a"}); err == nil {
		t.Fatal("missing node address opened tunnel")
	}
	if err := tunnel.Close(context.Background(), "node-a"); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestLANTunnelForwardsThroughLoopback(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/snapshot" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer target.Close()
	tunnel := NewLANTunnel()
	addr := strings.TrimPrefix(target.URL, "http://")
	loopback, err := tunnel.Open(context.Background(), readyJoinNode("node-a", addr))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := tunnel.Close(context.Background(), "node-a"); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	resp, err := http.Get("http://" + loopback + "/snapshot")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %s", resp.Status)
	}
}

func readyJoinNode(id, addr string) domain.Node {
	return domain.Node{
		ID:          id,
		Name:        id,
		Address:     addr,
		Status:      domain.NodeReady,
		OOMSeverity: domain.OOMSoft,
		MaxUtil:     0.9,
		Accelerators: []domain.Accelerator{{
			Index:         0,
			Vendor:        "apple",
			Kind:          "unified",
			VRAMTotalMB:   1024,
			UnifiedMemory: true,
		}},
		UnifiedMemory: true,
	}
}
