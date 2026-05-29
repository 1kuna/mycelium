package membership

import (
	"context"
	"encoding/json"
	"errors"
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
	if _, err := registry.AdmissionController(node.ID); err != nil {
		t.Fatalf("AdmissionController: %v", err)
	}
	if _, err := registry.AdmissionController("missing"); err == nil {
		t.Fatal("missing admission controller succeeded")
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

func TestPersistentRegistrySavesAdvertisedAddressAndRestoresTunnel(t *testing.T) {
	manager, err := NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	store := &memoryNodeStore{}
	snapshotServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/snapshot" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: readyJoinNode("node-a", "snapshot-addr")})
	}))
	defer snapshotServer.Close()
	tunnel := &recordingTunnel{addrs: []string{strings.TrimPrefix(snapshotServer.URL, "http://")}}
	registry, err := NewPersistentRegistry(context.Background(), manager, tunnel, store)
	if err != nil {
		t.Fatalf("NewPersistentRegistry: %v", err)
	}
	advertised := "10.0.0.2:51847"
	joined, err := registry.Join(context.Background(), JoinRequest{Token: "secret", Node: readyJoinNode("node-a", advertised)})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if joined.Address != strings.TrimPrefix(snapshotServer.URL, "http://") || joined.Labels[LabelAdvertisedAddress] != advertised || joined.Name != "node-a" {
		t.Fatalf("joined = %+v", joined)
	}
	if len(store.saved) != 1 || store.saved[0].Address != advertised {
		t.Fatalf("saved = %+v", store.saved)
	}
	if len(tunnel.opened) != 1 || tunnel.opened[0].Address != advertised {
		t.Fatalf("opened = %+v", tunnel.opened)
	}

	reloadStore := &memoryNodeStore{nodes: append([]domain.Node(nil), store.saved...)}
	reloadTunnel := &recordingTunnel{addrs: []string{"127.0.0.1:61002"}}
	reloaded, err := NewPersistentRegistry(context.Background(), manager, reloadTunnel, reloadStore)
	if err != nil {
		t.Fatalf("reload registry: %v", err)
	}
	if len(reloadTunnel.opened) != 1 || reloadTunnel.opened[0].Address != advertised {
		t.Fatalf("reload opened = %+v", reloadTunnel.opened)
	}
	fleet, err := reloaded.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(fleet.Nodes) != 1 || fleet.Nodes[0].Address != "127.0.0.1:61002" || fleet.Nodes[0].Labels[LabelAdvertisedAddress] != advertised {
		t.Fatalf("fleet = %+v", fleet)
	}
}

func TestPersistentRegistryPropagatesStoreErrors(t *testing.T) {
	manager, err := NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	wantErr := errors.New("store failed")
	if _, err := NewPersistentRegistry(context.Background(), manager, nil, &memoryNodeStore{err: wantErr}); !errors.Is(err, wantErr) {
		t.Fatalf("list err = %v", err)
	}
	registry, err := NewPersistentRegistry(context.Background(), manager, &recordingTunnel{}, &memoryNodeStore{saveErr: wantErr})
	if err != nil {
		t.Fatalf("NewPersistentRegistry: %v", err)
	}
	_, err = registry.Join(context.Background(), JoinRequest{Token: "secret", Node: readyJoinNode("node-a", "10.0.0.2:51847")})
	if !errors.Is(err, wantErr) {
		t.Fatalf("save err = %v", err)
	}
}

func TestOverlayDiscoveryAndTunnelErrorPaths(t *testing.T) {
	if err := (&OverlayDiscovery{}).Announce(context.Background(), readyJoinNode("node-a", "127.0.0.1:1")); err == nil {
		t.Fatal("overlay announce succeeded")
	}
	if _, err := (&OverlayDiscovery{}).Discover(context.Background()); err == nil {
		t.Fatal("overlay discover succeeded")
	}
	if _, err := (&OverlayTunnel{}).Open(context.Background(), readyJoinNode("node-a", "127.0.0.1:1")); err == nil {
		t.Fatal("overlay tunnel opened without host")
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

type memoryNodeStore struct {
	nodes   []domain.Node
	saved   []domain.Node
	err     error
	saveErr error
}

func (s *memoryNodeStore) SaveNode(_ context.Context, node domain.Node) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved = append(s.saved, node)
	return nil
}

func (s *memoryNodeStore) ListNodes(context.Context) ([]domain.Node, error) {
	if s.err != nil {
		return nil, s.err
	}
	return append([]domain.Node(nil), s.nodes...), nil
}

type recordingTunnel struct {
	addrs  []string
	opened []domain.Node
}

func (t *recordingTunnel) Open(_ context.Context, node domain.Node) (string, error) {
	t.opened = append(t.opened, node)
	if len(t.addrs) == 0 {
		return "127.0.0.1:1", nil
	}
	addr := t.addrs[0]
	t.addrs = t.addrs[1:]
	return addr, nil
}

func (t *recordingTunnel) Close(context.Context, string) error {
	return nil
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
