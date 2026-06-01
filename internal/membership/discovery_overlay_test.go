package membership

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestOverlayDiscoveryAndTunnelUseConfiguredBackend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tunnel := &mocks.Tunnel{Addr: "127.0.0.1:7000"}
	backend := NewMemoryOverlayBackend(tunnel)
	discovery := NewOverlayDiscovery(backend)
	overlayTunnel := NewOverlayTunnel(backend)

	watch, err := discovery.WatchPeers(ctx)
	if err != nil {
		t.Fatalf("WatchPeers: %v", err)
	}
	peer := fixtures.MakePeer(fixtures.WithPeerID("peer-b"))
	if err := discovery.Advertise(ctx, peer); err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	seen := <-watch
	if seen.ID != peer.ID {
		t.Fatalf("watch peer = %+v", seen)
	}
	peers, err := discovery.Peers(ctx)
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != peer.ID {
		t.Fatalf("peers = %+v", peers)
	}
	addr, err := overlayTunnel.Open(ctx, domain.Node{ID: "peer-b", Address: "10.0.0.2:51846"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if addr != tunnel.Addr {
		t.Fatalf("addr = %s", addr)
	}
	if err := overlayTunnel.Close(ctx, "peer-b"); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOverlayDiscoveryAndTunnelFailWithoutBackend(t *testing.T) {
	discovery := OverlayDiscovery{}
	if err := discovery.Advertise(context.Background(), fixtures.MakePeer()); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("Advertise err = %v", err)
	}
	if _, err := discovery.Peers(context.Background()); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("Peers err = %v", err)
	}
	if _, err := discovery.WatchPeers(context.Background()); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("WatchPeers err = %v", err)
	}

	tunnel := OverlayTunnel{}
	if _, err := tunnel.Open(context.Background(), fixtures.MakeNode()); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("Open err = %v", err)
	}
	if err := tunnel.Close(context.Background(), "node-a"); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("Close err = %v", err)
	}
}

func TestMemoryOverlayRejectsBadInputAndCanceledContext(t *testing.T) {
	backend := NewMemoryOverlayBackend(nil)
	if err := backend.Advertise(context.Background(), domain.Peer{}); err == nil {
		t.Fatal("missing peer id accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backend.Advertise(ctx, fixtures.MakePeer()); err != context.Canceled {
		t.Fatalf("advertise canceled err = %v", err)
	}
	if _, err := backend.Peers(ctx); err != context.Canceled {
		t.Fatalf("peers canceled err = %v", err)
	}
	if _, err := backend.WatchPeers(ctx); err != context.Canceled {
		t.Fatalf("watch canceled err = %v", err)
	}
	if _, err := backend.Open(context.Background(), fixtures.MakeNode()); err == nil || !strings.Contains(err.Error(), "tunnel backend") {
		t.Fatalf("open err = %v", err)
	}
	if err := backend.Close(context.Background(), "node-a"); err == nil || !strings.Contains(err.Error(), "tunnel backend") {
		t.Fatalf("close err = %v", err)
	}
}

func TestLibp2pOverlayDiscoversAndTunnels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/snapshot" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer target.Close()
	targetAddr := strings.TrimPrefix(target.URL, "http://")

	peerA, err := NewLibp2pOverlayBackend(ctx, Libp2pOverlayConfig{ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatalf("New peerA: %v", err)
	}
	defer peerA.CloseHost()
	peerB, err := NewLibp2pOverlayBackend(ctx, Libp2pOverlayConfig{
		ListenAddrs:    []string{"/ip4/127.0.0.1/tcp/0"},
		BootstrapPeers: []string{peerA.Addrs()[0]},
		LocalTarget:    targetAddr,
	})
	if err != nil {
		t.Fatalf("New peerB: %v", err)
	}
	defer peerB.CloseHost()

	watchA, err := peerA.WatchPeers(ctx)
	if err != nil {
		t.Fatalf("Watch A: %v", err)
	}
	if err := peerA.Advertise(ctx, fixtures.MakePeer(fixtures.WithPeerID("peer-a"), fixtures.WithPeerAddress("127.0.0.1:51001"))); err != nil {
		t.Fatalf("Advertise A: %v", err)
	}
	if err := peerB.Advertise(ctx, fixtures.MakePeer(fixtures.WithPeerID("peer-b"), fixtures.WithPeerAddress(targetAddr))); err != nil {
		t.Fatalf("Advertise B: %v", err)
	}
	if !watchSawPeer(ctx, watchA, "peer-b") {
		t.Fatal("peer A did not watch peer B")
	}
	peersA, err := peerA.Peers(ctx)
	if err != nil {
		t.Fatalf("Peers A: %v", err)
	}
	peersB, err := peerB.Peers(ctx)
	if err != nil {
		t.Fatalf("Peers B: %v", err)
	}
	if !hasPeer(peersA, "peer-b") || !hasPeer(peersB, "peer-a") {
		t.Fatalf("peersA=%+v peersB=%+v", peersA, peersB)
	}

	loopback, err := peerA.Open(ctx, domain.Node{ID: "peer-b", Address: targetAddr})
	if err != nil {
		t.Fatalf("Open tunnel: %v", err)
	}
	defer peerA.Close(ctx, "peer-b")
	resp, err := http.Get("http://" + loopback + "/snapshot")
	if err != nil {
		t.Fatalf("GET tunnel: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %s", resp.Status)
	}
	if again, err := peerA.Open(ctx, domain.Node{ID: "peer-b", Address: "ignored"}); err != nil || again != loopback {
		t.Fatalf("reused tunnel = %s %v", again, err)
	}
}

func TestLibp2pOverlayRejectsTokenMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peerA, err := NewLibp2pOverlayBackend(ctx, Libp2pOverlayConfig{
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
		Token:       "token-a",
	})
	if err != nil {
		t.Fatalf("New peerA: %v", err)
	}
	defer peerA.CloseHost()
	peerB, err := NewLibp2pOverlayBackend(ctx, Libp2pOverlayConfig{
		ListenAddrs:    []string{"/ip4/127.0.0.1/tcp/0"},
		BootstrapPeers: []string{peerA.Addrs()[0]},
		Token:          "token-b",
	})
	if err != nil {
		t.Fatalf("New peerB: %v", err)
	}
	defer peerB.CloseHost()

	if err := peerA.Advertise(ctx, fixtures.MakePeer(fixtures.WithPeerID("peer-a"), fixtures.WithPeerAddress("127.0.0.1:51001"))); err != nil {
		t.Fatalf("Advertise A: %v", err)
	}
	if err := peerB.Advertise(ctx, fixtures.MakePeer(fixtures.WithPeerID("peer-b"), fixtures.WithPeerAddress("127.0.0.1:51002"))); err == nil {
		t.Fatal("mismatched token advertise succeeded")
	}
	peers, err := peerA.Peers(ctx)
	if err != nil {
		t.Fatalf("Peers A: %v", err)
	}
	if hasPeer(peers, "peer-b") {
		t.Fatalf("peer with mismatched token was accepted: %+v", peers)
	}
}

func TestLibp2pOverlayErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewLibp2pOverlayBackend(ctx, Libp2pOverlayConfig{}); err != context.Canceled {
		t.Fatalf("canceled new err = %v", err)
	}
	if _, err := NewLibp2pOverlayBackend(context.Background(), Libp2pOverlayConfig{BootstrapPeers: []string{"not-a-multiaddr"}}); err == nil || !strings.Contains(err.Error(), "bootstrap") {
		t.Fatalf("bad bootstrap err = %v", err)
	}
	if _, err := NewLibp2pOverlayBackendWithHost(context.Background(), nil, Libp2pOverlayConfig{}); err == nil || !strings.Contains(err.Error(), "host") {
		t.Fatalf("nil host err = %v", err)
	}

	backend, err := NewLibp2pOverlayBackend(context.Background(), Libp2pOverlayConfig{ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatalf("New backend: %v", err)
	}
	defer backend.CloseHost()
	if err := backend.Advertise(context.Background(), domain.Peer{}); err == nil || !strings.Contains(err.Error(), "peer id") {
		t.Fatalf("empty advertise err = %v", err)
	}
	if _, err := backend.Open(context.Background(), domain.Node{}); err == nil || !strings.Contains(err.Error(), "node id") {
		t.Fatalf("empty open err = %v", err)
	}
	if _, err := backend.Open(context.Background(), domain.Node{ID: "missing"}); err == nil || !strings.Contains(err.Error(), "not known") {
		t.Fatalf("unknown open err = %v", err)
	}
	if err := backend.Close(ctx, "missing"); err != context.Canceled {
		t.Fatalf("canceled close err = %v", err)
	}
	if err := backend.Close(context.Background(), "missing"); err != nil {
		t.Fatalf("close missing: %v", err)
	}
	if _, err := backend.Peers(ctx); err != context.Canceled {
		t.Fatalf("canceled peers err = %v", err)
	}
	if _, err := backend.WatchPeers(ctx); err != context.Canceled {
		t.Fatalf("canceled watch err = %v", err)
	}
	if err := backend.Advertise(ctx, fixtures.MakePeer()); err != context.Canceled {
		t.Fatalf("canceled advertise err = %v", err)
	}
	if _, err := backend.Open(ctx, domain.Node{ID: "missing"}); err != context.Canceled {
		t.Fatalf("canceled open err = %v", err)
	}

	other, err := NewLibp2pOverlayBackend(context.Background(), Libp2pOverlayConfig{ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatalf("New other backend: %v", err)
	}
	defer other.CloseHost()
	if _, err := addrInfoFromStrings(backend.host.ID().String(), other.Addrs()); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched multiaddr err = %v", err)
	}
}

func watchSawPeer(ctx context.Context, ch <-chan domain.Peer, id string) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case peer := <-ch:
			if peer.ID == id {
				return true
			}
		}
	}
}

func hasPeer(peers []domain.Peer, id string) bool {
	for _, peer := range peers {
		if peer.ID == id {
			return true
		}
	}
	return false
}

func ExampleLibp2pOverlayBackend_Addrs() {
	ctx := context.Background()
	backend, err := NewLibp2pOverlayBackend(ctx, Libp2pOverlayConfig{ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		panic(err)
	}
	defer backend.CloseHost()
	fmt.Println(len(backend.Addrs()) > 0)
	// Output: true
}
