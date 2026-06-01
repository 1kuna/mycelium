//go:build smoke

package smoke

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/membership"
)

func TestLibp2pOverlaySmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("overlay-ok"))
	}))
	defer target.Close()
	targetAddr := strings.TrimPrefix(target.URL, "http://")

	peerA, err := membership.NewLibp2pOverlayBackend(ctx, membership.Libp2pOverlayConfig{
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
		Token:       "overlay-smoke",
	})
	if err != nil {
		t.Fatalf("peer A: %v", err)
	}
	defer peerA.CloseHost()
	peerB, err := membership.NewLibp2pOverlayBackend(ctx, membership.Libp2pOverlayConfig{
		ListenAddrs:    []string{"/ip4/127.0.0.1/tcp/0"},
		BootstrapPeers: []string{peerA.Addrs()[0]},
		LocalTarget:    targetAddr,
		Token:          "overlay-smoke",
	})
	if err != nil {
		t.Fatalf("peer B: %v", err)
	}
	defer peerB.CloseHost()

	watchA, err := peerA.WatchPeers(ctx)
	if err != nil {
		t.Fatalf("watch A: %v", err)
	}
	if err := peerA.Advertise(ctx, domain.Peer{ID: "overlay-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}); err != nil {
		t.Fatalf("advertise A: %v", err)
	}
	if err := peerB.Advertise(ctx, domain.Peer{ID: "overlay-b", Addresses: []string{targetAddr}, Compute: true}); err != nil {
		t.Fatalf("advertise B: %v", err)
	}
	if !watchSawPeer(ctx, watchA, "overlay-b") {
		t.Fatal("peer A did not watch peer B")
	}
	peersA, err := peerA.Peers(ctx)
	if err != nil {
		t.Fatalf("peers A: %v", err)
	}
	peersB, err := peerB.Peers(ctx)
	if err != nil {
		t.Fatalf("peers B: %v", err)
	}
	if !hasPeer(peersA, "overlay-b") || !hasPeer(peersB, "overlay-a") {
		t.Fatalf("peersA=%+v peersB=%+v", peersA, peersB)
	}
	loopback, err := peerA.Open(ctx, domain.Node{ID: "overlay-b", Address: targetAddr})
	if err != nil {
		t.Fatalf("open tunnel: %v", err)
	}
	defer peerA.Close(ctx, "overlay-b")
	resp, err := http.Get("http://" + loopback + "/")
	if err != nil {
		t.Fatalf("get tunnel: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %s", resp.Status)
	}
	if again, err := peerA.Open(ctx, domain.Node{ID: "overlay-b", Address: "ignored"}); err != nil || again != loopback {
		t.Fatalf("reused tunnel = %s %v", again, err)
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
