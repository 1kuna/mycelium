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

	if err := peerA.Advertise(ctx, domain.Peer{ID: "overlay-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}); err != nil {
		t.Fatalf("advertise A: %v", err)
	}
	if err := peerB.Advertise(ctx, domain.Peer{ID: "overlay-b", Addresses: []string{targetAddr}, Compute: true}); err != nil {
		t.Fatalf("advertise B: %v", err)
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
}
