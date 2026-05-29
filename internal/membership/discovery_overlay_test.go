package membership

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"

	"github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

func TestOverlayDiscoveryAnnouncesAndDiscoversNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	left := newOverlayTestHost(t)
	defer left.Close()
	right := newOverlayTestHost(t)
	defer right.Close()
	leftDiscovery := NewOverlayDiscovery(left, overlayAddrInfo(right))
	rightDiscovery := NewOverlayDiscovery(right, overlayAddrInfo(left))
	if err := leftDiscovery.ensure(); err != nil {
		t.Fatalf("left ensure: %v", err)
	}
	if err := rightDiscovery.ensure(); err != nil {
		t.Fatalf("right ensure: %v", err)
	}

	if err := rightDiscovery.Announce(ctx, readyJoinNode("node-b", "127.0.0.1:51847")); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	nodes, err := leftDiscovery.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != "node-b" || nodes[0].Labels[LabelOverlayPeerID] != right.ID().String() || nodes[0].Labels[LabelOverlayAddrs] == "" {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestOverlayTunnelForwardsHTTPToRemotePeerTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	left := newOverlayTestHost(t)
	defer left.Close()
	right := newOverlayTestHost(t)
	defer right.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/snapshot" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("overlay-ok"))
	}))
	defer target.Close()

	remoteTunnel := NewOverlayTunnel(right)
	if err := remoteTunnel.ensure(); err != nil {
		t.Fatalf("remote ensure: %v", err)
	}
	localTunnel := NewOverlayTunnel(left)
	node := readyJoinNode("node-b", strings.TrimPrefix(target.URL, "http://"))
	node.Labels = withOverlayLabels(right, node.Labels)
	addr, err := localTunnel.Open(ctx, node)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer localTunnel.Close(context.Background(), node.ID)

	resp, err := http.Get("http://" + addr + "/snapshot")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "overlay-ok" {
		t.Fatalf("body = %q", body)
	}
}

func TestOverlayPeerInfoFailsLoudly(t *testing.T) {
	if _, err := overlayPeerInfo(domain.Node{ID: "node-a"}); err == nil {
		t.Fatal("missing peer id accepted")
	}
	node := domain.Node{ID: "node-a", Labels: map[string]string{LabelOverlayPeerID: "not-a-peer"}}
	if _, err := overlayPeerInfo(node); err == nil {
		t.Fatal("bad peer id accepted")
	}
	h := newOverlayTestHost(t)
	defer h.Close()
	node = domain.Node{ID: "node-a", Labels: map[string]string{LabelOverlayPeerID: h.ID().String()}}
	if _, err := overlayPeerInfo(node); err == nil {
		t.Fatal("missing peer addresses accepted")
	}
	node.Labels[LabelOverlayAddrs] = "%"
	if _, err := overlayPeerInfo(node); err == nil {
		t.Fatal("malformed peer address accepted")
	}
}

func newOverlayTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	return h
}

func overlayAddrInfo(h host.Host) peer.AddrInfo {
	return peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}
}
