package membership

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/mocks"
)

func TestPeerLANDiscoveryAdvertisesPeer(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	peer := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:51846"}, Compute: true}

	if err := (PeerLANDiscovery{BroadcastAddr: conn.LocalAddr().String()}).Advertise(ctx, peer); err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	var got domain.Peer
	if err := json.Unmarshal(buf[:n], &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != peer.ID || got.Addresses[0] != peer.Addresses[0] || !got.Compute {
		t.Fatalf("got = %+v", got)
	}
}

func TestPeerLANDiscoveryPeersAndWatch(t *testing.T) {
	addr := reserveUDPAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	discovery := PeerLANDiscovery{ListenAddr: addr, BroadcastAddr: addr, MaxPackets: 2}
	peerA := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	peerB := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}}
	results := make(chan []domain.Peer, 1)
	errs := make(chan error, 1)
	go func() {
		peers, err := discovery.Peers(ctx)
		if err != nil {
			errs <- err
			return
		}
		results <- peers
	}()
	announceUntil(t, ctx, discovery, peerA, peerB, results, errs)

	watchAddr := reserveUDPAddr(t)
	watchCtx, watchCancel := context.WithTimeout(context.Background(), time.Second)
	defer watchCancel()
	watchDiscovery := PeerLANDiscovery{ListenAddr: watchAddr, BroadcastAddr: watchAddr}
	ch, err := watchDiscovery.WatchPeers(watchCtx)
	if err != nil {
		t.Fatalf("WatchPeers: %v", err)
	}
	peerC := domain.Peer{ID: "peer-c", Addresses: []string{"127.0.0.1:3"}}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case got, ok := <-ch:
			if !ok {
				t.Fatal("watch closed before peer arrived")
			}
			if got.ID != peerC.ID {
				t.Fatalf("watch peer = %+v", got)
			}
			watchCancel()
			if _, ok := <-ch; ok {
				t.Fatal("watch channel stayed open after cancel")
			}
			return
		case <-ticker.C:
			if err := watchDiscovery.Advertise(watchCtx, peerC); err != nil {
				t.Fatalf("watch Advertise: %v", err)
			}
		case <-watchCtx.Done():
			t.Fatal(watchCtx.Err())
		}
	}
}

func TestPeerLANDiscoveryFiltersByJoinToken(t *testing.T) {
	addr := reserveUDPAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	listener := PeerLANDiscovery{ListenAddr: addr, BroadcastAddr: addr, MaxPackets: 1, Token: "secret"}
	matching := PeerLANDiscovery{BroadcastAddr: addr, Token: "secret"}
	other := PeerLANDiscovery{BroadcastAddr: addr, Token: "other"}
	peerA := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	peerB := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}, Compute: true}
	results := make(chan []domain.Peer, 1)
	errs := make(chan error, 1)
	go func() {
		peers, err := listener.Peers(ctx)
		if err != nil {
			errs <- err
			return
		}
		results <- peers
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-errs:
			t.Fatalf("Peers: %v", err)
		case peers := <-results:
			if len(peers) != 1 || peers[0].ID != peerA.ID {
				t.Fatalf("peers = %+v", peers)
			}
			return
		case <-ticker.C:
			if err := other.Advertise(ctx, peerB); err != nil {
				t.Fatalf("Advertise other: %v", err)
			}
			if err := matching.Advertise(ctx, peerA); err != nil {
				t.Fatalf("Advertise matching: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestPeerLANDiscoveryFiltersWithPersistentTokenManager(t *testing.T) {
	addr := reserveUDPAddr(t)
	manager, err := NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	listener := PeerLANDiscovery{ListenAddr: addr, BroadcastAddr: addr, MaxPackets: 1, TokenManager: manager}
	matching := PeerLANDiscovery{BroadcastAddr: addr, TokenManager: manager}
	other := PeerLANDiscovery{BroadcastAddr: addr, Token: "other"}
	peerA := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	peerB := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}, Compute: true}
	results := make(chan []domain.Peer, 1)
	errs := make(chan error, 1)
	go func() {
		peers, err := listener.Peers(ctx)
		if err != nil {
			errs <- err
			return
		}
		results <- peers
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-errs:
			t.Fatalf("Peers: %v", err)
		case peers := <-results:
			if len(peers) != 1 || peers[0].ID != peerA.ID {
				t.Fatalf("peers = %+v", peers)
			}
			if err := manager.Revoke("secret"); err != nil {
				t.Fatalf("Revoke: %v", err)
			}
			if err := matching.Advertise(context.Background(), peerA); err == nil {
				t.Fatal("advertise with revoked current token succeeded")
			}
			return
		case <-ticker.C:
			if err := other.Advertise(ctx, peerB); err != nil {
				t.Fatalf("Advertise other: %v", err)
			}
			if err := matching.Advertise(ctx, peerA); err != nil {
				t.Fatalf("Advertise matching: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestPeerLANDiscoveryValidationAndTimeout(t *testing.T) {
	defaulted := NewPeerLANDiscovery("", "")
	if defaulted.ListenAddr != ":51850" || defaulted.BroadcastAddr != DefaultPeerDiscoveryAddr || defaulted.MaxPackets != 16 || defaulted.ScanDuration == 0 {
		t.Fatalf("defaulted = %+v", defaulted)
	}
	if err := (PeerLANDiscovery{BroadcastAddr: "127.0.0.1:1"}).Advertise(context.Background(), domain.Peer{}); err == nil || !strings.Contains(err.Error(), "peer id") {
		t.Fatalf("missing id err = %v", err)
	}
	if err := (PeerLANDiscovery{BroadcastAddr: "127.0.0.1:1"}).Advertise(context.Background(), domain.Peer{ID: "peer-a"}); err == nil || !strings.Contains(err.Error(), "reachable address") {
		t.Fatalf("missing address err = %v", err)
	}
	if err := (PeerLANDiscovery{BroadcastAddr: "%"}).Advertise(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}); err == nil {
		t.Fatal("bad broadcast address accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (PeerLANDiscovery{}).WatchPeers(canceled); err == nil {
		t.Fatal("canceled WatchPeers succeeded")
	}
	ctx, timeoutCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer timeoutCancel()
	peers, err := (PeerLANDiscovery{ListenAddr: "127.0.0.1:0", MaxPackets: 1}).Peers(ctx)
	if err != nil {
		t.Fatalf("Peers timeout: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("peers = %+v", peers)
	}
	if _, err := (PeerLANDiscovery{ListenAddr: "%"}).Peers(context.Background()); err == nil {
		t.Fatal("bad listen address accepted")
	}
	if _, err := (PeerLANDiscovery{ListenAddr: "%"}).WatchPeers(context.Background()); err == nil {
		t.Fatal("bad watch listen address accepted")
	}
	if peers, err := (PeerLANDiscovery{ListenAddr: "127.0.0.1:0", MaxPackets: 1, ScanDuration: time.Millisecond, Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))}).Peers(context.Background()); err != nil || len(peers) != 0 {
		t.Fatalf("Peers default deadline = %+v %v", peers, err)
	}
}

func TestPeerLANDiscoveryRejectsMalformedPackets(t *testing.T) {
	addr := reserveUDPAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	discovery := PeerLANDiscovery{ListenAddr: addr, MaxPackets: 1}
	errs := make(chan error, 1)
	go func() {
		_, err := discovery.Peers(ctx)
		errs <- err
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-errs:
			if err == nil || !strings.Contains(err.Error(), "peer id") {
				t.Fatalf("malformed err = %v", err)
			}
			return
		case <-ticker.C:
			conn, err := net.Dial("udp", addr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			_, _ = conn.Write([]byte(`{"addresses":["127.0.0.1:1"]}`))
			_ = conn.Close()
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func reserveUDPAddr(t *testing.T) string {
	t.Helper()
	holder, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	addr := holder.LocalAddr().String()
	if err := holder.Close(); err != nil {
		t.Fatalf("close reserve: %v", err)
	}
	return addr
}

func announceUntil(t *testing.T, ctx context.Context, discovery PeerLANDiscovery, peerA, peerB domain.Peer, results <-chan []domain.Peer, errs <-chan error) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-errs:
			t.Fatalf("Peers: %v", err)
		case peers := <-results:
			if len(peers) != 2 {
				t.Fatalf("peers = %+v", peers)
			}
			seen := map[string]bool{}
			for _, peer := range peers {
				seen[peer.ID] = true
			}
			if !seen[peerA.ID] || !seen[peerB.ID] {
				t.Fatalf("peers = %+v", peers)
			}
			return
		case <-ticker.C:
			if err := discovery.Advertise(ctx, peerA); err != nil {
				t.Fatalf("Advertise A: %v", err)
			}
			if err := discovery.Advertise(ctx, peerB); err != nil {
				t.Fatalf("Advertise B: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
