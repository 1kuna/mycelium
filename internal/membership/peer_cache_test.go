package membership

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/mocks"
)

func TestCachedPeerDiscoveryKeepsLastGoodScanUntilTTL(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(100, 0).UTC())
	upstream := &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}}}
	cache := NewCachedPeerDiscovery(upstream, clk, time.Second)

	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	upstream.PeersVal = nil
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh empty: %v", err)
	}
	peers, err := cache.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != "peer-a" {
		t.Fatalf("cached peers = %+v", peers)
	}

	clk.Advance(time.Second + time.Nanosecond)
	peers, err = cache.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers expired: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("expired peers = %+v", peers)
	}
}

func TestCachedPeerDiscoveryRefreshUpdatesAndSorts(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(200, 0).UTC())
	upstream := &mocks.PeerDiscovery{PeersVal: []domain.Peer{
		{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}},
		{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}},
	}}
	cache := NewCachedPeerDiscovery(upstream, clk, time.Minute)

	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	peers, err := cache.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 2 || peers[0].ID != "peer-a" || peers[1].ID != "peer-b" {
		t.Fatalf("peers = %+v", peers)
	}
	peers[0].Addresses[0] = "mutated"
	again, err := cache.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers again: %v", err)
	}
	if again[0].Addresses[0] == "mutated" {
		t.Fatal("Peers returned mutable cached address slice")
	}
}

func TestCachedPeerDiscoveryRememberSeedsPeer(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(250, 0).UTC())
	upstream := &mocks.PeerDiscovery{}
	cache := NewCachedPeerDiscovery(upstream, clk, time.Minute)

	peer := domain.Peer{ID: "seed", Addresses: []string{"127.0.0.1:2"}, Compute: true}
	if err := cache.Remember(peer); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	peer.Addresses[0] = "mutated"

	peers, err := cache.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != "seed" || peers[0].Addresses[0] != "127.0.0.1:2" {
		t.Fatalf("remembered peers = %+v", peers)
	}
	if err := cache.Remember(domain.Peer{}); err == nil {
		t.Fatal("peer without id accepted")
	}
}

func TestCachedPeerDiscoveryDelegatesAdvertiseAndErrors(t *testing.T) {
	boom := errors.New("boom")
	upstream := &mocks.PeerDiscovery{Err: boom}
	cache := NewCachedPeerDiscovery(upstream, mocks.NewFakeClock(time.Unix(300, 0).UTC()), time.Minute)
	peer := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}

	if err := cache.Advertise(context.Background(), peer); !errors.Is(err, boom) {
		t.Fatalf("Advertise err = %v", err)
	}
	if err := cache.Refresh(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Refresh err = %v", err)
	}
	if _, err := (*CachedPeerDiscovery)(nil).Peers(context.Background()); err == nil {
		t.Fatal("nil cache Peers succeeded")
	}
}

func TestCachedPeerDiscoveryWatchPeersEmitsFreshEntries(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(400, 0).UTC())
	upstream := &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}}}
	cache := NewCachedPeerDiscovery(upstream, clk, time.Minute)
	cache.WatchInterval = time.Second
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := cache.WatchPeers(ctx)
	if err != nil {
		t.Fatalf("WatchPeers: %v", err)
	}
	got := <-ch
	if got.ID != "peer-a" {
		t.Fatalf("watch peer = %+v", got)
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("watch channel stayed open after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("watch channel did not close after cancel")
	}
}

func TestCachedPeerDiscoveryStartConsumesUpstreamWatch(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(500, 0).UTC())
	watch := make(chan domain.Peer, 1)
	upstream := &mocks.PeerDiscovery{WatchCh: watch}
	cache := NewCachedPeerDiscovery(upstream, clk, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cache.Start(ctx, time.Second); err != nil {
		t.Fatalf("Start: %v", err)
	}
	watch <- domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}
	for i := 0; i < 1000; i++ {
		peers, err := cache.Peers(context.Background())
		if err != nil {
			t.Fatalf("Peers: %v", err)
		}
		if len(peers) == 1 && peers[0].ID == "peer-a" {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("watched peer was not cached")
}

func TestCachedPeerDiscoveryDefaultAndContextPaths(t *testing.T) {
	upstream := &mocks.PeerDiscovery{}
	cache := NewCachedPeerDiscovery(upstream, nil, 0)
	peer := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}
	if err := cache.Remember(peer); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if err := cache.Advertise(context.Background(), peer); err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	if len(upstream.PeersVal) != 1 || upstream.PeersVal[0].ID != peer.ID {
		t.Fatalf("upstream peers = %+v", upstream.PeersVal)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cache.Peers(canceled); err != context.Canceled {
		t.Fatalf("Peers canceled err = %v", err)
	}
	if _, err := cache.WatchPeers(canceled); err != context.Canceled {
		t.Fatalf("WatchPeers canceled err = %v", err)
	}
	if err := (*CachedPeerDiscovery)(nil).Remember(peer); err == nil {
		t.Fatal("nil Remember succeeded")
	}
}
