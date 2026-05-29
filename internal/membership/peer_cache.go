package membership

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const defaultPeerCacheTTL = 30 * time.Second

type CachedPeerDiscovery struct {
	Upstream      ports.PeerDiscovery
	Clock         ports.Clock
	TTL           time.Duration
	WatchInterval time.Duration

	mu    sync.Mutex
	peers map[string]cachedPeer
}

type cachedPeer struct {
	peer   domain.Peer
	seenAt time.Time
}

func NewCachedPeerDiscovery(upstream ports.PeerDiscovery, clk ports.Clock, ttl time.Duration) *CachedPeerDiscovery {
	if clk == nil {
		clk = clock.System{}
	}
	return &CachedPeerDiscovery{Upstream: upstream, Clock: clk, TTL: ttl, peers: map[string]cachedPeer{}}
}

func (d *CachedPeerDiscovery) Start(ctx context.Context, interval time.Duration) error {
	if err := d.validate(); err != nil {
		return err
	}
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	d.WatchInterval = interval
	updates, err := d.Upstream.WatchPeers(ctx)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case peer, ok := <-updates:
				if !ok {
					if ctx.Err() == nil {
						log.Printf("mycelium peer discovery cache watcher stopped")
					}
					return
				}
				if err := d.remember([]domain.Peer{peer}); err != nil {
					log.Printf("mycelium peer discovery cache update failed: %v", err)
				}
			}
		}
	}()
	return nil
}

func (d *CachedPeerDiscovery) Refresh(ctx context.Context) error {
	if err := d.validate(); err != nil {
		return err
	}
	peers, err := d.Upstream.Peers(ctx)
	if err != nil {
		return err
	}
	return d.remember(peers)
}

func (d *CachedPeerDiscovery) Remember(peers ...domain.Peer) error {
	if err := d.validate(); err != nil {
		return err
	}
	return d.remember(peers)
}

func (d *CachedPeerDiscovery) remember(peers []domain.Peer) error {
	now := d.clock().Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.peers == nil {
		d.peers = map[string]cachedPeer{}
	}
	for _, peer := range peers {
		if peer.ID == "" {
			return fmt.Errorf("cached peer discovery received peer without id")
		}
		d.peers[peer.ID] = cachedPeer{peer: copyPeer(peer), seenAt: now}
	}
	d.pruneLocked(now)
	return nil
}

func (d *CachedPeerDiscovery) Advertise(ctx context.Context, self domain.Peer) error {
	if err := d.validate(); err != nil {
		return err
	}
	return d.Upstream.Advertise(ctx, self)
}

func (d *CachedPeerDiscovery) Peers(ctx context.Context) ([]domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.validate(); err != nil {
		return nil, err
	}
	now := d.clock().Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pruneLocked(now)
	out := make([]domain.Peer, 0, len(d.peers))
	for _, entry := range d.peers {
		out = append(out, copyPeer(entry.peer))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (d *CachedPeerDiscovery) WatchPeers(ctx context.Context) (<-chan domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.validate(); err != nil {
		return nil, err
	}
	interval := d.WatchInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	ch := make(chan domain.Peer, 16)
	go func() {
		defer close(ch)
		seen := map[string]time.Time{}
		for {
			peers, err := d.Peers(ctx)
			if err != nil {
				return
			}
			for _, peer := range peers {
				stamp := d.peerSeenAt(peer.ID)
				if !stamp.After(seen[peer.ID]) {
					continue
				}
				select {
				case ch <- peer:
					seen[peer.ID] = stamp
				case <-ctx.Done():
					return
				}
			}
			timer := d.clock().NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
		}
	}()
	return ch, nil
}

func (d *CachedPeerDiscovery) peerSeenAt(id string) time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.peers[id].seenAt
}

func (d *CachedPeerDiscovery) pruneLocked(now time.Time) {
	ttl := d.TTL
	if ttl <= 0 {
		ttl = defaultPeerCacheTTL
	}
	for id, entry := range d.peers {
		if now.Sub(entry.seenAt) > ttl {
			delete(d.peers, id)
		}
	}
}

func (d *CachedPeerDiscovery) validate() error {
	if d == nil || d.Upstream == nil {
		return fmt.Errorf("cached peer discovery is not configured")
	}
	return nil
}

func (d *CachedPeerDiscovery) clock() ports.Clock {
	if d.Clock == nil {
		return clock.System{}
	}
	return d.Clock
}

func copyPeer(peer domain.Peer) domain.Peer {
	peer.Addresses = append([]string(nil), peer.Addresses...)
	return peer
}

var _ ports.PeerDiscovery = (*CachedPeerDiscovery)(nil)
