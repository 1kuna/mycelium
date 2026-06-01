package membership

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type OverlayBackend interface {
	Advertise(ctx context.Context, self domain.Peer) error
	Peers(ctx context.Context) ([]domain.Peer, error)
	WatchPeers(ctx context.Context) (<-chan domain.Peer, error)
	Open(ctx context.Context, node domain.Node) (string, error)
	Close(ctx context.Context, nodeID string) error
}

type OverlayDiscovery struct {
	Backend OverlayBackend
}

func NewOverlayDiscovery(backend OverlayBackend) OverlayDiscovery {
	return OverlayDiscovery{Backend: backend}
}

func (d OverlayDiscovery) Advertise(ctx context.Context, self domain.Peer) error {
	if d.Backend == nil {
		return fmt.Errorf("overlay discovery backend is not configured")
	}
	return d.Backend.Advertise(ctx, self)
}

func (d OverlayDiscovery) Peers(ctx context.Context) ([]domain.Peer, error) {
	if d.Backend == nil {
		return nil, fmt.Errorf("overlay discovery backend is not configured")
	}
	return d.Backend.Peers(ctx)
}

func (d OverlayDiscovery) WatchPeers(ctx context.Context) (<-chan domain.Peer, error) {
	if d.Backend == nil {
		return nil, fmt.Errorf("overlay discovery backend is not configured")
	}
	return d.Backend.WatchPeers(ctx)
}

type OverlayTunnel struct {
	Backend OverlayBackend
}

func NewOverlayTunnel(backend OverlayBackend) OverlayTunnel {
	return OverlayTunnel{Backend: backend}
}

func (t OverlayTunnel) Open(ctx context.Context, node domain.Node) (string, error) {
	if t.Backend == nil {
		return "", fmt.Errorf("overlay tunnel backend is not configured")
	}
	return t.Backend.Open(ctx, node)
}

func (t OverlayTunnel) Close(ctx context.Context, nodeID string) error {
	if t.Backend == nil {
		return fmt.Errorf("overlay tunnel backend is not configured")
	}
	return t.Backend.Close(ctx, nodeID)
}

type MemoryOverlayBackend struct {
	Tunnel ports.Tunnel

	mu       sync.Mutex
	peers    map[string]domain.Peer
	watchers map[int]chan domain.Peer
	next     int
}

func NewMemoryOverlayBackend(tunnel ports.Tunnel) *MemoryOverlayBackend {
	return &MemoryOverlayBackend{
		Tunnel:   tunnel,
		peers:    map[string]domain.Peer{},
		watchers: map[int]chan domain.Peer{},
	}
}

func (b *MemoryOverlayBackend) Advertise(ctx context.Context, self domain.Peer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if self.ID == "" {
		return fmt.Errorf("overlay peer id is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	peer := clonePeer(self)
	b.peers[peer.ID] = peer
	for _, watcher := range b.watchers {
		select {
		case watcher <- clonePeer(peer):
		default:
		}
	}
	return nil
}

func (b *MemoryOverlayBackend) Peers(ctx context.Context) ([]domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]domain.Peer, 0, len(b.peers))
	for _, peer := range b.peers {
		out = append(out, clonePeer(peer))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (b *MemoryOverlayBackend) WatchPeers(ctx context.Context) (<-chan domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.next
	b.next++
	ch := make(chan domain.Peer, len(b.peers)+16)
	for _, peer := range b.peers {
		ch <- clonePeer(peer)
	}
	b.watchers[id] = ch
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		if watcher, ok := b.watchers[id]; ok {
			delete(b.watchers, id)
			close(watcher)
		}
		b.mu.Unlock()
	}()
	return ch, nil
}

func (b *MemoryOverlayBackend) Open(ctx context.Context, node domain.Node) (string, error) {
	if b.Tunnel == nil {
		return "", fmt.Errorf("overlay tunnel backend is not configured")
	}
	return b.Tunnel.Open(ctx, node)
}

func (b *MemoryOverlayBackend) Close(ctx context.Context, nodeID string) error {
	if b.Tunnel == nil {
		return fmt.Errorf("overlay tunnel backend is not configured")
	}
	return b.Tunnel.Close(ctx, nodeID)
}

func clonePeer(peer domain.Peer) domain.Peer {
	peer.Addresses = append([]string(nil), peer.Addresses...)
	return peer
}

var _ ports.PeerDiscovery = OverlayDiscovery{}
var _ ports.Tunnel = OverlayTunnel{}
var _ OverlayBackend = (*MemoryOverlayBackend)(nil)
