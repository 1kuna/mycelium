package peer

import (
	"context"
	"fmt"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const (
	DefaultHeartbeatInterval  = 5 * time.Second
	DefaultHeartbeatMaxMisses = 3
)

type ProbeFunc func(ctx context.Context, peer domain.Peer) error

type DeadPeerFunc func(ctx context.Context, peer domain.Peer) error

type Heartbeat struct {
	Self      domain.Peer
	Discovery ports.PeerDiscovery
	Clock     ports.Clock
	Interval  time.Duration
	MaxMisses int
	Probe     ProbeFunc
	OnDead    DeadPeerFunc

	known map[string]peerBeat
}

type peerBeat struct {
	peer   domain.Peer
	misses int
	dead   bool
}

func (h *Heartbeat) Tick(ctx context.Context) ([]domain.Peer, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}
	self := h.Self
	self.LastSeen = h.Clock.Now()
	if err := h.Discovery.Advertise(ctx, self); err != nil {
		return nil, err
	}
	peers, err := h.Discovery.Peers(ctx)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var newlyDead []domain.Peer
	for _, found := range peers {
		if found.ID == h.Self.ID {
			continue
		}
		if err := validateHeartbeatPeer(found); err != nil {
			return nil, err
		}
		seen[found.ID] = true
		if h.probe(ctx, found) == nil {
			h.known[found.ID] = peerBeat{peer: found}
			continue
		}
		dead, err := h.miss(ctx, found)
		if err != nil {
			return nil, err
		}
		if dead.ID != "" {
			newlyDead = append(newlyDead, dead)
		}
	}
	for id, beat := range h.known {
		if !seen[id] && !beat.dead {
			dead, err := h.miss(ctx, beat.peer)
			if err != nil {
				return nil, err
			}
			if dead.ID != "" {
				newlyDead = append(newlyDead, dead)
			}
		}
	}
	return newlyDead, nil
}

func (h *Heartbeat) validate() error {
	if h.Self.ID == "" || h.Discovery == nil || h.Clock == nil {
		return fmt.Errorf("peer heartbeat is not fully configured")
	}
	if h.Interval == 0 {
		h.Interval = DefaultHeartbeatInterval
	}
	if h.MaxMisses == 0 {
		h.MaxMisses = DefaultHeartbeatMaxMisses
	}
	if h.MaxMisses < 0 {
		return fmt.Errorf("heartbeat max misses must be non-negative")
	}
	if h.known == nil {
		h.known = map[string]peerBeat{}
	}
	return nil
}

func (h *Heartbeat) probe(ctx context.Context, peer domain.Peer) error {
	if h.Probe == nil {
		return nil
	}
	return h.Probe(ctx, peer)
}

func (h *Heartbeat) miss(ctx context.Context, peer domain.Peer) (domain.Peer, error) {
	beat := h.known[peer.ID]
	beat.peer = peer
	if beat.dead {
		h.known[peer.ID] = beat
		return domain.Peer{}, nil
	}
	beat.misses++
	if beat.misses >= h.MaxMisses {
		beat.dead = true
		h.known[peer.ID] = beat
		if h.OnDead != nil {
			if err := h.OnDead(ctx, peer); err != nil {
				return domain.Peer{}, err
			}
		}
		return peer, nil
	}
	h.known[peer.ID] = beat
	return domain.Peer{}, nil
}

func validateHeartbeatPeer(peer domain.Peer) error {
	if peer.ID == "" {
		return fmt.Errorf("discovered peer is missing id")
	}
	if len(peer.Addresses) == 0 {
		return fmt.Errorf("discovered peer %q has no reachable address", peer.ID)
	}
	return nil
}
