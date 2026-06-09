package peer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestHeartbeatAdvertisesProbesAndMarksDead(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(200, 0).UTC())
	remote := fixtures.MakePeer(fixtures.WithPeerID("peer-b"))
	discovery := &mocks.PeerDiscovery{PeersVal: []domain.Peer{remote}}
	probes := 0
	dead := []domain.Peer{}
	heartbeat := &Heartbeat{
		Self:      fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		Discovery: discovery,
		Clock:     clock,
		MaxMisses: 2,
		Probe: func(context.Context, domain.Peer) error {
			probes++
			return domain.ErrUnreachable
		},
		OnDead: func(_ context.Context, peer domain.Peer) error {
			dead = append(dead, peer)
			return nil
		},
	}

	first, err := heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("first Tick: %v", err)
	}
	if len(first) != 0 || len(dead) != 0 {
		t.Fatalf("first dead=%+v callback=%+v", first, dead)
	}
	clock.Advance(time.Second)
	second, err := heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("second Tick: %v", err)
	}
	if len(second) != 1 || second[0].ID != remote.ID || len(dead) != 1 || probes != 2 {
		t.Fatalf("second=%+v dead=%+v probes=%d", second, dead, probes)
	}
	third, err := heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("third Tick: %v", err)
	}
	if len(third) != 0 || len(dead) != 1 {
		t.Fatalf("dead repeated: third=%+v dead=%+v", third, dead)
	}
	lastAdvertised := discovery.PeersVal[len(discovery.PeersVal)-1]
	if lastAdvertised.ID != heartbeat.Self.ID || !lastAdvertised.LastSeen.Equal(clock.Now()) {
		t.Fatalf("advertised peers = %+v", discovery.PeersVal)
	}
}

func TestHeartbeatResetsMissesOnSuccessfulProbeAndSkipsSelf(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(210, 0).UTC())
	self := fixtures.MakePeer(fixtures.WithPeerID("peer-a"))
	remote := fixtures.MakePeer(fixtures.WithPeerID("peer-b"))
	heartbeat := &Heartbeat{
		Self:      self,
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{self, remote}},
		Clock:     clock,
		MaxMisses: 2,
		Probe: func(context.Context, domain.Peer) error {
			return nil
		},
	}

	dead, err := heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(dead) != 0 || len(heartbeat.known) != 1 || heartbeat.known[remote.ID].misses != 0 {
		t.Fatalf("dead=%+v known=%+v", dead, heartbeat.known)
	}
}

func TestHeartbeatCountsMissingKnownPeers(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(220, 0).UTC())
	remote := fixtures.MakePeer(fixtures.WithPeerID("peer-b"))
	discovery := &mocks.PeerDiscovery{PeersVal: []domain.Peer{remote}}
	heartbeat := &Heartbeat{
		Self:      fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		Discovery: discovery,
		Clock:     clock,
		MaxMisses: 2,
	}
	if _, err := heartbeat.Tick(ctx); err != nil {
		t.Fatalf("first Tick: %v", err)
	}
	discovery.PeersVal = nil
	dead, err := heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("second Tick: %v", err)
	}
	if len(dead) != 0 || heartbeat.known[remote.ID].misses != 1 {
		t.Fatalf("second dead=%+v known=%+v", dead, heartbeat.known)
	}
	dead, err = heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("third Tick: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != remote.ID {
		t.Fatalf("third dead=%+v", dead)
	}
}

func TestHeartbeatProbesMissingKnownPeerBeforeCountingMiss(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(225, 0).UTC())
	remote := fixtures.MakePeer(fixtures.WithPeerID("peer-b"))
	discovery := &mocks.PeerDiscovery{PeersVal: []domain.Peer{remote}}
	probes := 0
	heartbeat := &Heartbeat{
		Self:      fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		Discovery: discovery,
		Clock:     clock,
		MaxMisses: 1,
		Probe: func(context.Context, domain.Peer) error {
			probes++
			return nil
		},
	}
	if _, err := heartbeat.Tick(ctx); err != nil {
		t.Fatalf("first Tick: %v", err)
	}
	discovery.PeersVal = nil
	dead, err := heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("second Tick: %v", err)
	}
	if len(dead) != 0 || heartbeat.known[remote.ID].misses != 0 || probes != 2 {
		t.Fatalf("second dead=%+v known=%+v probes=%d", dead, heartbeat.known, probes)
	}
	heartbeat.Probe = func(context.Context, domain.Peer) error {
		probes++
		return domain.ErrUnreachable
	}
	dead, err = heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("third Tick: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != remote.ID || probes != 3 {
		t.Fatalf("third dead=%+v probes=%d", dead, probes)
	}
}

func TestHeartbeatErrorPaths(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(230, 0).UTC())
	self := fixtures.MakePeer(fixtures.WithPeerID("peer-a"))
	remote := fixtures.MakePeer(fixtures.WithPeerID("peer-b"))
	boom := errors.New("boom")

	if _, err := (&Heartbeat{}).Tick(ctx); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("unconfigured err = %v", err)
	}
	heartbeat := &Heartbeat{Self: self, Discovery: &mocks.PeerDiscovery{}, Clock: clock, MaxMisses: -1}
	if _, err := heartbeat.Tick(ctx); err == nil || !strings.Contains(err.Error(), "max misses") {
		t.Fatalf("negative max err = %v", err)
	}
	heartbeat = &Heartbeat{Self: self, Discovery: &mocks.PeerDiscovery{Err: boom}, Clock: clock}
	if _, err := heartbeat.Tick(ctx); !errors.Is(err, boom) {
		t.Fatalf("advertise err = %v", err)
	}
	peersErr := &peerDiscoverySplit{advertiseErr: nil, peersErr: boom}
	heartbeat = &Heartbeat{Self: self, Discovery: peersErr, Clock: clock}
	if _, err := heartbeat.Tick(ctx); !errors.Is(err, boom) {
		t.Fatalf("peers err = %v", err)
	}
	heartbeat = &Heartbeat{Self: self, Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{Addresses: []string{"127.0.0.1:1"}}}}, Clock: clock}
	if _, err := heartbeat.Tick(ctx); err == nil || !strings.Contains(err.Error(), "missing id") {
		t.Fatalf("missing id err = %v", err)
	}
	heartbeat = &Heartbeat{Self: self, Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}}, Clock: clock}
	if _, err := heartbeat.Tick(ctx); err == nil || !strings.Contains(err.Error(), "reachable address") {
		t.Fatalf("missing address err = %v", err)
	}
	heartbeat = &Heartbeat{
		Self:      self,
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{remote}},
		Clock:     clock,
		MaxMisses: 1,
		Probe: func(context.Context, domain.Peer) error {
			return domain.ErrUnreachable
		},
		OnDead: func(context.Context, domain.Peer) error {
			return boom
		},
	}
	if _, err := heartbeat.Tick(ctx); !errors.Is(err, boom) {
		t.Fatalf("on dead err = %v", err)
	}
	if heartbeat.known[remote.ID].dead {
		t.Fatalf("peer marked dead after failed OnDead: %+v", heartbeat.known[remote.ID])
	}
	recovered := 0
	heartbeat.OnDead = func(context.Context, domain.Peer) error {
		recovered++
		return nil
	}
	if dead, err := heartbeat.Tick(ctx); err != nil || len(dead) != 1 || recovered != 1 || !heartbeat.known[remote.ID].dead {
		t.Fatalf("retry dead=%+v recovered=%d known=%+v err=%v", dead, recovered, heartbeat.known, err)
	}
	heartbeat = &Heartbeat{
		Self:      self,
		Discovery: &mocks.PeerDiscovery{},
		Clock:     clock,
		MaxMisses: 1,
		OnDead: func(context.Context, domain.Peer) error {
			return boom
		},
		known: map[string]peerBeat{
			remote.ID: {peer: remote},
		},
	}
	if _, err := heartbeat.Tick(ctx); !errors.Is(err, boom) {
		t.Fatalf("missing known on dead err = %v", err)
	}
}

type peerDiscoverySplit struct {
	advertiseErr error
	peersErr     error
}

func (d *peerDiscoverySplit) Advertise(context.Context, domain.Peer) error {
	return d.advertiseErr
}

func (d *peerDiscoverySplit) Peers(context.Context) ([]domain.Peer, error) {
	return nil, d.peersErr
}

func (d *peerDiscoverySplit) WatchPeers(context.Context) (<-chan domain.Peer, error) {
	return nil, nil
}
