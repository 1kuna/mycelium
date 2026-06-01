package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunJobRegistryConformance(t *testing.T, name string, newRegistry func() ports.JobRegistry, rec domain.JobRecord) {
	t.Run(name+"/put_snapshot_watch", func(t *testing.T) {
		registry := newRegistry()
		assert.NoError(t, "Put", registry.Put(context.Background(), rec))
		records, err := registry.Snapshot(context.Background())
		assert.NoError(t, "Snapshot", err)
		assert.True(t, len(records) == 1 && records[0].JobID == rec.JobID, "snapshot records = %+v", records)
		ch, err := registry.Watch(context.Background(), "")
		assert.NoError(t, "Watch", err)
		assert.True(t, ch != nil, "watch channel should not be nil")
	})
}

func RunPeerDiscoveryConformance(t *testing.T, name string, newDiscovery func() ports.PeerDiscovery, peer domain.Peer) {
	t.Run(name+"/advertise_peers_watch", func(t *testing.T) {
		discovery := newDiscovery()
		assert.NoError(t, "Advertise", discovery.Advertise(context.Background(), peer))
		peers, err := discovery.Peers(context.Background())
		assert.NoError(t, "Peers", err)
		assert.True(t, len(peers) > 0 && peers[len(peers)-1].ID == peer.ID, "peers = %+v", peers)
		ch, err := discovery.WatchPeers(context.Background())
		assert.NoError(t, "WatchPeers", err)
		assert.True(t, ch != nil, "peer watch channel should not be nil")
	})
}

func RunTunnelConformance(t *testing.T, name string, newTunnel func() ports.Tunnel, node domain.Node) {
	t.Run(name+"/open_close", func(t *testing.T) {
		tunnel := newTunnel()
		addr, err := tunnel.Open(context.Background(), node)
		assert.NoError(t, "Open", err)
		assert.True(t, addr != "", "tunnel address should not be empty")
		assert.NoError(t, "Close", tunnel.Close(context.Background(), node.ID))
	})
}
