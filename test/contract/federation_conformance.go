package contract

import (
	"context"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

type fatalReporter interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

var conformanceWatchTimeout = time.Second

func RunJobRegistryConformance(t *testing.T, name string, newRegistry func() ports.JobRegistry, rec domain.JobRecord) {
	t.Run(name+"/put_snapshot_watch", func(t *testing.T) {
		registry := newRegistry()
		assert.NoError(t, "Put", registry.Put(context.Background(), rec))
		records, err := registry.Snapshot(context.Background())
		assert.NoError(t, "Snapshot", err)
		assert.True(t, len(records) == 1 && records[0].JobID == rec.JobID, "snapshot records = %+v", records)
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := registry.Watch(ctx, "")
		assert.NoError(t, "Watch", err)
		assert.True(t, ch != nil, "watch channel should not be nil")
		watchRec := rec
		watchRec.JobID = rec.JobID + "-watch"
		assert.NoError(t, "Put watch record", registry.Put(context.Background(), watchRec))
		got := receiveJobRecord(t, ch, watchRec.JobID)
		assert.Equal(t, watchRec.JobID, got.JobID, "watched job id")
		cancel()
		assertChannelClosed(t, ch)
	})

	t.Run(name+"/snapshot_keeps_current_fence_per_job", func(t *testing.T) {
		registry := newRegistry()
		assert.NoError(t, "Put original", registry.Put(context.Background(), rec))
		currentFence := rec
		currentFence.Status = domain.JobLoading
		currentFence.Fence = rec.Fence + 10
		currentFence.UpdatedAt = rec.UpdatedAt.Add(-time.Second)
		assert.NoError(t, "Put current fence", registry.Put(context.Background(), currentFence))
		staleClock := rec
		staleClock.Status = domain.JobRunning
		staleClock.Fence = rec.Fence + 1
		staleClock.UpdatedAt = rec.UpdatedAt.Add(time.Second)
		assert.NoError(t, "Put stale clock", registry.Put(context.Background(), staleClock))
		records, err := registry.Snapshot(context.Background())
		assert.NoError(t, "Snapshot", err)
		assert.True(t, len(records) == 1, "snapshot should contain one current record, got %+v", records)
		assert.Equal(t, domain.JobLoading, records[0].Status, "current status")
		assert.Equal(t, currentFence.Fence, records[0].Fence, "current fence")
	})

	t.Run(name+"/watch_cursor_replays_only_newer_records", func(t *testing.T) {
		registry := newRegistry()
		assert.NoError(t, "Put base", registry.Put(context.Background(), rec))
		newer := rec
		newer.JobID = rec.JobID + "-newer"
		newer.UpdatedAt = rec.UpdatedAt.Add(time.Second)
		assert.NoError(t, "Put newer", registry.Put(context.Background(), newer))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := registry.Watch(ctx, rec.UpdatedAt.UTC().Format(time.RFC3339Nano))
		assert.NoError(t, "Watch from cursor", err)
		got := receiveJobRecord(t, ch, newer.JobID)
		assert.Equal(t, newer.JobID, got.JobID, "cursor replay job id")
	})
}

func RunPeerDiscoveryConformance(t *testing.T, name string, newDiscovery func() ports.PeerDiscovery, peer domain.Peer) {
	t.Run(name+"/advertise_peers_watch", func(t *testing.T) {
		discovery := newDiscovery()
		assert.NoError(t, "Advertise", discovery.Advertise(context.Background(), peer))
		peers, err := discovery.Peers(context.Background())
		assert.NoError(t, "Peers", err)
		assert.True(t, len(peers) > 0 && peers[len(peers)-1].ID == peer.ID, "peers = %+v", peers)
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := discovery.WatchPeers(ctx)
		assert.NoError(t, "WatchPeers", err)
		assert.True(t, ch != nil, "peer watch channel should not be nil")
		watchPeer := peer
		watchPeer.ID = peer.ID + "-watch"
		assert.NoError(t, "Advertise watch peer", discovery.Advertise(context.Background(), watchPeer))
		got := receivePeer(t, ch, watchPeer.ID)
		assert.Equal(t, watchPeer.ID, got.ID, "watched peer id")
		cancel()
		assertChannelClosed(t, ch)
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

func receiveJobRecord(t fatalReporter, ch <-chan domain.JobRecord, want string) domain.JobRecord {
	t.Helper()
	deadline := time.After(conformanceWatchTimeout)
	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				t.Fatal("job watch channel closed before update")
			}
			if rec.JobID == want {
				return rec
			}
		case <-deadline:
			t.Fatalf("timed out waiting for job watch update %q", want)
			return domain.JobRecord{}
		}
	}
}

func receivePeer(t fatalReporter, ch <-chan domain.Peer, want string) domain.Peer {
	t.Helper()
	deadline := time.After(conformanceWatchTimeout)
	for {
		select {
		case peer, ok := <-ch:
			if !ok {
				t.Fatal("peer watch channel closed before update")
			}
			if peer.ID == want {
				return peer
			}
		case <-deadline:
			t.Fatalf("timed out waiting for peer watch update %q", want)
			return domain.Peer{}
		}
	}
}

func assertChannelClosed[T any](t fatalReporter, ch <-chan T) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("watch channel remained open after cancellation")
		}
	case <-time.After(conformanceWatchTimeout):
		t.Fatal("timed out waiting for watch channel to close")
	}
}
