package peer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/mocks"
)

func TestRegistryReplicatorSyncOncePullsMergesAndPushes(t *testing.T) {
	ctx := context.Background()
	local := NewJobRegistry()
	localRec := registryRecord("local", "peer-a", domain.JobRunning, time.Unix(10, 0).UTC())
	if err := local.Put(ctx, localRec); err != nil {
		t.Fatalf("Put local: %v", err)
	}
	remoteRec := registryRecord("remote", "peer-b", domain.JobQueued, time.Unix(11, 0).UTC())
	client := &recordingRegistryClient{snapshots: map[string][]domain.JobRecord{"peer-b": {remoteRec}}}
	discovery := &mocks.PeerDiscovery{PeersVal: []domain.Peer{
		{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}},
		{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}},
	}}
	replicator := RegistryReplicator{Local: local, Peers: discovery, Client: client, SelfID: "peer-a"}

	if err := replicator.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	got, err := local.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(got) != 2 || got[0].JobID != "local" || got[1].JobID != "remote" {
		t.Fatalf("local registry = %+v", got)
	}
	pushed := client.pushes["peer-b"]
	if len(pushed) != 1 || len(pushed[0]) != 2 {
		t.Fatalf("pushes = %+v", pushed)
	}
}

func TestRegistryReplicatorPushRecordSkipsSelf(t *testing.T) {
	ctx := context.Background()
	client := &recordingRegistryClient{}
	discovery := &mocks.PeerDiscovery{PeersVal: []domain.Peer{
		{ID: "peer-a"},
		{ID: "peer-b"},
	}}
	replicator := RegistryReplicator{Local: NewJobRegistry(), Peers: discovery, Client: client, SelfID: "peer-a"}

	if err := replicator.PushRecord(ctx, registryRecord("job-a", "peer-a", domain.JobRunning, time.Unix(12, 0).UTC())); err != nil {
		t.Fatalf("PushRecord: %v", err)
	}
	if _, ok := client.pushes["peer-a"]; ok {
		t.Fatalf("pushed to self: %+v", client.pushes)
	}
	if pushed := client.pushes["peer-b"]; len(pushed) != 1 || pushed[0][0].JobID != "job-a" {
		t.Fatalf("pushes = %+v", client.pushes)
	}
}

func TestRegistryReplicatorRedactsPrivatePayloads(t *testing.T) {
	ctx := context.Background()
	local := NewJobRegistry()
	private := registryRecord("private", "peer-a", domain.JobRunning, time.Unix(12, 0).UTC())
	private.Handling = domain.HandlingPrivate
	if err := local.Put(ctx, private); err != nil {
		t.Fatalf("Put private: %v", err)
	}
	client := &recordingRegistryClient{}
	discovery := &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}}
	replicator := RegistryReplicator{Local: local, Peers: discovery, Client: client, SelfID: "peer-a"}

	if err := replicator.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	pushed := client.pushes["peer-b"]
	if len(pushed) != 1 || len(pushed[0]) != 1 {
		t.Fatalf("pushes = %+v", pushed)
	}
	got := pushed[0][0]
	if got.JobID != private.JobID || len(got.Request) != 0 || !got.PayloadRedacted || got.Handling != domain.HandlingPrivate {
		t.Fatalf("pushed private record = %+v", got)
	}
	if err := NewJobRegistry().Put(ctx, got); err != nil {
		t.Fatalf("redacted record should remain registry-valid: %v", err)
	}
}

func TestRegistryReplicatorReportsPeerFailuresAfterProgress(t *testing.T) {
	ctx := context.Background()
	local := NewJobRegistry()
	client := &recordingRegistryClient{
		snapshots:   map[string][]domain.JobRecord{"peer-ok": {registryRecord("remote", "peer-ok", domain.JobRunning, time.Unix(13, 0).UTC())}},
		snapshotErr: map[string]error{"peer-pull-bad": errors.New("pull bad")},
		pushErr:     map[string]error{"peer-push-bad": errors.New("push bad")},
	}
	discovery := &mocks.PeerDiscovery{PeersVal: []domain.Peer{
		{ID: "peer-pull-bad"},
		{ID: "peer-ok"},
		{ID: "peer-push-bad"},
	}}
	replicator := RegistryReplicator{Local: local, Peers: discovery, Client: client, SelfID: "peer-a"}

	err := replicator.SyncOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "pull bad") || !strings.Contains(err.Error(), "push bad") {
		t.Fatalf("SyncOnce err = %v", err)
	}
	got, snapErr := local.Snapshot(ctx)
	if snapErr != nil {
		t.Fatalf("Snapshot: %v", snapErr)
	}
	if len(got) != 1 || got[0].JobID != "remote" {
		t.Fatalf("progress not preserved: %+v", got)
	}

	err = replicator.PushRecord(ctx, registryRecord("job-a", "peer-a", domain.JobRunning, time.Unix(14, 0).UTC()))
	if err == nil || !strings.Contains(err.Error(), "push bad") {
		t.Fatalf("PushRecord err = %v", err)
	}
}

func TestRegistryReplicatorErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	rec := registryRecord("job-a", "peer-a", domain.JobRunning, time.Unix(15, 0).UTC())

	if err := (RegistryReplicator{}).SyncOnce(ctx); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("invalid SyncOnce err = %v", err)
	}
	if err := (RegistryReplicator{}).PushRecord(ctx, rec); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("invalid PushRecord err = %v", err)
	}

	peerErr := RegistryReplicator{Local: NewJobRegistry(), Peers: &mocks.PeerDiscovery{Err: boom}, Client: &recordingRegistryClient{}, SelfID: "peer-a"}
	if err := peerErr.SyncOnce(ctx); !errors.Is(err, boom) {
		t.Fatalf("peer SyncOnce err = %v", err)
	}
	if err := peerErr.PushRecord(ctx, rec); !errors.Is(err, boom) {
		t.Fatalf("peer PushRecord err = %v", err)
	}

	putErr := RegistryReplicator{
		Local:  &failingRegistry{err: boom},
		Peers:  &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}},
		Client: &recordingRegistryClient{snapshots: map[string][]domain.JobRecord{"peer-b": {rec}}},
		SelfID: "peer-a",
	}
	if err := putErr.SyncOnce(ctx); !errors.Is(err, boom) {
		t.Fatalf("put SyncOnce err = %v", err)
	}

	snapshotErr := RegistryReplicator{
		Local:  &snapshotOnlyFailingRegistry{err: boom},
		Peers:  &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}},
		Client: &recordingRegistryClient{snapshots: map[string][]domain.JobRecord{"peer-b": {rec}}},
		SelfID: "peer-a",
	}
	if err := snapshotErr.SyncOnce(ctx); !errors.Is(err, boom) {
		t.Fatalf("snapshot SyncOnce err = %v", err)
	}
}

func registryRecord(id, coordinator string, status domain.JobStatus, updated time.Time) domain.JobRecord {
	return domain.JobRecord{
		JobID:       id,
		Coordinator: coordinator,
		Status:      status,
		Request:     []byte(`{"job":"` + id + `"}`),
		UpdatedAt:   updated.UTC(),
	}
}

type recordingRegistryClient struct {
	snapshots   map[string][]domain.JobRecord
	snapshotErr map[string]error
	pushErr     map[string]error
	pushes      map[string][][]domain.JobRecord
}

func (c *recordingRegistryClient) Snapshot(_ context.Context, peer domain.Peer) ([]domain.JobRecord, error) {
	if err := c.snapshotErr[peer.ID]; err != nil {
		return nil, err
	}
	return append([]domain.JobRecord(nil), c.snapshots[peer.ID]...), nil
}

func (c *recordingRegistryClient) Push(_ context.Context, peer domain.Peer, records []domain.JobRecord) error {
	if c.pushes == nil {
		c.pushes = map[string][][]domain.JobRecord{}
	}
	cloned := append([]domain.JobRecord(nil), records...)
	c.pushes[peer.ID] = append(c.pushes[peer.ID], cloned)
	return c.pushErr[peer.ID]
}

type snapshotOnlyFailingRegistry struct {
	err error
}

func (r *snapshotOnlyFailingRegistry) Put(context.Context, domain.JobRecord) error {
	return nil
}

func (r *snapshotOnlyFailingRegistry) Watch(context.Context, string) (<-chan domain.JobRecord, error) {
	ch := make(chan domain.JobRecord)
	close(ch)
	return ch, nil
}

func (r *snapshotOnlyFailingRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	return nil, r.err
}
