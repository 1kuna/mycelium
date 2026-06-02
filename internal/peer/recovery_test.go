package peer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestRecoveryRescuesDeadPeerUnfinishedJobsAfterOwnerCheck(t *testing.T) {
	ctx := context.Background()
	registry := NewJobRegistry()
	records := []domain.JobRecord{
		recoveryRecord("queued", "dead-peer", "", domain.JobQueued),
		recoveryRecord("placing", "dead-peer", "", domain.JobPlacing),
		recoveryRecord("owner-dead", "dead-peer", "dead-peer", domain.JobRunning),
		recoveryRecord("assigned-dead", "live-peer", "dead-peer", domain.JobRunning),
		recoveryRecord("assigned-done", "live-peer", "dead-peer", domain.JobDone),
		recoveryRecord("owner-live", "dead-peer", "node-live", domain.JobRunning),
		recoveryRecord("owner-finished", "dead-peer", "node-finished", domain.JobRunning),
		recoveryRecord("owner-unreachable", "dead-peer", "node-unreachable", domain.JobRunning),
		recoveryRecord("owner-missing", "dead-peer", "node-missing", domain.JobRunning),
		recordWith(recoveryRecord("redacted-private", "dead-peer", "", domain.JobQueued), func(r *domain.JobRecord) {
			r.Request = nil
			r.Handling = domain.HandlingPrivate
			r.PayloadRedacted = true
		}),
		recoveryRecord("done", "dead-peer", "", domain.JobDone),
		recoveryRecord("other-peer", "other-peer", "", domain.JobQueued),
	}
	if err := registry.Merge(ctx, records); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	var rescued []string
	recovery := Recovery{
		Registry: registry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-live":        staticLeaseInspector{lease: domain.Lease{ID: "lease-live"}, found: true},
			"node-finished":    staticLeaseInspector{},
			"node-unreachable": staticLeaseInspector{err: domain.ErrUnreachable},
		}},
		Clock: mocks.NewFakeClock(time.Unix(300, 0).UTC()),
		Rescue: func(_ context.Context, rec domain.JobRecord) error {
			rescued = append(rescued, rec.JobID)
			return nil
		},
	}

	count, err := recovery.RecoverPeer(ctx, "dead-peer")
	if err != nil {
		t.Fatalf("RecoverPeer: %v", err)
	}
	want := []string{"queued", "placing", "owner-dead", "assigned-dead", "owner-finished"}
	if count != len(want) || !reflect.DeepEqual(rescued, want) {
		t.Fatalf("rescued count=%d ids=%+v", count, rescued)
	}
	snap, err := registry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	notes := map[string]string{}
	for _, rec := range snap {
		notes[rec.JobID] = rec.RecoveryNote
	}
	for _, id := range []string{"owner-unreachable", "owner-missing"} {
		if !strings.Contains(notes[id], "partition") {
			t.Fatalf("missing partition note for %s: %q", id, notes[id])
		}
	}
}

func TestRecoveryErrorPaths(t *testing.T) {
	ctx := context.Background()
	rec := recoveryRecord("job-a", "dead-peer", "node-a", domain.JobRunning)
	registry := NewJobRegistry()
	if err := registry.Put(ctx, rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := (Recovery{}).RecoverPeer(ctx, "dead-peer"); err == nil {
		t.Fatal("unconfigured recovery succeeded")
	}
	if _, err := (Recovery{Registry: registry, Rescue: func(context.Context, domain.JobRecord) error { return nil }}).RecoverPeer(ctx, ""); err == nil {
		t.Fatal("empty dead peer succeeded")
	}
	registryErr := errors.New("registry")
	if _, err := (Recovery{Registry: &failingRegistry{err: registryErr}, Rescue: func(context.Context, domain.JobRecord) error { return nil }}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, registryErr) {
		t.Fatalf("registry err = %v", err)
	}
	if _, err := (Recovery{Registry: registry, Rescue: func(context.Context, domain.JobRecord) error { return nil }}).RecoverPeer(ctx, "dead-peer"); err == nil || !strings.Contains(err.Error(), "resolver") {
		t.Fatalf("missing owners err = %v", err)
	}
	ownerErr := errors.New("owner")
	if _, err := (Recovery{
		Registry: registry,
		Owners:   recoveryOwners{err: ownerErr},
		Rescue:   func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, ownerErr) {
		t.Fatalf("owner err = %v", err)
	}
	inspectErr := errors.New("inspect")
	if _, err := (Recovery{
		Registry: registry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{err: inspectErr},
		}},
		Rescue: func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, inspectErr) {
		t.Fatalf("inspect err = %v", err)
	}
	rescueErr := errors.New("rescue")
	if count, err := (Recovery{
		Registry: registry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{},
		}},
		Rescue: func(context.Context, domain.JobRecord) error { return rescueErr },
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, rescueErr) || count != 0 {
		t.Fatalf("rescue err/count = %v %d", err, count)
	}
	helpers := Recovery{}
	if !unfinished(domain.JobLoading) || unfinished(domain.JobDone) || !helpers.shouldConsider("dead-peer", recoveryRecord("queued", "dead-peer", "", domain.JobQueued)) || !helpers.shouldConsider("dead-peer", recoveryRecord("assigned", "other-peer", "dead-peer", domain.JobRunning)) || helpers.shouldConsider("dead-peer", recoveryRecord("other", "other-peer", "", domain.JobQueued)) {
		t.Fatal("status consideration helpers drifted")
	}
}

func TestRecoveryPartitionEvidenceErrorAndDefaultClock(t *testing.T) {
	ctx := context.Background()
	rec := recoveryRecord("partition", "dead-peer", "node-partition", domain.JobRunning)
	putErr := errors.New("put partition")
	if count, err := (Recovery{
		Registry: partitionPutFailRegistry{records: []domain.JobRecord{rec}, err: putErr},
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-partition": staticLeaseInspector{err: domain.ErrUnreachable},
		}},
		Rescue: func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, putErr) || count != 0 {
		t.Fatalf("partition put err/count = %v %d", err, count)
	}

	registry := NewJobRegistry()
	future := rec
	future.UpdatedAt = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := (Recovery{Registry: registry}).recordPartition(ctx, future); err != nil {
		t.Fatalf("recordPartition: %v", err)
	}
	snap, err := registry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 1 || !strings.Contains(snap[0].RecoveryNote, "partition") || !snap[0].UpdatedAt.After(future.UpdatedAt) {
		t.Fatalf("partition record = %+v future=%s", snap, future.UpdatedAt)
	}
}

func recoveryRecord(id, coordinator, node string, status domain.JobStatus) domain.JobRecord {
	rec := fixtures.MakeJobRecord(fixtures.WithRecordJobID(id))
	rec.Coordinator = coordinator
	rec.AssignedNode = node
	rec.Status = status
	rec.UpdatedAt = time.Unix(200, 0).UTC().Add(time.Duration(len(id)) * time.Second)
	rec.Request = []byte(`{"job":"` + id + `"}`)
	return rec
}

type recoveryOwners struct {
	inspectors map[string]ports.LeaseInspector
	err        error
}

func (r recoveryOwners) LeaseInspector(nodeID string) (ports.LeaseInspector, error) {
	if r.err != nil {
		return nil, r.err
	}
	inspector := r.inspectors[nodeID]
	if inspector == nil {
		return nil, domain.ErrUnreachable
	}
	return inspector, nil
}

type staticLeaseInspector struct {
	lease domain.Lease
	found bool
	err   error
}

func (s staticLeaseInspector) LeaseForJob(context.Context, string) (domain.Lease, bool, error) {
	return s.lease, s.found, s.err
}

func (s staticLeaseInspector) LeaseForInstance(context.Context, string) (domain.Lease, bool, error) {
	return s.lease, s.found, s.err
}

type partitionPutFailRegistry struct {
	records []domain.JobRecord
	err     error
}

func (r partitionPutFailRegistry) Put(context.Context, domain.JobRecord) error {
	return r.err
}

func (r partitionPutFailRegistry) Watch(context.Context, string) (<-chan domain.JobRecord, error) {
	return nil, r.err
}

func (r partitionPutFailRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	return append([]domain.JobRecord(nil), r.records...), nil
}
