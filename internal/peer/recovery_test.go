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
)

func TestRecoveryRescuesDeadPeerUnfinishedJobsAfterOwnerCheck(t *testing.T) {
	ctx := context.Background()
	registry := NewJobRegistry()
	records := []domain.JobRecord{
		recoveryRecord("queued", "dead-peer", "", domain.JobQueued),
		recoveryRecord("placing", "dead-peer", "", domain.JobPlacing),
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
		Rescue: func(_ context.Context, rec domain.JobRecord) error {
			rescued = append(rescued, rec.JobID)
			return nil
		},
	}

	count, err := recovery.RecoverPeer(ctx, "dead-peer")
	if err != nil {
		t.Fatalf("RecoverPeer: %v", err)
	}
	want := []string{"queued", "placing", "owner-missing", "owner-finished", "owner-unreachable"}
	if count != len(want) || !reflect.DeepEqual(rescued, want) {
		t.Fatalf("rescued count=%d ids=%+v", count, rescued)
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
	if !unfinished(domain.JobLoading) || unfinished(domain.JobDone) || !helpers.shouldConsider("dead-peer", recoveryRecord("queued", "dead-peer", "", domain.JobQueued)) || helpers.shouldConsider("dead-peer", recoveryRecord("other", "other-peer", "", domain.JobQueued)) {
		t.Fatal("status consideration helpers drifted")
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
