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
	"mycelium/internal/trace"
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
	liveOwner := &mocks.AdmissionController{
		LeaseForJobVal:   domain.Lease{ID: "lease-live", JobID: "owner-live"},
		LeaseForJobFound: true,
	}
	tr := trace.New(func() time.Time { return time.Unix(300, int64(len(rescued))).UTC() })
	recovery := Recovery{
		Registry: registry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-live":        liveOwner,
			"node-finished":    staticLeaseInspector{},
			"node-unreachable": staticLeaseInspector{err: domain.ErrUnreachable},
		}, admissions: map[string]ports.AdmissionController{
			"node-live": liveOwner,
		}, statuses: map[string]staticJobStatusInspector{
			"node-finished": {status: domain.JobDone, found: true},
		}},
		Clock: mocks.NewFakeClock(time.Unix(300, 0).UTC()),
		Trace: tr,
		Rescue: func(_ context.Context, rec domain.JobRecord) error {
			rescued = append(rescued, rec.JobID)
			return nil
		},
	}

	count, err := recovery.RecoverPeer(ctx, "dead-peer")
	if err != nil {
		t.Fatalf("RecoverPeer: %v", err)
	}
	want := []string{"queued", "placing", "owner-dead", "owner-live", "assigned-dead"}
	if count != len(want) || !reflect.DeepEqual(rescued, want) {
		t.Fatalf("rescued count=%d ids=%+v", count, rescued)
	}
	if !reflect.DeepEqual(liveOwner.Calls, []string{"job-status:owner-live", "lease-for-job:owner-live", "release:lease-live"}) {
		t.Fatalf("live owner calls = %+v", liveOwner.Calls)
	}
	if !hasPeerTrace(tr.Steps, "recovery/snapshot", "success") || !hasPeerTrace(tr.Steps, "recovery/rescue", "success") || !hasPeerTrace(tr.Steps, "recovery/partition", "success") {
		t.Fatalf("trace = %+v", tr.Steps)
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

func TestRecoveryContinuesAfterRecordError(t *testing.T) {
	ctx := context.Background()
	registry := NewJobRegistry()
	records := []domain.JobRecord{
		recoveryRecord("bad", "dead-peer", "", domain.JobQueued),
		recoveryRecord("good", "dead-peer", "", domain.JobQueued),
	}
	if err := registry.Merge(ctx, records); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	boom := errors.New("rescue bad")
	var rescued []string
	count, err := (Recovery{
		Registry: registry,
		Rescue: func(_ context.Context, rec domain.JobRecord) error {
			if rec.JobID == "bad" {
				return boom
			}
			rescued = append(rescued, rec.JobID)
			return nil
		},
	}).RecoverPeer(ctx, "dead-peer")
	if !errors.Is(err, boom) {
		t.Fatalf("RecoverPeer err = %v", err)
	}
	if count != 1 || !reflect.DeepEqual(rescued, []string{"good"}) {
		t.Fatalf("count=%d rescued=%+v", count, rescued)
	}
}

func TestRecoveryCleansTerminalCleanupRequiredRecordWithoutRescue(t *testing.T) {
	ctx := context.Background()
	registry := NewJobRegistry()
	rec := recoveryRecord("cleanup-done", "dead-peer", "node-live", domain.JobDone)
	rec.CleanupRequired = true
	rec.CleanupError = "owner release failed"
	if err := registry.Put(ctx, rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	owner := &mocks.AdmissionController{
		LeaseForJobVal:   domain.Lease{ID: "lease-cleanup", JobID: rec.JobID, NodeID: rec.AssignedNode},
		LeaseForJobFound: true,
	}
	rescued := false
	recovery := Recovery{
		Registry: registry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-live": owner,
		}, admissions: map[string]ports.AdmissionController{
			"node-live": owner,
		}},
		Clock: mocks.NewFakeClock(time.Unix(300, 0).UTC()),
		Rescue: func(context.Context, domain.JobRecord) error {
			rescued = true
			return nil
		},
	}

	count, err := recovery.RecoverPeer(ctx, "dead-peer")
	if err != nil {
		t.Fatalf("RecoverPeer: %v", err)
	}
	if count != 0 || rescued {
		t.Fatalf("terminal cleanup should not rescue count=%d rescued=%v", count, rescued)
	}
	if !reflect.DeepEqual(owner.Calls, []string{"lease-for-job:cleanup-done", "release:lease-cleanup"}) {
		t.Fatalf("owner calls = %+v", owner.Calls)
	}
	snap, err := registry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 1 || snap[0].Status != domain.JobDone || snap[0].CleanupRequired || snap[0].CleanupError != "" || !strings.Contains(snap[0].RecoveryNote, "terminal cleanup recovered") {
		t.Fatalf("cleanup evidence was not cleared: %+v", snap)
	}
}

func TestRecoveryCleanupRequiredErrorPaths(t *testing.T) {
	ctx := context.Background()
	base := recoveryRecord("cleanup", "dead-peer", "node-a", domain.JobDone)
	base.CleanupRequired = true
	base.CleanupError = "owner release failed"
	rescue := func(context.Context, domain.JobRecord) error { return nil }
	withRecord := func(rec domain.JobRecord) *JobRegistry {
		registry := NewJobRegistry()
		if err := registry.Put(ctx, rec); err != nil {
			t.Fatalf("Put %s: %v", rec.JobID, err)
		}
		return registry
	}

	if _, err := (Recovery{Registry: withRecord(base), Rescue: rescue}).RecoverPeer(ctx, "dead-peer"); err == nil || !strings.Contains(err.Error(), "resolver") {
		t.Fatalf("cleanup missing owners err = %v", err)
	}
	ownerErr := errors.New("owner")
	if _, err := (Recovery{Registry: withRecord(base), Owners: recoveryOwners{err: ownerErr}, Rescue: rescue}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, ownerErr) {
		t.Fatalf("cleanup owner err = %v", err)
	}
	leaseErr := errors.New("lease")
	if _, err := (Recovery{
		Registry: withRecord(base),
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{err: leaseErr},
		}},
		Rescue: rescue,
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, leaseErr) {
		t.Fatalf("cleanup lease err = %v", err)
	}
	resolverPartitionRegistry := withRecord(base)
	if count, err := (Recovery{
		Registry: resolverPartitionRegistry,
		Owners:   recoveryOwners{},
		Rescue:   rescue,
	}).RecoverPeer(ctx, "dead-peer"); err != nil || count != 0 {
		t.Fatalf("cleanup resolver partition err/count = %v %d", err, count)
	}
	resolverPartitionSnap, err := resolverPartitionRegistry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("resolver partition Snapshot: %v", err)
	}
	if len(resolverPartitionSnap) != 1 || !strings.Contains(resolverPartitionSnap[0].RecoveryNote, "partition") {
		t.Fatalf("cleanup resolver partition snap = %+v", resolverPartitionSnap)
	}
	if _, err := (Recovery{
		Registry: withRecord(base),
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{lease: domain.Lease{JobID: base.JobID}, found: true},
		}},
		Rescue: rescue,
	}).RecoverPeer(ctx, "dead-peer"); err == nil || !strings.Contains(err.Error(), "without lease id") {
		t.Fatalf("cleanup empty lease err = %v", err)
	}
	lease := domain.Lease{ID: "lease-cleanup", JobID: base.JobID, NodeID: base.AssignedNode}
	if _, err := (Recovery{
		Registry: withRecord(base),
		Owners: inspectorOnlyRecoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{lease: lease, found: true},
		}},
		Rescue: rescue,
	}).RecoverPeer(ctx, "dead-peer"); err == nil || !strings.Contains(err.Error(), "admission resolver") {
		t.Fatalf("cleanup missing admission resolver err = %v", err)
	}
	releaseErr := errors.New("release cleanup")
	if _, err := (Recovery{
		Registry: withRecord(base),
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{lease: lease, found: true},
		}, admissions: map[string]ports.AdmissionController{
			"node-a": &mocks.AdmissionController{ReleaseErr: releaseErr},
		}},
		Rescue: rescue,
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, releaseErr) {
		t.Fatalf("cleanup release err = %v", err)
	}
	putErr := errors.New("cleanup put")
	if _, err := (Recovery{
		Registry: partitionPutFailRegistry{records: []domain.JobRecord{base}, err: putErr},
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{},
		}, admissions: map[string]ports.AdmissionController{
			"node-a": &mocks.AdmissionController{},
		}},
		Rescue: rescue,
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, putErr) {
		t.Fatalf("cleanup clear err = %v", err)
	}

	partitionRegistry := withRecord(base)
	if count, err := (Recovery{
		Registry: partitionRegistry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{err: domain.ErrUnreachable},
		}},
		Rescue: rescue,
	}).RecoverPeer(ctx, "dead-peer"); err != nil || count != 0 {
		t.Fatalf("cleanup partition err/count = %v %d", err, count)
	}
	snap, err := partitionRegistry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("partition Snapshot: %v", err)
	}
	if len(snap) != 1 || !strings.Contains(snap[0].RecoveryNote, "partition") || !snap[0].CleanupRequired {
		t.Fatalf("cleanup partition snap = %+v", snap)
	}

	noOwner := base
	noOwner.JobID = "cleanup-no-owner"
	noOwner.AssignedNode = ""
	deadOwner := base
	deadOwner.JobID = "cleanup-dead-owner"
	deadOwner.AssignedNode = "dead-peer"
	skipRegistry := NewJobRegistry()
	for _, rec := range []domain.JobRecord{noOwner, deadOwner} {
		if err := skipRegistry.Put(ctx, rec); err != nil {
			t.Fatalf("Put skip %s: %v", rec.JobID, err)
		}
	}
	called := false
	if count, err := (Recovery{
		Registry: skipRegistry,
		Owners:   recoveryOwners{},
		Rescue: func(context.Context, domain.JobRecord) error {
			called = true
			return nil
		},
	}).RecoverPeer(ctx, "dead-peer"); err != nil || count != 0 || called {
		t.Fatalf("cleanup skip err/count/called = %v %d %v", err, count, called)
	}

	futureRegistry := NewJobRegistry()
	future := base
	future.JobID = "cleanup-future"
	future.UpdatedAt = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := (Recovery{Registry: futureRegistry}).recordCleanupCleared(ctx, future); err != nil {
		t.Fatalf("recordCleanupCleared future: %v", err)
	}
	futureSnap, err := futureRegistry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("future Snapshot: %v", err)
	}
	if len(futureSnap) != 1 || !futureSnap[0].UpdatedAt.After(future.UpdatedAt) || futureSnap[0].CleanupRequired {
		t.Fatalf("future cleanup record = %+v", futureSnap)
	}
}

func hasPeerTrace(steps []trace.Step, op, status string) bool {
	for _, step := range steps {
		if step.Operation == op && step.Status == status {
			return true
		}
	}
	return false
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
	partitionRegistry := NewJobRegistry()
	if err := partitionRegistry.Put(ctx, rec); err != nil {
		t.Fatalf("Put partition rec: %v", err)
	}
	if count, err := (Recovery{
		Registry: partitionRegistry,
		Owners: recoveryOwners{
			statuses: map[string]staticJobStatusInspector{"node-a": {}},
		},
		Rescue: func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); err != nil || count != 0 {
		t.Fatalf("partition resolver err/count = %v %d", err, count)
	}
	partitionSnap, err := partitionRegistry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("partition Snapshot: %v", err)
	}
	if len(partitionSnap) != 1 || !strings.Contains(partitionSnap[0].RecoveryNote, "partition") {
		t.Fatalf("partition snap = %+v", partitionSnap)
	}
	leaseResolverErr := errors.New("lease resolver")
	if _, err := (Recovery{
		Registry: registry,
		Owners: statusThenLeaseResolverError{
			status:   staticJobStatusInspector{},
			leaseErr: leaseResolverErr,
		},
		Rescue: func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, leaseResolverErr) {
		t.Fatalf("lease resolver err = %v", err)
	}
	if _, err := (Recovery{
		Registry: registry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{lease: domain.Lease{JobID: rec.JobID}, found: true},
		}},
		Rescue: func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); err == nil || !strings.Contains(err.Error(), "without lease id") {
		t.Fatalf("empty owner lease err = %v", err)
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
	lease := domain.Lease{ID: "lease-a", JobID: rec.JobID, NodeID: "node-a"}
	if count, err := (Recovery{
		Registry: registry,
		Owners: inspectorOnlyRecoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{lease: lease, found: true},
		}},
		Rescue: func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); err == nil || !strings.Contains(err.Error(), "admission resolver") || count != 0 {
		t.Fatalf("missing admission resolver err/count = %v %d", err, count)
	}
	releaseErr := errors.New("release owner")
	admission := &mocks.AdmissionController{ReleaseErr: releaseErr}
	if count, err := (Recovery{
		Registry: registry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{lease: lease, found: true},
		}, admissions: map[string]ports.AdmissionController{
			"node-a": admission,
		}},
		Rescue: func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, releaseErr) || count != 0 {
		t.Fatalf("release owner err/count = %v %d", err, count)
	}
	if count, err := (Recovery{
		Registry: registry,
		Owners: recoveryOwners{inspectors: map[string]ports.LeaseInspector{
			"node-a": staticLeaseInspector{lease: lease, found: true},
		}},
		Rescue: func(context.Context, domain.JobRecord) error { return nil },
	}).RecoverPeer(ctx, "dead-peer"); !errors.Is(err, domain.ErrUnreachable) || count != 0 {
		t.Fatalf("admission lookup err/count = %v %d", err, count)
	}
	helpers := Recovery{}
	if !unfinished(domain.JobLoading) || unfinished(domain.JobDone) || !helpers.shouldConsider("dead-peer", recoveryRecord("queued", "dead-peer", "", domain.JobQueued)) || !helpers.shouldConsider("dead-peer", recoveryRecord("assigned", "other-peer", "dead-peer", domain.JobRunning)) || helpers.shouldConsider("dead-peer", recoveryRecord("other", "other-peer", "", domain.JobQueued)) {
		t.Fatal("status consideration helpers drifted")
	}
	if _, _, err := helpers.ownerJobStatus(ctx, "node-a", "job-a"); err == nil || !strings.Contains(err.Error(), "resolver") {
		t.Fatalf("missing status owner err = %v", err)
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

	terminalRegistry := NewJobRegistry()
	done := rec
	done.Status = domain.JobDone
	done.UpdatedAt = time.Unix(400, 0).UTC()
	if err := terminalRegistry.Put(ctx, done); err != nil {
		t.Fatalf("Put done: %v", err)
	}
	staleRunning := done
	staleRunning.Status = domain.JobRunning
	staleRunning.UpdatedAt = done.UpdatedAt.Add(time.Hour)
	if err := (Recovery{Registry: terminalRegistry, Clock: mocks.NewFakeClock(time.Unix(500, 0).UTC())}).recordPartition(ctx, staleRunning); err != nil {
		t.Fatalf("recordPartition stale running: %v", err)
	}
	snap, err = terminalRegistry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("terminal Snapshot: %v", err)
	}
	if len(snap) != 1 || snap[0].Status != domain.JobDone || snap[0].RecoveryNote != "" {
		t.Fatalf("terminal record was overwritten by recovery evidence: %+v", snap)
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
	admissions map[string]ports.AdmissionController
	statuses   map[string]staticJobStatusInspector
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

func (r recoveryOwners) JobStatusInspector(nodeID string) (ports.JobStatusInspector, error) {
	if r.err != nil {
		return nil, r.err
	}
	if status, ok := r.statuses[nodeID]; ok {
		return status, nil
	}
	if inspector := r.inspectors[nodeID]; inspector != nil {
		if statusInspector, ok := inspector.(ports.JobStatusInspector); ok {
			return statusInspector, nil
		}
		return staticJobStatusInspector{}, nil
	}
	return nil, domain.ErrUnreachable
}

func (r recoveryOwners) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	if r.err != nil {
		return nil, r.err
	}
	admission := r.admissions[nodeID]
	if admission == nil {
		return nil, domain.ErrUnreachable
	}
	return admission, nil
}

type inspectorOnlyRecoveryOwners struct {
	inspectors map[string]ports.LeaseInspector
}

func (r inspectorOnlyRecoveryOwners) LeaseInspector(nodeID string) (ports.LeaseInspector, error) {
	inspector := r.inspectors[nodeID]
	if inspector == nil {
		return nil, domain.ErrUnreachable
	}
	return inspector, nil
}

func (r inspectorOnlyRecoveryOwners) JobStatusInspector(nodeID string) (ports.JobStatusInspector, error) {
	inspector := r.inspectors[nodeID]
	if inspector == nil {
		return nil, domain.ErrUnreachable
	}
	if statusInspector, ok := inspector.(ports.JobStatusInspector); ok {
		return statusInspector, nil
	}
	return staticJobStatusInspector{}, nil
}

type statusThenLeaseResolverError struct {
	status   ports.JobStatusInspector
	leaseErr error
}

func (r statusThenLeaseResolverError) JobStatusInspector(string) (ports.JobStatusInspector, error) {
	return r.status, nil
}

func (r statusThenLeaseResolverError) LeaseInspector(string) (ports.LeaseInspector, error) {
	return nil, r.leaseErr
}

func (r statusThenLeaseResolverError) AdmissionController(string) (ports.AdmissionController, error) {
	return nil, domain.ErrUnreachable
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

type staticJobStatusInspector struct {
	status domain.JobStatus
	found  bool
	err    error
}

func (s staticJobStatusInspector) JobStatus(context.Context, string) (domain.JobStatus, bool, error) {
	return s.status, s.found, s.err
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
