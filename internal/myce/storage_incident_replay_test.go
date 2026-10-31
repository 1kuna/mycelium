package myce

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStorageIncidentReplayOctober31ZeroReclaim(t *testing.T) {
	// Sanitized/scaled incident facts: ceiling reached, protected references,
	// zero eligible reclaim, physical room beyond the configured ceiling.
	f := migrationQualifiedCycle(t, StorageLegacySettings{64 << 10, 8 << 10})
	q := f.q
	_, ref := retentionUpload(t, f.w.Client, strings.Repeat("p", 64<<10))
	legacy := spec("production")
	legacy.Resumable = false
	legacy.Files = FileManifest{"protected-input": ref}
	j := submit(t, q, legacy)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	before := q.fileStoreStatus(context.Background())
	if before.FreeBytes != 0 || before.ReclaimableBytes != 0 {
		t.Fatal("did not reproduce configured ceiling", before)
	}
	manifest := FileManifest{"next": {SHA256: strings.Repeat("a", 64), SizeBytes: 10 << 10}}
	if err := q.PreflightFileStore(context.Background(), manifest); err == nil {
		t.Fatal("legacy cap unexpectedly admitted")
	}
	migrationAction(t, q, "activate")
	q.files.diskProbe = func(string) (DiskReading, error) { return DiskReading{TotalBytes: 1 << 30, FreeBytes: 100 << 10}, nil }
	active := q.fileStoreStatus(context.Background())
	if active.ReclaimableBytes != 0 || active.FreeBytes != 100<<10 || active.LimitBytes != 0 {
		t.Fatal("auto conjured reclaim or retained cap", active)
	}
	if err := q.PreflightFileStore(context.Background(), manifest); err != nil {
		t.Fatal("physical room beyond ceiling unavailable", err)
	}
	q.deliveryRoot = t.TempDir()
	// Protected production/input growth can exhaust physical capacity. It must
	// hold truthfully and alert both owners, rather than delete retained inputs.
	held := submit(t, q, storageAdmissionSpec("production", 101<<10))
	if !held.OutputAdmissionPending || !strings.Contains(held.Reason, "deficit=1024 bytes") || !strings.Contains(held.Reason, "holders=") {
		t.Fatal("inexact production surge hold", held.Reason)
	}
	alerts := alertsOf(q, "production_storage_capacity")
	if len(alerts) != 1 || len(alerts[0].Recipients) != 2 {
		t.Fatal("missing operator/project alert", alerts)
	}
	if err := q.reclaimStorageForAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(context.Background(), ref); err != nil {
		t.Fatal("protected incident bytes lost", err)
	}
	if q.fileStoreStatus(context.Background()).ReclaimableBytes != 0 {
		t.Fatal("zero reclaim changed")
	}
	rolled := migrationAction(t, q, "rollback")
	if rolled.Saved.LimitBytes != 64<<10 || rolled.Saved.ProductionReserveBytes != 8<<10 {
		t.Fatal("incident rollback lost settings", rolled)
	}
}

// Real CPU output, real HTTP custody and destination receipts, a fake clock and
// a deliberately fake boundary. This proves placement/accounting, not native
// denial or confinement. The separate native bounded-volume test proves denial.
func migrationExperiment(t *testing.T, f *taskOutputFixture, units int) *taskOutputFixture {
	t.Helper()
	q, m := f.q, f.w.Machine
	check(t, q, m, nil)
	s := protectionSpec("experiment")
	s.OutputDestination = &OutputDestination{Protocol: taskOutputProtocol, Kind: "nas", ID: "archive"}
	offered := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != offered.ID {
		t.Fatal("experiment not assigned", assigned)
	}
	root := &workerStore{dir: newTestStateRoot(t), state: workerState{Session: m.Session}}
	w := f.w
	w.StorageBoundary = &taskOutputBoundary{}
	store, err := newAttemptStore(root, *assigned, w)
	if err != nil {
		t.Fatal(err)
	}
	j := *store.state.Job
	j.Spec.Run.Workdir = filepath.Join(store.dir, "job-files", j.ID, fmt.Sprint(j.Attempt))
	if err = os.MkdirAll(j.Spec.Run.Workdir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = q.Started(result(assigned, false)); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("sh", "-c", fmt.Sprintf("dd if=/dev/zero of=out.txt bs=1024 count=%d 2>/dev/null", units))
	command.Dir = j.Spec.Run.Workdir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("CPU output command: %v %s", err, output)
	}
	r := result(&j, true)
	store.state.Job, store.state.Result = &j, &r
	if err = store.save(); err != nil {
		t.Fatal(err)
	}
	return &taskOutputFixture{q: q, c: f.c, db: f.db, w: w, store: store, j: j, r: r, nas: f.nas, boundary: w.StorageBoundary.(*taskOutputBoundary)}
}

func TestStorageIncidentReplayIndependentCustodyOutageAndProduction(t *testing.T) {
	f := migrationQualifiedCycle(t)
	migrationAction(t, f.q, "activate")
	q := f.q
	q.files.diskProbe = func(string) (DiskReading, error) { return DiskReading{TotalBytes: 1 << 30, FreeBytes: 100 << 10}, nil }
	low := migrationExperiment(t, f, 95)
	// Destination was healthy at admission. Its loss during publication cannot
	// authorize queue-side staging of unique experiment output.
	interrupted := false
	low.w.outputDeliveryProgress = func() {
		stage := filepath.Join(low.nas, "results", low.j.ID, ".delivery-1")
		if st, err := os.Stat(filepath.Join(stage, "out.txt")); err == nil && st.Size() > 0 && !interrupted {
			interrupted = true
			if err := os.RemoveAll(stage); err != nil {
				t.Error(err)
			}
		}
	}
	low.pending(t)
	if !interrupted || job(t, q, low.j.ID).Delivery.State != "failed" {
		t.Fatal("mid-transfer outage did not fail publication")
	}
	taskOutputAssertSpool(t, low, true)
	if got := q.fileStoreStatus(context.Background()); got.UsedBytes != 0 || got.FreeBytes != 100<<10 {
		t.Fatal("95 local units consumed shared production capacity", got)
	}
	q.deliveryRoot = t.TempDir()
	high := submit(t, q, storageAdmissionSpec("production", 10<<10))
	if high.OutputAdmissionPending {
		t.Fatal("production starved behind experiment outage", high.Reason)
	}
	next := submit(t, q, protectionSpec("experiment"))
	if offer := check(t, q, f.w.Machine, nil).Job; offer != nil {
		t.Fatal("full local budget admitted more work", next.ID, offer.ID)
	}
	alternate := protectionMachine("alternate", "fake")
	alternate.Capabilities = append(alternate.Capabilities, taskOutputCapability)
	alternate.OutputDestinationIDs = []string{"archive"}
	if err := q.ConfigureStorageProtection(context.Background(), protectionBackend(f.w.Machine, alternate), protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	if offer := check(t, q, alternate, nil).Job; offer == nil || offer.ID != next.ID {
		t.Fatal("full producer globally paused an eligible alternate", offer)
	}

	if err := q.Cancel(high.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(next.ID); err != nil {
		t.Fatal(err)
	}
	// Source loss never converts a local receipt into NAS/shared availability.
	f.c.Advance(LossAfter + time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(q.s.StorageProtection.Custody) != 1 {
		t.Fatal("outage/loss erased unique custody")
	}
	taskOutputAssertSpool(t, low, true)
	if alerts := alertsOf(q, "worker_output_custodian_unavailable"); len(alerts) == 0 {
		t.Fatal("lost source did not alert owners")
	}
}

func TestStorageIncidentReplayOriginalPredictionMiss(t *testing.T) {
	f := migrationQualifiedCycle(t)
	migrationAction(t, f.q, "activate")
	q, m := f.q, f.w.Machine
	// Freeze source identity before reducing synthetic shared capacity.
	_, source := retentionUpload(t, f.w.Client, "")
	b := protectionBackend(m)
	b.snapshot.Workers[0].ExplorationBytes = 6 << 10
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	// Experiment output uses its own 96-unit budget while NAS is healthy.
	b.snapshot.Workers[0].ExplorationBytes = 96 << 10
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	low := migrationExperiment(t, f, 96)
	if err := os.Rename(f.nas, f.nas+"-outage"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Rename(f.nas+"-outage", f.nas) })
	low.pending(t)
	b.snapshot.Workers[0].ExplorationBytes = 6 << 10
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	q.files.diskProbe = func(string) (DiskReading, error) { return DiskReading{TotalBytes: 1 << 30, FreeBytes: 100 << 10}, nil }
	s := protectionSpec("production")
	s.Files = FileManifest{".myce-source.tar": source}
	key := q.storageKey(&Job{Spec: s, FileTransport: true}, &m)
	if key == nil {
		t.Fatal("missing frozen source identity")
	}
	point := StorageObservation{Key: *key, RecordedAt: f.c.Now(), Managed: StorageRange{Min: 4 << 10, Peak: 4 << 10, Samples: 1}, Files: StorageRange{Min: 8, Peak: 8, Samples: 1}, AllocationUnit: 1}
	if err := q.update(func() error {
		q.s.StorageObservations = map[string]StorageObservation{identityHash(key): point}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first := submit(t, q, s)
	a := check(t, q, m, nil).Job
	if a == nil || a.ID != first.ID || a.StorageAllocation.Bytes != 4<<10 || *a.StorageEnvelope.PredictedNeed != 4<<10 {
		t.Fatal("inflated zero-error four-unit bound", a)
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	failed := result(a, false)
	failed.ExecutionCause = &RuntimeCause{Operation: "command", Class: diskLimitExceeded}
	failed.StoragePeak = &AttemptStoragePeak{ManagedBytes: 4 << 10, Files: 8, Samples: 1, AllocationUnit: 1, Constrained: true}
	if err := q.Complete(failed); err != nil {
		t.Fatal(err)
	}
	next := submit(t, q, s)
	larger := check(t, q, m, nil).Job
	if larger == nil || larger.ID != next.ID || larger.StorageAllocation.Bytes != 6<<10 || *larger.StorageEnvelope.PredictedNeed != 4<<10 {
		t.Fatal("six-unit readmission starved or history inflated", larger)
	}
	if clean := q.s.StorageObservations[identityHash(key)]; clean.Managed != point.Managed || clean.LowerBound == nil {
		t.Fatal("miss erased accepted evidence", clean)
	}
	if err := q.Started(result(larger, false)); err != nil {
		t.Fatal(err)
	}
	// Small publication and WAL progress succeed while the 96 unique units remain
	// undelivered. The first quota failure is intentional and never retried as I/O.
	_, published := retentionUpload(t, f.w.Client, strings.Repeat("p", 6<<10))
	ok := result(larger, true)
	ok.OutputRefs = FileManifest{"out.txt": published}
	if err := q.Complete(ok); err != nil {
		t.Fatal(err)
	}
	if job(t, q, larger.ID).State != "succeeded" {
		t.Fatal("production did not finish")
	}
	if _, err := q.db.Exec("PRAGMA wal_checkpoint(FULL)"); err != nil {
		t.Fatal(err)
	}
	taskOutputAssertSpool(t, low, true)
	if job(t, q, low.j.ID).Delivery.State != "failed" {
		t.Fatal("NAS recovered during replay")
	}
}

func TestStorageIncidentReplayBurstBoundaryAndWAL(t *testing.T) {
	f := migrationQualifiedCycle(t)
	migrationAction(t, f.q, "activate")
	q, m := f.q, f.w.Machine
	b := protectionBackend(m)
	b.snapshot.Workers[0].ExplorationBytes = 2 << 10
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	// Scheduling units exclude already funded backing/metadata: production 8,
	// control 2, experiment 2. A fake boundary proves denial handling only.
	experiment := submit(t, q, protectionSpec("experiment"))
	a := check(t, q, m, nil).Job
	if a == nil || a.ID != experiment.ID || a.StorageAllocation.Bytes != 2<<10 {
		t.Fatal("two-unit boundary not installed", a)
	}
	boundary := &fakeStorageBoundary{}
	w := f.w
	w.StorageBoundary = boundary
	root := &workerStore{dir: newTestStateRoot(t), state: workerState{Session: m.Session}}
	store, err := newAttemptStore(root, *a, w)
	if err != nil {
		t.Fatal(err)
	}
	if len(boundary.paths) != 1 || boundary.paths[0].State != store.dir || boundary.paths[0].Checkpoint == "" || boundary.paths[0].Publication == "" {
		t.Fatal("spool/stop/sidecar writes escaped complete boundary", boundary.paths)
	}
	if err = q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	boundary.deny = true // Native inter-probe EDQUOT/contained ENOSPC signal.
	failed, err := w.finishFileResult(*a, result(a, false), store.dir)
	if err != nil || !attemptDiskLimitFailure(failed) {
		t.Fatal("boundary miss escaped as infrastructure I/O", failed, err)
	}
	if err = q.Complete(failed); err != nil {
		t.Fatal(err)
	}
	q.files.diskProbe = func(string) (DiskReading, error) { return DiskReading{TotalBytes: 1 << 30, FreeBytes: 12 << 10}, nil }

	b.snapshot.Workers[0].ExplorationBytes = 8 << 10
	if err = q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	production := submit(t, q, protectionSpec("production"))
	prod := check(t, q, m, nil).Job
	if prod == nil || prod.ID != production.ID || prod.StorageAllocation.Bytes != 8<<10 {
		t.Fatal("burst blocked production staging", prod)
	}
	if err = q.Started(result(prod, false)); err != nil {
		t.Fatal(err)
	}
	client := f.w.Client
	_, ref := retentionUpload(t, client, strings.Repeat("p", 8<<10))
	complete := result(prod, true)
	complete.OutputRefs = FileManifest{"out.txt": ref}
	if err = q.Complete(complete); err != nil {
		t.Fatal(err)
	}
	if ref.SizeBytes != 8<<10 || job(t, q, prod.ID).State != "succeeded" {
		t.Fatal("burst blocked production publication")
	}

	if _, err = q.db.Exec("PRAGMA wal_checkpoint(FULL)"); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.s.StorageProtection.Allocations[a.StorageAllocation.ID]; !ok {
		t.Fatal("burst discarded unique attempt progress")
	}
}
