package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func storageAdmissionSpec(tier string, bytes uint64) Spec {
	s := asyncDeliverySpec()
	s.Priority, s.Class = tier, tier
	s.Disk.OutputBytes = bytes
	return s
}

func TestStorageAdmissionYieldsQueuedCommitmentsAndReacquiresAfterRestart(t *testing.T) {
	q, c, dbpath := protectionQueue(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	q.files.limit = 16
	low := submit(t, q, storageAdmissionSpec("experiment", 8))
	c.Advance(time.Second)
	newer := submit(t, q, storageAdmissionSpec("experiment", 8))
	high := submit(t, q, storageAdmissionSpec("production", 8))
	if job(t, q, low.ID).OutputAdmissionPending || !job(t, q, newer.ID).OutputAdmissionPending || job(t, q, high.ID).OutputAdmissionPending {
		t.Fatal("did not yield only newest lowest-tier estimate")
	}
	if err := q.admitOutputRetries(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !job(t, q, newer.ID).OutputAdmissionPending {
		t.Fatal("yielded commitment stole production capacity")
	}
	root, files := q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbpath, root, c, WithFileStore(files, 16, true), WithOutputDeliveryRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored := job(t, reopened, high.ID)
	if !reopened.s.StorageAdmissionActive || !job(t, reopened, newer.ID).OutputAdmissionPending || restored.OutputReservationBytes == nil || *restored.OutputReservationBytes != 8 {
		t.Fatal("restart lost reservations")
	}
	if err = reopened.Cancel(high.ID); err != nil {
		t.Fatal(err)
	}
	if err = reopened.admitOutputRetries(context.Background()); err != nil {
		t.Fatal(err)
	}
	if job(t, reopened, newer.ID).OutputAdmissionPending {
		t.Fatal("did not reacquire after cancellation")
	}
	if status := reopened.fileStoreStatus(context.Background()); status.ReservedOutputBytes != 16 {
		t.Fatal("wrong restored commitments", status)
	}
}

func TestStorageAdmissionProtectsAssignedWritersAndAlertsProduction(t *testing.T) {
	q, c := fixture(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	q.files.limit = 8
	low := submit(t, q, storageAdmissionSpec("experiment", 8))
	m := machine("writer", "fake")
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != low.ID || assigned.StartedAt != nil {
		t.Fatal("missing unstarted assignment")
	}
	high := submit(t, q, storageAdmissionSpec("production", 6))
	held := job(t, q, high.ID)
	if !held.OutputAdmissionPending || !strings.Contains(held.Reason, "deficit=6 bytes") || !strings.Contains(held.Reason, low.ID) {
		t.Fatalf("inexact shortage: %+v", held)
	}
	if job(t, q, low.ID).OutputAdmissionPending {
		t.Fatal("revoked assignment which may have writers")
	}
	for range 2 {
		if err := q.admitOutputRetries(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	alerts := alertsOf(q, "production_storage_capacity")
	if len(alerts) != 1 || alerts[0].At != c.Now() || alerts[0].Project != held.Spec.Project || !reflect.DeepEqual(alerts[0].Recipients, []string{"operator", "project"}) || !strings.Contains(alerts[0].Reason, "deficit=6") {
		t.Fatalf("alert missing routing/holders or duplicated: %+v", alerts)
	}
	if err := q.Started(result(assigned, false)); err != nil {
		t.Fatal("yield must not revoke a protected assigned writer", err)
	}
	if _, err := q.Submit(storageAdmissionSpec("experiment", 1)); err == nil {
		t.Fatal("lower-tier submission bypassed blocked production")
	}
	if err := q.Cancel(high.ID); err != nil {
		t.Fatal(err)
	}
	if _, exists := q.s.Health.Conditions["storage-admission/"+high.ID]; exists {
		t.Fatal("cancelled demand retained a pressure condition")
	}
}

func TestStorageAdmissionRetryOrderUsesOwnerPolicy(t *testing.T) {
	q, c := fixture(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	q.files.limit = 16
	low := submit(t, q, storageAdmissionSpec("experiment", 8))
	c.Advance(time.Second)
	highSpec := storageAdmissionSpec("experiment", 8)
	highSpec.Project = "service"
	high := submit(t, q, highSpec)
	policy := PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "urgent", Selectors: []string{"service/*"}}, {Name: "other", Selectors: []string{"*/*"}}}}
	if _, err := q.SetPriorityPolicy(0, policy); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		for _, id := range []string{low.ID, high.ID} {
			q.touchJob(id).OutputAdmissionPending = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q.files.limit = 8
	if err := q.admitOutputRetries(context.Background()); err != nil {
		t.Fatal(err)
	}
	if job(t, q, high.ID).OutputAdmissionPending || !job(t, q, low.ID).OutputAdmissionPending {
		t.Fatal("FIFO won over configured tier")
	}
}

func TestStorageAdmissionLearnsQualifiedDemandAndFreezesCommitment(t *testing.T) {
	q, c := fixture(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	client := deliveryClient(t, q)
	_, source := retentionUpload(t, client, "")
	s := storageAdmissionSpec("experiment", 8)
	s.Files = FileManifest{".myce-source.tar": source}
	m := machine("learned", "fake")
	check(t, q, m, nil)
	baseline := submit(t, q, s)
	if err := q.Cancel(baseline.ID); err != nil {
		t.Fatal(err)
	}
	key := q.storageKey(job(t, q, baseline.ID), &m)
	if key == nil {
		t.Fatal("no comparable key")
	}
	q.s.StorageObservations = map[string]StorageObservation{identityHash(key): {Key: *key, Output: StorageRange{Samples: 1, Min: 4, Peak: 4}, OutputRecordedAt: c.Now()}}
	q.files.limit = 4
	learned := submit(t, q, s)
	if outputCommitment(job(t, q, learned.ID)) != 4 {
		t.Fatal("cold guess overrode learned logical need")
	}
	observation := q.s.StorageObservations[identityHash(key)]
	observation.Output.Peak = 12
	q.s.StorageObservations[identityHash(key)] = observation
	if got := q.reservedOutputBytes(nil); got != 4 {
		t.Fatal("later learning mutated an accepted commitment", got)
	}
	if got := q.sharedOutputDemand(job(t, q, learned.ID)); got != 20 {
		t.Fatal("new admission ignored range margin", got)
	}
}

func TestStorageAdmissionProductionInputYieldsOnlyUnassignedEstimates(t *testing.T) {
	q, _ := fixture(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	q.files.limit = 8
	low := submit(t, q, storageAdmissionSpec("experiment", 8))
	p := q.PriorityPolicy()
	p.Policy.Projects = map[string]ProjectPolicy{"service": {DefaultCallPriority: "production", Weight: 1}}
	if _, err := q.SetPriorityPolicy(p.Revision, p.Policy); err != nil {
		t.Fatal(err)
	}
	client := deliveryClient(t, q)
	path, ref := policyInput(t, "123456")
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "service", Class: "production"})
	if err := client.UploadJobInput(ctx, path, ref); err != nil {
		t.Fatal(err)
	}
	if !job(t, q, low.ID).OutputAdmissionPending {
		t.Fatal("input did not yield unstarted lower-tier commitment")
	}
	if err := q.files.verify(context.Background(), ref); err != nil {
		t.Fatal("input not durable", err)
	}
	status := q.fileStoreStatus(context.Background())
	if status.UsedBytes != 6 || status.ReservedOutputBytes != 0 {
		t.Fatal("input double charged", status)
	}
}

func TestStorageAdmissionCollectionProgressAndPublicationFence(t *testing.T) {
	q, c := fixture(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	client := retentionClient(t, q)
	// More than one collector batch, each small file already explicitly eligible
	// through the existing retired-file ledger. No expiry permission is invented.
	for i := 0; i < fileReclaimBatch+1; i++ {
		_, ref := retentionUpload(t, client, fmt.Sprintf("expired-%d", i))
		if q.s.RetiredJobFiles == nil {
			q.s.RetiredJobFiles = map[string]bool{}
		}
		q.s.RetiredJobFiles[ref.SHA256] = true
	}
	_, unique := retentionUpload(t, client, "unique")
	c.Advance(filePublicationGrace + time.Second)
	q.fileLifecycle.RLock()
	if err := q.reclaimStorageForAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.fileLifecycle.RUnlock()
	if !q.fileCollectionDeferred {
		t.Fatal("publication fence concealed")
	}
	// Admission must progress through >64 authorized files before refusing.
	q.files.limit = 10
	high := submit(t, q, storageAdmissionSpec("production", 4))
	if job(t, q, high.ID).OutputAdmissionPending {
		t.Fatal("refused before progressing collection batches", job(t, q, high.ID).Reason)
	}
	if got := q.fileStoreStatus(context.Background()); got.UsedBytes != 6 || got.ReclaimableBytes != 0 {
		t.Fatal("did not collect all eligible bytes", got)
	}
	if err := q.files.verify(context.Background(), unique); err != nil {
		t.Fatal("unique imported bytes deleted", err)
	}
}

func TestStorageAdmissionWorkerCustodyDoesNotReserveSharedStore(t *testing.T) {
	q, c, _ := protectionQueue(t)
	q.s.StorageAdmissionActive = true
	m := protectionMachine("local", "fake")
	backend := protectionBackend(m)
	if err := q.ConfigureStorageProtection(context.Background(), backend, protectionControlWork(t, c)); err != nil {
		t.Fatal(err)
	}
	q.deliveryRoot = q.root
	q.files.limit = 1
	s := protectionSpec("experiment")
	s.Disk = &DiskNeeds{OutputBytes: 95 << 20}
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID || !assigned.StorageAllocation.WorkerCustody {
		t.Fatal("isolated experiment held by shared cap")
	}
	if got := q.reservedOutputBytes(nil); got != 0 {
		t.Fatal("worker custody charged to shared store", got)
	}
}

func TestStorageAdmissionCollectionValueOrderDoesNotAuthorizeDeletion(t *testing.T) {
	q, _ := fixture(t)
	q.s.StorageAdmissionActive = true
	refs := []string{strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)}
	q.s.Jobs = map[string]*Job{
		"production": {ID: "production", Spec: Spec{Class: "production"}, OutputRefs: FileManifest{"out": {SHA256: refs[0]}}},
		"terminal":   {ID: "terminal", Spec: Spec{Class: "experiment"}, OutputRefs: FileManifest{"out": {SHA256: refs[1]}}},
		"delivered":  {ID: "delivered", Spec: Spec{Class: "experiment"}, Delivery: &OutputDelivery{State: "verified"}, OutputRefs: FileManifest{"out": {SHA256: refs[2]}}},
	}
	order := append([]string(nil), refs...)
	q.orderStorageReclaim(order)
	if !reflect.DeepEqual(order, []string{refs[2], refs[1], refs[0]}) {
		t.Fatal("wrong replacement/tier order", order)
	}
	for _, hash := range refs {
		if err := os.WriteFile(filepath.Join(q.files.root, "blobs", hash), []byte("unique"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.reclaimStorageForAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, hash := range refs {
		if _, err := os.Stat(filepath.Join(q.files.root, "blobs", hash)); err != nil {
			t.Fatal("ordering granted deletion authority", err)
		}
	}
}

func TestStorageAdmissionOriginalNinetyFiveTenReplay(t *testing.T) {
	q, c, _ := protectionQueue(t)
	q.s.StorageAdmissionActive = true
	m := protectionMachine("producer", "fake")
	b := protectionBackend(m)
	b.snapshot.Pools[0].FreeBytes = 100
	b.snapshot.Workers[0].ExplorationBytes = 95
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, c)); err != nil {
		t.Fatal(err)
	}
	experiment := submit(t, q, protectionSpec("experiment"))
	local := check(t, q, m, nil).Job
	if local == nil || local.ID != experiment.ID || local.StorageAllocation.Bytes != 95 {
		t.Fatal("missing bounded experiment")
	}
	path := filepath.Join(t.TempDir(), "immutable-result")
	if err := os.WriteFile(path, make([]byte, 95), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	receipt := StorageCustodyReceipt{Protocol: workerCustodyProtocol, Execution: Execution{ID: local.ID, Attempt: local.Attempt}, Machine: local.Machine, Session: local.Session, Allocation: local.StorageAllocation.ID, Files: FileManifest{"out.txt": ref}}
	if err = q.RecordStorageCustody(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if err = q.Complete(Result{Execution: receipt.Execution, Machine: receipt.Machine, Session: receipt.Session, ReleaseOnly: true}); err != nil {
		t.Fatal(err)
	}
	b.snapshot.Workers[0].ExplorationBytes = 10
	if err = q.ConfigureStorageProtection(context.Background(), b, b.protected); err != nil {
		t.Fatal(err)
	}
	production := submit(t, q, protectionSpec("production"))
	offer := check(t, q, m, nil).Job
	if offer == nil || offer.ID != production.ID || offer.StorageAllocation.Bytes != 10 {
		t.Fatal("production's ten unavailable behind worker-local ninety-five")
	}
	if q.protectedSharedFree() != 100 || len(q.s.StorageProtection.Custody) != 1 {
		t.Fatal("experiment custody consumed shared room or was discarded")
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("unique result deleted", err)
	}
}

func TestStorageAdmissionProtectedDeficitConsidersOtherQualifiedHosts(t *testing.T) {
	q, c, _ := protectionQueue(t)
	q.s.StorageAdmissionActive = true
	m, other := protectionMachine("full", "fake"), protectionMachine("available", "fake")
	b := protectionBackend(m, other)
	for i := range b.snapshot.Workers {
		b.snapshot.Workers[i].ExplorationBytes = 6
	}
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, c)); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, nil)
	q.s.StorageProtection.Allocations["retained"] = StorageAllocation{ID: "retained", Execution: Execution{ID: "protected-holder"}, Backing: b.snapshot.Workers[0].ProductionBacking, Bytes: 100 << 20, Files: 100}
	j := submit(t, q, protectionSpec("production"))
	reason := q.diskWaitingReason(job(t, q, j.ID), "")
	if !strings.Contains(reason, "deficit=6 bytes") || !strings.Contains(reason, "protected-holder") {
		t.Fatal("missing protected-backing deficit", reason)
	}
	if len(alertsOf(q, "production_storage_capacity")) != 1 {
		t.Fatal("protected production shortage did not alert")
	}
	offer := check(t, q, other, nil).Job
	if offer == nil || offer.ID != j.ID || offer.Machine != other.ID {
		t.Fatal("full worker globally paused eligible alternate")
	}
	if len(q.s.StorageProtection.Allocations) != 2 || q.s.Health.Conditions["storage-admission/"+j.ID].Active {
		t.Fatal("lost installed commitment or did not clear alert")
	}
}

func TestStorageAdmissionActualProductionResultYieldsEstimate(t *testing.T) {
	q, _ := fixture(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	q.files.limit = 16
	client := deliveryClient(t, q)
	m := machine("writer", "fake")
	high := submit(t, q, storageAdmissionSpec("production", 8))
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != high.ID {
		t.Fatal("missing production assignment")
	}
	low := submit(t, q, storageAdmissionSpec("experiment", 8))
	assigned.Spec.Run.Workdir = t.TempDir()
	if err := os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("123456789"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := (Worker{Client: client}).finishFileResult(*assigned, result(assigned, true), t.TempDir())
	if err != nil {
		t.Fatal("actual production result blocked by lower queued estimate", err)
	}
	if !job(t, q, low.ID).OutputAdmissionPending {
		t.Fatal("did not yield unstarted lower-tier result commitment")
	}
	if err = q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if status := q.fileStoreStatus(context.Background()); status.UsedBytes != 9 || status.ReservedOutputBytes != 0 {
		t.Fatal("result and estimate charged twice", status)
	}
}

func TestStorageAdmissionProductionInputPressureAlertAndRecovery(t *testing.T) {
	q, c := fixture(t)
	q.s.StorageAdmissionActive = true
	client := retentionClient(t, q)
	q.files.limit = 8
	_, unique := retentionUpload(t, client, "12345678")
	p := q.PriorityPolicy()
	p.Policy.Projects = map[string]ProjectPolicy{"service": {DefaultCallPriority: "production", Weight: 1}}
	if _, err := q.SetPriorityPolicy(p.Revision, p.Policy); err != nil {
		t.Fatal(err)
	}
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "service", Class: "production"})
	_, ref := policyInput(t, "sixsix")
	for range 2 {
		assertFilePolicyRefusal(t, client.PreflightFileStore(ctx, FileManifest{"input": ref}), "shortfall=6")
	}
	events := alertsOf(q, "production_storage_capacity")
	if len(events) != 1 || events[0].Project != "service" || !strings.Contains(events[0].Reason, "deficit=6 bytes") || !strings.Contains(events[0].Reason, "unmarked imports") {
		t.Fatalf("write pressure missing attribution/deficit: %+v", events)
	}
	q.files.limit = 14
	if err := client.PreflightFileStore(ctx, FileManifest{"input": ref}); err != nil {
		t.Fatal(err)
	}
	c.Advance(healthAlertCooldown)
	q.files.limit = 8
	assertFilePolicyRefusal(t, client.PreflightFileStore(ctx, FileManifest{"input": ref}), "shortfall=6")
	if len(alertsOf(q, "production_storage_capacity")) != 2 {
		t.Fatal("input recovery did not rearm pressure alert")
	}
	if err := q.files.verify(context.Background(), unique); err != nil {
		t.Fatal("protected import deleted to resolve pressure", err)
	}
}

func TestStorageAdmissionProductionResultPressurePreservesUniqueBytes(t *testing.T) {
	q, _ := fixture(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	q.files.limit = 16
	client := deliveryClient(t, q)
	_, unique := retentionUpload(t, client, "preserve")
	high := submit(t, q, storageAdmissionSpec("production", 8))
	m := machine("writer", "fake")
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != high.ID {
		t.Fatal("missing production")
	}
	assigned.Spec.Run.Workdir = t.TempDir()
	if err := os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("123456789"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := (Worker{Client: client}).finishFileResult(*assigned, result(assigned, true), t.TempDir())
	assertFilePolicyRefusal(t, err, "shortfall=1")
	events := alertsOf(q, "production_storage_capacity")
	if len(events) != 1 || events[0].JobID != high.ID || !strings.Contains(events[0].Reason, "deficit=1 bytes") {
		t.Fatalf("missing exact result pressure alert: %+v", events)
	}
	if job(t, q, high.ID).State != "running" {
		t.Fatal("capacity hold manufactured compute completion")
	}
	if err := q.files.verify(context.Background(), unique); err != nil {
		t.Fatal("unique holder deleted", err)
	}
	if err := q.Cancel(high.ID); err != nil {
		t.Fatal(err)
	}
	if _, exists := q.s.Health.Conditions["storage-write/job/"+high.ID]; exists {
		t.Fatal("cancelled writer retained a pressure condition")
	}
}

func TestStorageAdmissionResultReclaimsBeforeRefusal(t *testing.T) {
	q, c := fixture(t)
	q.s.StorageAdmissionActive = true
	q.deliveryRoot = q.root
	q.files.limit = 16
	client := deliveryClient(t, q)
	high := submit(t, q, storageAdmissionSpec("production", 8))
	assigned := check(t, q, machine("writer", "fake"), nil).Job
	if assigned == nil || assigned.ID != high.ID {
		t.Fatal("missing production")
	}
	_, expired := retentionUpload(t, client, "retired!")
	q.s.RetiredJobFiles = map[string]bool{expired.SHA256: true}
	old := c.Now().Add(-filePublicationGrace - time.Second)
	if err := os.Chtimes(q.files.blob(expired), old, old); err != nil {
		t.Fatal(err)
	}
	assigned.Spec.Run.Workdir = t.TempDir()
	if err := os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("123456789"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := (Worker{Client: client}).finishFileResult(*assigned, result(assigned, true), t.TempDir())
	if err != nil {
		t.Fatal("result refused before reclaiming authorized retired bytes", err)
	}
	if err = q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(q.files.blob(expired)); !os.IsNotExist(err) {
		t.Fatal("retired bytes not collected", err)
	}
	if got := q.fileStoreStatus(context.Background()); got.UsedBytes != 9 {
		t.Fatal("wrong observed reclaimed usage", got)
	}
}
