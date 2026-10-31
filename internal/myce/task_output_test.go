package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type taskOutputBoundary struct {
	fakeStorageBoundary
	collected  int
	collectErr error
}

func (b *taskOutputBoundary) Collect(context.Context, StorageAllocation) error {
	b.collected++
	return b.collectErr
}

type taskOutputFixture struct {
	q        *Queue
	c        *fakeClock
	db       string
	w        Worker
	store    *workerStore
	j        Job
	r        Result
	nas      string
	boundary *taskOutputBoundary
}

func newTaskOutputFixture(t *testing.T, kind, retention string, queued ...func(*Queue, Machine)) *taskOutputFixture {
	t.Helper()
	q, c, db := protectionQueue(t)
	m := protectionMachine("producer", "fake")
	m.Capabilities = append(m.Capabilities, taskOutputCapability)
	m.OutputDestinationIDs = []string{"archive"}
	if err := q.ConfigureStorageProtection(context.Background(), protectionBackend(m), protectionControlWork(t, c)); err != nil {
		t.Fatal(err)
	}
	if len(queued) > 0 {
		check(t, q, m, nil)
	}
	s := protectionSpec("experiment")
	s.SubmissionKey = "destination-result"
	s.OutputDestination = &OutputDestination{Protocol: taskOutputProtocol, Kind: kind, ID: "archive"}
	s.OutputRetention = retention
	s.Outputs["second.txt"] = "other"
	submitted := submit(t, q, s)
	for _, hook := range queued {
		hook(q, m)
	}
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != submitted.ID {
		t.Fatal("not assigned")
	}
	boundary := &taskOutputBoundary{}
	nas := t.TempDir()
	identity, err := ObserveStorageBackingIdentity(nas)
	if err != nil {
		t.Fatal(err)
	}
	w := Worker{Clock: c, Machine: m, StorageBoundary: boundary, Client: deliveryClient(t, q), OutputDestinations: map[string]OutputDestinationRoot{"archive": {Path: nas, NativeIdentity: identity}}, outputDeliverySlots: make(chan struct{}, 1)}
	root := &workerStore{dir: newTestStateRoot(t), state: workerState{Session: m.Session}}
	store, err := newAttemptStore(root, *assigned, w)
	if err != nil {
		t.Fatal(err)
	}
	j := *store.state.Job
	j.Spec.Run.Workdir = filepath.Join(store.dir, "job-files", j.ID, fmt.Sprint(j.Attempt))
	if err = os.MkdirAll(j.Spec.Run.Workdir, 0700); err != nil {
		t.Fatal(err)
	}
	for name := range j.Spec.Outputs {
		if err = os.WriteFile(filepath.Join(j.Spec.Run.Workdir, name), []byte("unique "+name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	r := result(&j, true)
	store.state.Job, store.state.Result = &j, &r
	if err = store.save(); err != nil {
		t.Fatal(err)
	}
	return &taskOutputFixture{q: q, c: c, db: db, w: w, store: store, j: j, r: r, nas: nas, boundary: boundary}
}

func (f *taskOutputFixture) step(t *testing.T) error {
	t.Helper()
	_, err := f.w.finishTaskOutputs(f.j, f.r, f.store.dir)
	return err
}
func (f *taskOutputFixture) pending(t *testing.T) {
	t.Helper()
	if err := f.step(t); !errors.Is(err, errStorageCustodyPending) {
		t.Fatal("expected retained custody", err)
	}
}
func taskOutputAssertSpool(t *testing.T, f *taskOutputFixture, present bool) {
	t.Helper()
	_, err := os.Stat(filepath.Join(deliverySpoolPath(f.j, f.store.dir), "data", "out.txt"))
	if present && err != nil || !present && !os.IsNotExist(err) {
		t.Fatalf("spool present=%t: %v", present, err)
	}
}

func TestTaskOutputsNASOutageResumeAndAutomaticCollection(t *testing.T) {
	f := newTaskOutputFixture(t, "nas", "")
	// The destination goes away after compute, including with an otherwise
	// healthy queue. No shared copy is ever used as an outage fallback.
	if err := os.Rename(f.nas, f.nas+"-offline"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Rename(f.nas+"-offline", f.nas) })
	f.pending(t)
	j := job(t, f.q, f.j.ID)
	if j.State != "succeeded" || !j.ReleaseConfirmed || j.Spec.OutputRetention != "until_delivered" || len(j.OutputRefs) != 0 || j.LocalOutputs == nil || j.Delivery.State != "failed" || j.Delivery.VerifiedAt != nil {
		t.Fatalf("false availability: %+v", j)
	}
	if got, err := f.q.files.inventory(context.Background()); err != nil || len(got) != 0 {
		t.Fatal("unique output entered shared store", got, err)
	}
	// Retention cannot discard undelivered unique bytes, even after 168 hours.
	f.c.Advance(DefaultFileRetention + time.Hour)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	f.pending(t)
	taskOutputAssertSpool(t, f, true)
	if job(t, f.q, f.j.ID).FilesExpired {
		t.Fatal("pending delivery expired")
	}
	if err := os.Rename(f.nas+"-offline", f.nas); err != nil {
		t.Fatal(err)
	}
	f.c.Advance(deliveryRetryDelay)
	f.pending(t)
	j = job(t, f.q, f.j.ID)
	if j.Delivery.State != "verified" || j.Delivery.VerifiedAt == nil || *j.Delivery.VerifiedAt != f.c.Now() {
		t.Fatal("delivery not verified by injected clock", j.Delivery)
	}
	for name, prefix := range f.j.Spec.Outputs {
		path := filepath.Join(f.nas, prefix, f.j.ID, fmt.Sprint(f.j.Attempt), name)
		if err := verifyFile(context.Background(), path, j.LocalOutputs.Files[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal("authorized collection failed", err)
	}
	taskOutputAssertSpool(t, f, false)
	j = job(t, f.q, f.j.ID)
	if !j.FilesExpired || j.LocalOutputs.CollectedAt == nil || len(f.q.s.StorageProtection.Allocations) != 0 || len(f.q.s.StorageProtection.Custody) != 0 || f.boundary.collected != 1 {
		t.Fatal("collection did not release charged custody")
	}
	var expiry, verified, collected int
	for _, e := range j.LedgerEntries {
		switch e.Kind {
		case fileReleaseLedgerKind:
			expiry++
		case "output_delivery_verified":
			verified++
		case "worker_outputs_collected":
			collected++
		}
	}
	if expiry != 1 || verified != 1 || collected != 1 {
		t.Fatal("missing/duplicate ledger receipts", expiry, verified, collected)
	}
	// Collection intent survives ack loss: replay never recaptures absent output.
	if err := f.step(t); err != nil {
		t.Fatal("collection replay failed", err)
	}
	if f.boundary.collected != 1 {
		t.Fatal("collected boundary repeated after confirmed ACK")
	}
}

func TestTaskOutputsCustodyPrecedesComputeRelease(t *testing.T) {
	f := newTaskOutputFixture(t, "submitter", "")
	path := filepath.Join(f.j.Spec.Run.Workdir, "out.txt")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = f.step(t); err == nil {
		t.Fatal("missing unique output accepted")
	}
	j := job(t, f.q, f.j.ID)
	if j.ReleaseConfirmed || j.LocalOutputs != nil || len(f.q.s.StorageProtection.Allocations) != 1 {
		t.Fatal("compute released before synced custody was recorded")
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f.pending(t)
	j = job(t, f.q, j.ID)
	if !j.ReleaseConfirmed || j.LocalOutputs == nil || j.Delivery.State != "pending" || len(f.q.s.StorageProtection.Allocations) != 1 {
		t.Fatal("custody receipt did not atomically settle compute and retain storage")
	}
}

func TestTaskOutputsRestartRechecksSyncedCustodyContent(t *testing.T) {
	f := newTaskOutputFixture(t, "submitter", "")
	f.pending(t)
	path := filepath.Join(deliverySpoolPath(f.j, f.store.dir), "data", "out.txt")
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	// A new supervisor has no process-local verification. An exact accepted
	// queue receipt alone cannot stand in for rechecking the saved spool.
	if err := f.step(t); err == nil || errors.Is(err, errStorageCustodyPending) {
		t.Fatal("restart trusted receipt without checking source content", err)
	}
	j := job(t, f.q, f.j.ID)
	if j.Delivery.State != "pending" || j.LocalOutputs.CollectedAt != nil || len(f.q.s.StorageProtection.Allocations) != 1 {
		t.Fatal("corrupt source authorized success or release")
	}
}

func TestTaskOutputsLocalCustodyCannotExpireUnknownSharedImport(t *testing.T) {
	f := newTaskOutputFixture(t, "nas", "")
	f.pending(t)
	j := job(t, f.q, f.j.ID)
	ref := j.LocalOutputs.Files["out.txt"]
	source := filepath.Join(deliverySpoolPath(f.j, f.store.dir), "data", "out.txt")
	if err := f.w.Client.UploadFile(context.Background(), source, ref); err != nil {
		t.Fatal(err)
	}
	old := f.c.Now().Add(-DefaultFileRetention - filePublicationGrace)
	if err := os.Chtimes(f.q.files.blob(ref), old, old); err != nil {
		t.Fatal(err)
	}
	f.c.Advance(DefaultFileRetention)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if err := verifyFile(context.Background(), f.q.files.blob(ref), ref); err != nil {
		t.Fatal("local-only expiry deleted an unrelated shared import", err)
	}
	taskOutputAssertSpool(t, f, false)
}

func TestTaskOutputsPartialNASStaleAndReceiptReplay(t *testing.T) {
	f := newTaskOutputFixture(t, "submitter", "")
	f.pending(t)
	j := job(t, f.q, f.j.ID)
	snapshot := cloneJob(j)
	// Two destination prefixes exercise partial publication: the first complete
	// tree is durable, but the second source disappears during transfer.
	deleteSource := filepath.Join(deliverySpoolPath(f.j, f.store.dir), "data", "out.txt")
	content, err := os.ReadFile(deleteSource)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(deleteSource); err != nil {
		t.Fatal(err)
	}
	snapshot.Spec.OutputDestination.Kind = "nas"
	snapshot.Delivery.ManifestSHA256 = deliveryDigest(snapshot)
	if err = publishTaskOutputDelivery(snapshot, filepath.Dir(deleteSource), f.nas, func() { f.q.mu.Lock(); f.q.mu.Unlock() }); err == nil {
		t.Fatal("partial copy claimed delivery")
	}
	first := filepath.Join(f.nas, "other", f.j.ID, fmt.Sprint(f.j.Attempt), "second.txt")
	if _, err = os.Stat(first); err != nil {
		t.Fatal("first prefix not durably published", err)
	}
	if err = os.WriteFile(deleteSource, content, 0600); err != nil {
		t.Fatal(err)
	}
	if err = publishTaskOutputDelivery(snapshot, filepath.Dir(deleteSource), f.nas); err != nil {
		t.Fatal("partial delivery did not resume", err)
	}
	if err = publishTaskOutputDelivery(snapshot, filepath.Dir(deleteSource), f.nas); err != nil {
		t.Fatal("receipt replay failed", err)
	}
	if err = os.WriteFile(first, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = publishTaskOutputDelivery(snapshot, filepath.Dir(deleteSource), f.nas); err == nil {
		t.Fatal("receipt existence bypassed content verification")
	}
	confirmed := taskOutputReceipt(*j)
	staleReceipt := confirmed
	staleReceipt.Execution.Attempt++
	if err = f.q.ConfirmTaskOutputs(context.Background(), staleReceipt); !errors.Is(err, ErrStale) {
		t.Fatal("stale delivery accepted", err)
	}
	staleReceipt = confirmed
	staleReceipt.ManifestSHA256 = strings.Repeat("f", 64)
	if err = f.q.ConfirmTaskOutputs(context.Background(), staleReceipt); !errors.Is(err, ErrStale) {
		t.Fatal("conflicting manifest accepted", err)
	}
	if err = f.q.CollectedTaskOutputs(context.Background(), confirmed); err == nil {
		t.Fatal("undelivered output collection accepted")
	}
	taskOutputAssertSpool(t, f, true)
}

func TestTaskOutputsRetentionGraceReferencesAndBackingRelease(t *testing.T) {
	for _, retention := range []string{"", "retained"} {
		t.Run(retention, func(t *testing.T) {
			f := newTaskOutputFixture(t, "nas", retention)
			f.pending(t)
			if err := f.q.Tick(); err != nil {
				t.Fatal(err)
			}
			f.pending(t) // Six-hour local publication grace, or explicit recovery.
			taskOutputAssertSpool(t, f, true)
			f.c.Advance(filePublicationGrace)
			if err := f.q.Tick(); err != nil {
				t.Fatal(err)
			}
			f.pending(t) // Delivery plus grace alone never triggers collection.
			taskOutputAssertSpool(t, f, true)
			f.c.Advance(DefaultFileRetention)
			// A different retained reference keeps its shared/worker copy. The
			// collector rechecks the live graph rather than trusting an old plan.
			j := job(t, f.q, f.j.ID)
			ref := j.LocalOutputs.Files["out.txt"]
			if err := f.q.update(func() error {
				f.q.putJob("downstream", &Job{ID: "downstream", Spec: Spec{Files: FileManifest{"input": ref}}, State: "queued"})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.q.Tick(); err != nil {
				t.Fatal(err)
			}
			f.pending(t)
			taskOutputAssertSpool(t, f, true)
			if err := f.q.update(func() error { other := f.q.touchJob("downstream"); other.Spec.Files = nil; return nil }); err != nil {
				t.Fatal(err)
			}
			f.boundary.collectErr = errors.New("boundary release interrupted")
			if err := f.step(t); err == nil || errors.Is(err, errStorageCustodyPending) {
				t.Fatal("partial collection not attempted", err)
			}
			if len(f.q.s.StorageProtection.Allocations) != 1 {
				t.Fatal("failed boundary release freed capacity")
			}
			f.boundary.collectErr = nil
			if err := f.step(t); err != nil {
				t.Fatal("partial collection replay failed", err)
			}
			if len(f.q.s.StorageProtection.Allocations) != 0 {
				t.Fatal("collected backing remained charged")
			}
		})
	}
}

func TestTaskOutputsCustodyRestartLossAndLegacyPolicy(t *testing.T) {
	f := newTaskOutputFixture(t, "submitter", "")
	f.pending(t)
	old := job(t, f.q, f.j.ID)
	receipt := taskOutputReceipt(*old)
	if err := f.q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err := Open(f.db, f.q.root, f.c, WithFileStore(filepath.Join(filepath.Dir(f.db), "files"), 185<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	f.q = q
	f.w.Client = deliveryClient(t, q)
	if err = q.RecordStorageCustody(context.Background(), receipt.StorageCustodyReceipt); err != nil {
		t.Fatal("custody ACK replay lost after SQLite reopen", err)
	}
	f.c.Advance(DefaultFileRetention + time.Hour)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	j := job(t, q, old.ID)
	if j.LocalOutputs == nil || j.FilesExpired || !maps.Equal(j.LocalOutputs.Files, old.LocalOutputs.Files) || len(q.s.StorageProtection.Allocations) != 1 {
		t.Fatal("lost source erased custody")
	}
	found := false
	for _, e := range q.s.Health.Events {
		found = found || e.Kind == "worker_output_custodian_unavailable" && e.JobID == j.ID && e.Project == j.Spec.Project && len(e.Recipients) == 2
	}
	if !found {
		t.Fatal("missing custodian did not alert owner")
	}
	staleReceipt := receipt.StorageCustodyReceipt
	staleReceipt.Session = "other"
	if err = q.RecordStorageCustody(context.Background(), staleReceipt); !errors.Is(err, ErrStale) {
		t.Fatal("replacement session claimed unique result", err)
	}
	taskOutputAssertSpool(t, f, true)
	// Frozen legacy and keyed specs do not acquire the new retention default.
	if err = q.ConfigureStorageProtection(context.Background(), protectionBackend(f.w.Machine), protectionControlWork(t, f.c)); err != nil {
		t.Fatal(err)
	}
	legacy := asyncDeliverySpec()
	q.deliveryRoot = t.TempDir()
	lj := submit(t, q, legacy)
	if lj.Spec.OutputRetention != "" || lj.Spec.OutputDestination != nil {
		t.Fatal("legacy policy rewritten")
	}
}

func TestTaskOutputsDestinationIdentityAndClientGate(t *testing.T) {
	f := newTaskOutputFixture(t, "nas", "")
	f.w.OutputDestinations["archive"] = OutputDestinationRoot{Path: f.nas, NativeIdentity: strings.Repeat("f", 64)}
	f.pending(t)
	if job(t, f.q, f.j.ID).Delivery.State != "failed" {
		t.Fatal("replacement volume accepted")
	}
	taskOutputAssertSpool(t, f, true)
	for _, s := range []Spec{
		{OutputDestination: &OutputDestination{Protocol: "old", Kind: "nas", ID: "archive"}},
		{OutputRetention: "until_delivered"},
	} {
		if validateTaskOutputs(s) == nil {
			t.Fatal("invalid/ungated contract accepted")
		}
	}
	data, _ := json.Marshal(f.q.s.StorageProtection)
	if strings.Contains(string(data), f.store.dir) || strings.Contains(string(data), f.nas) {
		t.Fatal("private root leaked into custody")
	}
}

func TestTaskOutputsStartRequiresDeliveryAndStorageCapabilities(t *testing.T) {
	for _, missing := range []string{"protocol", "destination", "storage"} {
		t.Run(missing, func(t *testing.T) {
			f := newTaskOutputFixture(t, "nas", "")
			if err := f.q.update(func() error {
				copy := *f.q.s.Machines[f.j.Machine]
				m := &copy
				f.q.s.Machines[f.j.Machine] = m
				switch missing {
				case "protocol":
					m.Capabilities = []string{boundedStorageCapability}
				case "destination":
					m.OutputDestinationIDs = nil
				case "storage":
					p := *f.q.s.StorageProtection
					p.Enabled = false
					f.q.s.StorageProtection = &p
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.q.Started(result(&f.j, false)); err == nil {
				t.Fatal("destination task started without", missing)
			}
		})
	}
}

func TestTaskOutputsWorkerActorReleasesComputeAndCollects(t *testing.T) {
	f := newTaskOutputFixture(t, "nas", "")
	clock := &workerFakeClock{now: f.c.Now()}
	f.q.clock, f.w.Clock = clock, clock
	f.w.Interval = 10 * time.Millisecond
	f.w.control = &processControl{clock: clock}
	actor := &attemptSupervisor{store: f.store, job: f.j, worker: f.w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.run(ctx)
	t.Cleanup(func() { cancel(); <-actor.done })
	await(t, func() bool { j := job(t, f.q, f.j.ID); return j.Delivery != nil && j.Delivery.State == "verified" })
	j := job(t, f.q, f.j.ID)
	if !j.ReleaseConfirmed || j.LocalOutputs == nil || j.LocalOutputs.CollectedAt != nil {
		t.Fatal("compute did not release independently of storage")
	}
	// The normal keyed heartbeat keeps the delivery actor, rather than killing
	// it as a terminal process. Chunk progress uses the queue's injected Clock.
	report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, OutputDeliveryProgress: 7}
	var reply Reply
	if err := f.w.Client.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: f.w.Machine, Attempts: []AttemptReport{report}}, &reply); err != nil {
		t.Fatal(err)
	}
	for _, d := range reply.Attempts {
		if d.ID == j.ID && (d.Stop || d.Kill) {
			t.Fatal("output custody was treated as a live process")
		}
	}
	clock.Advance(DefaultFileRetention)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		select {
		case <-actor.done:
			return true
		default:
			return false
		}
	})
	_, _, _, err := actor.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if job(t, f.q, f.j.ID).LocalOutputs.CollectedAt == nil || f.store.state.Job != nil {
		t.Fatal("worker actor did not finish authorized collection")
	}
	taskOutputAssertSpool(t, f, false)
}

func TestTaskOutputsDestinationFailsDuringCopy(t *testing.T) {
	f := newTaskOutputFixture(t, "nas", "")
	var interrupted bool
	f.w.outputDeliveryProgress = func() {
		f.q.mu.Lock()
		f.q.mu.Unlock() // All delivery I/O permits control transactions.
		stage := filepath.Join(f.nas, "other", f.j.ID, ".delivery-1")
		if st, err := os.Stat(filepath.Join(stage, "second.txt")); err == nil && st.Size() > 0 && !interrupted {
			interrupted = true
			if err = os.RemoveAll(stage); err != nil {
				t.Error(err)
			}
		}
	}
	f.pending(t)
	if !interrupted || job(t, f.q, f.j.ID).Delivery.State != "failed" {
		t.Fatal("mid-transfer outage became verified delivery")
	}
	taskOutputAssertSpool(t, f, true)
	if payloads, err := f.q.files.inventory(context.Background()); err != nil || len(payloads) != 0 {
		t.Fatal("outage spilled unique outputs into shared store", payloads, err)
	}
	f.w.outputDeliveryProgress = nil
	f.c.Advance(deliveryRetryDelay)
	f.pending(t)
	if job(t, f.q, f.j.ID).Delivery.State != "verified" {
		t.Fatal("delivery did not resume")
	}
}

func TestTaskOutputsFenceRecheckedAfterPublication(t *testing.T) {
	f := newTaskOutputFixture(t, "submitter", "")
	f.pending(t)
	j := job(t, f.q, f.j.ID)
	var once sync.Once
	err := publishTaskOutputDelivery(*j, filepath.Join(deliverySpoolPath(f.j, f.store.dir), "data"), f.nas, func() {
		once.Do(func() {
			if err := f.q.update(func() error { current := f.q.touchJob(j.ID); current.Session = "replacement"; return nil }); err != nil {
				t.Error(err)
			}
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.q.ConfirmTaskOutputs(context.Background(), taskOutputReceipt(*j)); !errors.Is(err, ErrStale) {
		t.Fatal("publication bypassed post-I/O session fence", err)
	}
	if err = f.q.CollectedTaskOutputs(context.Background(), taskOutputReceipt(*j)); !errors.Is(err, ErrStale) {
		t.Fatal("stale publication authorized collection", err)
	}
	taskOutputAssertSpool(t, f, true)
}
