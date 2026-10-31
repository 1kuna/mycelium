package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

// This backend is deliberately a fake. It proves control/admission contracts,
// never native block/inode denial or host confinement qualification.
type fakeStorageProtection struct {
	snapshot  StorageProtectionSnapshot
	protected ControlStorageDemand
	protect   func()
}

func (b *fakeStorageProtection) Protect(_ context.Context, c ControlStorageDemand) error {
	if b.protect != nil {
		b.protect()
	}
	b.protected = c
	return nil
}
func (b *fakeStorageProtection) Snapshot(context.Context) (StorageProtectionSnapshot, error) {
	return b.snapshot, nil
}

type fakeStorageBoundary struct {
	allocations []StorageAllocation
	paths       []StorageWritePaths
	deny        bool
	err         error
}

func (b *fakeStorageBoundary) Bind(_ context.Context, a StorageAllocation, p StorageWritePaths) error {
	b.allocations = append(b.allocations, a)
	b.paths = append(b.paths, p)
	return b.err
}
func (b *fakeStorageBoundary) Denied(context.Context, StorageAllocation) (bool, error) {
	return b.deny, b.err
}

func protectionBackend(machines ...Machine) *fakeStorageProtection {
	b := &fakeStorageProtection{snapshot: StorageProtectionSnapshot{
		Pools: []StoragePool{{ID: "shared", FreeBytes: 100 << 20, FreeFiles: 10000}}, SharedPool: "shared", ControlBacking: "control", ControlIsolated: true, UnrelatedWritersIsolated: true,
		Backings: []StorageBacking{{ID: "control", Pool: "shared", Kind: "allocated", CapacityBytes: 8 << 20, CapacityFiles: 10000, BackedBytes: 9 << 20, OverheadBytes: 1 << 20, Verified: true}},
	}}
	for i, m := range machines {
		id := fmt.Sprintf("worker-%d", i)
		b.snapshot.Backings = append(b.snapshot.Backings, StorageBacking{ID: id, Pool: "shared", Kind: "allocated", CapacityBytes: 100 << 20, CapacityFiles: 10000, BackedBytes: 101 << 20, OverheadBytes: 1 << 20, Verified: true})
		prod := id + "-production"
		b.snapshot.Backings = append(b.snapshot.Backings, StorageBacking{ID: prod, Pool: "shared", Kind: "allocated", CapacityBytes: 100 << 20, CapacityFiles: 10000, BackedBytes: 101 << 20, OverheadBytes: 1 << 20, Verified: true})
		b.snapshot.Workers = append(b.snapshot.Workers, StorageWorkerBacking{ProductionBacking: prod, Machine: m.ID, Session: m.Session, Backing: id, Confined: true, ExplorationBytes: 96 << 20, ExplorationFiles: 100})
	}
	for i := range b.snapshot.Backings {
		// Fake volume identity for accounting tests; native proof is separate.
		b.snapshot.Backings[i].NativeIdentity = identityHash("fake-native-" + b.snapshot.Backings[i].ID)
	}
	return b
}

func protectionControlWork(t *testing.T, clock Clock) ControlStorageDemand {
	t.Helper()
	dir := t.TempDir()
	dbpath := filepath.Join(dir, "working.db")
	db, err := sql.Open("sqlite", dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{"PRAGMA journal_mode=WAL", "CREATE TABLE rehearsal (value BLOB)", "INSERT INTO rehearsal VALUES(zeroblob(16384))"} {
		if _, err = db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	// These are concurrently live files from an operation rehearsal: original
	// SQLite/WAL, bounded logs, sync copy, checkpoint copy and backend metadata.
	work := ControlStorageWork{SQLite: []string{dbpath, dbpath + "-wal", dbpath + "-shm"}}
	for _, category := range []*[]string{&work.Logs, &work.Sync, &work.Checkpoint, &work.Metadata} {
		path := filepath.Join(dir, fmt.Sprintf("work-%d", len(*category)))
		path, err = func() (string, error) {
			f, e := os.CreateTemp(dir, "working-")
			if e != nil {
				return "", e
			}
			p := f.Name()
			_, e = f.Write(make([]byte, 8192))
			return p, errors.Join(e, f.Sync(), f.Close())
		}()
		if err != nil {
			t.Fatal(err)
		}
		*category = []string{path}
	}
	measured, err := MeasureControlStorage(context.Background(), clock, work)
	if err != nil {
		t.Fatal(err)
	}
	if measured.SQLiteBytes == 0 || measured.LogBytes == 0 || measured.SyncBytes == 0 || measured.CheckpointBytes == 0 || measured.MetadataBytes == 0 || measured.MeasuredAt != clock.Now() {
		t.Fatalf("incomplete control measurement: %+v", measured)
	}
	// Repeated aliases/hardlinks add no capacity, bytes or inode pressure.
	alias := filepath.Join(dir, "hardlink")
	if err = os.Link(dbpath, alias); err != nil {
		t.Fatal(err)
	}
	work.SQLite = append(work.SQLite, dbpath, alias)
	dedup, err := MeasureControlStorage(context.Background(), clock, work)
	if err != nil || dedup != measured {
		t.Fatalf("control alias charged twice: %+v %+v %v", measured, dedup, err)
	}
	return measured
}

func protectionSpec(priority string) Spec {
	s := spec(priority)
	s.Resumable = false
	s.Outputs = map[string]string{"out.txt": "results"}
	s.StorageProtocol = workerCustodyProtocol
	return s
}
func protectionQueue(t *testing.T) (*Queue, *fakeClock, string) {
	t.Helper()
	dir := t.TempDir()
	c := &fakeClock{now: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	dbpath := filepath.Join(dir, "queue.db")
	q, err := Open(dbpath, filepath.Join(dir, "checkpoints"), c, WithFileStore(filepath.Join(dir, "files"), 185<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, q, true)
	t.Cleanup(func() { _ = q.Close() })
	return q, c, dbpath
}

func TestStorageProtectionMeasuredControlBacking(t *testing.T) {
	q, c, _ := protectionQueue(t)
	control := protectionControlWork(t, c)
	m := protectionMachine("worker", "fake")
	b := protectionBackend(m)
	b.protect = func() { // The backend must be able to acquire q.mu during I/O.
		q.mu.Lock()
		q.mu.Unlock()
	}
	if err := q.ConfigureStorageProtection(context.Background(), b, control); err != nil {
		t.Fatal(err)
	}
	if b.protected != control || !q.storageProtectionVerified {
		t.Fatal("measured control demand was not backed")
	}
	if got := q.protectedSharedFree(); got != 100<<20 {
		t.Fatalf("fully allocated backing deducted twice: %d", got)
	}
	// Native reserve's still-free bytes are removed exactly once, including aliases.
	reserve := b.snapshot.Backings[1]
	reserve.Kind = "native-reserve"
	reserve.ExcludedFreeBytes = 20 << 20
	b.snapshot.Backings[1] = reserve
	b.snapshot.Backings = append(b.snapshot.Backings, reserve)
	// Fresh fixture: retained allocation backing is deliberately immutable.
	q.s.StorageProtection = nil
	if err := q.ConfigureStorageProtection(context.Background(), b, control); err != nil {
		t.Fatal(err)
	}
	if got := q.protectedSharedFree(); got != 80<<20 {
		t.Fatalf("aliased native reserve counted incorrectly: %d", got)
	}
	q.files.diskProbe = func(string) (DiskReading, error) { return DiskReading{TotalBytes: 1 << 30, FreeBytes: 400 << 20}, nil }
	status, _, err := q.files.capacity(context.Background())
	if err != nil || status.FreeBytes != (400<<20)-diskFreeFloor(1<<30)-(20<<20) {
		t.Fatalf("fresh capacity missed backing reserve: %+v %v", status, err)
	}
}

// Regression for #506 comment 6547502293: consuming a native reserve changes
// unused reservation and physical free together, not the backing's identity.
func TestStorageProtectionNativeReserveRefresh(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			q, c, dbpath := protectionQueue(t)
			m, producer := protectionMachine("experiment", "fake"), protectionMachine("production", "fake")
			b := protectionBackend(m, producer)
			b.snapshot.Pools[0].FreeBytes = 600 << 20
			b.snapshot.Backings[1].Kind = "native-reserve"
			b.snapshot.Backings[1].ExcludedFreeBytes = 100 << 20
			control := protectionControlWork(t, c)
			if err := q.ConfigureStorageProtection(context.Background(), b, control); err != nil {
				t.Fatal(err)
			}
			submit(t, q, protectionSpec("experiment"))
			experiment := check(t, q, m, nil).Job
			if experiment == nil {
				t.Fatal("missing allocation")
			}
			refs := FileManifest{"out.txt": {SHA256: identityHash("unique experiment"), SizeBytes: 13}}
			receipt := StorageCustodyReceipt{Protocol: workerCustodyProtocol, Execution: Execution{ID: experiment.ID, Attempt: experiment.Attempt}, Machine: m.ID, Session: m.Session, Allocation: experiment.StorageAllocation.ID, Files: refs}
			if err := q.RecordStorageCustody(context.Background(), receipt); err != nil {
				t.Fatal(err)
			}
			allocations, custody := maps.Clone(q.s.StorageProtection.Allocations), maps.Clone(q.s.StorageProtection.Custody)
			// Fill, then empty the unchanged reserve. Reopen SQLite before each
			// verification in the restart case.
			for _, unused := range []uint64{80 << 20, 0, 40 << 20, 100 << 20} {
				b.snapshot.Pools[0].FreeBytes = (500 << 20) + unused
				b.snapshot.Backings[1].ExcludedFreeBytes = unused
				if restart {
					root, files := q.root, q.files.root
					if err := q.Close(); err != nil {
						t.Fatal(err)
					}
					var err error
					q, err = Open(dbpath, root, c, WithFileStore(files, 185<<30, true))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = q.Close() })
					fakeCheckpointAvailability(t, q, true)
				}
				q.files.diskProbe = func(string) (DiskReading, error) {
					return DiskReading{TotalBytes: 1 << 30, FreeBytes: b.snapshot.Pools[0].FreeBytes}, nil
				}
				err := q.ConfigureStorageProtection(context.Background(), b, control)
				status, _, uploadErr := q.files.capacity(context.Background())
				if err != nil || uploadErr != nil || !q.storageProtectionVerified {
					t.Fatalf("same native backing cannot refresh: configure=%v; file capacity=%v; verified=%v", err, uploadErr, q.storageProtectionVerified)
				}
				if q.protectedSharedFree() != 500<<20 || status.FreeBytes != (500<<20)-diskFreeFloor(1<<30) {
					t.Fatal("reserve consumption changed shared production capacity", status.FreeBytes)
				}
				if !reflect.DeepEqual(allocations, q.s.StorageProtection.Allocations) || !reflect.DeepEqual(custody, q.s.StorageProtection.Custody) {
					t.Fatal("refresh lost retained allocation or custody")
				}
			}
			// Exercise real production HTTP upload and completion after filling and
			// emptying the reserve, rather than checking the verification flag only.
			production := submit(t, q, protectionSpec("production"))
			assigned := check(t, q, producer, nil).Job
			if assigned == nil || assigned.ID != production.ID {
				t.Fatal("production held behind unchanged experiment backing", assigned)
			}
			if err := q.Started(result(assigned, false)); err != nil {
				t.Fatal(err)
			}
			client := deliveryClient(t, q)
			ctx := withFileStoreAttempt(context.Background(), *assigned)
			_, ref := retentionUploadFile(t, func(_ context.Context, path string, ref FileRef) error {
				return client.UploadFile(ctx, path, ref)
			}, "production output after native reserve consumption")
			completed := result(assigned, true)
			completed.OutputRefs = FileManifest{"out.txt": ref}
			if err := q.Complete(completed); err != nil {
				t.Fatal("production publication failed", err)
			}
			if job(t, q, assigned.ID).State != "succeeded" {
				t.Fatal("production did not complete")
			}
		})
	}
}

func TestStorageProtectionInstalledCapacityChange(t *testing.T) {
	q, c, dbpath := protectionQueue(t)
	m := protectionMachine("producer", "fake")
	b := protectionBackend(m)
	control := protectionControlWork(t, c)
	if err := q.ConfigureStorageProtection(context.Background(), b, control); err != nil {
		t.Fatal(err)
	}
	submit(t, q, protectionSpec("experiment"))
	if check(t, q, m, nil).Job == nil {
		t.Fatal("missing allocation")
	}
	original := q.s.StorageProtection
	queueRoot, files := q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	q, err = Open(dbpath, queueRoot, c, WithFileStore(files, 185<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	b.snapshot.Backings[1].CapacityBytes--
	if err := q.ConfigureStorageProtection(context.Background(), b, control); err == nil || err.Error() != "retained storage backing changed" {
		t.Fatal("installed capacity change was not fenced", err)
	}
	if q.storageProtectionVerified {
		t.Fatal("changed capacity renewed native verification")
	}
	if _, _, err := q.files.capacity(context.Background()); err == nil {
		t.Fatal("changed boundary granted unverified capacity")
	}
	if !reflect.DeepEqual(original, q.s.StorageProtection) {
		t.Fatal("rejected boundary changed retained accounting")
	}
}

func TestStorageProtectionRejectsUnqualifiedBacking(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	control := protectionControlWork(t, c)
	for _, tc := range []struct {
		name   string
		change func(*StorageProtectionSnapshot)
	}{
		{"quota is not availability", func(s *StorageProtectionSnapshot) { s.Backings[1].Kind = "project-quota" }},
		{"sparse maximum", func(s *StorageProtectionSnapshot) { s.Backings[1].BackedBytes = 1 }},
		{"control too small", func(s *StorageProtectionSnapshot) { s.Backings[0].CapacityBytes = control.RequiredBytes - 1 }},
		{"unrelated writers", func(s *StorageProtectionSnapshot) { s.UnrelatedWritersIsolated = false }},
		{"unconfined", func(s *StorageProtectionSnapshot) { s.Workers[0].Confined = false }},
		{"unverified", func(s *StorageProtectionSnapshot) { s.Backings[1].Verified = false }},
		{"missing native identity", func(s *StorageProtectionSnapshot) { s.Backings[1].NativeIdentity = "" }},
		{"shared control", func(s *StorageProtectionSnapshot) { s.Workers[0].Backing = s.ControlBacking }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := protectionBackend(protectionMachine("worker", "fake"))
			tc.change(&b.snapshot)
			if err := validateStorageProtection(b.snapshot, control); err == nil {
				t.Fatal("unsafe backing accepted")
			}
		})
	}
	// Incomplete observations never return a partial demand that could be backed.
	v, err := MeasureControlStorage(context.Background(), c, ControlStorageWork{SQLite: []string{"missing"}})
	if err == nil || v.RequiredBytes != 0 {
		t.Fatal("incomplete control evidence accepted", v, err)
	}
}

func TestStorageProtectionIndependentCustodyAdmission(t *testing.T) {
	q, c, _ := protectionQueue(t)
	m, other := protectionMachine("producer", "fake"), protectionMachine("other", "fake")
	b := protectionBackend(m, other)
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, c)); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, protectionSpec("experiment"))
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID || assigned.StorageAllocation == nil {
		t.Fatal("missing backed assignment", assigned)
	}
	a := assigned.StorageAllocation
	if a.Bytes != 96<<20 || !a.WorkerCustody || a.At != c.Now() || assigned.StorageEnvelope.HardLimit != nil {
		t.Fatalf("missing exact installed demand: %+v", a)
	}
	if reason := q.protectedStorageLaunchReason(assigned); reason != "" {
		t.Fatal("self allocation charged twice", reason)
	}
	// Retained bytes and every active allocation remain inside the worker budget;
	// stopping compute does not manufacture available capacity.
	if err := q.Complete(Result{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Machine: m.ID, Session: m.Session, ReleaseOnly: true}); err != nil {
		t.Fatal(err)
	}
	next := submit(t, q, protectionSpec("experiment"))
	if reason := q.diskHeadroomReason(next, &m); !strings.Contains(reason, "custody budget full") {
		t.Fatal("worker backlog did not hold admissions", reason)
	}
	if reason := q.diskHeadroomReason(next, &other); reason != "" {
		t.Fatal("full worker paused another worker", reason)
	}
	if got := q.protectedSharedFree(); got != 100<<20 {
		t.Fatal("retained worker bytes consumed shared production backing", got)
	}
	if offer := check(t, q, other, nil).Job; offer == nil || offer.ID != next.ID {
		t.Fatal("other host did not remain eligible", offer)
	}
	// No path or volume name is exposed by persisted allocation/custody state.
	data, _ := json.Marshal(q.s.StorageProtection)
	if strings.Contains(string(data), q.root) {
		t.Fatal("private path in public storage evidence")
	}
}

func TestStorageProtectionCustodyHTTPAndRestart(t *testing.T) {
	q, c, path := protectionQueue(t)
	m := protectionMachine("producer", "fake")
	b := protectionBackend(m)
	control := protectionControlWork(t, c)
	if err := q.ConfigureStorageProtection(context.Background(), b, control); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, protectionSpec("experiment"))
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("missing assignment")
	}
	root := &workerStore{dir: t.TempDir(), state: workerState{Session: m.Session}}
	boundary := &fakeStorageBoundary{}
	w := Worker{Machine: m, Clock: c, StorageBoundary: boundary, Client: deliveryClient(t, q)}
	store, err := newAttemptStore(root, *assigned, w)
	if err != nil {
		t.Fatal(err)
	}
	local := *store.state.Job
	local.Spec.Run.Workdir = filepath.Join(store.dir, "job-files", local.ID, fmt.Sprint(local.Attempt))
	if err = os.MkdirAll(local.Spec.Run.Workdir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(local.Spec.Run.Workdir, "out.txt"), []byte("unique result"), 0600); err != nil {
		t.Fatal(err)
	}
	r := result(&local, true)
	for i := 0; i < 2; i++ {
		got, e := w.finishFileResult(local, r, store.dir)
		if !errors.Is(e, errStorageCustodyPending) || len(got.OutputRefs) != 0 {
			t.Fatal("local custody became shared success", got, e)
		}
	}
	refs := q.s.StorageProtection.Custody[local.StorageAllocation.ID].Files
	if len(refs) != 1 || q.s.StorageProtection.Custody[local.StorageAllocation.ID].At != c.Now() {
		t.Fatal("missing durable custody", refs)
	}
	payloads, e := q.files.inventory(context.Background())
	if e != nil || len(payloads) != 0 {
		t.Fatal("unique experiment bytes entered shared store", payloads, e)
	}
	ctx := withFileStoreAttempt(context.Background(), local)
	if e = w.Client.PreflightFileStore(ctx, refs); e == nil {
		t.Fatal("custody experiment allowed shared upload")
	}
	var refusal *HTTPError
	if !errors.As(e, &refusal) || refusal.Code != "worker_custody_required" {
		t.Fatal("custody refusal lost its typed protocol diagnostic", e)
	}
	if e = q.Complete(result(assigned, true)); e == nil {
		t.Fatal("local custody claimed task/file-store success")
	}
	receipt := StorageCustodyReceipt{Protocol: workerCustodyProtocol, Execution: r.Execution, Machine: m.ID, Session: m.Session, Allocation: local.StorageAllocation.ID, Files: maps.Clone(refs)}
	receipt.Files["out.txt"] = FileRef{SHA256: strings.Repeat("f", 64), SizeBytes: 13}
	if e = q.RecordStorageCustody(context.Background(), receipt); e == nil {
		t.Fatal("conflicting manifest accepted")
	}
	receipt.Execution.Attempt++
	if e = q.RecordStorageCustody(context.Background(), receipt); !errors.Is(e, ErrStale) {
		t.Fatal("stale receipt accepted", e)
	}
	// A crash after local state sync but before root indexing cannot lose custody.
	if e = os.Remove(filepath.Join(root.dir, store.stateName)); e != nil {
		t.Fatal(e)
	}
	orphans, e := root.attemptStores()
	if e != nil || len(orphans) != 1 || orphans[0].dir != store.dir {
		t.Fatal("unindexed protected custody lost", orphans, e)
	}
	if e = store.save(); e != nil {
		t.Fatal(e)
	}
	// Root index plus independently synced attempt state reconstruct exact custody.
	root.state.Attempts = []string{store.stateName}
	if e = root.save(); e != nil {
		t.Fatal(e)
	}
	restored, e := root.attemptStores()
	if e != nil || len(restored) != 1 || restored[0].dir != store.dir || restored[0].state.Job.StorageAllocation.ID != local.StorageAllocation.ID {
		t.Fatal("worker backing not reconstructed", restored, e)
	}
	if e = workerRetention(root.dir, c.Now().Add(365*24*time.Hour), time.Hour); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(deliverySpoolPath(local, store.dir), "data", "out.txt")); e != nil {
		t.Fatal("retention deleted unique custody", e)
	}
	snapshot := q.s.StorageProtection
	rootPath := q.root
	if e = q.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := Open(path, rootPath, c, WithFileStore(filepath.Join(filepath.Dir(path), "files"), 185<<30, true))
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	fakeCheckpointAvailability(t, reopened, true)
	if !reflect.DeepEqual(snapshot, reopened.s.StorageProtection) {
		t.Fatal("queue restart lost accounting/custody")
	}
	if reason := reopened.diskHeadroomReason(&Job{Spec: protectionSpec("experiment"), FileTransport: true}, &m); !strings.Contains(reason, "fresh backend verification") {
		t.Fatal("restart renewed native credit", reason)
	}
	if e = reopened.ConfigureStorageProtection(context.Background(), b, control); e != nil {
		t.Fatal(e)
	}
	receipt.Execution.Attempt--
	receipt.Files = refs
	if e = reopened.RecordStorageCustody(context.Background(), receipt); e != nil {
		t.Fatal("lost ACK replay failed after restart", e)
	}
	if reopened.protectedSharedFree() != 100<<20 {
		t.Fatal("restart double-deducted fully allocated backing")
	}
}

func TestStorageProtectionWorkerDenialAndConfinement(t *testing.T) {
	q, c, _ := protectionQueue(t)
	m := protectionMachine("producer", "fake")
	b := protectionBackend(m)
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, c)); err != nil {
		t.Fatal(err)
	}
	assigned := check(t, q, m, nil).Job
	if assigned != nil {
		t.Fatal("unexpected work")
	}
	_ = submit(t, q, protectionSpec("experiment"))
	assigned = check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("missing assignment")
	}
	boundary := &fakeStorageBoundary{}
	root := &workerStore{dir: t.TempDir(), state: workerState{Session: m.Session}}
	w := Worker{Machine: m, Clock: c, StorageBoundary: boundary, Client: deliveryClient(t, q)}
	store, err := newAttemptStore(root, *assigned, w)
	if err != nil {
		t.Fatal(err)
	}
	if len(boundary.paths) != 1 || boundary.paths[0].State != store.dir || !strings.HasPrefix(boundary.paths[0].Work, store.dir+string(filepath.Separator)) || !strings.HasPrefix(boundary.paths[0].Checkpoint, store.dir+string(filepath.Separator)) {
		t.Fatal("attempt write paths escape exact boundary", boundary.paths)
	}
	// The boundary is required before preparation. Missing/unqualified native
	// runtime cannot fall through to an ordinary writable worker directory.
	w.StorageBoundary = nil
	if _, err = w.start(store, *assigned); err == nil {
		t.Fatal("unconfined worker launched")
	}
	boundary.deny = true
	w.StorageBoundary = boundary
	failed, err := w.finishFileResult(*assigned, result(assigned, false), store.dir)
	if err != nil || failed.Success || !attemptDiskLimitFailure(failed) {
		t.Fatal("native denial did not become terminal disk failure", failed, err)
	}
	if err = q.Started(result(assigned, false)); err != nil {
		t.Fatal(err)
	}
	if err = q.Complete(failed); err != nil {
		t.Fatal(err)
	}
	current := job(t, q, assigned.ID)
	if current.State != "failed" || current.InfrastructureRetries != 0 {
		t.Fatal("disk denial retried infrastructure", current.State, current.InfrastructureRetries)
	}
	if len(q.s.StorageProtection.Allocations) != 1 {
		t.Fatal("failure released unique progress budget")
	}
}

func TestStorageProtectionFrozenPoliciesAndUnknownHosts(t *testing.T) {
	q, c, _ := protectionQueue(t)
	m := protectionMachine("producer", "fake")
	b := protectionBackend(m)
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, c)); err != nil {
		t.Fatal(err)
	}
	legacy := &Job{Spec: spec("experiment"), FileTransport: true}
	before := cloneJob(legacy)
	if reason := q.diskHeadroomReason(legacy, &m); !strings.Contains(reason, "frozen policy unchanged") {
		t.Fatal("legacy contract silently changed", reason)
	}
	if !reflect.DeepEqual(*legacy, before) {
		t.Fatal("frozen spec changed")
	}
	current := &Job{Spec: protectionSpec("experiment"), FileTransport: true}
	oldWorker := m
	oldWorker.Capabilities = nil
	if reason := q.diskHeadroomReason(current, &oldWorker); !strings.Contains(reason, "unqualified") {
		t.Fatal("old worker silently ignored boundary contract", reason)
	}
	unknown := protectionMachine("unknown", "fake")
	if reason := q.diskHeadroomReason(current, &unknown); !strings.Contains(reason, "unqualified") {
		t.Fatal("unknown host got protected allowance", reason)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := MeasureControlStorage(ctx, c, ControlStorageWork{SQLite: []string{"missing"}, Logs: []string{"missing"}, Sync: []string{"missing"}, Checkpoint: []string{"missing"}, Metadata: []string{"missing"}}); err == nil {
		t.Fatal("cancelled measurement granted headroom")
	}
}

func TestStorageProtectionPredictionMissReacquiresIndependentProduction(t *testing.T) {
	q, c, _ := protectionQueue(t)
	m := protectionMachine("producer", "fake")
	b := protectionBackend(m)
	b.snapshot.Workers[0].ExplorationBytes = 6 << 20
	if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, c)); err != nil {
		t.Fatal(err)
	}
	// Model the reviewed premises exactly: 96 worker-local retained units do not
	// enter the shared 100, even though the producer and queue are co-located.
	p := q.s.StorageProtection
	p.Allocations["retained-experiment"] = StorageAllocation{ID: "retained-experiment", Backing: b.snapshot.Workers[0].Backing, Bytes: 96 << 20, Files: 100, WorkerCustody: true}
	client := deliveryClient(t, q)
	_, source := retentionUpload(t, client, "")
	s := protectionSpec("production")
	s.Files = FileManifest{".myce-source.tar": source}
	probe := &Job{Spec: s, FileTransport: true}
	key := q.storageKey(probe, &m)
	if key == nil {
		t.Fatal("missing comparable storage identity")
	}
	point := StorageObservation{Key: *key, RecordedAt: c.Now(), Managed: StorageRange{Min: 4 << 20, Peak: 4 << 20, Samples: 1}, Files: StorageRange{Min: 8, Peak: 8, Samples: 1}, AllocationUnit: 1}
	q.s.StorageObservations = map[string]StorageObservation{identityHash(key): point}
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID || assigned.StorageAllocation.Bytes != 4<<20 || *assigned.StorageEnvelope.PredictedNeed != 4<<20 {
		t.Fatal("zero-error prediction was inflated", assigned)
	}
	if err := q.Started(result(assigned, false)); err != nil {
		t.Fatal(err)
	}
	failed := result(assigned, false)
	failed.ExecutionCause = &RuntimeCause{Operation: "command", Class: diskLimitExceeded}
	failed.StoragePeak = &AttemptStoragePeak{ManagedBytes: 4 << 20, Files: 8, Samples: 1, AllocationUnit: 1, Constrained: true}
	if err := q.Complete(failed); err != nil {
		t.Fatal(err)
	}
	clean := q.s.StorageObservations[identityHash(key)]
	if clean.Managed != point.Managed || clean.LowerBound == nil {
		t.Fatal("failed first boundary erased clean four-unit history", clean)
	}
	next := submit(t, q, s)
	offer := check(t, q, m, nil).Job
	if offer == nil || offer.ID != next.ID || offer.StorageAllocation.Bytes != 6<<20 || *offer.StorageEnvelope.PredictedNeed != 4<<20 {
		t.Fatal("fresh six-unit exploration not separately admitted", offer)
	}
	if q.protectedSharedFree() != 100<<20 || len(q.s.StorageProtection.Allocations) != 3 {
		t.Fatal("retention/miss consumed shared production backing or lost custody")
	}
}

// Opt in only with an explicitly provisioned small disposable filesystem and
// its fully allocated backing file. This checks real bounded-volume ENOSPC and
// independent HTTP/SQLite progress. It does NOT qualify confinement of arbitrary
// main/sidecar/OS writes, per-attempt quotas, inode denial or fleet backends.
func TestStorageProtectionNativeBoundedVolume(t *testing.T) {
	proveStorageProtectionNativeBoundedVolume(t, nil)
}

func proveStorageProtectionNativeBoundedVolume(t *testing.T, q *Queue) {
	root := os.Getenv("MYCE_TEST_BOUNDED_VOLUME_ROOT")
	backing := os.Getenv("MYCE_TEST_BOUNDED_VOLUME_BACKING")
	if root == "" || backing == "" {
		t.Skip("requires separately prepared disposable bounded volume and fully allocated backing")
	}
	info, err := os.Stat(backing)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Blocks)*512 < uint64(info.Size()) {
		t.Fatal("sparse maximum is not physical backing")
	}
	var volume syscall.Statfs_t
	err = syscall.Statfs(root, &volume)
	reading := DiskReading{TotalBytes: volume.Blocks * diskBlockSize(&volume), FreeBytes: volume.Bavail * diskBlockSize(&volume)}
	if err != nil || reading.TotalBytes == 0 || reading.TotalBytes > 128<<20 {
		t.Fatal("test requires a bounded disposable filesystem <=128 MiB", reading, err)
	}
	file, err := os.CreateTemp(root, "myce-boundary-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close(); _ = os.Remove(file.Name()) })
	chunk := make([]byte, 1<<20)
	x := uint32(1)
	for i := range chunk {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		chunk[i] = byte(x)
	}
	var written uint64
	for written <= reading.TotalBytes {
		n, e := file.Write(chunk)
		written += uint64(n)
		if e != nil {
			err = e
			break
		}
	}
	if !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("bounded volume failed to deny inter-probe write: bytes=%d err=%v", written, err)
	}
	// The exhausted filesystem is still full during both publication and WAL sync.
	if q == nil {
		q, _, _ = protectionQueue(t)
	}
	client := deliveryClient(t, q)
	_, ref := retentionUpload(t, client, "production publication after experiment volume denial")
	if ref.SizeBytes == 0 {
		t.Fatal("production publication did not progress")
	}
	_ = submit(t, q, spec("production"))
	if _, e := q.db.Exec("PRAGMA wal_checkpoint(FULL)"); e != nil {
		t.Fatal("WAL checkpoint failed behind bounded attempt", e)
	}
	t.Logf("native bounded filesystem denied at %d bytes; independent HTTP publication and SQLite WAL checkpoint succeeded", written)
}

func protectionMachine(id, gpu string) Machine {
	m := machine(id, gpu)
	m.Capabilities = append(m.Capabilities, boundedStorageCapability)
	return m
}
