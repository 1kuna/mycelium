package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/fstest"
	"time"
)

func storageFaultFixture(t *testing.T) (Worker, *hostHealthMonitor, *fakeClock) {
	t.Helper()
	m, clock := healthFixture(t, 1)
	w := Worker{Clock: clock, CheckpointRoot: "/share/checkpoints", JobFileRoot: "/share/inputs", ServiceConfig: &ServiceConfig{NAS: "/share"}, storageHealth: m}
	m.mounts["checkpoint"] = &hostMountProbe{root: "checkpoint", path: w.CheckpointRoot, clock: clock, probe: func(string, func()) error { return nil }}
	return w, m, clock
}

func TestStorageFaultScopesAdmissionAndFencesRecovery(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	p := m.mounts["checkpoint"]
	before := make(chan struct{})
	p.measure = func(string, func()) hostMountResult { <-before; return hostMountResult{latency: time.Millisecond} }
	p.sample(clock.Now(), nil) // The old metadata request must retain custody.
	clock.Advance(time.Second)
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: syscall.EIO})
	if p.fault == nil || p.fault.Generation != 1 || !p.fault.Data {
		t.Fatal("missing typed mount fault", p.fault)
	}
	machine := Machine{HostHealth: m.snapshot(nil)}
	applyHostMountHealth(w, &machine)
	dependent := &Job{Spec: Spec{Priority: "experiment"}}
	local := &Job{Spec: Spec{Priority: "production"}}
	if storageRootReason(dependent, &machine) == "" || storageRootReason(local, &machine) != "" {
		t.Fatal("fault failed to scope admission", machine.UnavailableRoots)
	}
	var proofs atomic.Int32
	p.dataProbe = func(path string, cycle uint64, progress func()) error {
		proofs.Add(1)
		files := fstest.MapFS{"source": &fstest.MapFile{Mode: 0444, Data: []byte("read-only verified data")}}
		return verifyStorageReopen(path, cycle, progress, storageTestOpen(files, "source"), storageTestBypass)
	}
	close(before)
	await(t, func() bool { return len(p.pending) == 1 })
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	if p.consecutive != 0 || !p.fault.Active {
		t.Fatal("pre-fault metadata success recovered data fault", p)
	}
	await(t, func() bool { return len(p.pending) == 1 })
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	if p.consecutive != 1 || !p.fault.Active {
		t.Fatal("first fresh data proof incorrectly recovered", p)
	}
	// Another observed failure invalidates even an already-completed proof
	// whose request started before that new fault.
	clock.Advance(time.Second)
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: syscall.EINVAL})
	if p.fault.Generation != 2 || p.consecutive != 0 {
		t.Fatal("new fault did not reset recovery", p)
	}
	await(t, func() bool { return len(p.pending) == 1 })
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	if p.consecutive != 0 || !p.fault.Active {
		t.Fatal("earlier generation recovered new fault", p)
	}
	for i := 0; i < hostHealthConfirmation; i++ {
		await(t, func() bool { return len(p.pending) == 1 })
		clock.Advance(time.Second)
		p.sample(clock.Now(), nil)
		if p.fault.Active != (i < hostHealthConfirmation-1) {
			t.Fatal("recovery did not require three fresh proofs", i, p)
		}
	}
	await(t, func() bool { return len(p.pending) == 1 })
	if proofs.Load() < hostHealthConfirmation {
		t.Fatal("metadata replaced read/reopen proofs", proofs.Load())
	}
}

func TestStorageFaultMetadataCannotClearDataFault(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	p := m.mounts["checkpoint"]
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: syscall.EAGAIN})
	// Inject successful metadata results directly: none is a verified data read.
	p.dataProbe = func(string, uint64, func()) error { return errors.New("no data proof yet") }
	for i := 0; i < hostHealthConfirmation+1; i++ {
		clock.Advance(time.Second)
		p.started = clock.Now()
		p.pending = make(chan hostMountResult, 1)
		p.pending <- hostMountResult{latency: time.Millisecond, generation: p.fault.Generation}
		p.sample(clock.Now(), nil)
		await(t, func() bool { return len(p.pending) == 1 })
		if p.consecutive != 0 || !p.fault.Active {
			t.Fatal("metadata-only success recovered data fault", p)
		}
	}
}

type storageFaultReader struct {
	err    error
	cancel context.CancelFunc
}

func (r storageFaultReader) Read([]byte) (int, error) {
	if r.cancel != nil {
		r.cancel()
	}
	return 0, r.err
}

func TestStorageFaultOwnedReadSurvivesCancellationAndSanitizes(t *testing.T) {
	w, m, _ := storageFaultFixture(t)
	ctx, cancel := context.WithCancel(w.storageIOContext(context.Background()))
	err := &os.PathError{Op: "read", Path: "/share/inputs/private-source", Err: syscall.EIO}
	reader := contextReader{ctx: ctx, reader: storageFaultReader{err: err, cancel: cancel}}
	if _, got := reader.Read(make([]byte, 1)); !errors.Is(got, context.Canceled) {
		t.Fatal("diagnostics changed the original cancellation outcome", got)
	}
	f := m.mounts["checkpoint"].fault
	if f == nil || f.Operation != "read" || f.Errno != int(syscall.EIO) || f.At.IsZero() {
		t.Fatal("lost exact read error behind cancellation", f)
	}
	b, e := json.Marshal(f)
	if e != nil || strings.Contains(string(b), "private-source") || strings.Contains(string(b), "/share") {
		t.Fatal("typed fault contains a path", string(b), e)
	}
	for _, e := range []error{
		&os.PathError{Op: "read", Path: "/local/cache", Err: syscall.EIO},
		&os.PathError{Op: "read", Path: "/share/inputs/missing", Err: os.ErrNotExist},
		&os.PathError{Op: "open", Path: "/share/inputs/denied", Err: os.ErrPermission},
		errors.New("model file size or SHA-256 mismatch"),
	} {
		w.observeStorageError(e)
	}
	if m.mounts["checkpoint"].fault.Generation != f.Generation {
		t.Fatal("local/producer/path error quarantined storage")
	}
}

func TestStorageFaultSharedInputStagingOwnsObservation(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	c := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock}}
	c.resolveNAS = func(string, string) (string, error) {
		return "", &os.PathError{Op: "stat", Path: "/share/checkpoints/asset", Err: syscall.EIO}
	}
	w.sharedInputs = c
	_, _, err := w.acquireSharedInputs(context.Background(), Job{Spec: Spec{SharedInputs: map[string]SharedInput{"asset": {NAS: "asset", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}}})
	if err == nil || m.mounts["checkpoint"].fault == nil || m.mounts["checkpoint"].fault.Generation != 1 {
		t.Fatal("staging did not publish one exact owned fault", err, m.mounts["checkpoint"].fault)
	}
}

func TestStorageFaultReadOnlyReopenRequiresConsistentData(t *testing.T) {
	files := fstest.MapFS{"data": &fstest.MapFile{Data: []byte("verified"), Mode: 0444}, "empty": &fstest.MapFile{Mode: 0444}}
	var opens, progress int
	open := func(path string) (fs.File, error) { opens++; return storageTestOpen(files, path)(path) }
	if err := verifyStorageReopen("data", 0, func() { progress++ }, open, storageTestBypass); err != nil || opens != 2 || progress != 12 {
		t.Fatal("read-only data could not recover without writes", err, opens, progress)
	}
	if err := verifyStorageReopen("empty", 0, func() {}, open, storageTestBypass); err == nil {
		t.Fatal("metadata/EOF-only source counted as data")
	}
	opens = 0
	if err := verifyStorageReopen("data", 0, func() {}, func(string) (fs.File, error) {
		opens++
		data := "verified"
		if opens == 2 {
			data = "modified"
		}
		return storageTestOpen(fstest.MapFS{"data": &fstest.MapFile{Data: []byte(data), Mode: 0444}}, "data")("data")
	}, storageTestBypass); err == nil {
		t.Fatal("inconsistent reopened data recovered storage")
	}
}

func TestStorageFaultPersistenceRetainsLatchWithoutRecoveryCandidate(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: syscall.EIO})
	if err := m.persist(*m.snapshot(nil)); err != nil {
		t.Fatal(err)
	}
	reopened, err := openHostHealth(m.dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	f := reopened.restoredFaults["checkpoint"]
	p := &hostMountProbe{root: "checkpoint", path: "/share/checkpoints", clock: clock}
	restoreStorageFault(p, f, reopened.restoredSources["checkpoint"])
	reopened.mounts["checkpoint"] = p
	if !p.fault.Active || p.consecutive != 0 || p.recoverySource != "/share/inputs/source" {
		t.Fatal("restart cleared fault or retained success count", p)
	}
	machine := Machine{HostHealth: reopened.snapshot(nil)}
	applyHostMountHealth(w, &machine)
	if storageRootReason(&Job{Spec: Spec{Priority: "experiment"}}, &machine) == "" {
		t.Fatal("restart advertised old successful metadata")
	}
	w.storageHealth = reopened
	clock.Advance(time.Second)
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: io.ErrUnexpectedEOF})
	if p.fault.Generation != f.Generation+1 || p.fault.Class != "incomplete_read" || p.fault.Errno != 0 {
		t.Fatal("restart lost monotonic fault identity", p.fault)
	}
}

// MapFS lacks seek; expose only a read-only descriptor interface for proof tests.
type storageTestFile struct {
	*bytes.Reader
	fs.File
}

func (f storageTestFile) Read(b []byte) (int, error) { return f.Reader.Read(b) }
func storageTestOpen(files fstest.MapFS, path string) func(string) (fs.File, error) {
	return func(string) (fs.File, error) {
		f, err := files.Open(path)
		if err != nil {
			return nil, err
		}
		return storageTestFile{Reader: bytes.NewReader(files[path].Data), File: f}, nil
	}
}
func storageTestBypass(fs.File, int64, int64) error { return nil }

func TestStorageFaultRecoveryBypassAndRotatingOffsets(t *testing.T) {
	files := fstest.MapFS{"data": &fstest.MapFile{Data: bytes.Repeat([]byte("x"), 4*os.Getpagesize()), Mode: 0444}}
	var windows [][2]int64
	for cycle := uint64(0); cycle < 3; cycle++ {
		bypass := func(_ fs.File, offset, length int64) error {
			windows = append(windows, [2]int64{offset, length})
			return nil
		}
		if err := verifyStorageReopen("data", cycle, func() {}, storageTestOpen(files, "data"), bypass); err != nil {
			t.Fatal(err)
		}
	}
	if len(windows) != 6 || windows[0] != windows[1] || windows[2] != windows[3] || windows[4] != windows[5] || windows[0][0] == windows[2][0] || windows[2][0] == windows[4][0] {
		t.Fatal("proof did not bypass each reopened fd at rotating offsets", windows)
	}
	w, m, clock := storageFaultFixture(t)
	p := m.mounts["checkpoint"]
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: syscall.EIO})
	var bypassCalls int
	p.dataProbe = func(_ string, cycle uint64, progress func()) error {
		return verifyStorageReopen("data", cycle, progress, storageTestOpen(files, "data"), func(fs.File, int64, int64) error { bypassCalls++; return syscall.ENOTSUP })
	}
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	await(t, func() bool { return len(p.pending) == 1 })
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	if bypassCalls != 1 || p.fault.CacheBypass != "unavailable" || !p.fault.Active || p.consecutive != 0 {
		t.Fatal("unsupported bypass granted recovery", p.fault, bypassCalls)
	}
}

func TestStorageFaultCheckpointPreparationPublishesExactFault(t *testing.T) {
	w := recoveryWorker(t)
	_, m, clock := storageFaultFixture(t)
	w.storageHealth, w.Clock = m, clock
	w.CheckpointRoot = "/share/checkpoints"
	w.ServiceConfig = &ServiceConfig{NAS: "/share"}
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"true"})
	fault := &os.PathError{Op: "read", Path: "/share/checkpoints/previous/save", Err: syscall.EIO}
	w.checkpointPrepare = func(Job) (string, error) { return "", fault }
	if _, err = startAfterFixtureInputs(t, w, s, j); !errors.Is(err, errWorkerCheckpoint) {
		t.Fatal("preparation outcome changed", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f := m.mounts["checkpoint"].fault
	if f == nil || f.Errno != int(syscall.EIO) || f.Operation != "read" || f.Generation != 1 {
		t.Fatal("preparation lost exact owned syscall", f)
	}
}

func TestStorageFaultDoesNotRecountHostPressure(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	// One real transient pressure sample must remain one observation even
	// when multiple storage syscalls fail before the next sensor sample.
	clock.Advance(signalRefresh)
	m.health.observe(HostHealthSample{At: clock.Now(), HostHealthReading: HostHealthReading{Pressure: 4, Load: 1, WakeSeconds: .001}})
	if m.health.Consecutive != 1 || m.health.DrainReason != "" {
		t.Fatal("fixture lacks one unconfirmed pressure sample", m.health)
	}
	history, baseline := len(m.health.History), len(m.health.Baseline)
	for i := 0; i < hostHealthConfirmation; i++ {
		clock.Advance(time.Second)
		w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: syscall.EIO})
	}
	machine := Machine{HostHealth: m.snapshot(nil)}
	applyHostMountHealth(w, &machine)
	if len(m.health.History) != history || len(m.health.Baseline) != baseline || m.health.Consecutive != 1 || hostHygieneReason(&machine) != "" {
		t.Fatal("storage faults recounted a cached pressure reading", m.health)
	}
	if storageRootReason(&Job{Spec: Spec{Priority: "experiment"}}, &machine) == "" || storageRootReason(&Job{Spec: Spec{Priority: "production"}}, &machine) != "" {
		t.Fatal("mount latch lost dependency-only admission", machine.UnavailableRoots)
	}
}

func TestStorageFaultJoinedErrorsObserveEverySyscallOnce(t *testing.T) {
	w, m, _ := storageFaultFixture(t)
	ctx := w.storageIOContext(context.Background())
	read := &os.PathError{Op: "read", Path: "/share/inputs/read", Err: syscall.EIO}
	if _, err := (contextReader{ctx: ctx, reader: storageFaultReader{err: read}}).Read(make([]byte, 1)); !errors.Is(err, read) {
		t.Fatal(err)
	}
	missing := &os.PathError{Op: "open", Path: "/share/inputs/missing", Err: os.ErrNotExist}
	metadata := &os.PathError{Op: "stat", Path: "/share/inputs/other", Err: syscall.EIO}
	joined := errors.Join(missing, errors.Join(read, metadata))
	observeStorageContext(ctx, joined)
	observeStorageContext(ctx, joined) // Another wrapper must not count it again.
	f := m.mounts["checkpoint"].fault
	if f == nil || f.Generation != 2 || f.Operation != "stat" || !f.Data {
		t.Fatal("joined ordinary/read error hid a sibling fault or duplicated reads", f)
	}
	w, m, _ = storageFaultFixture(t)
	w.observeStorageError(errors.Join(missing, metadata))
	if f := m.mounts["checkpoint"].fault; f == nil || f.Operation != "stat" || f.Generation != 1 {
		t.Fatal("direct owned-I/O aggregation hid a qualifying sibling", f)
	}
}

func TestStorageFaultStartupRetainsLearnedSiblingCustody(t *testing.T) {
	oldForeign := workerForeignConsumers
	workerForeignConsumers = func(context.Context, string) ([]ForeignConsumer, error) { return nil, nil }
	t.Cleanup(func() { workerForeignConsumers = oldForeign })
	for _, reconfigured := range []bool{false, true} {
		t.Run(map[bool]string{false: "same_backing", true: "source_root_removed"}[reconfigured], func(t *testing.T) {
			w, m, clock := storageFaultFixture(t)
			backing := t.TempDir()
			w.CheckpointRoot, w.JobFileRoot = filepath.Join(backing, "checkpoints"), filepath.Join(backing, "inputs")
			m.mounts["checkpoint"].path = w.CheckpointRoot
			w.ServiceConfig = nil
			w.rootMonitor = &workerRootMonitor{expected: map[string]string{"checkpoint": backing, "job-files": backing}}
			source := filepath.Join(w.JobFileRoot, "source")
			w.observeStorageError(&os.PathError{Op: "read", Path: source, Err: syscall.EIO})
			if err := m.persist(*m.snapshot(nil)); err != nil {
				t.Fatal(err)
			}
			if reconfigured {
				w.JobFileRoot = t.TempDir()
			}
			w.HostHealthSource = func(ctx context.Context) (HostHealthReading, error) {
				<-ctx.Done()
				return HostHealthReading{}, ctx.Err()
			}
			ctx, cancel := context.WithCancel(context.Background())
			root := &workerStore{dir: m.dir}
			root.state.CheckpointRoot = w.CheckpointRoot
			restarted, err := startWorkerHealth(ctx, w, root)
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			defer func() { cancel(); restarted.wait() }()
			restarted.mu.Lock()
			defer restarted.mu.Unlock()
			for _, probe := range restarted.mounts {
				probe.probe = func(string, func()) error { return nil }
				probe.dataProbe = func(string, uint64, func()) error { return nil }
			}
			p := restarted.mounts["checkpoint"]
			if p.fault == nil || !p.fault.Active || p.consecutive != 0 || p.recoveryBacking != backing {
				t.Fatal("startup lost learned backing/latch", p)
			}
			if reconfigured {
				if p.recoverySource != "" {
					t.Fatal("startup retained a source outside configured roots", p.recoverySource)
				}
				return
			}
			if p.recoverySource != source {
				t.Fatal("startup stranded sibling recovery source", p.recoverySource)
			}
			p.dataProbe = func(path string, _ uint64, _ func()) error {
				if path != source {
					return errors.New("wrong source custody")
				}
				return nil
			}
			clock.Advance(time.Second)
			p.sample(clock.Now(), nil)
			for i := 0; i < hostHealthConfirmation; i++ {
				await(t, func() bool { return len(p.pending) == 1 })
				clock.Advance(time.Second)
				p.sample(clock.Now(), nil)
			}
			if p.fault.Active {
				t.Fatal("restored sibling custody could not recover")
			}
			await(t, func() bool { return len(p.pending) == 1 })
		})
	}
}
