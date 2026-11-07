package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
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
	m.storageRoots = w.storageRoots()
	for role, path := range map[string]string{"checkpoint": w.CheckpointRoot, "job-files": w.JobFileRoot} {
		m.mounts[role] = &hostMountProbe{root: role, path: path, clock: clock, storage: m, probe: func(string, func()) error { return nil }}
	}
	return w, m, clock
}

func storageTestFault(p *hostMountProbe) *StorageFault {
	if p == nil {
		return nil
	}
	if state := p.backingFault(); state != nil {
		return &state.fault
	}
	return nil
}

func TestStorageFaultSharedBackingRecoveryHasOneOwner(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	w.ServiceConfig = nil // Independently configured roles learned one backing.
	w.rootMonitor = &workerRootMonitor{expected: map[string]string{"checkpoint": "/share", "job-files": "/share"}}
	p, sibling := m.mounts["checkpoint"], m.mounts["job-files"]
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/checkpoints/source", Err: syscall.EIO})
	state := p.backingFault()
	if state != sibling.backingFault() || len(m.storageBackings) != 1 || state.owner != "checkpoint" {
		t.Fatal("roles did not share one recovery owner", m.storageBackings)
	}
	files := fstest.MapFS{"source": &fstest.MapFile{Mode: 0444, Data: []byte("verified data")}}
	var proofs, siblingProofs atomic.Int32
	p.dataProbe = func(path string, cycle uint64, progress func()) error {
		if proofs.Add(1) == 3 {
			return &os.PathError{Op: "read", Path: path, Err: syscall.EIO}
		}
		return verifyStorageReopen(path, cycle, progress, storageTestOpen(files, "source"), storageTestBypass)
	}
	sibling.dataProbe = func(string, uint64, func()) error { siblingProofs.Add(1); return nil }
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	for i := 0; i < 2; i++ {
		await(t, func() bool { return len(p.pending) == 1 })
		clock.Advance(time.Second)
		p.sample(clock.Now(), nil)
	}
	if state.successes != 2 || !state.fault.Active {
		t.Fatal("missing two fresh owner proofs", state)
	}
	// A sibling has an older completed request when the owner's recovery read
	// fails. It must not finish an independent recovery counter afterward.
	sibling.started, sibling.pending = clock.Now(), make(chan hostMountResult, 1)
	sibling.pending <- hostMountResult{latency: time.Millisecond, generation: state.fault.Generation}
	await(t, func() bool { return len(p.pending) == 1 })
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	sibling.sample(clock.Now(), nil)
	if state.fault.Generation != 2 || state.successes != 0 || !state.fault.Active || sibling.backingFault() != state {
		t.Fatal("recovery fault failed to fence sibling evidence", state)
	}
	machine := Machine{HostHealth: m.snapshot(nil)}
	applyHostMountHealth(w, &machine)
	for _, job := range []*Job{
		{Spec: Spec{Priority: "experiment"}},
		{Spec: Spec{Priority: "production", Inputs: map[string]string{"asset": "source"}}},
	} {
		if storageRootReason(job, &machine) == "" {
			t.Fatal("shared backing fault lost dependent role projection", machine.UnavailableRoots)
		}
	}
	clock.Advance(signalMaxAge)
	p.sample(clock.Now(), nil)
	for i := 0; i < hostHealthConfirmation; i++ {
		await(t, func() bool { return len(p.pending) == 1 })
		clock.Advance(time.Second)
		p.sample(clock.Now(), nil)
		if state.fault.Active != (i < hostHealthConfirmation-1) {
			t.Fatal("shared recovery did not require three new proofs", i, state)
		}
	}
	await(t, func() bool { return len(p.pending) == 1 })
	await(t, func() bool { return len(sibling.pending) == 1 })
	if siblingProofs.Load() != 0 {
		t.Fatal("sibling launched a parallel data recovery lane")
	}
	machine = Machine{HostHealth: m.snapshot(nil)}
	applyHostMountHealth(w, &machine)
	if len(machine.UnavailableRoots) != 0 {
		t.Fatal("shared backing retained a copied sibling latch", machine.UnavailableRoots)
	}
}

func TestStorageFaultScopesAdmissionAndFencesRecovery(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	p := m.mounts["checkpoint"]
	before := make(chan struct{})
	p.measure = func(string, func()) hostMountResult { <-before; return hostMountResult{latency: time.Millisecond} }
	p.sample(clock.Now(), nil) // The old metadata request must retain custody.
	clock.Advance(time.Second)
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/checkpoints/source", Err: syscall.EIO})
	if storageTestFault(p) == nil || storageTestFault(p).Generation != 1 || !storageTestFault(p).Data {
		t.Fatal("missing typed mount fault", storageTestFault(p))
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
	if p.backingFault().successes != 0 || !storageTestFault(p).Active {
		t.Fatal("pre-fault metadata success recovered data fault", p)
	}
	await(t, func() bool { return len(p.pending) == 1 })
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	if p.backingFault().successes != 1 || !storageTestFault(p).Active {
		t.Fatal("first fresh data proof incorrectly recovered", p)
	}
	// Another observed failure invalidates even an already-completed proof
	// whose request started before that new fault.
	clock.Advance(time.Second)
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/checkpoints/source", Err: syscall.EINVAL})
	if storageTestFault(p).Generation != 2 || p.backingFault().successes != 0 {
		t.Fatal("new fault did not reset recovery", p)
	}
	await(t, func() bool { return len(p.pending) == 1 })
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	if p.backingFault().successes != 0 || !storageTestFault(p).Active {
		t.Fatal("earlier generation recovered new fault", p)
	}
	for i := 0; i < hostHealthConfirmation; i++ {
		await(t, func() bool { return len(p.pending) == 1 })
		clock.Advance(time.Second)
		p.sample(clock.Now(), nil)
		if storageTestFault(p).Active != (i < hostHealthConfirmation-1) {
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
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/checkpoints/source", Err: syscall.EAGAIN})
	// Inject successful metadata results directly: none is a verified data read.
	p.dataProbe = func(string, uint64, func()) error { return errors.New("no data proof yet") }
	for i := 0; i < hostHealthConfirmation+1; i++ {
		clock.Advance(time.Second)
		p.started = clock.Now()
		p.pending = make(chan hostMountResult, 1)
		p.pending <- hostMountResult{latency: time.Millisecond, generation: storageTestFault(p).Generation}
		p.sample(clock.Now(), nil)
		await(t, func() bool { return len(p.pending) == 1 })
		if p.backingFault().successes != 0 || !storageTestFault(p).Active {
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
	f := storageTestFault(m.mounts["checkpoint"])
	if f == nil || f.Operation != "read" || f.Errno != int(syscall.EIO) || f.At.IsZero() {
		t.Fatal("lost exact read error behind cancellation", f)
	}
	b, e := json.Marshal(f)
	if e != nil || strings.Contains(string(b), "private-source") || strings.Contains(string(b), "/share") {
		t.Fatal("typed fault contains a path", string(b), e)
	}
	generation := f.Generation
	for _, e := range []error{
		&os.PathError{Op: "read", Path: "/local/cache", Err: syscall.EIO},
		&os.PathError{Op: "read", Path: "/share/inputs/missing", Err: os.ErrNotExist},
		&os.PathError{Op: "open", Path: "/share/inputs/denied", Err: os.ErrPermission},
		errors.New("model file size or SHA-256 mismatch"),
	} {
		w.observeStorageError(e)
	}
	if storageTestFault(m.mounts["checkpoint"]).Generation != generation {
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
	if err == nil || storageTestFault(m.mounts["checkpoint"]) == nil || storageTestFault(m.mounts["checkpoint"]).Generation != 1 {
		t.Fatal("staging did not publish one exact owned fault", err, storageTestFault(m.mounts["checkpoint"]))
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

// Restart intentionally forgets the data latch (maintainer decision, #894).
// Existing sampled health history is retained, but cannot restore data custody.
func TestStorageFaultRestartForgetsDataLatch(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: syscall.EIO})
	if err := m.persist(*m.snapshot(nil)); err != nil {
		t.Fatal(err)
	}
	restarted, err := openHostHealth(m.dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.storageBackings) != 0 || len(restarted.health.History) == 0 {
		t.Fatal("restart restored data custody or dropped health history")
	}
	w.storageHealth = restarted
	clock.Advance(time.Second)
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: io.ErrUnexpectedEOF})
	f := storageTestFault(restarted.mounts["job-files"])
	if f == nil || f.Generation != 1 || !f.Active || f.Class != "incomplete_read" {
		t.Fatal("first post-restart fault did not re-latch", f)
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
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/checkpoints/source", Err: syscall.EIO})
	var bypassCalls int
	p.dataProbe = func(_ string, cycle uint64, progress func()) error {
		return verifyStorageReopen("data", cycle, progress, storageTestOpen(files, "data"), func(fs.File, int64, int64) error { bypassCalls++; return syscall.ENOTSUP })
	}
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	await(t, func() bool { return len(p.pending) == 1 })
	clock.Advance(time.Second)
	p.sample(clock.Now(), nil)
	if bypassCalls != 1 || storageTestFault(p).CacheBypass != "unavailable" || !storageTestFault(p).Active || p.backingFault().successes != 0 {
		t.Fatal("unsupported bypass granted recovery", storageTestFault(p), bypassCalls)
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
	f := storageTestFault(m.mounts["checkpoint"])
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
	f := storageTestFault(m.mounts["checkpoint"])
	if f == nil || f.Generation != 2 || f.Operation != "stat" || !f.Data {
		t.Fatal("joined ordinary/read error hid a sibling fault or duplicated reads", f)
	}
	w, m, _ = storageFaultFixture(t)
	w.observeStorageError(errors.Join(missing, metadata))
	if f := storageTestFault(m.mounts["checkpoint"]); f == nil || f.Operation != "stat" || f.Generation != 1 {
		t.Fatal("direct owned-I/O aggregation hid a qualifying sibling", f)
	}
}

func TestStorageFaultBlocksPreparationRetryBeforeHeartbeat(t *testing.T) {
	w := recoveryWorker(t)
	_, m, clock := storageFaultFixture(t)
	w.storageHealth, w.Clock = m, clock
	w.CheckpointRoot, w.ServiceConfig = "/share/checkpoints", &ServiceConfig{NAS: "/share"}
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"true"})
	var calls int
	w.checkpointPrepare = func(Job) (string, error) {
		calls++
		if calls == 1 {
			return "", &os.PathError{Op: "read", Path: "/share/checkpoints/previous/save", Err: syscall.EIO}
		}
		return t.TempDir(), nil
	}
	if _, err = startAfterFixtureInputs(t, w, s, j); !errors.Is(err, errWorkerCheckpoint) {
		t.Fatal("missing preparation fault", err)
	}
	// No new health sample or queue report has occurred. Even a successful
	// filesystem retry must not bypass the active data-fault latch.
	if p, err := startAfterFixtureInputs(t, w, s, j); p != nil || err == nil || !strings.Contains(err.Error(), "checkpoint unavailable") || calls != 1 {
		if p != nil {
			p.stop(clock.Now(), 0, true)
		}
		t.Fatal("retry bypassed live fault before three proofs", err, calls)
	}
}

func TestStorageFaultFencesFinalStartAuthorization(t *testing.T) {
	w, _, _ := storageFaultFixture(t)
	w.Machine.ID = "test-worker"
	j := Job{ID: "test-job", Attempt: 1, Spec: Spec{Priority: "experiment"}}
	var calls int
	w.Client = Client{URL: "http://test.invalid", HTTP: &http.Client{Transport: fetchTransport(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			w.observeStorageError(&os.PathError{Op: "read", Path: "/share/inputs/source", Err: syscall.EIO})
		}
		reply := httptest.NewRecorder()
		reply.Header().Set("Content-Type", "application/json")
		_, _ = reply.WriteString(`{"ok":true,"start_gate":"` + startGateProtocol + `"}`)
		return reply.Result(), nil
	})}}
	if err := w.authorizeStart(j, "test-session"); !errors.Is(err, errWorkerStartAuthorization) || !strings.Contains(err.Error(), "checkpoint unavailable") || calls != 1 {
		t.Fatal("queue's earlier healthy report bypassed new fault", err, calls)
	}
	if err := w.authorizeStart(j, "test-session"); !errors.Is(err, errWorkerStartAuthorization) || calls != 1 {
		t.Fatal("active fault was not checked before authorization", err, calls)
	}
	j.Spec.Priority = "production" // No NAS dependencies.
	if err := w.authorizeStart(j, "test-session"); err != nil || calls != 2 {
		t.Fatal("live mount fault blocked independent local start", err, calls)
	}
}

func TestStorageFaultKeepsNestedBackingIndependent(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	w.JobFileRoot = "/share/nested/inputs"
	w.rootMonitor = &workerRootMonitor{expected: map[string]string{"checkpoint": "/share", "job-files": "/share/nested"}}
	m.mounts["nas"] = &hostMountProbe{root: "nas", path: "/share", clock: clock, storage: m}
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/checkpoints/source", Err: syscall.EIO})
	if storageTestFault(m.mounts["checkpoint"]) == nil || storageTestFault(m.mounts["nas"]) == nil || storageTestFault(m.mounts["job-files"]) != nil {
		t.Fatal("path containment conflated distinct backing mounts", m.mounts)
	}
	machine := Machine{HostHealth: m.snapshot(nil)}
	applyHostMountHealth(w, &machine)
	for _, root := range machine.UnavailableRoots {
		if root.Root == "job-files" {
			t.Fatal("NAS projection withheld a separate nested backing", machine.UnavailableRoots)
		}
	}
	if w.storageRootReason(&Job{Spec: Spec{Priority: "experiment"}}) == "" {
		t.Fatal("affected checkpoint root lost its hold")
	}
	inputJob := Job{Spec: Spec{Priority: "production", Inputs: map[string]string{"asset": "source"}}}
	if reason := w.storageRootReason(&inputJob); reason != "" {
		t.Fatal("independent nested input dependency was withheld", reason)
	}
	w.observeStorageError(&os.PathError{Op: "read", Path: "/share/nested/inputs/source", Err: syscall.EIO})
	if storageTestFault(m.mounts["checkpoint"]).Generation != 1 || storageTestFault(m.mounts["nas"]).Generation != 1 || storageTestFault(m.mounts["job-files"]) == nil {
		t.Fatal("nested fault propagated to the parent backing")
	}
	if reason := w.storageRootReason(&inputJob); reason == "" {
		t.Fatal("faulted nested input dependency remained eligible")
	}
}

func TestStorageFaultCheckpointCopyKeepsReadAttribution(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	w.CheckpointRoot = t.TempDir()
	w.ServiceConfig.NAS = w.CheckpointRoot
	w.JobFileRoot = ""
	p := m.mounts["checkpoint"]
	p.path = w.CheckpointRoot
	j := Job{ID: "copy-fault", Attempt: 2, Checkpoint: filepath.Join(w.CheckpointRoot, "job")}
	source := filepath.Join(j.Checkpoint, "attempt-1", "checkpoint.json")
	if err := os.MkdirAll(filepath.Dir(source), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte(`{"myce_checkpoint":{"generation":4}}`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := prepareCheckpointCopySync(j, func(out io.Writer, in io.Reader) (int64, error) {
		// Model the Linux fast-copy error boundary: an optimized file copy
		// cannot tell which descriptor caused ESTALE and labels it write.
		if _, optimized := in.(io.WriterTo); optimized {
			return 0, &os.PathError{Op: "write", Path: filepath.Join(j.Checkpoint, ".staging"), Err: syscall.ESTALE}
		}
		if _, optimized := out.(io.ReaderFrom); optimized {
			return 0, &os.PathError{Op: "write", Path: filepath.Join(j.Checkpoint, ".staging"), Err: syscall.ESTALE}
		}
		return io.Copy(out, storageFaultReader{err: &os.PathError{Op: "read", Path: source, Err: syscall.ESTALE}})
	}, func(string) error { return nil })
	if !errors.Is(err, syscall.ESTALE) {
		t.Fatal("missing copy fault", err)
	}
	w.observeStorageError(err)
	if storageTestFault(p) == nil || !storageTestFault(p).Data || storageTestFault(p).Operation != "read" || p.backingFault().source != source {
		t.Fatal("source data fault became destination metadata failure", storageTestFault(p), p.backingFault().source)
	}
	p.dataProbe = func(string, uint64, func()) error { return errors.New("no fresh data proof") }
	clock.Advance(time.Second)
	p.started, p.pending = clock.Now(), make(chan hostMountResult, 1)
	p.pending <- hostMountResult{generation: storageTestFault(p).Generation, latency: time.Millisecond}
	p.sample(clock.Now(), nil)
	await(t, func() bool { return len(p.pending) == 1 })
	if !storageTestFault(p).Active || p.backingFault().successes != 0 {
		t.Fatal("metadata recovered a checkpoint source read fault")
	}
}
