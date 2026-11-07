package myce

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func bulkIOHeartbeat(p *workerBulkIO, clock *fakeClock, duration time.Duration, err error) {
	p.begin()
	clock.Advance(duration)
	p.end(err)
}

func TestWorkerBulkIOBackoffAndRecovery(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	p := newWorkerBulkIO(clock)
	bulkIOHeartbeat(p, clock, 10*time.Millisecond, nil)
	var out bytes.Buffer
	var windows []int
	last := 0
	p.wait = func(ctx context.Context, d time.Duration, wake <-chan struct{}) error {
		windows = append(windows, out.Len()-last)
		last = out.Len()
		elapsed := 10 * time.Millisecond
		if len(windows) == 4 {
			elapsed = time.Second
		}
		bulkIOHeartbeat(p, clock, elapsed, nil)
		return nil
	}
	data := bytes.Repeat([]byte("control-first\n"), 100000)
	n, err := bulkWriter(p.context(context.Background()), &out).Write(data)
	if err != nil || n != len(data) || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("paced write changed bytes: n=%d err=%v", n, err)
	}
	if len(windows) < 6 || windows[3] <= windows[0] || windows[4] >= windows[3] || windows[5] <= windows[4] {
		t.Fatalf("healthy allowance must grow, back off on delay, then recover: %v", windows)
	}
	if baseline := p.baseline(); baseline != 10*time.Millisecond {
		t.Fatalf("bulk pressure poisoned the idle baseline: %s", baseline)
	}
	finish := trackBulkIO(p.context(context.Background()))
	bulkIOHeartbeat(p, clock, time.Second, nil)
	if p.baseline() != 10*time.Millisecond {
		t.Fatal("an outstanding copy/sync taught a delayed idle baseline")
	}
	finish()
	t.Logf("bytes per control window=%v; delayed RTT=1s; idle RTT=10ms", windows)
}

func TestWorkerBulkIOInflightFailureAndCancel(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	p := newWorkerBulkIO(clock)
	bulkIOHeartbeat(p, clock, time.Millisecond, nil)
	p.begin()
	clock.Advance(time.Second)
	ctx, cancel := context.WithCancel(p.context(context.Background()))
	defer cancel()
	waits := 0
	p.wait = func(context.Context, time.Duration, <-chan struct{}) error {
		waits++
		cancel()
		return ctx.Err()
	}
	var out bytes.Buffer
	if n, err := bulkWriter(ctx, &out).Write([]byte("must wait")); n != 0 || !errors.Is(err, context.Canceled) || waits != 1 {
		t.Fatalf("slow in-flight control must stop writes and remain cancellable: %d %v waits=%d", n, err, waits)
	}
	p.end(errors.New("check-in timed out"))
	p.wait = func(context.Context, time.Duration, <-chan struct{}) error {
		if out.Len() != 0 {
			t.Fatal("failed check-in allowed more writes before recovery")
		}
		bulkIOHeartbeat(p, clock, time.Millisecond, nil)
		return nil
	}
	if _, err := bulkWriter(p.context(context.Background()), &out).Write([]byte("recovered")); err != nil || out.String() != "recovered" {
		t.Fatal("healthy check-in did not resume writes", err)
	}
}

func TestWorkerBulkIOSharedAllowanceAndIntentionalProgress(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	p := newWorkerBulkIO(clock)
	bulkIOHeartbeat(p, clock, time.Second, nil)
	ctx := p.context(context.Background())
	progress := clock.Now()
	ctx = context.WithValue(ctx, ioProgressKey{}, func() { progress = clock.Now() })
	var first, second bytes.Buffer
	if _, err := bulkWriter(ctx, &first).Write(make([]byte, bulkIOQuantum)); err != nil {
		t.Fatal(err)
	}
	waits := 0
	p.wait = func(_ context.Context, interval time.Duration, _ <-chan struct{}) error {
		waits++
		if interval != checkpointAvailabilityStallBudget/2 {
			t.Fatalf("renewal must follow watchdog budget, got %s", interval)
		}
		if second.Len() != 0 {
			t.Fatal("a second copy obtained an independent allowance")
		}
		if age := clock.Now().Sub(progress); age >= checkpointAvailabilityStallBudget {
			t.Fatalf("intentional pacing became an operation stall: %s", age)
		}
		clock.Advance(interval)
		if waits == 5 {
			bulkIOHeartbeat(p, clock, time.Second, nil)
		}
		return nil
	}
	if _, err := bulkWriter(ctx, &second).Write([]byte("second")); err != nil || waits != 5 {
		t.Fatalf("long deliberate waits did not preserve staging progress: %v waits=%d", err, waits)
	}
}

// A laptop proof of the entire feedback -> shared staging -> verified publication
// path. SQLite and HTTP are real; induced control duration uses fake time, not a
// claim that the laptop reproduces native NVMe contention.
func TestWorkerBulkIOSharedStagingControlFeedback(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 16<<20, true); err != nil {
		t.Fatal(err)
	}
	p := newWorkerBulkIO(clock)
	previous := int64(0)
	var observed []time.Duration
	api := API{Queue: q, Token: "test"}.Handler()
	server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/checkin" {
			p.mu.Lock()
			written := p.used
			p.mu.Unlock()
			delay := 10 * time.Millisecond
			if written > 4*bulkIOQuantum {
				delay = 100 * time.Millisecond
			}
			previous = written
			observed = append(observed, delay)
			clock.Advance(delay)
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	client := Client{URL: server.URL, Token: "test", bulkIO: p}
	w := Worker{Client: client, Clock: clock}
	heartbeat := func() error {
		_, err := w.checkin(context.Background(), Checkin{Machine: machine("bulk-fixture", "cpu")})
		return err
	}
	if err := heartbeat(); err != nil {
		t.Fatal(err)
	}
	var highBytes, reducedBytes int64
	p.wait = func(context.Context, time.Duration, <-chan struct{}) error {
		err := heartbeat()
		if previous > 4*bulkIOQuantum {
			highBytes = previous
		} else if highBytes > 0 {
			reducedBytes = previous
		}
		return err
	}
	root := t.TempDir()
	source := bytes.Repeat([]byte("verified snapshot\n"), 131072)
	if err := os.WriteFile(filepath.Join(root, "input"), source, 0600); err != nil {
		t.Fatal(err)
	}
	ref := FileRef{SHA256: digestBytes(source), SizeBytes: int64(len(source))}
	if err := client.UploadFile(context.Background(), filepath.Join(root, "input"), ref); err != nil {
		t.Fatal(err)
	}
	cache := &SharedInputCache{Client: client, cache: EnvironmentCache{Root: t.TempDir(), Limit: 2 * ref.SizeBytes, Clock: clock}}
	path, release, err := cache.Acquire(context.Background(), SharedInput{SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, source) {
		t.Fatal("paced staging did not publish the verified bytes", err)
	}
	if highBytes == 0 || reducedBytes == 0 || reducedBytes >= highBytes {
		t.Fatalf("HTTP feedback did not reduce staging allowance: high=%d reduced=%d", highBytes, reducedBytes)
	}
	t.Logf("real SQLite/HTTP, fake latency: staged=%d bytes control_samples=%d high_window=%d reduced_window=%d", len(got), len(observed), highBytes, reducedBytes)
}

func TestWorkerBulkIOLocalLinuxOnly(t *testing.T) {
	clock := &fakeClock{}
	p := localWorkerBulkIO(context.Background(), clock, "http://127.0.0.1:7331")
	if (p != nil) != (runtime.GOOS == "linux") {
		t.Fatal("local pacing must leave Darwin unchanged")
	}
	if localWorkerBulkIO(context.Background(), clock, "http://192.0.2.1:7331") != nil {
		t.Fatal("remote worker was paced")
	}
	// Unmeasurable timing is unknown, not an indefinite bulk-I/O hold.
	p = newWorkerBulkIO(clock)
	bulkIOHeartbeat(p, clock, 0, nil)
	if _, err := io.Copy(bulkWriter(p.context(context.Background()), io.Discard), bytes.NewReader(make([]byte, 2*bulkIOQuantum))); err != nil {
		t.Fatal(err)
	}
}

// A sync present at begin, or one entirely inside the request, excludes the
// delayed observation even if it ends before the heartbeat does.
func TestWorkerBulkIOOverlapAndTimeoutEvidence(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	p := newWorkerBulkIO(clock)
	bulkIOHeartbeat(p, clock, time.Millisecond, nil)
	ctx := p.context(context.Background())
	for _, presentAtBegin := range []bool{true, false} {
		var finish func()
		if presentAtBegin {
			finish = trackBulkIO(ctx)
		}
		p.begin()
		if !presentAtBegin {
			finish = trackBulkIO(ctx)
		}
		clock.Advance(time.Second)
		finish()
		p.end(nil)
		if p.baseline() != time.Millisecond {
			t.Fatal("overlapping sync contaminated idle baseline")
		}
	}
	p.budget = 64 * bulkIOQuantum
	p.used = int64(p.budget)
	bulkIOHeartbeat(p, clock, 10*time.Millisecond, context.DeadlineExceeded)
	backedOff := p.budget
	if backedOff >= 64*bulkIOQuantum {
		t.Fatal("timeout retained offending allowance")
	}
	bulkIOHeartbeat(p, clock, time.Millisecond, nil)
	if p.budget != backedOff || p.baseline() != time.Millisecond {
		t.Fatal("recovery reply grew allowance or learned idle pressure")
	}
	p.used = int64(p.budget)
	bulkIOHeartbeat(p, clock, time.Millisecond, nil)
	if p.budget <= backedOff {
		t.Fatal("successful work after recovery did not grow allowance")
	}
}

func TestWorkerBulkIOConcurrentHeartbeat(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	p := newWorkerBulkIO(clock)
	bulkIOHeartbeat(p, clock, time.Millisecond, nil)
	ctx, cancel := context.WithCancel(p.context(context.Background()))
	defer cancel()
	// The wait seam only signals entry, then uses the production wait. No
	// scripted heartbeat: another goroutine completes the request independently.
	waiting := make(chan time.Duration, 1)
	p.wait = func(ctx context.Context, interval time.Duration, wake <-chan struct{}) error {
		waiting <- interval
		return waitWorkerBulkIO(ctx, interval, wake)
	}
	if _, err := bulkWriter(ctx, io.Discard).Write(make([]byte, bulkIOQuantum)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := bulkWriter(ctx, io.Discard).Write([]byte("next")); done <- err }()
	if interval := <-waiting; interval != 0 {
		t.Fatalf("writer without watchdog must not poll at %s", interval)
	}
	bulkIOHeartbeat(p, clock, time.Millisecond, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Cancellation also releases the real wait without a heartbeat or timer.
	p.mu.Lock()
	p.failed = true
	p.mu.Unlock()
	go func() { _, err := bulkWriter(ctx, io.Discard).Write([]byte("cancel")); done <- err }()
	<-waiting
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWorkerBulkIOSourceAndRecovery(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	p := newWorkerBulkIO(clock)
	bulkIOHeartbeat(p, clock, time.Millisecond, nil)
	ctx := p.context(context.Background())
	waits := 0
	p.wait = func(context.Context, time.Duration, <-chan struct{}) error {
		waits++
		bulkIOHeartbeat(p, clock, time.Millisecond, nil)
		return nil
	}
	data := bytes.Repeat([]byte("source"), bulkIOQuantum)
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.WriteHeader(&tar.Header{Name: "source", Size: int64(len(data)), Mode: 0600}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	archivePath := filepath.Join(root, "source.tar")
	if err := os.WriteFile(archivePath, archive.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := extractJobSourceContext(ctx, archivePath, root); err != nil {
		t.Fatal(err)
	}
	if waits == 0 {
		t.Fatal("source extraction bypassed pacing")
	}
	before := waits
	p.used = int64(p.budget)
	cache := &ModelCache{}
	original := filepath.Join(root, "source")
	dest := filepath.Join(root, "recovery")
	if err := cache.finishTrashMove(ctx, original, dest); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(got, data) || waits == before {
		t.Fatal("recovery bypassed pacing or changed bytes", err)
	}
	if _, err := os.Stat(original); !os.IsNotExist(err) {
		t.Fatal("verified move retained original", err)
	}
	// Recovery obtains the controller even with a fresh caller context.
	cache.FileClient = &Client{bulkIO: p}
	p.used = int64(p.budget)
	cancelCtx, cancel := context.WithCancel(context.Background())
	p.wait = func(ctx context.Context, _ time.Duration, _ <-chan struct{}) error { cancel(); return ctx.Err() }
	// Exercise the paced recovery helper's cancellation contract without needing
	// two filesystems on the laptop; discardContext attaches this same controller.
	if err := cache.finishTrashMove(cancelCtx, dest, filepath.Join(root, "cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(dest); err != nil || !bytes.Equal(got, data) {
		t.Fatal("cancelled recovery lost source", err)
	}
}
